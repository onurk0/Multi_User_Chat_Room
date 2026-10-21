/*
 * main_server.c - Multi-user chat room server.
 *
 * Accepts TCP clients, places each one in a numbered chat room, relays chat
 * between clients in the same room, and brokers file transfers between two
 * users in the same room. Each client is served by its own thread.
 *
 * Wire protocol: every message is a '\n'-terminated line (see net_common.h).
 *
 *   Joining (C = client, S = server):
 *     C: "list" | "new" | "<room>"    initial room request
 *     S: info lines, then one of
 *          "[CHOOSE]"                 C must answer "new" or "<room>"
 *          "[JOINED] <text>"          C is in a room and sends its username
 *          "[ERROR] <text>"           S closes the connection
 *     C: "<username>"
 *
 *   File transfer (A = sender, B = receiver):
 *     A -> S: "[FILE_SEND] <B> <filename> <size>"
 *     S -> B: "[FILE_REQUEST] <A> <filename> <size>"
 *     B -> S: "[FILE_ACCEPT] <A>"      or "[FILE_REJECT] <A>"
 *     S -> B: "[FILE_START] <filename> <size> <A>"   (on accept)
 *     S -> A: "[FILE_ACCEPTED] <text>" or "[FILE_REJECTED] <text>"
 *     A -> S: "[FILE_CHUNK] <n>" + n raw bytes, repeated, forwarded to B
 *     A -> S: "[FILE_END]"             forwarded to B as-is
 *     A -> S: "[FILE_CANCEL]"          S -> B: "[FILE_ABORT] <text>"
 *     S -> A: "[FILE_ERROR] <text>"    when anything goes wrong
 *
 * File data is length-prefixed, so chat sent to B during a transfer can sit
 * between two chunks without corrupting the file.
 */
#include <arpa/inet.h>
#include <limits.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/time.h>
#include <unistd.h>

#include "net_common.h"

#define MAX_FDS 1024         // write_locks[] is indexed by socket fd
#define MAX_ROOMS 20         // max rooms shown in the room list
#define MAX_RECIPIENTS 100   // max clients a single broadcast reaches
#define HANDSHAKE_TIMEOUT 30 // seconds a client may idle before joining

static void error(const char *msg) {
  perror(msg);
  exit(1);
}

/* ------------------------------------------------------------------------ */
/* Socket writes                                                            */
/* ------------------------------------------------------------------------ */

// Several threads can write to the same client socket at once: broadcasts
// from other clients' threads and file chunks forwarded by a sender's
// thread. One lock per fd keeps each message (including a chunk header plus
// its payload) contiguous on the wire.
static pthread_mutex_t write_locks[MAX_FDS];

static int send_locked(int fd, const void *buf, size_t len) {
  pthread_mutex_lock(&write_locks[fd]);
  int result = send_all(fd, buf, len);
  pthread_mutex_unlock(&write_locks[fd]);
  return result;
}

// printf-style send to one client.
static int send_msg(int fd, const char *fmt, ...) {
  char buf[LINE_SIZE + 128];
  va_list ap;
  va_start(ap, fmt);
  size_t len = format_msg(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  return send_locked(fd, buf, len);
}

/* ------------------------------------------------------------------------ */
/* Connected clients and rooms                                              */
/* ------------------------------------------------------------------------ */

// One node per client that has finished joining a room.
typedef struct Client {
  int fd;
  char name[NAME_SIZE];
  char ip[INET_ADDRSTRLEN];
  int room_id;
  struct Client *next;
} Client;

static Client *clients_head = NULL;
static Client *clients_tail = NULL;
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

static int next_room_id = 1;
static pthread_mutex_t room_mutex = PTHREAD_MUTEX_INITIALIZER;

// Hands out a fresh room number.
static int new_room_id(void) {
  pthread_mutex_lock(&room_mutex);
  int id = next_room_id++;
  pthread_mutex_unlock(&room_mutex);
  return id;
}

// Appends a client to the end of the list.
static void add_client(int fd, const char *name, const char *ip, int room_id) {
  Client *node = malloc(sizeof(Client));
  if (!node) {
    perror("malloc");
    return;
  }
  node->fd = fd;
  snprintf(node->name, sizeof(node->name), "%s", name);
  snprintf(node->ip, sizeof(node->ip), "%s", ip);
  node->room_id = room_id;
  node->next = NULL;

  pthread_mutex_lock(&clients_mutex);
  if (clients_tail)
    clients_tail->next = node;
  else
    clients_head = node;
  clients_tail = node;
  pthread_mutex_unlock(&clients_mutex);
}

// Removes the client with the given socket from the list, if present.
static void remove_client(int fd) {
  pthread_mutex_lock(&clients_mutex);
  Client *prev = NULL;
  for (Client *cur = clients_head; cur; prev = cur, cur = cur->next) {
    if (cur->fd != fd)
      continue;
    if (prev)
      prev->next = cur->next;
    else
      clients_head = cur->next;
    if (cur == clients_tail)
      clients_tail = prev;
    free(cur);
    break;
  }
  pthread_mutex_unlock(&clients_mutex);
}

// Prints every connected client to the server console.
static void print_clients(void) {
  pthread_mutex_lock(&clients_mutex);
  printf("Currently connected clients:\n");
  for (Client *cur = clients_head; cur; cur = cur->next)
    printf("%s (%s) - Room %d\n", cur->name, cur->ip, cur->room_id);
  printf("---\n");
  pthread_mutex_unlock(&clients_mutex);
}

// A room exists as long as at least one client is in it.
static int room_exists(int room_id) {
  int found = 0;
  pthread_mutex_lock(&clients_mutex);
  for (Client *cur = clients_head; cur && !found; cur = cur->next)
    found = (cur->room_id == room_id);
  pthread_mutex_unlock(&clients_mutex);
  return found;
}

// Fills ids[]/counts[] with each distinct room and its number of users.
// Returns the number of rooms found (at most max).
static int get_room_list(int *ids, int *counts, int max) {
  int n = 0;
  pthread_mutex_lock(&clients_mutex);
  for (Client *cur = clients_head; cur; cur = cur->next) {
    int i = 0;
    while (i < n && ids[i] != cur->room_id)
      i++;
    if (i < n) {
      counts[i]++;
    } else if (n < max) {
      ids[n] = cur->room_id;
      counts[n] = 1;
      n++;
    }
  }
  pthread_mutex_unlock(&clients_mutex);
  return n;
}

// Returns the socket of the user with this name in the room, or -1.
static int find_user_fd(const char *name, int room_id) {
  int fd = -1;
  pthread_mutex_lock(&clients_mutex);
  for (Client *cur = clients_head; cur; cur = cur->next) {
    if (cur->room_id == room_id && strcmp(cur->name, name) == 0) {
      fd = cur->fd;
      break;
    }
  }
  pthread_mutex_unlock(&clients_mutex);
  return fd;
}

// Sends a printf-style message to everyone in a room except exclude_fd
// (pass -1 to include everyone).
static void broadcast_room(int room_id, int exclude_fd, const char *fmt, ...) {
  char buf[LINE_SIZE + 128];
  va_list ap;
  va_start(ap, fmt);
  size_t len = format_msg(buf, sizeof(buf), fmt, ap);
  va_end(ap);

  // Copy the recipients out so we don't hold clients_mutex while sending.
  int fds[MAX_RECIPIENTS];
  int n = 0;
  pthread_mutex_lock(&clients_mutex);
  for (Client *cur = clients_head; cur && n < MAX_RECIPIENTS; cur = cur->next) {
    if (cur->room_id == room_id && cur->fd != exclude_fd)
      fds[n++] = cur->fd;
  }
  pthread_mutex_unlock(&clients_mutex);

  for (int i = 0; i < n; i++)
    send_locked(fds[i], buf, len);
}

/* ------------------------------------------------------------------------ */
/* File transfers                                                           */
/* ------------------------------------------------------------------------ */

typedef enum {
  TRANSFER_PENDING, // waiting for the receiver to accept or reject
  TRANSFER_ACTIVE   // accepted; the sender is streaming chunks
} TransferState;

typedef struct Transfer {
  int sender_fd;
  int receiver_fd;
  char sender_name[NAME_SIZE];
  char filename[FILENAME_SIZE];
  long filesize;
  TransferState state;
  struct Transfer *next;
} Transfer;

// A client sends at most one file and receives at most one file at a time.
// Lock order: transfer_mutex is always taken before any write_locks[] entry.
static Transfer *transfers_head = NULL;
static pthread_mutex_t transfer_mutex = PTHREAD_MUTEX_INITIALIZER;

// Finds a transfer by sender and/or receiver socket; -1 matches anything.
// Caller must hold transfer_mutex.
static Transfer *find_transfer(int sender_fd, int receiver_fd) {
  for (Transfer *t = transfers_head; t; t = t->next) {
    if ((sender_fd < 0 || t->sender_fd == sender_fd) &&
        (receiver_fd < 0 || t->receiver_fd == receiver_fd))
      return t;
  }
  return NULL;
}

// Unlinks and frees a transfer. Caller must hold transfer_mutex.
static void remove_transfer(Transfer *target) {
  Transfer **link = &transfers_head;
  while (*link && *link != target)
    link = &(*link)->next;
  if (*link) {
    *link = target->next;
    free(target);
  }
}

// Per-connection state owned by the thread serving that client.
typedef struct {
  Conn conn; // buffered reader for this client's socket
  char name[NAME_SIZE];
  char ip[INET_ADDRSTRLEN];
  int room_id;
} ClientCtx;

// "[FILE_SEND] <recipient> <filename> <size>": registers a pending transfer
// and asks the recipient to accept it.
static void handle_file_send(ClientCtx *ctx, const char *args) {
  int fd = ctx->conn.fd;
  char recipient[NAME_SIZE], filename[FILENAME_SIZE];
  long filesize;

  if (sscanf(args, "%49s %255s %ld", recipient, filename, &filesize) != 3 ||
      filesize < 0) {
    send_msg(fd, "[FILE_ERROR] Malformed file transfer request\n");
    return;
  }
  if (strcmp(recipient, ctx->name) == 0) {
    send_msg(fd, "[FILE_ERROR] You can't send a file to yourself\n");
    return;
  }

  int recipient_fd = find_user_fd(recipient, ctx->room_id);
  if (recipient_fd < 0) {
    send_msg(fd, "[FILE_ERROR] User '%s' not found in your room\n", recipient);
    return;
  }

  const char *busy = NULL;
  pthread_mutex_lock(&transfer_mutex);
  if (find_transfer(fd, -1)) {
    busy = "You already have a file transfer in progress";
  } else if (find_transfer(-1, recipient_fd)) {
    busy = "The recipient is busy with another file transfer";
  } else {
    Transfer *t = malloc(sizeof(Transfer));
    if (t) {
      t->sender_fd = fd;
      t->receiver_fd = recipient_fd;
      snprintf(t->sender_name, sizeof(t->sender_name), "%s", ctx->name);
      snprintf(t->filename, sizeof(t->filename), "%s", filename);
      t->filesize = filesize;
      t->state = TRANSFER_PENDING;
      t->next = transfers_head;
      transfers_head = t;
    } else {
      busy = "Server out of memory";
    }
  }
  pthread_mutex_unlock(&transfer_mutex);

  if (busy) {
    send_msg(fd, "[FILE_ERROR] %s\n", busy);
    return;
  }
  send_msg(recipient_fd, "[FILE_REQUEST] %s %s %ld\n", ctx->name, filename,
           filesize);
}

// "[FILE_ACCEPT] <sender>" / "[FILE_REJECT] <sender>": the receiver's answer
// to a pending request.
static void handle_file_reply(ClientCtx *ctx, const char *args, int accepted) {
  int fd = ctx->conn.fd;
  char sender_name[NAME_SIZE];
  if (sscanf(args, "%49s", sender_name) != 1)
    return;

  int sender_fd = -1;
  char filename[FILENAME_SIZE];
  long filesize = 0;

  pthread_mutex_lock(&transfer_mutex);
  Transfer *t = find_transfer(-1, fd);
  if (t && t->state == TRANSFER_PENDING &&
      strcmp(t->sender_name, sender_name) == 0) {
    sender_fd = t->sender_fd;
    snprintf(filename, sizeof(filename), "%s", t->filename);
    filesize = t->filesize;
    if (accepted)
      t->state = TRANSFER_ACTIVE;
    else
      remove_transfer(t);
  }
  pthread_mutex_unlock(&transfer_mutex);

  if (sender_fd < 0) {
    // The sender cancelled or left before the answer arrived.
    send_msg(fd, "[FILE_ABORT] That file transfer is no longer available\n");
    return;
  }

  if (accepted) {
    // Tell the receiver to open the file *before* telling the sender to
    // start: the sender's first chunk can only arrive after it hears back.
    send_msg(fd, "[FILE_START] %s %ld %s\n", filename, filesize, sender_name);
    send_msg(sender_fd, "[FILE_ACCEPTED] %s accepted the file transfer\n",
             ctx->name);
  } else {
    send_msg(sender_fd, "[FILE_REJECTED] %s declined the file transfer\n",
             ctx->name);
  }
}

// "[FILE_CHUNK] <n>" + n raw bytes: forwards one chunk to the receiver.
// Returns -1 if the client must be disconnected.
static int handle_file_chunk(ClientCtx *ctx, const char *args) {
  int fd = ctx->conn.fd;
  char *end;
  long n = strtol(args, &end, 10);

  // With a bad length we can't tell where the raw bytes stop and the next
  // line starts, so the stream can't be recovered.
  if (end == args || n <= 0 || n > CHUNK_SIZE)
    return -1;

  // Build the forwarded packet (header + payload) in one buffer so it goes
  // out in a single locked write.
  char packet[32 + CHUNK_SIZE];
  int header_len = snprintf(packet, 32, "[FILE_CHUNK] %ld\n", n);
  if (conn_read_exact(&ctx->conn, packet + header_len, (size_t)n) < 0)
    return -1;

  int failed = 0;
  pthread_mutex_lock(&transfer_mutex);
  Transfer *t = find_transfer(fd, -1);
  if (t && t->state == TRANSFER_ACTIVE) {
    // Sending while holding transfer_mutex guarantees the receiver's socket
    // can't be closed (and its fd reused) halfway through.
    failed = send_locked(t->receiver_fd, packet, header_len + n) < 0;
    if (failed)
      remove_transfer(t);
  }
  // No active transfer (e.g. the receiver left): the chunk is discarded.
  pthread_mutex_unlock(&transfer_mutex);

  if (failed)
    send_msg(fd, "[FILE_ERROR] Lost connection to the receiver; transfer "
                 "aborted\n");
  return 0;
}

// "[FILE_END]" (completed = 1) or "[FILE_CANCEL]" (completed = 0) from the
// sender: notifies the receiver and forgets the transfer.
static void handle_file_finish(ClientCtx *ctx, int completed) {
  int fd = ctx->conn.fd;
  pthread_mutex_lock(&transfer_mutex);
  Transfer *t = find_transfer(fd, -1);
  if (t && completed && t->state == TRANSFER_ACTIVE) {
    send_msg(t->receiver_fd, "[FILE_END]\n");
    remove_transfer(t);
  } else if (t && !completed) {
    send_msg(t->receiver_fd, "[FILE_ABORT] %s cancelled the file transfer\n",
             ctx->name);
    remove_transfer(t);
  }
  pthread_mutex_unlock(&transfer_mutex);
}

// Called when a client disconnects: aborts any transfer it was part of and
// lets the other side know.
static void cleanup_transfers(ClientCtx *ctx) {
  int fd = ctx->conn.fd;
  pthread_mutex_lock(&transfer_mutex);
  Transfer *t = transfers_head;
  while (t) {
    Transfer *next = t->next;
    if (t->sender_fd == fd) {
      send_msg(t->receiver_fd, "[FILE_ABORT] %s left; file transfer aborted\n",
               ctx->name);
      remove_transfer(t);
    } else if (t->receiver_fd == fd) {
      send_msg(t->sender_fd, "[FILE_ERROR] %s left; file transfer aborted\n",
               ctx->name);
      remove_transfer(t);
    }
    t = next;
  }
  pthread_mutex_unlock(&transfer_mutex);
}

/* ------------------------------------------------------------------------ */
/* Per-client thread                                                        */
/* ------------------------------------------------------------------------ */

// Sets (or with 0, clears) a receive timeout on the socket.
static void set_recv_timeout(int fd, int seconds) {
  struct timeval tv = {.tv_sec = seconds, .tv_usec = 0};
  setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}

// Handles a room choice ("new" or a room number), replying with [JOINED] or
// [ERROR]. Returns the room id, or -1 on failure.
static int handle_room_choice(int fd, const char *choice) {
  if (strcmp(choice, "new") == 0) {
    int room = new_room_id();
    send_msg(fd, "[JOINED] Connected with new room number: %d\n", room);
    return room;
  }

  char *end;
  long room = strtol(choice, &end, 10);
  if (end == choice || *end != '\0' || room <= 0 || room > INT_MAX) {
    send_msg(fd, "[ERROR] Invalid room number\n");
    return -1;
  }
  if (!room_exists((int)room)) {
    send_msg(fd, "[ERROR] Room %ld does not exist\n", room);
    return -1;
  }
  send_msg(fd, "[JOINED] Connected to room %ld\n", room);
  return (int)room;
}

// Runs the room-selection handshake. Returns the room id, or -1 on failure.
static int negotiate_room(ClientCtx *ctx) {
  int fd = ctx->conn.fd;
  char line[LINE_SIZE];

  if (conn_read_line(&ctx->conn, line, sizeof(line)) < 0)
    return -1;
  if (strcmp(line, "list") != 0)
    return handle_room_choice(fd, line);

  int ids[MAX_ROOMS], counts[MAX_ROOMS];
  int num_rooms = get_room_list(ids, counts, MAX_ROOMS);

  // First client on the server: no need to ask, just create a room.
  if (num_rooms == 0) {
    int room = new_room_id();
    send_msg(fd, "[JOINED] No rooms available. Created new room: %d\n", room);
    return room;
  }

  send_msg(fd, "The following room options are available:\n");
  for (int i = 0; i < num_rooms; i++)
    send_msg(fd, "Room %d: %d %s\n", ids[i], counts[i],
             counts[i] == 1 ? "person" : "people");
  send_msg(fd, "[CHOOSE]\n");

  if (conn_read_line(&ctx->conn, line, sizeof(line)) < 0)
    return -1;
  return handle_room_choice(fd, line);
}

// Processes one line from a joined client. Returns -1 to disconnect it.
static int handle_line(ClientCtx *ctx, const char *line) {
  const char *args;

  if ((args = after_prefix(line, "[FILE_SEND]")))
    handle_file_send(ctx, args);
  else if ((args = after_prefix(line, "[FILE_ACCEPT]")))
    handle_file_reply(ctx, args, 1);
  else if ((args = after_prefix(line, "[FILE_REJECT]")))
    handle_file_reply(ctx, args, 0);
  else if ((args = after_prefix(line, "[FILE_CHUNK]")))
    return handle_file_chunk(ctx, args);
  else if (after_prefix(line, "[FILE_END]"))
    handle_file_finish(ctx, 1);
  else if (after_prefix(line, "[FILE_CANCEL]"))
    handle_file_finish(ctx, 0);
  else if (line[0] != '\0')
    broadcast_room(ctx->room_id, ctx->conn.fd, "[%s (%s)] %s\n", ctx->name,
                   ctx->ip, line);
  return 0;
}

// Serves one client from connection to disconnect.
static void *thread_client(void *arg) {
  ClientCtx *ctx = arg;
  int fd = ctx->conn.fd;
  char line[LINE_SIZE];

  // Don't let a client that never finishes joining tie up a thread forever.
  set_recv_timeout(fd, HANDSHAKE_TIMEOUT);

  ctx->room_id = negotiate_room(ctx);
  if (ctx->room_id < 0)
    goto done;

  if (conn_read_line(&ctx->conn, line, sizeof(line)) < 0)
    goto done;
  // Long names are truncated to fit.
  snprintf(ctx->name, sizeof(ctx->name), "%.*s", NAME_SIZE - 1, line);
  if (ctx->name[0] == '\0')
    goto done;

  set_recv_timeout(fd, 0);

  add_client(fd, ctx->name, ctx->ip, ctx->room_id);
  print_clients();
  broadcast_room(ctx->room_id, -1, "%s (%s) joined the chat room!\n",
                 ctx->name, ctx->ip);

  while (conn_read_line(&ctx->conn, line, sizeof(line)) >= 0) {
    if (handle_line(ctx, line) < 0)
      break;
  }

  cleanup_transfers(ctx);
  remove_client(fd);
  broadcast_room(ctx->room_id, -1, "%s (%s) left the room\n", ctx->name,
                 ctx->ip);
  print_clients();

done:
  // Close under the write lock so no other thread is mid-send to this fd.
  pthread_mutex_lock(&write_locks[fd]);
  close(fd);
  pthread_mutex_unlock(&write_locks[fd]);
  free(ctx);
  return NULL;
}

// Listens for connections and starts one thread per client.
int main(void) {
  for (int i = 0; i < MAX_FDS; i++)
    pthread_mutex_init(&write_locks[i], NULL);

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("ERROR opening socket");

  // Allow restarting the server right away without "address in use".
  int opt = 1;
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(PORT_NUM);

  if (bind(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    error("ERROR on binding");
  if (listen(sockfd, 5) < 0)
    error("ERROR on listen");

  printf("[SERVER] File transfer enabled chat server started on port %d\n",
         PORT_NUM);

  while (1) {
    struct sockaddr_in cli_addr;
    socklen_t clen = sizeof(cli_addr);
    int newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clen);
    if (newsockfd < 0) {
      perror("ERROR on accept");
      continue;
    }
    if (newsockfd >= MAX_FDS) {
      fprintf(stderr, "Too many connections; rejecting client\n");
      close(newsockfd);
      continue;
    }

    ClientCtx *ctx = calloc(1, sizeof(ClientCtx));
    if (!ctx) {
      close(newsockfd);
      continue;
    }
    conn_init(&ctx->conn, newsockfd);
    inet_ntop(AF_INET, &cli_addr.sin_addr, ctx->ip, sizeof(ctx->ip));
    printf("Connected: %s\n", ctx->ip);

    pthread_t tid;
    if (pthread_create(&tid, NULL, thread_client, ctx) != 0) {
      free(ctx);
      close(newsockfd);
      continue;
    }
    pthread_detach(tid);
  }

  return 0;
}
