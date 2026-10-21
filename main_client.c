/*
 * main_client.c - Multi-user chat room client.
 *
 * Usage: ./main_client <server IP> [room number | new]
 *
 * After joining a room, two threads run side by side:
 *   - the send thread reads the keyboard and sends chat lines and files;
 *   - the receive thread prints incoming chat and saves incoming files.
 * Only the send thread reads stdin or writes to the socket, so neither
 * needs a lock. The threads share only the small pieces of state below.
 *
 * The wire protocol is described at the top of main_server.c.
 */
#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdlib.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "net_common.h"

#define ACCEPT_TIMEOUT 60 // seconds to wait for a recipient to answer

// Text colors for usernames, picked by hashing the name.
static const char *COLORS[] = {
    "\033[31m", "\033[32m", "\033[33m", "\033[34m", "\033[35m", "\033[36m",
};
#define NUM_COLORS (sizeof(COLORS) / sizeof(COLORS[0]))
#define COLOR_RESET "\033[0m"

// The connection to the server. Used by main() during the handshake, then
// read only by the receive thread and written only by the send thread.
static Conn server_conn;

// The server's answer to our latest outgoing [FILE_SEND], set by the receive
// thread: 0 = no answer yet, 1 = accepted, -1 = rejected or failed.
static int transfer_response = 0;
static pthread_mutex_t transfer_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t transfer_cond = PTHREAD_COND_INITIALIZER;

// An incoming file offer waiting for the user's Y/N. The receive thread sets
// it; the send thread treats the next line the user types as the answer.
static int offer_pending = 0;
static char offer_sender[NAME_SIZE];
static pthread_mutex_t offer_mutex = PTHREAD_MUTEX_INITIALIZER;

static void error(const char *msg) {
  perror(msg);
  exit(1);
}

// printf-style send to the server.
static int send_fmt(int fd, const char *fmt, ...) {
  char buf[LINE_SIZE + 128];
  va_list ap;
  va_start(ap, fmt);
  size_t len = format_msg(buf, sizeof(buf), fmt, ap);
  va_end(ap);
  return send_all(fd, buf, len);
}

static void set_transfer_response(int value) {
  pthread_mutex_lock(&transfer_mutex);
  transfer_response = value;
  pthread_cond_signal(&transfer_cond);
  pthread_mutex_unlock(&transfer_mutex);
}

static int get_transfer_response(void) {
  pthread_mutex_lock(&transfer_mutex);
  int value = transfer_response;
  pthread_mutex_unlock(&transfer_mutex);
  return value;
}

// Blocks until the receive thread records an answer or the timeout expires.
// Returns the answer, or 0 on timeout.
static int wait_for_transfer_response(int timeout_sec) {
  struct timespec deadline;
  clock_gettime(CLOCK_REALTIME, &deadline);
  deadline.tv_sec += timeout_sec;

  pthread_mutex_lock(&transfer_mutex);
  while (transfer_response == 0) {
    if (pthread_cond_timedwait(&transfer_cond, &transfer_mutex, &deadline) != 0)
      break;
  }
  int value = transfer_response;
  pthread_mutex_unlock(&transfer_mutex);
  return value;
}

// Returns the last path component, so "dir/a.txt" becomes "a.txt".
static const char *path_basename(const char *path) {
  const char *slash = strrchr(path, '/');
  return slash ? slash + 1 : path;
}

/* ------------------------------------------------------------------------ */
/* Receiving                                                                */
/* ------------------------------------------------------------------------ */

// State of the file currently being received, if any.
typedef struct {
  int active;
  FILE *fp; // NULL if the output file couldn't be opened (data is discarded)
  char path[FILENAME_SIZE + 16];
  long expected;
  long received;
  int last_percent;
} IncomingFile;

// Chat lines look like "[name (ip)] message". Returns the color for name, or
// NULL for anything else (join/leave notices, etc.).
static const char *chat_color(const char *line) {
  if (line[0] != '[')
    return NULL;
  const char *name_end = strstr(line, " (");
  if (!name_end)
    return NULL;

  unsigned hash = 0;
  for (const char *p = line + 1; p < name_end; p++)
    hash += (unsigned char)*p;
  return COLORS[hash % NUM_COLORS];
}

// Closes the incoming file. If the transfer didn't complete, the partial
// file is deleted.
static void incoming_finish(IncomingFile *in, int completed) {
  if (!in->active)
    return;
  in->active = 0;
  if (!in->fp)
    return;
  fclose(in->fp);
  in->fp = NULL;

  if (completed && in->received == in->expected) {
    printf("\n[FILE TRANSFER] File received successfully: %s\n", in->path);
  } else {
    if (completed)
      printf("\n[FILE TRANSFER] Size mismatch (got %ld of %ld bytes)\n",
             in->received, in->expected);
    else
      printf("\n[FILE TRANSFER] Transfer incomplete; deleted %s\n", in->path);
    unlink(in->path);
  }
}

// "[FILE_START] <filename> <size> <sender>": opens the output file.
static void incoming_start(IncomingFile *in, const char *args) {
  char filename[FILENAME_SIZE], sender[NAME_SIZE];
  long size;
  if (sscanf(args, "%255s %ld %49s", filename, &size, sender) != 3)
    return;

  incoming_finish(in, 0);
  in->active = 1;
  in->expected = size;
  in->received = 0;
  in->last_percent = -1;

  // Strip directories so a sender can't write outside the current directory
  // with a name like "../../.bashrc".
  snprintf(in->path, sizeof(in->path), "received_%s", path_basename(filename));
  in->fp = fopen(in->path, "wb");
  if (!in->fp) {
    perror("Error opening file for writing");
    return;
  }
  printf("\n[FILE TRANSFER] Receiving file from %s: %s (%ld bytes)\n", sender,
         filename, size);
}

// "[FILE_CHUNK] <n>" + n raw bytes: appends the bytes to the incoming file.
// Returns -1 if the stream is broken.
static int incoming_chunk(IncomingFile *in, const char *args) {
  char data[CHUNK_SIZE];
  long n = strtol(args, NULL, 10);
  if (n <= 0 || n > CHUNK_SIZE)
    return -1;
  // Always consume the bytes, even if we're discarding them, so the next
  // read starts on a line boundary.
  if (conn_read_exact(&server_conn, data, (size_t)n) < 0)
    return -1;
  if (!in->active || !in->fp)
    return 0;

  fwrite(data, 1, (size_t)n, in->fp);
  in->received += n;

  if (in->expected > 0) {
    int percent = (int)(in->received * 100 / in->expected);
    if (percent != in->last_percent) {
      in->last_percent = percent;
      printf("\r[FILE TRANSFER] Progress: %d%%", percent);
      fflush(stdout);
    }
  }
  return 0;
}

// Reads lines from the server and handles each one until disconnect.
static void *thread_recv(void *arg) {
  (void)arg;
  IncomingFile incoming = {0};
  char line[LINE_SIZE];
  const char *args;

  while (conn_read_line(&server_conn, line, sizeof(line)) >= 0) {
    if ((args = after_prefix(line, "[FILE_REQUEST]"))) {
      char sender[NAME_SIZE], filename[FILENAME_SIZE];
      long size;
      if (sscanf(args, "%49s %255s %ld", sender, filename, &size) == 3) {
        pthread_mutex_lock(&offer_mutex);
        offer_pending = 1;
        snprintf(offer_sender, sizeof(offer_sender), "%s", sender);
        pthread_mutex_unlock(&offer_mutex);

        printf("\n[FILE TRANSFER] %s wants to send you: %s (%ld bytes)\n",
               sender, filename, size);
        printf("Accept? (Y/N): ");
        fflush(stdout);
      }
    } else if ((args = after_prefix(line, "[FILE_ACCEPTED]"))) {
      printf("[FILE TRANSFER] %s\n", args);
      set_transfer_response(1);
    } else if ((args = after_prefix(line, "[FILE_REJECTED]")) ||
               (args = after_prefix(line, "[FILE_ERROR]"))) {
      printf("\n[FILE TRANSFER] %s\n", args);
      set_transfer_response(-1);
    } else if ((args = after_prefix(line, "[FILE_START]"))) {
      incoming_start(&incoming, args);
    } else if ((args = after_prefix(line, "[FILE_CHUNK]"))) {
      if (incoming_chunk(&incoming, args) < 0)
        break;
    } else if (after_prefix(line, "[FILE_END]")) {
      incoming_finish(&incoming, 1);
    } else if ((args = after_prefix(line, "[FILE_ABORT]"))) {
      // The offer was withdrawn, or the transfer died partway through.
      pthread_mutex_lock(&offer_mutex);
      offer_pending = 0;
      pthread_mutex_unlock(&offer_mutex);
      printf("\n[FILE TRANSFER] %s\n", args);
      incoming_finish(&incoming, 0);
    } else {
      const char *color = chat_color(line);
      if (color)
        printf("%s%s%s\n", color, line, COLOR_RESET);
      else
        printf("%s\n", line);
    }
  }

  incoming_finish(&incoming, 0);
  printf("\n[Disconnected from server]\n");
  return NULL;
}

/* ------------------------------------------------------------------------ */
/* Sending                                                                  */
/* ------------------------------------------------------------------------ */

// Parses "SEND <recipient> <file>". Returns 1 if line is a SEND command.
static int parse_send_command(const char *line, char *recipient,
                              char *filename) {
  return sscanf(line, "SEND %49s %255s", recipient, filename) == 2;
}

// If a file offer is waiting, treats line as the user's Y/N answer and
// returns 1. Otherwise returns 0.
static int answer_pending_offer(int fd, const char *line) {
  char sender[NAME_SIZE];

  pthread_mutex_lock(&offer_mutex);
  int pending = offer_pending;
  offer_pending = 0;
  snprintf(sender, sizeof(sender), "%s", offer_sender);
  pthread_mutex_unlock(&offer_mutex);

  if (!pending)
    return 0;

  if (line[0] == 'Y' || line[0] == 'y') {
    send_fmt(fd, "[FILE_ACCEPT] %s\n", sender);
    printf("[FILE TRANSFER] Waiting to receive file...\n");
  } else {
    send_fmt(fd, "[FILE_REJECT] %s\n", sender);
    printf("[FILE TRANSFER] Transfer declined.\n");
  }
  return 1;
}

// Offers a file to recipient and, if accepted, streams it in chunks.
// Returns -1 if the connection to the server is lost.
static int send_file(int fd, const char *recipient, const char *path) {
  struct stat st;
  if (stat(path, &st) < 0 || !S_ISREG(st.st_mode)) {
    printf("[ERROR] Cannot open file: %s\n", path);
    return 0;
  }
  FILE *fp = fopen(path, "rb");
  if (!fp) {
    printf("[ERROR] Cannot open file: %s\n", path);
    return 0;
  }
  long filesize = (long)st.st_size;
  int result = -1;

  // Reset before sending: the server's answer could otherwise arrive and be
  // overwritten before we start waiting for it.
  set_transfer_response(0);
  if (send_fmt(fd, "[FILE_SEND] %s %s %ld\n", recipient, path_basename(path),
               filesize) < 0)
    goto out;

  printf("[FILE TRANSFER] Waiting for %s to accept...\n", recipient);
  int response = wait_for_transfer_response(ACCEPT_TIMEOUT);
  if (response == 0) {
    printf("[FILE TRANSFER] Timeout waiting for response\n");
    result = send_fmt(fd, "[FILE_CANCEL]\n");
    goto out;
  }
  if (response < 0) {
    printf("[FILE TRANSFER] Transfer was not accepted\n");
    result = 0;
    goto out;
  }

  printf("[FILE TRANSFER] Sending file...\n");
  char chunk[CHUNK_SIZE];
  long total_sent = 0;
  int last_percent = -1;
  int complete = 1;

  while (total_sent < filesize) {
    // The server reports [FILE_ERROR] if the receiver goes away mid-transfer.
    if (get_transfer_response() < 0) {
      complete = 0;
      break;
    }

    size_t to_read = CHUNK_SIZE;
    if (filesize - total_sent < CHUNK_SIZE)
      to_read = (size_t)(filesize - total_sent);
    size_t n = fread(chunk, 1, to_read, fp);
    if (n == 0) { // read error, or the file shrank while we were sending
      complete = 0;
      break;
    }

    if (send_fmt(fd, "[FILE_CHUNK] %zu\n", n) < 0 ||
        send_all(fd, chunk, n) < 0)
      goto out;
    total_sent += (long)n;

    int percent = (int)(total_sent * 100 / filesize);
    if (percent != last_percent) {
      last_percent = percent;
      printf("\r[FILE TRANSFER] Sent: %d%%", percent);
      fflush(stdout);
    }
  }

  if (complete) {
    result = send_fmt(fd, "[FILE_END]\n");
    printf("\n[FILE TRANSFER] File sent successfully: %ld bytes\n",
           total_sent);
  } else {
    result = send_fmt(fd, "[FILE_CANCEL]\n");
    printf("\n[FILE TRANSFER] Transfer aborted\n");
  }

out:
  fclose(fp);
  return result < 0 ? -1 : 0;
}

// Reads the keyboard and sends each line to the server.
static void *thread_send(void *arg) {
  (void)arg;
  int fd = server_conn.fd;
  char line[LINE_SIZE];
  char recipient[NAME_SIZE], filename[FILENAME_SIZE];

  while (fgets(line, sizeof(line), stdin) != NULL) {
    line[strcspn(line, "\r\n")] = '\0';

    if (answer_pending_offer(fd, line))
      continue;
    if (line[0] == '\0')
      continue;

    if (parse_send_command(line, recipient, filename)) {
      if (send_file(fd, recipient, filename) < 0)
        break;
      continue;
    }

    // These would be mistaken for protocol messages by the server.
    if (after_prefix(line, "[FILE_")) {
      printf("[ERROR] Messages starting with [FILE_ are reserved\n");
      continue;
    }

    if (send_fmt(fd, "%s\n", line) < 0)
      break;
  }

  // stdin closed or the send failed: shut the socket down so the receive
  // thread's recv() returns and the program can exit.
  shutdown(fd, SHUT_RDWR);
  return NULL;
}

/* ------------------------------------------------------------------------ */
/* Startup                                                                  */
/* ------------------------------------------------------------------------ */

// Reads one line from stdin into buf, without the trailing newline.
static void read_stdin_line(char *buf, size_t size, const char *what) {
  if (fgets(buf, (int)size, stdin) == NULL) {
    fprintf(stderr, "ERROR reading %s\n", what);
    exit(1);
  }
  buf[strcspn(buf, "\r\n")] = '\0';
}

// Runs the room-selection handshake (see main_server.c). Exits on failure.
static void join_room(const char *initial_request) {
  int fd = server_conn.fd;
  char line[LINE_SIZE];
  const char *args;

  if (send_fmt(fd, "%s\n", initial_request) < 0)
    error("ERROR sending room request");

  while (1) {
    if (conn_read_line(&server_conn, line, sizeof(line)) < 0) {
      fprintf(stderr, "ERROR: server closed the connection\n");
      exit(1);
    }

    if ((args = after_prefix(line, "[JOINED]"))) {
      printf("%s\n", args);
      return;
    }
    if ((args = after_prefix(line, "[ERROR]"))) {
      fprintf(stderr, "ERROR: %s\n", args);
      exit(1);
    }
    if (after_prefix(line, "[CHOOSE]")) {
      char choice[LINE_SIZE];
      printf("Choose the room number or type [new] to create a new room: ");
      fflush(stdout);
      read_stdin_line(choice, sizeof(choice), "room choice");
      if (send_fmt(fd, "%s\n", choice) < 0)
        error("ERROR sending room choice");
      continue;
    }

    printf("%s\n", line); // room list entries and other info
  }
}

// Connects, joins a room, then runs the send/receive threads.
int main(int argc, char *argv[]) {
  if (argc < 2) {
    fprintf(stderr, "Usage: %s <server IP> [room_number | new]\n", argv[0]);
    return 1;
  }

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(PORT_NUM);
  if (inet_pton(AF_INET, argv[1], &serv_addr.sin_addr) != 1) {
    fprintf(stderr, "Invalid IP address: %s\n", argv[1]);
    return 1;
  }

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("ERROR opening socket");

  printf("Try connecting to %s...\n", argv[1]);
  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0)
    error("ERROR connecting");
  conn_init(&server_conn, sockfd);

  // With a room argument, ask for it directly; otherwise get the room list.
  join_room(argc >= 3 ? argv[2] : "list");

  char name[NAME_SIZE];
  do {
    printf("Type your user name:\n");
    read_stdin_line(name, sizeof(name), "username");
  } while (name[0] == '\0');

  if (send_fmt(sockfd, "%s\n", name) < 0)
    error("ERROR sending username");

  pthread_t send_tid, recv_tid;
  if (pthread_create(&send_tid, NULL, thread_send, NULL) != 0 ||
      pthread_create(&recv_tid, NULL, thread_recv, NULL) != 0)
    error("ERROR creating threads");

  // The send thread may be blocked reading the keyboard, so only wait for
  // the receive thread; it finishes when the connection ends either way.
  pthread_detach(send_tid);
  pthread_join(recv_tid, NULL);

  printf("Disconnected. Exiting...\n");
  close(sockfd);
  return 0;
}
