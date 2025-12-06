#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

#define PORT_NUM 1004
#define BUFFER_SIZE 4096

// function prototypes for room and user management
void print_clients();
void remove_client(int clisockfd);
int room_exists(int room_id);
int count_users_in_room(int room_id);
int get_room_list(int *room_ids, int max_rooms);
int find_user_socket(const char *username, int room_id);

void error(const char *msg)
{
  perror(msg);
  exit(1);
}

// linked list node for connected clients
typedef struct _USR
{
  int clisockfd;
  struct _USR *next;
  char name[50];
  int room_id;
} USR;

USR *head = NULL;
USR *tail = NULL;

// mutex for client list
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

// room ID assignment state
int next_room_id = 1;
pthread_mutex_t room_mutex = PTHREAD_MUTEX_INITIALIZER;

// file transfer session state struct
typedef struct _FileTransferSession
{
  int sender_sockfd;
  int receiver_sockfd;
  char sender_name[50];
  char receiver_name[50];
  char filename[256];
  long filesize;
  int active;
  pthread_mutex_t *ready_mutex;
  pthread_cond_t *ready_cond;
  int *ready_flag_ptr;
  struct _FileTransferSession *next;
} FileTransferSession;

FileTransferSession *transfer_head = NULL;
pthread_mutex_t transfer_mutex = PTHREAD_MUTEX_INITIALIZER;

// check if client is actively sending a file
int is_sending_file(int sockfd)
{
  pthread_mutex_lock(&transfer_mutex);
  FileTransferSession *session = transfer_head;
  while (session != NULL)
  {
    if (session->sender_sockfd == sockfd && session->active == 3)
    {
      pthread_mutex_unlock(&transfer_mutex);
      return 1;
    }
    session = session->next;
  }
  pthread_mutex_unlock(&transfer_mutex);
  return 0;
}

// check if a room ID exists
int room_exists(int room_id)
{
  pthread_mutex_lock(&clients_mutex);
  USR *cur = head;

  while (cur != NULL)
  {
    if (cur->room_id == room_id)
    {
      pthread_mutex_unlock(&clients_mutex);
      return 1;
    }
    cur = cur->next;
  }
  pthread_mutex_unlock(&clients_mutex);
  return 0;
}

// count connected users in the specified room
int count_users_in_room(int room_id)
{
  int count = 0;
  pthread_mutex_lock(&clients_mutex);
  USR *cur = head;

  while (cur != NULL)
  {
    if (cur->room_id == room_id)
      count++;
    cur = cur->next;
  }
  pthread_mutex_unlock(&clients_mutex);
  return count;
}

// gather all room IDs into an array
int get_room_list(int *room_ids, int max_rooms)
{
  pthread_mutex_lock(&clients_mutex);
  int num_rooms = 0;
  USR *cur = head;

  while (cur != NULL && num_rooms < max_rooms)
  {
    int found = 0;
    for (int i = 0; i < num_rooms; i++)
    {
      if (room_ids[i] == cur->room_id)
      {
        found = 1;
        break;
      }
    }

    if (!found)
    {
      room_ids[num_rooms] = cur->room_id;
      num_rooms++;
    }
    cur = cur->next;
  }
  pthread_mutex_unlock(&clients_mutex);
  return num_rooms;
}

// look up a user's socket in a room by username
int find_user_socket(const char *username, int room_id)
{
  pthread_mutex_lock(&clients_mutex);
  USR *cur = head;

  while (cur != NULL)
  {
    if (cur->room_id == room_id && strcmp(cur->name, username) == 0)
    {
      int sockfd = cur->clisockfd;
      pthread_mutex_unlock(&clients_mutex);
      return sockfd;
    }
    cur = cur->next;
  }

  pthread_mutex_unlock(&clients_mutex);
  return -1;
}

// append a new connected user to the linked list
void add_tail(int newclisockfd, const char *name, int room_id)
{
  pthread_mutex_lock(&clients_mutex);

  USR *newnode = malloc(sizeof(USR));
  if (!newnode)
  {
    perror("malloc");
    pthread_mutex_unlock(&clients_mutex);
    return;
  }

  newnode->clisockfd = newclisockfd;
  strncpy(newnode->name, name, sizeof(newnode->name) - 1);
  newnode->name[sizeof(newnode->name) - 1] = '\0';
  newnode->room_id = room_id;
  newnode->next = NULL;

  if (head == NULL)
  {
    head = newnode;
    tail = newnode;
  }
  else
  {
    tail->next = newnode;
    tail = newnode;
  }

  pthread_mutex_unlock(&clients_mutex);
}

// print a list of connected clients
void print_clients()
{
  pthread_mutex_lock(&clients_mutex);
  printf("Currently connected clients:\n");

  USR *cur = head;
  while (cur != NULL)
  {
    struct sockaddr_in cliaddr;
    socklen_t len = sizeof(cliaddr);

    if (getpeername(cur->clisockfd, (struct sockaddr *)&cliaddr, &len) == 0)
    {
      char ip_str[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &(cliaddr.sin_addr), ip_str, INET_ADDRSTRLEN);
      printf("%s (%s) - Room %d\n", cur->name, ip_str, cur->room_id);
    }
    else
    {
      printf("%s | unknown IP - Room %d\n", cur->name, cur->room_id);
    }
    cur = cur->next;
  }
  printf("---\n");
  pthread_mutex_unlock(&clients_mutex);
}

// remove a client from the linked list
void remove_client(int clisockfd)
{
  pthread_mutex_lock(&clients_mutex);

  if (head == NULL)
  {
    printf("List is empty. Nothing to delete\n");
    pthread_mutex_unlock(&clients_mutex);
    return;
  }

  USR *cur = head;
  USR *prev = NULL;

  if (head->clisockfd == clisockfd)
  {
    cur = head;
    head = head->next;

    if (head == NULL)
      tail = NULL;

    free(cur);
    pthread_mutex_unlock(&clients_mutex);
    return;
  }

  while (cur != NULL)
  {
    if (cur->clisockfd == clisockfd)
    {
      prev->next = cur->next;
      if (cur == tail)
        tail = prev;
      free(cur);
      pthread_mutex_unlock(&clients_mutex);
      return;
    }
    prev = cur;
    cur = cur->next;
  }

  printf("Client FD %d not in list.\n", clisockfd);
  pthread_mutex_unlock(&clients_mutex);
}

// safe send wrapper for server
int safe_send(int sockfd, const char *buffer, size_t len)
{
  size_t total_sent = 0;
  while (total_sent < len)
  {
    int result = send(sockfd, buffer + total_sent, len - total_sent, MSG_NOSIGNAL);
    if (result < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
      {
        usleep(1000);
        continue;
      }
      if (errno != EPIPE && errno != ECONNRESET)
      {
        perror("send");
      }
      return -1;
    }
    if (result == 0)
    {
      return -1;
    }
    total_sent += result;
  }
  return (int)total_sent;
}

// broadcast a message to all clients in sender's room
void broadcast(int fromfd, char *message)
{
  char sender_name[50] = "Unknown";
  int sender_room = -1;
  struct sockaddr_in cliaddr;
  socklen_t clen = sizeof(cliaddr);
  char ip_str[INET_ADDRSTRLEN] = "unknown";

  if (getpeername(fromfd, (struct sockaddr *)&cliaddr, &clen) == 0)
  {
    inet_ntop(AF_INET, &(cliaddr.sin_addr), ip_str, INET_ADDRSTRLEN);
  }

  pthread_mutex_lock(&clients_mutex);
  USR *cur = head;

  while (cur != NULL)
  {
    if (cur->clisockfd == fromfd)
    {
      strncpy(sender_name, cur->name, sizeof(sender_name) - 1);
      sender_name[sizeof(sender_name) - 1] = '\0';
      sender_room = cur->room_id;
      break;
    }
    cur = cur->next;
  }

  int recipient_fds[100];
  int num_recipients = 0;

  cur = head;
  while (cur != NULL && num_recipients < 100)
  {
    if (cur->clisockfd != fromfd && cur->room_id == sender_room)
    {
      recipient_fds[num_recipients++] = cur->clisockfd;
    }
    cur = cur->next;
  }
  pthread_mutex_unlock(&clients_mutex);

  char buffer[512];
  snprintf(buffer, sizeof(buffer), "[%s (%s)] %s", sender_name, ip_str, message);
  size_t nmsg = strlen(buffer);

  for (int i = 0; i < num_recipients; i++)
  {
    safe_send(recipient_fds[i], buffer, nmsg);
  }
}

// broadcast a message to all users in a specific room
void broadcast_all(char *message, int room_id)
{
  pthread_mutex_lock(&clients_mutex);
  
  int recipient_fds[100];
  int num_recipients = 0;
  
  USR *cur = head;
  while (cur != NULL && num_recipients < 100)
  {
    if (cur->room_id == room_id)
    {
      recipient_fds[num_recipients++] = cur->clisockfd;
    }
    cur = cur->next;
  }
  pthread_mutex_unlock(&clients_mutex);

  size_t len = strlen(message);
  for (int i = 0; i < num_recipients; i++)
  {
    safe_send(recipient_fds[i], message, len);
  }
}

typedef struct _ThreadArgs
{
  int clisockfd;
} ThreadArgs;

typedef struct _FileRelayArgs
{
  int sender_sockfd;
  int receiver_sockfd;
  long filesize;
  char sender_name[50];
  char receiver_name[50];
  int *ready_flag_ptr;
  pthread_mutex_t *ready_mutex;
  pthread_cond_t *ready_cond;
} FileRelayArgs;

// thread that relays file data between sender and receiver
void *thread_file_relay(void *args)
{
  pthread_detach(pthread_self());
  FileRelayArgs *relay_args = (FileRelayArgs *)args;

  int sender_fd = relay_args->sender_sockfd;
  int receiver_fd = relay_args->receiver_sockfd;
  long filesize = relay_args->filesize;
  pthread_mutex_t *ready_mutex = relay_args->ready_mutex;
  pthread_cond_t *ready_cond = relay_args->ready_cond;

  char sender_name[50], receiver_name[50];
  strncpy(sender_name, relay_args->sender_name, sizeof(sender_name) - 1);
  strncpy(receiver_name, relay_args->receiver_name, sizeof(receiver_name) - 1);
  sender_name[sizeof(sender_name) - 1] = '\0';
  receiver_name[sizeof(receiver_name) - 1] = '\0';

  pthread_mutex_lock(ready_mutex);
  struct timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  ts.tv_sec += 10;
  
  int wait_failed = 0;
  while (*(relay_args->ready_flag_ptr) == 0)
  {
    int wait_result = pthread_cond_timedwait(ready_cond, ready_mutex, &ts);
    if (wait_result != 0)
    {
      wait_failed = 1;
      break;
    }
  }
  pthread_mutex_unlock(ready_mutex);

  if (wait_failed)
  {
    pthread_mutex_lock(&transfer_mutex);
    FileTransferSession *session = transfer_head;
    FileTransferSession *prev = NULL;

    while (session != NULL)
    {
      if (session->sender_sockfd == sender_fd && session->receiver_sockfd == receiver_fd)
      {
        if (prev)
          prev->next = session->next;
        else
          transfer_head = session->next;

        if (session->ready_mutex)
        {
          pthread_mutex_destroy(session->ready_mutex);
          free(session->ready_mutex);
        }
        if (session->ready_cond)
        {
          pthread_cond_destroy(session->ready_cond);
          free(session->ready_cond);
        }
        if (session->ready_flag_ptr)
          free(session->ready_flag_ptr);

        free(session);
        break;
      }

      prev = session;
      session = session->next;
    }
    pthread_mutex_unlock(&transfer_mutex);

    free(relay_args);
    return NULL;
  }

  char go_msg[] = "[FILE_GO]";
  safe_send(sender_fd, go_msg, strlen(go_msg));

  char buffer[BUFFER_SIZE];
  long total_relayed = 0;

  while (total_relayed < filesize)
  {
    long to_receive = filesize - total_relayed;
    if (to_receive > BUFFER_SIZE)
      to_receive = BUFFER_SIZE;

    int n = recv(sender_fd, buffer, to_receive, 0);
    if (n <= 0)
    {
      break;
    }

    int sent = safe_send(receiver_fd, buffer, n);
    if (sent <= 0)
    {
      break;
    }

    total_relayed += n;
  }

  pthread_mutex_lock(&transfer_mutex);
  FileTransferSession *session = transfer_head;
  FileTransferSession *prev_session = NULL;

  while (session != NULL)
  {
    if (session->sender_sockfd == sender_fd &&
        session->receiver_sockfd == receiver_fd &&
        session->active == 3)
    {
      if (prev_session)
        prev_session->next = session->next;
      else
        transfer_head = session->next;

      if (session->ready_flag_ptr)
        free(session->ready_flag_ptr);

      free(session);
      break;
    }

    prev_session = session;
    session = session->next;
  }
  pthread_mutex_unlock(&transfer_mutex);

  pthread_mutex_destroy(ready_mutex);
  pthread_cond_destroy(ready_cond);
  free(ready_mutex);
  free(ready_cond);
  free(relay_args);

  return NULL;
}

// main client-handling thread
void *thread_main(void *args)
{
  pthread_detach(pthread_self());
  int clisockfd = ((ThreadArgs *)args)->clisockfd;
  free(args);

  int assigned_room = -1;

  char buffer[256];
  int nrcv;

  struct timeval tv;
  tv.tv_sec = 30;
  tv.tv_usec = 0;
  setsockopt(clisockfd, SOL_SOCKET, SO_RCVTIMEO, (const char*)&tv, sizeof tv);

  nrcv = recv(clisockfd, buffer, 255, 0);
  if (nrcv <= 0)
  {
    close(clisockfd);
    return NULL;
  }
  buffer[nrcv] = '\0';

  // handle room selection request
  if (strcmp(buffer, "list") == 0)
  {
    int room_ids[20];
    int num_rooms = get_room_list(room_ids, 20);

    if (num_rooms == 0)
    {
      pthread_mutex_lock(&room_mutex);
      assigned_room = next_room_id++;
      pthread_mutex_unlock(&room_mutex);

      char response[200];
      snprintf(response, sizeof(response),
               "No rooms available. Created new room: %d\n", assigned_room);
      safe_send(clisockfd, response, strlen(response));
    }
    else
    {
      char response[1024];
      snprintf(response, sizeof(response), "The following room options are available:\n");

      for (int i = 0; i < num_rooms; i++)
      {
        char line[100];
        int count = count_users_in_room(room_ids[i]);
        snprintf(line, sizeof(line),
                 "Room %d: %d %s\n", room_ids[i], count,
                 count == 1 ? "person" : "people");
        strncat(response, line, sizeof(response) - strlen(response) - 1);
      }

      safe_send(clisockfd, response, strlen(response));

      nrcv = recv(clisockfd, buffer, 255, 0);
      if (nrcv <= 0)
      {
        close(clisockfd);
        return NULL;
      }
      buffer[nrcv] = '\0';

      if (strcmp(buffer, "new") == 0)
      {
        pthread_mutex_lock(&room_mutex);
        assigned_room = next_room_id++;
        pthread_mutex_unlock(&room_mutex);

        char response2[100];
        snprintf(response2, sizeof(response2),
                 "Connected with new room number: %d\n", assigned_room);
        safe_send(clisockfd, response2, strlen(response2));
      }
      else
      {
        int requested_room = atoi(buffer);

        if (requested_room <= 0)
        {
          char response2[] = "ERROR: Invalid room number\n";
          safe_send(clisockfd, response2, strlen(response2));
          close(clisockfd);
          return NULL;
        }

        if (room_exists(requested_room))
        {
          assigned_room = requested_room;
          char response2[100];
          snprintf(response2, sizeof(response2),
                   "Connected to room %d\n", assigned_room);
          safe_send(clisockfd, response2, strlen(response2));
        }
        else
        {
          char response2[100];
          snprintf(response2, sizeof(response2),
                   "ERROR: Room %d does not exist\n", requested_room);
          safe_send(clisockfd, response2, strlen(response2));
          close(clisockfd);
          return NULL;
        }
      }
    }
  }
  else if (strcmp(buffer, "new") == 0)
  {
    pthread_mutex_lock(&room_mutex);
    assigned_room = next_room_id++;
    pthread_mutex_unlock(&room_mutex);

    char response[100];
    snprintf(response, sizeof(response),
             "Connected with new room number %d\n", assigned_room);
    safe_send(clisockfd, response, strlen(response));
  }
  else
  {
    int requested_room = atoi(buffer);

    if (requested_room < 0)
    {
      char response[] = "ERROR: Invalid room number\n";
      safe_send(clisockfd, response, strlen(response));
      close(clisockfd);
      return NULL;
    }

    if (room_exists(requested_room))
    {
      assigned_room = requested_room;
      char response[100];
      snprintf(response, sizeof(response),
               "Connected to room %d\n", assigned_room);
      safe_send(clisockfd, response, strlen(response));
    }
    else
    {
      char response[100];
      snprintf(response, sizeof(response),
               "ERROR: Room %d does not exist\n", requested_room);
      safe_send(clisockfd, response, strlen(response));
      close(clisockfd);
      return NULL;
    }
  }

  // receive username
  char username[50];
  memset(buffer, 0, sizeof(buffer));

  nrcv = recv(clisockfd, buffer, 255, 0);
  if (nrcv <= 0)
  {
    close(clisockfd);
    return NULL;
  }

  buffer[nrcv] = '\0';
  strncpy(username, buffer, sizeof(username) - 1);
  username[sizeof(username) - 1] = '\0';

  size_t len = strlen(username);
  if (len > 0 && username[len - 1] == '\n')
    username[len - 1] = '\0';

  if (strlen(username) == 0)
  {
    close(clisockfd);
    return NULL;
  }

  add_tail(clisockfd, username, assigned_room);
  print_clients();

  struct sockaddr_in cliaddr;
  socklen_t clen = sizeof(cliaddr);
  if (getpeername(clisockfd, (struct sockaddr *)&cliaddr, &clen) == 0)
  {
    char join_msg[512];
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(cliaddr.sin_addr), ip_str, INET_ADDRSTRLEN);
    snprintf(join_msg, sizeof(join_msg),
             "%s (%s) joined the chat room!\n", username, ip_str);
    broadcast_all(join_msg, assigned_room);
  }

  // main message loop
  while (1)
  {
    if (is_sending_file(clisockfd))
    {
      usleep(100000);
      continue;
    }

    memset(buffer, 0, 256);
    nrcv = recv(clisockfd, buffer, 255, 0);

    if (nrcv < 0)
    {
      if (errno == EAGAIN || errno == EWOULDBLOCK)
        continue;

      break;
    }
    if (nrcv == 0)
    {
      struct sockaddr_in cliaddr_local;
      socklen_t clen_local = sizeof(cliaddr_local);
      char leave_msg[512];

      if (getpeername(clisockfd, (struct sockaddr *)&cliaddr_local, &clen_local) == 0)
      {
        char ip_str[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &(cliaddr_local.sin_addr), ip_str, INET_ADDRSTRLEN);
        snprintf(leave_msg, sizeof(leave_msg),
                 "%s (%s) left the room\n", username, ip_str);
      }
      else
      {
        snprintf(leave_msg, sizeof(leave_msg),
                 "%s left the room\n", username);
      }

      broadcast_all(leave_msg, assigned_room);
      break;
    }

    // handle file send request
    if (strncmp(buffer, "[FILE_SEND]", 11) == 0)
    {
      char recipient[50], filename[256];
      long filesize;

      if (sscanf(buffer + 11, "%49s %255s %ld",
                 recipient, filename, &filesize) == 3)
      {
        int recipient_fd = find_user_socket(recipient, assigned_room);

        if (recipient_fd < 0)
        {
          char error_msg[200];
          snprintf(error_msg, sizeof(error_msg),
                   "[FILE_ERROR] User '%s' not found in your room\n", recipient);
          safe_send(clisockfd, error_msg, strlen(error_msg));
        }
        else
        {
          pthread_mutex_lock(&transfer_mutex);

          FileTransferSession *session = malloc(sizeof(FileTransferSession));
          if (session)
          {
            session->sender_sockfd = clisockfd;
            session->receiver_sockfd = recipient_fd;
            strncpy(session->sender_name, username, sizeof(session->sender_name) - 1);
            strncpy(session->receiver_name, recipient, sizeof(session->receiver_name) - 1);
            strncpy(session->filename, filename, sizeof(session->filename) - 1);
            session->filesize = filesize;
            session->active = 0;
            session->ready_mutex = NULL;
            session->ready_cond = NULL;
            session->ready_flag_ptr = NULL;
            session->next = transfer_head;
            transfer_head = session;

            char request_msg[512];
            snprintf(request_msg, sizeof(request_msg),
                     "[FILE_REQUEST] %s %s %ld", username, filename, filesize);
            safe_send(recipient_fd, request_msg, strlen(request_msg));
          }

          pthread_mutex_unlock(&transfer_mutex);
        }
      }
      continue;
    }

    // receiver accepted the file
    if (strncmp(buffer, "[FILE_ACCEPT]", 13) == 0)
    {
      char sender_name[50], filename[256];
      long filesize;

      if (sscanf(buffer + 13, "%49s %255s %ld",
                 sender_name, filename, &filesize) == 3)
      {
        int sender_fd = find_user_socket(sender_name, assigned_room);

        if (sender_fd >= 0)
        {
          pthread_mutex_lock(&transfer_mutex);
          FileTransferSession *session = transfer_head;
          FileTransferSession *found_session = NULL;

          while (session != NULL)
          {
            if (session->sender_sockfd == sender_fd &&
                session->receiver_sockfd == clisockfd &&
                session->active == 0)
            {
              found_session = session;
              session->active = 1;
              break;
            }
            session = session->next;
          }
          pthread_mutex_unlock(&transfer_mutex);

          if (found_session)
          {
            char accept_msg[200];
            snprintf(accept_msg, sizeof(accept_msg),
                     "[FILE_ACCEPTED] %s accepted the file transfer\n", username);
            safe_send(sender_fd, accept_msg, strlen(accept_msg));

            pthread_mutex_t *ready_mutex = malloc(sizeof(pthread_mutex_t));
            pthread_cond_t *ready_cond = malloc(sizeof(pthread_cond_t));
            int *ready_flag = malloc(sizeof(int));

            if (ready_mutex && ready_cond && ready_flag)
            {
              pthread_mutex_init(ready_mutex, NULL);
              pthread_cond_init(ready_cond, NULL);
              *ready_flag = 0;

              pthread_mutex_lock(&transfer_mutex);
              found_session->ready_mutex = ready_mutex;
              found_session->ready_cond = ready_cond;
              found_session->ready_flag_ptr = ready_flag;
              found_session->active = 2;
              pthread_mutex_unlock(&transfer_mutex);

              FileRelayArgs *relay_args = malloc(sizeof(FileRelayArgs));
              if (relay_args)
              {
                relay_args->sender_sockfd = sender_fd;
                relay_args->receiver_sockfd = clisockfd;
                relay_args->filesize = found_session->filesize;
                strncpy(relay_args->sender_name, sender_name,
                        sizeof(relay_args->sender_name) - 1);
                strncpy(relay_args->receiver_name, username,
                        sizeof(relay_args->receiver_name) - 1);
                relay_args->ready_flag_ptr = ready_flag;
                relay_args->ready_mutex = ready_mutex;
                relay_args->ready_cond = ready_cond;

                char start_msg[512];
                snprintf(start_msg, sizeof(start_msg),
                         "[FILE_START] %s %ld %s",
                         filename, filesize, sender_name);
                safe_send(clisockfd, start_msg, strlen(start_msg));

                pthread_t relay_thread;
                pthread_create(&relay_thread, NULL, thread_file_relay, relay_args);
              }
            }
          }
        }
      }
      continue;
    }

    // receiver is ready for file
    if (strncmp(buffer, "[FILE_READY]", 12) == 0)
    {
      char sender_name[50];

      if (sscanf(buffer + 12, "%49s", sender_name) == 1)
      {
        pthread_mutex_lock(&transfer_mutex);
        FileTransferSession *session = transfer_head;

        while (session != NULL)
        {
          if (session->receiver_sockfd == clisockfd &&
              session->active == 2 &&
              strcmp(session->sender_name, sender_name) == 0)
          {
            session->active = 3;

            if (session->ready_mutex &&
                session->ready_cond &&
                session->ready_flag_ptr)
            {
              pthread_mutex_lock(session->ready_mutex);
              *(session->ready_flag_ptr) = 1;
              pthread_cond_signal(session->ready_cond);
              pthread_mutex_unlock(session->ready_mutex);
            }
            break;
          }

          session = session->next;
        }
        pthread_mutex_unlock(&transfer_mutex);
      }
      continue;
    }

    // receiver rejects file
    if (strncmp(buffer, "[FILE_REJECT]", 13) == 0)
    {
      char sender_name[50];
      sscanf(buffer + 13, "%49s", sender_name);

      int sender_fd = find_user_socket(sender_name, assigned_room);

      if (sender_fd >= 0)
      {
        pthread_mutex_lock(&transfer_mutex);
        FileTransferSession *session = transfer_head;
        FileTransferSession *prev = NULL;

        while (session != NULL)
        {
          if (session->sender_sockfd == sender_fd &&
              session->receiver_sockfd == clisockfd)
          {
            if (prev)
              prev->next = session->next;
            else
              transfer_head = session->next;

            free(session);
            break;
          }

          prev = session;
          session = session->next;
        }
        pthread_mutex_unlock(&transfer_mutex);

        char reject_msg[200];
        snprintf(reject_msg, sizeof(reject_msg),
                 "[FILE_REJECTED] %s declined the file transfer\n", username);
        safe_send(sender_fd, reject_msg, strlen(reject_msg));
      }
      continue;
    }

    broadcast(clisockfd, buffer);
  }

  remove_client(clisockfd);
  print_clients();
  close(clisockfd);

  return NULL;
}

// server main: listens, accepts, spawns thread
int main(int argc, char *argv[])
{
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("ERROR opening socket");

  int opt = 1;
  setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

  struct sockaddr_in serv_addr;
  socklen_t slen = sizeof(serv_addr);
  memset((char *)&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  serv_addr.sin_port = htons(PORT_NUM);

  int status = bind(sockfd, (struct sockaddr *)&serv_addr, slen);
  if (status < 0)
    error("ERROR on binding");

  listen(sockfd, 5);

  printf("[SERVER] File transfer enabled chat server started on port %d\n", PORT_NUM);

  while (1)
  {
    struct sockaddr_in cli_addr;
    socklen_t clen = sizeof(cli_addr);
    int newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clen);

    if (newsockfd < 0)
    {
      perror("ERROR on accept");
      continue;
    }

    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(cli_addr.sin_addr), ip_str, INET_ADDRSTRLEN);
    printf("Connected: %s\n", ip_str);

    ThreadArgs *args = malloc(sizeof(ThreadArgs));
    if (!args)
    {
      close(newsockfd);
      continue;
    }

    args->clisockfd = newsockfd;

    pthread_t tid;
    if (pthread_create(&tid, NULL, thread_main, (void *)args) != 0)
    {
      free(args);
      close(newsockfd);
      continue;
    }
  }

  return 0;
}
