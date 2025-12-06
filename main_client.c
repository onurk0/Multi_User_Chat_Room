#include <arpa/inet.h>
#include <bits/pthreadtypes.h>
#include <bits/types/struct_iovec.h>
#include <errno.h>
#include <libgen.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

// CONSTANTS
#define PORT_NUM 1004
#define BUFFER_SIZE 4096

// color definitions
const char *COLORS[] = {
    "\033[31m", "\033[32m", "\033[33m", "\033[34m", "\033[35m", "\033[36m",
};
#define NUM_COLORS 6
#define COLOR_RESET "\033[0m"

// function prototypes
int extract_username(const char *message, char *username_out);
const char *get_user_color(const char *username);
int hash_username(const char *username);
int is_send_command(const char *message, char *recipient, char *filename);

// username-color mapping
typedef struct {
  char username[50];
  int color_index;
} UserColor;

// file receiving thread arguments
typedef struct {
  int sockfd;
  char filename[256];
  long filesize;
  char sender_name[50];
} FileRecvArgs;

// global color tracking
UserColor user_colors[10];
int num_users = 0;
pthread_mutex_t color_mutex = PTHREAD_MUTEX_INITIALIZER;

// global socket
int global_sockfd = -1;
pthread_mutex_t sockfd_mutex = PTHREAD_MUTEX_INITIALIZER;

// file-transfer state
int file_transfer_accepted = 0;
int file_transfer_go = 0;
pthread_mutex_t transfer_ready_mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t transfer_ready_cond = PTHREAD_COND_INITIALIZER;

// pause receiving during file transfer
int receiving_file = 0;
pthread_mutex_t recv_pause_mutex = PTHREAD_MUTEX_INITIALIZER;

// connection status
volatile int connection_alive = 1;
pthread_mutex_t connection_mutex = PTHREAD_MUTEX_INITIALIZER;

// simple error exit wrapper
void error(const char *msg) {
  perror(msg);
  exit(0);
}

typedef struct _ThreadArgs {
  int clisockfd;
} ThreadArgs;

// safe send (handles short writes)
int safe_send(int sockfd, const void *buf, size_t len) {
  size_t total_sent = 0;
  const char *ptr = (const char *)buf;

  while (total_sent < len) {
    int result = send(sockfd, ptr + total_sent, len - total_sent, MSG_NOSIGNAL);
    if (result < 0) {
      if (errno == EAGAIN || errno == EWOULDBLOCK) {
        usleep(1000);
        continue;
      }
      if (errno != EPIPE && errno != ECONNRESET) {
        perror("send");
      }
      pthread_mutex_lock(&connection_mutex);
      connection_alive = 0;
      pthread_mutex_unlock(&connection_mutex);
      return -1;
    }
    if (result == 0) {
      pthread_mutex_lock(&connection_mutex);
      connection_alive = 0;
      pthread_mutex_unlock(&connection_mutex);
      return -1;
    }
    total_sent += result;
  }
  return (int)total_sent;
}

// safe recv wrapper (tracks disconnects)
int safe_recv(int sockfd, void *buf, size_t len, int flags) {
  int result = recv(sockfd, buf, len, flags);
  if (result <= 0) {
    if (result < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
      perror("recv");
    }
    if (result == 0) {
      printf("\n[Disconnected from server]\n");
    }
    pthread_mutex_lock(&connection_mutex);
    connection_alive = 0;
    pthread_mutex_unlock(&connection_mutex);
  }
  return result;
}

// hash username deterministically for color assignment
int hash_username(const char *username) {
  int hash = 0;
  for (int i = 0; username[i] != '\0'; i++) {
    hash += username[i];
  }
  return hash;
}

// extract username from "[name (room)] msg"
int extract_username(const char *message, char *username_out) {
  const char *start = strchr(message, '[');
  if (!start)
    return 0;
  start++;

  const char *end = strchr(start, '(');
  if (!end)
    return 0;

  size_t len = end - start;
  if (len <= 0 || len >= 50)
    return 0;

  strncpy(username_out, start, len);
  username_out[len] = '\0';

  for (int i = strlen(username_out) - 1; i >= 0; i--) {
    if (username_out[i] == ' ')
      username_out[i] = '\0';
    else
      break;
  }

  return 1;
}

// returns color for username, assigns new color if needed
const char *get_user_color(const char *username) {
  pthread_mutex_lock(&color_mutex);

  for (int i = 0; i < num_users; i++) {
    if (strcmp(user_colors[i].username, username) == 0) {
      const char *color = COLORS[user_colors[i].color_index];
      pthread_mutex_unlock(&color_mutex);
      return color;
    }
  }

  if (num_users < 10) {
    strncpy(user_colors[num_users].username, username, 49);
    user_colors[num_users].username[49] = '\0';
    user_colors[num_users].color_index = hash_username(username) % NUM_COLORS;
    const char *assigned = COLORS[user_colors[num_users].color_index];
    num_users++;
    pthread_mutex_unlock(&color_mutex);
    return assigned;
  }

  pthread_mutex_unlock(&color_mutex);
  return COLORS[0];
}

// detects SEND <user> <file>
int is_send_command(const char *message, char *recipient, char *filename) {
  char temp[512];
  strncpy(temp, message, sizeof(temp) - 1);
  temp[sizeof(temp) - 1] = '\0';

  size_t len = strlen(temp);
  if (len > 0 && temp[len - 1] == '\n')
    temp[len - 1] = '\0';

  if (strncmp(temp, "SEND ", 5) != 0)
    return 0;

  char *token = strtok(temp + 5, " ");
  if (!token)
    return 0;
  strncpy(recipient, token, 49);
  recipient[49] = '\0';

  token = strtok(NULL, " ");
  if (!token)
    return 0;
  strncpy(filename, token, 255);
  filename[255] = '\0';

  return 1;
}

// thread that receives raw file bytes
void *thread_file_recv(void *arg) {
  pthread_detach(pthread_self());
  FileRecvArgs *args = (FileRecvArgs *)arg;

  int sockfd = args->sockfd;
  const char *filename = args->filename;
  long filesize = args->filesize;

  char output_filename[300];
  snprintf(output_filename, sizeof(output_filename), "received_%s", filename);

  FILE *fp = fopen(output_filename, "wb");
  if (!fp) {
    perror("Error opening file for writing");
    pthread_mutex_lock(&recv_pause_mutex);
    receiving_file = 0;
    pthread_mutex_unlock(&recv_pause_mutex);
    free(args);
    return NULL;
  }

  printf("\n[FILE TRANSFER] Receiving file: %s (%ld bytes)\n", filename,
         filesize);

  char buffer[BUFFER_SIZE];
  long total_received = 0;

  while (total_received < filesize) {
    pthread_mutex_lock(&connection_mutex);
    int alive = connection_alive;
    pthread_mutex_unlock(&connection_mutex);

    if (!alive) {
      printf("\n[FILE TRANSFER] Connection lost during transfer\n");
      break;
    }

    long to_receive = filesize - total_received;
    if (to_receive > BUFFER_SIZE)
      to_receive = BUFFER_SIZE;

    int n = safe_recv(sockfd, buffer, to_receive, 0);
    if (n <= 0) {
      printf("\n[FILE TRANSFER] Transfer interrupted!\n");
      fclose(fp);
      unlink(output_filename);
      pthread_mutex_lock(&recv_pause_mutex);
      receiving_file = 0;
      pthread_mutex_unlock(&recv_pause_mutex);
      free(args);
      return NULL;
    }

    fwrite(buffer, 1, n, fp);
    total_received += n;

    if (filesize > 0) {
      int percent = (total_received * 100) / filesize;
      printf("\r[FILE TRANSFER] Progress: %d%%", percent);
      fflush(stdout);
    }
  }

  printf("\n[FILE TRANSFER] File received successfully: %s\n", output_filename);
  fclose(fp);

  pthread_mutex_lock(&recv_pause_mutex);
  receiving_file = 0;
  pthread_mutex_unlock(&recv_pause_mutex);

  free(args);
  return NULL;
}

// main receiving thread (messages + file control)
void *thread_main_recv(void *args) {
  pthread_detach(pthread_self());
  int sockfd = ((ThreadArgs *)args)->clisockfd;
  free(args);

  char buffer[512];
  int n;

  while (1) {
    pthread_mutex_lock(&connection_mutex);
    int alive = connection_alive;
    pthread_mutex_unlock(&connection_mutex);

    if (!alive) {
      break;
    }

    pthread_mutex_lock(&recv_pause_mutex);
    int paused = receiving_file;
    pthread_mutex_unlock(&recv_pause_mutex);

    if (paused) {
      usleep(100000);
      continue;
    }

    memset(buffer, 0, 512);
    n = safe_recv(sockfd, buffer, 512, 0);

    if (n <= 0)
      break;

    // handle control signals
    if (strncmp(buffer, "[FILE_REQUEST]", 14) == 0) {
      char sender[50], filename[256];
      long filesize;

      if (sscanf(buffer + 14, "%49s %255s %ld", sender, filename, &filesize) ==
          3) {
        printf("\n[FILE TRANSFER] %s wants to send you: %s (%ld bytes)\n",
               sender, filename, filesize);
        printf("Accept? (Y/N): ");
        fflush(stdout);

        char response[10];
        if (fgets(response, sizeof(response), stdin) == NULL) {
          break;
        }

        if (response[0] == 'Y' || response[0] == 'y') {
          char accept_msg[350];
          snprintf(accept_msg, sizeof(accept_msg), "[FILE_ACCEPT] %s %s %ld",
                   sender, filename, filesize);
          if (safe_send(sockfd, accept_msg, strlen(accept_msg)) < 0)
            break;
          printf("[FILE TRANSFER] Waiting to receive file...\n");
        } else {
          char reject_msg[100];
          snprintf(reject_msg, sizeof(reject_msg), "[FILE_REJECT] %s", sender);
          if (safe_send(sockfd, reject_msg, strlen(reject_msg)) < 0)
            break;
          printf("[FILE TRANSFER] Transfer declined.\n");
        }
      }
      continue;
    }

    if (strncmp(buffer, "[FILE_START]", 12) == 0) {
      char filename_buf[256];
      long filesize;
      char sender_name[50];

      if (sscanf(buffer + 12, "%255s %ld %49s", filename_buf, &filesize,
                 sender_name) == 3) {
        pthread_mutex_lock(&recv_pause_mutex);
        receiving_file = 1;
        pthread_mutex_unlock(&recv_pause_mutex);

        FileRecvArgs *recv_args = (FileRecvArgs *)malloc(sizeof(FileRecvArgs));
        if (!recv_args) {
          perror("malloc for file args");
          pthread_mutex_lock(&recv_pause_mutex);
          receiving_file = 0;
          pthread_mutex_unlock(&recv_pause_mutex);
          continue;
        }
        recv_args->sockfd = sockfd;
        strncpy(recv_args->filename, filename_buf,
                sizeof(recv_args->filename) - 1);
        recv_args->filename[sizeof(recv_args->filename) - 1] = '\0';
        recv_args->filesize = filesize;
        strncpy(recv_args->sender_name, sender_name,
                sizeof(recv_args->sender_name) - 1);
        recv_args->sender_name[sizeof(recv_args->sender_name) - 1] = '\0';

        char ready_msg[100];
        snprintf(ready_msg, sizeof(ready_msg), "[FILE_READY] %s", sender_name);
        if (safe_send(sockfd, ready_msg, strlen(ready_msg)) < 0) {
          free(recv_args);
          pthread_mutex_lock(&recv_pause_mutex);
          receiving_file = 0;
          pthread_mutex_unlock(&recv_pause_mutex);
          break;
        }

        pthread_t file_tid;
        if (pthread_create(&file_tid, NULL, thread_file_recv,
                           (void *)recv_args) != 0) {
          perror("ERROR creating file receive thread");
          free(recv_args);
          pthread_mutex_lock(&recv_pause_mutex);
          receiving_file = 0;
          pthread_mutex_unlock(&recv_pause_mutex);
        }
      }
      continue;
    }

    if (strncmp(buffer, "[FILE_ACCEPTED]", 15) == 0) {
      printf("\n%s", buffer + 15);

      pthread_mutex_lock(&transfer_ready_mutex);
      file_transfer_accepted = 1;
      pthread_cond_signal(&transfer_ready_cond);
      pthread_mutex_unlock(&transfer_ready_mutex);
      continue;
    }

    if (strncmp(buffer, "[FILE_GO]", 9) == 0) {
      pthread_mutex_lock(&transfer_ready_mutex);
      file_transfer_go = 1;
      pthread_cond_signal(&transfer_ready_cond);
      pthread_mutex_unlock(&transfer_ready_mutex);
      continue;
    }

    if (strncmp(buffer, "[FILE_REJECTED]", 15) == 0) {
      printf("\n%s", buffer + 15);

      pthread_mutex_lock(&transfer_ready_mutex);
      file_transfer_accepted = -1;
      pthread_cond_signal(&transfer_ready_cond);
      pthread_mutex_unlock(&transfer_ready_mutex);
      continue;
    }

    if (strncmp(buffer, "[FILE_ERROR]", 12) == 0) {
      printf("\n%s", buffer + 12);

      pthread_mutex_lock(&transfer_ready_mutex);
      file_transfer_accepted = -1;
      pthread_cond_signal(&transfer_ready_cond);
      pthread_mutex_unlock(&transfer_ready_mutex);
      continue;
    }

    // regular chat message
    char username[50];
    extract_username(buffer, username);
    if (extract_username(buffer, username)) {
      const char *color = get_user_color(username);

      size_t len = strlen(buffer);
      if (len > 0 && buffer[len - 1] == '\n')
        buffer[len - 1] = '\0';

      printf("%s%s%s\n", color, buffer, COLOR_RESET);
    } else {
      printf("%s", buffer);
    }
  }

  pthread_mutex_lock(&connection_mutex);
  connection_alive = 0;
  pthread_mutex_unlock(&connection_mutex);

  return NULL;
}

// sending thread (user → server)
void *thread_main_send(void *args) {
  pthread_detach(pthread_self());
  int sockfd = ((ThreadArgs *)args)->clisockfd;
  free(args);

  char buffer[256];
  int n;

  while (1) {
    pthread_mutex_lock(&connection_mutex);
    int alive = connection_alive;
    pthread_mutex_unlock(&connection_mutex);

    if (!alive) {
      break;
    }

    memset(buffer, 0, 256);

    fd_set readfds;
    struct timeval tv;
    FD_ZERO(&readfds);
    FD_SET(STDIN_FILENO, &readfds);
    tv.tv_sec = 0;
    tv.tv_usec = 500000;

    int ready = select(STDIN_FILENO + 1, &readfds, NULL, NULL, &tv);
    if (ready < 0) {
      perror("select");
      break;
    }
    if (ready == 0) {
      continue;
    }

    if (fgets(buffer, 255, stdin) == NULL) {
      break;
    }

    if (strlen(buffer) == 1)
      buffer[0] = '\0';

    char recipient[50], filename[256];
    if (is_send_command(buffer, recipient, filename)) {
      FILE *fp = fopen(filename, "rb");
      if (!fp) {
        printf("[ERROR] Cannot open file: %s\n", filename);
        continue;
      }

      fseek(fp, 0, SEEK_END);
      long filesize = ftell(fp);
      fseek(fp, 0, SEEK_SET);

      char *base_filename = basename(filename);

      char init_msg[512];
      snprintf(init_msg, sizeof(init_msg), "[FILE_SEND] %s %s %ld", recipient,
               base_filename, filesize);

      n = safe_send(sockfd, init_msg, strlen(init_msg));

      if (n < 0) {
        fclose(fp);
        break;
      }

      printf("[FILE TRANSFER] Waiting for %s to accept...\n", recipient);

      pthread_mutex_lock(&transfer_ready_mutex);
      file_transfer_accepted = 0;

      struct timespec ts;
      clock_gettime(CLOCK_REALTIME, &ts);
      ts.tv_sec += 60;

      while (file_transfer_accepted == 0) {
        pthread_mutex_lock(&connection_mutex);
        int alive = connection_alive;
        pthread_mutex_unlock(&connection_mutex);

        if (!alive) {
          pthread_mutex_unlock(&transfer_ready_mutex);
          fclose(fp);
          printf("\n[FILE TRANSFER] Connection lost\n");
          goto exit_send_thread;
        }

        int result = pthread_cond_timedwait(&transfer_ready_cond,
                                            &transfer_ready_mutex, &ts);
        if (result != 0) {
          printf("\n[FILE TRANSFER] Timeout waiting for response\n");
          pthread_mutex_unlock(&transfer_ready_mutex);
          fclose(fp);
          goto next_iteration;
        }
      }

      int accepted = file_transfer_accepted;
      pthread_mutex_unlock(&transfer_ready_mutex);

      if (accepted != 1) {
        printf("[FILE TRANSFER] Transfer was not accepted\n");
        fclose(fp);
        continue;
      }

      printf("[FILE TRANSFER] Waiting for receiver to be ready...\n");

      pthread_mutex_lock(&transfer_ready_mutex);
      file_transfer_go = 0;

      struct timespec ts2;
      clock_gettime(CLOCK_REALTIME, &ts2);
      ts2.tv_sec += 30;

      while (file_transfer_go == 0) {
        pthread_mutex_lock(&connection_mutex);
        int alive = connection_alive;
        pthread_mutex_unlock(&connection_mutex);

        if (!alive) {
          pthread_mutex_unlock(&transfer_ready_mutex);
          fclose(fp);
          printf("\n[FILE TRANSFER] Connection lost\n");
          goto exit_send_thread;
        }

        int result = pthread_cond_timedwait(&transfer_ready_cond,
                                            &transfer_ready_mutex, &ts2);
        if (result != 0) {
          printf(
              "\n[FILE TRANSFER] Timeout waiting for receiver to be ready\n");
          pthread_mutex_unlock(&transfer_ready_mutex);
          fclose(fp);
          goto next_iteration;
        }
      }
      pthread_mutex_unlock(&transfer_ready_mutex);

      printf("[FILE TRANSFER] Sending file...\n");

      char file_buffer[BUFFER_SIZE];
      long total_sent = 0;

      while (total_sent < filesize) {
        pthread_mutex_lock(&connection_mutex);
        int alive = connection_alive;
        pthread_mutex_unlock(&connection_mutex);

        if (!alive) {
          printf("\n[FILE TRANSFER] Connection lost during transfer\n");
          fclose(fp);
          goto exit_send_thread;
        }

        size_t to_read = BUFFER_SIZE;
        if (filesize - total_sent < BUFFER_SIZE)
          to_read = filesize - total_sent;

        size_t bytes_read = fread(file_buffer, 1, to_read, fp);
        if (bytes_read <= 0)
          break;

        n = safe_send(sockfd, file_buffer, bytes_read);
        if (n < 0) {
          printf("\n[FILE TRANSFER] Send failed\n");
          break;
        }

        total_sent += n;

        if (filesize > 0) {
          int percent = (total_sent * 100) / filesize;
          printf("\r[FILE TRANSFER] Sent: %d%%", percent);
          fflush(stdout);
        }
      }

      fclose(fp);
      printf("\n[FILE TRANSFER] File sent successfully: %ld bytes\n",
             total_sent);

    next_iteration:
      continue;
    }

    n = safe_send(sockfd, buffer, strlen(buffer));

    if (n <= 0)
      break;
  }

exit_send_thread:
  pthread_mutex_lock(&connection_mutex);
  connection_alive = 0;
  pthread_mutex_unlock(&connection_mutex);

  return NULL;
}

// main program: connect, choose room, launch threads
int main(int argc, char *argv[]) {
  char name[100];
  char room_request[50];

  if (argc < 2)
    error("Usage: ./main_client <IP> [room_number or 'new']");

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("ERROR opening socket");

  pthread_mutex_lock(&sockfd_mutex);
  global_sockfd = sockfd;
  pthread_mutex_unlock(&sockfd_mutex);

  struct sockaddr_in serv_addr;
  socklen_t slen = sizeof(serv_addr);
  memset((char *)&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(argv[1]);
  serv_addr.sin_port = htons(PORT_NUM);

  printf("Try connecting to %s...\n", inet_ntoa(serv_addr.sin_addr));

  int status = connect(sockfd, (struct sockaddr *)&serv_addr, slen);
  if (status < 0)
    error("ERROR connecting");

  if (argc == 3) {
    strncpy(room_request, argv[2], sizeof(room_request) - 1);
    room_request[sizeof(room_request) - 1] = '\0';
    if (safe_send(sockfd, room_request, strlen(room_request)) < 0)
      error("ERROR sending room request");
  } else {
    if (safe_send(sockfd, "list", 4) < 0)
      error("ERROR sending list request");
  }

  char room_response[512];
  int n = safe_recv(sockfd, room_response, 511, 0);
  if (n <= 0)
    error("ERROR receiving room assignment");
  room_response[n] = '\0';

  printf("%s\n", room_response);

  if (argc == 2) {
    if (strstr(room_response, "Created new room") == NULL) {
      printf("Choose the room number or type [new] to create a new room: ");
      if (fgets(room_request, sizeof(room_request), stdin) == NULL)
        error("ERROR reading room choice");

      size_t len = strlen(room_request);
      if (len > 0 && room_request[len - 1] == '\n')
        room_request[len - 1] = '\0';

      if (safe_send(sockfd, room_request, strlen(room_request)) < 0)
        error("ERROR sending room choice");

      n = safe_recv(sockfd, room_response, 511, 0);
      if (n <= 0)
        error("ERROR receiving room assignment");
      room_response[n] = '\0';
      printf("%s\n", room_response);
    }
  }

  printf("Type your user name:\n");
  if (fgets(name, 99, stdin) == NULL)
    error("ERROR reading username");

  if (safe_send(sockfd, name, strlen(name)) < 0)
    error("ERROR sending username");

  pthread_t tid1;
  pthread_t tid2;

  ThreadArgs *args;

  args = (ThreadArgs *)malloc(sizeof(ThreadArgs));
  if (!args)
    error("ERROR allocating thread args");
  args->clisockfd = sockfd;
  pthread_create(&tid1, NULL, thread_main_send, (void *)args);

  args = (ThreadArgs *)malloc(sizeof(ThreadArgs));
  if (!args)
    error("ERROR allocating thread args");
  args->clisockfd = sockfd;
  pthread_create(&tid2, NULL, thread_main_recv, (void *)args);

  pthread_join(tid1, NULL);
  pthread_join(tid2, NULL);

  printf("\nDisconnected. Exiting...\n");
  close(sockfd);

  return 0;
}
