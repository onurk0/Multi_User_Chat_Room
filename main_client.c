#include <arpa/inet.h>
#include <bits/pthreadtypes.h>
#include <bits/types/struct_iovec.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

// CONSTANTS
#define PORT_NUM 1004

// color definitions
// ANSI escape codes to add color in terminal
const char *COLORS[] = {
    "\033[31m", // Red
    "\033[32m", // Green
    "\033[33m", // Yellow
    "\033[34m", // Blue
    "\033[35m", // Purple
    "\033[36m", // Cyan
};
#define NUM_COLORS 6
#define COLOR_RESET "\033[0m"

// function prototypes
int extract_username(const char *message, char *username_out);
const char *get_user_color(const char *username);
int hash_username(const char *username);

// struct to store username-to-color mapping
typedef struct {
  char username[50];
  int color_index; // index into COLORS array
} UserColor;

// global array to track user user colors
UserColor user_colors[10];
int num_users = 0; // track how many users we've seen so far

// use mutex to protect the array
pthread_mutex_t color_mutex = PTHREAD_MUTEX_INITIALIZER;

void error(const char *msg) {
  perror(msg);
  exit(0);
}

typedef struct _ThreadArgs {
  int clisockfd;
} ThreadArgs;

// Simple hash function for usernames
int hash_username(const char *username) {
  int hash = 0;
  for (int i = 0; username[i] != '\0'; i++) {
    hash += username[i];
  }
  return hash;
}

// Returns 1 if username extracted successfully, 0 otherwise
int extract_username(const char *message, char *username_out) {

  // Copy the text between '[' and '(' to username_out
  const char *start = strchr(message, '[');
  if (!start)
    return 0;
  start++; // Move past '['

  const char *end = strchr(start, '(');
  if (!end)
    return 0;

  // Copy characters from 'start' to 'end' into username_out
  // and trim any space Null-terminate
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

// Returns the color string for this user
const char *get_user_color(const char *username) {
  pthread_mutex_lock(&color_mutex);

  // search existing users
  for (int i = 0; i < num_users; i++) {
    if (strcmp(user_colors[i].username, username) == 0) {
      const char *color = COLORS[user_colors[i].color_index];
      pthread_mutex_unlock(&color_mutex);
      return color;
    }
  }
  // user not found - assign new color
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

void *thread_main_recv(void *args) {
  pthread_detach(pthread_self());
  int sockfd = ((ThreadArgs *)args)->clisockfd;
  free(args);

  char buffer[512];
  int n;

  while (1) {
    memset(buffer, 0, 512);
    n = recv(sockfd, buffer, 512, 0);

    if (n < 0)
      error("ERROR recv() failed");
    if (n == 0)
      break;

    // TODO: extract username from buffer
    char username[50];
    extract_username(buffer, username);
    if (extract_username(buffer, username)) {
      // TODO: get color for this user
      const char *color = get_user_color(username);

      // remove trailing new line if present
      size_t len = strlen(buffer);
      if (len > 0 && buffer[len - 1] == '\n') {
        buffer[len - 1] = '\0';
      }

      // print message with color
      printf("%s%s%s\n", color, buffer, COLOR_RESET);
    } else {
      // Not a user message (i.e "joined" or "left"), print normally
      printf("%s", buffer);
    }
  }
  return NULL;
}

void *thread_main_send(void *args) {
  pthread_detach(pthread_self());
  int sockfd = ((ThreadArgs *)args)->clisockfd;
  free(args);

  char buffer[256];
  int n;

  while (1) {
    memset(buffer, 0, 256);
    fgets(buffer, 255, stdin);

    if (strlen(buffer) == 1)
      buffer[0] = '\0';

    n = send(sockfd, buffer, strlen(buffer), 0);

    if (n < 0)
      error("ERROR writing to socket");
    if (n == 0)
      break;
  }

  return NULL;
}

int main(int argc, char *argv[]) {
  char name[100];
  if (argc < 2)
    error("Please speicify hostname");

  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("ERROR opening socket");

  struct sockaddr_in serv_addr;
  socklen_t slen = sizeof(serv_addr);
  memset((char *)&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(argv[1]);
  serv_addr.sin_port = htons(PORT_NUM);

  printf("Try connecting to %s...\n", inet_ntoa(serv_addr.sin_addr));

  // get user's name (up to 99 characters plus null terminator)
  printf("Type your user name:\n");
  fgets(name, 99, stdin);

  int status = connect(sockfd, (struct sockaddr *)&serv_addr, slen);
  if (status < 0)
    error("ERROR connecting");
  send(sockfd, name, strlen(name), 0);

  pthread_t tid1;
  pthread_t tid2;

  ThreadArgs *args;

  args = (ThreadArgs *)malloc(sizeof(ThreadArgs));
  args->clisockfd = sockfd;
  pthread_create(&tid1, NULL, thread_main_send, (void *)args);

  args = (ThreadArgs *)malloc(sizeof(ThreadArgs));
  args->clisockfd = sockfd;
  pthread_create(&tid2, NULL, thread_main_recv, (void *)args);

  // parent will wait for sender to finish (= user stop sending message and
  // disconnect from server)
  pthread_join(tid1, NULL);

  close(sockfd);

  return 0;
}
