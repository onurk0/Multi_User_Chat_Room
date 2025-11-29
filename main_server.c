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

#define PORT_NUM 1004

void print_clients();
void remove_client(int clisockfd);

void error(const char *msg) {
  perror(msg);
  exit(1);
}

typedef struct _USR {
  int clisockfd;     // socket file descriptor
  struct _USR *next; // for linked list queue
  char name[50];     // array to hold name
} USR;

USR *head = NULL;
USR *tail = NULL;

// we need a mutex in order to prevent race condition so threads
// are not modifying the list at the same time
pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

void add_tail(int newclisockfd, const char *name) {

  // lock to prevent unintended behavior & race conditions
  pthread_mutex_lock(&clients_mutex);

  // allocate new node
  USR *newnode = malloc(sizeof(USR));

  // return error if malloc failed
  if (!newnode) {
    perror("malloc");
    pthread_mutex_unlock(&clients_mutex);
    return;
  }

  newnode->clisockfd = newclisockfd;
  strncpy(newnode->name, name, sizeof(newnode->name) - 1);
  newnode->name[sizeof(newnode->name) - 1] = '\0'; // safety null-terminator
  newnode->next = NULL;

  // empty list case
  if (head == NULL) {
    head = newnode;
    tail = newnode;
  } else {
    tail->next = newnode;
    tail = newnode;
  }

  // unlock to allow other threads to access
  pthread_mutex_unlock(&clients_mutex);
}

void print_clients() {
  pthread_mutex_lock(&clients_mutex);
  printf("Currently connected clients:\n");

  // traverse linked list starting from head
  // for each client, get their name and IP address
  // and print the information
  USR *cur = head;
  while (cur != NULL) {
    struct sockaddr_in cliaddr;
    socklen_t len = sizeof(cliaddr);

    if (getpeername(cur->clisockfd, (struct sockaddr *)&cliaddr, &len) == 0) {
      char ip_str[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &(cliaddr.sin_addr), ip_str, INET_ADDRSTRLEN);
      printf("%s (%s)\n", cur->name, ip_str);
    } else {

      printf("%s | unknown IP\n", cur->name);
    }
    cur = cur->next;
  }
  printf("---\n");
  pthread_mutex_unlock(&clients_mutex);
}

void remove_client(int clisockfd) {
  pthread_mutex_lock(&clients_mutex);
  // handle empty list
  if (head == NULL) {
    printf("List is empty. Nothing to delete\n");
    pthread_mutex_unlock(&clients_mutex);
    return;
  }
  USR *cur = head;
  USR *prev = NULL;

  // handle removing head
  if (head->clisockfd == clisockfd) {
    cur = head;
    head = head->next;

    // if list becomes empty, fix tail
    if (head == NULL) {
      tail = NULL;
    }
    free(cur);
    pthread_mutex_unlock(&clients_mutex);
    return;
  }

  // handle removing from middle or tail
  while (cur != NULL) {
    if (cur->clisockfd == clisockfd) {
      // unlink
      prev->next = cur->next;

      // tail is being removed
      if (cur == tail)
        tail = prev;

      free(cur);
      pthread_mutex_unlock(&clients_mutex);
      return;
    }
    prev = cur;
    cur = cur->next;
  }
  // if this prints, then client is not found
  printf("Client FD %d not in list.\n", clisockfd);
}

void broadcast(int fromfd, char *message) {
  // figure out sender address
  pthread_mutex_lock(&clients_mutex);

  char sender_name[50] = "Unknown";
  struct sockaddr_in cliaddr;
  socklen_t clen = sizeof(cliaddr);
  if (getpeername(fromfd, (struct sockaddr *)&cliaddr, &clen) < 0)
    error("ERROR Unknown sender!");

  USR *cur = head;
  // TODO: Traverse the list to find the USR with clisockfd == fromfd
  // TODO: Copy their name to sender_name
  // TODO: Get their IP with getpeername()
  while (cur != NULL) {
    if (cur->clisockfd == fromfd) {
      strncpy(sender_name, cur->name, sizeof(cur->name) - 1);
      sender_name[sizeof(sender_name) - 1] = '\0';
      break;
    }
    cur = cur->next;
  }

  // reset cur to head
  cur = head;
  while (cur != NULL) {

    // check if cur is not the one who sent the message
    if (cur->clisockfd != fromfd) {
      char buffer[512];

      // prepare message in thread-safe manner
      char ip_str[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &(cliaddr.sin_addr), ip_str, INET_ADDRSTRLEN);
      sprintf(buffer, "[%s (%s)] %s", sender_name, ip_str, message);
      int nmsg = strlen(buffer);

      // send!
      int nsen = send(cur->clisockfd, buffer, nmsg, 0);
      if (nsen != nmsg)
        error("ERROR send() failed");
    }
    cur = cur->next;
  }
  pthread_mutex_unlock(&clients_mutex);
}

void broadcast_all(char *message) {
  pthread_mutex_lock(&clients_mutex);
  USR *cur = head;
  while (cur != NULL) {
    int nsen = send(cur->clisockfd, message, strlen(message), 0);
    if (nsen != strlen(message))
      error("ERROR send() failed");
    cur = cur->next;
  }
  pthread_mutex_unlock(&clients_mutex);
}

typedef struct _ThreadArgs {
  int clisockfd;
} ThreadArgs;

void *thread_main(void *args) {
  pthread_detach(pthread_self());
  int clisockfd = ((ThreadArgs *)args)->clisockfd;
  free(args);
  char username[50];

  char buffer[256];
  int nrcv;

  // receive username
  nrcv = recv(clisockfd, buffer, 255, 0);
  // close if connection failed
  if (nrcv <= 0) {
    close(clisockfd);
    return NULL;
  }

  buffer[nrcv] = '\0'; // null terminate what we received
  // copy to username, removing newline if present
  strncpy(username, buffer, sizeof(username) - 1);
  username[sizeof(username) - 1] = '\0';

  // remove trailing newline
  size_t len = strlen(username);
  if (len > 0 && username[len - 1] == '\n') {
    username[len - 1] = '\0';
  }

  add_tail(clisockfd, username);
  print_clients();

  // Get client's IP address
  struct sockaddr_in cliaddr;
  socklen_t clen = sizeof(cliaddr);
  if (getpeername(clisockfd, (struct sockaddr *)&cliaddr, &clen) == 0) {
    char join_msg[512];
    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(cliaddr.sin_addr), ip_str, INET_ADDRSTRLEN);
    sprintf(join_msg, "%s (%s) joined the chat room!\n", username, ip_str);
    broadcast_all(join_msg);
  }

  // receive and broadcast messages
  while (1) {
    memset(buffer, 0, 256);
    nrcv = recv(clisockfd, buffer, 255, 0);

    if (nrcv < 0)
      error("ERROR recv() failed");
    if (nrcv == 0) {
      char leave_msg[512];
      sprintf(leave_msg, "%s (%s) left the room!\n", username,
              inet_ntoa(cliaddr.sin_addr));
      broadcast_all(leave_msg);
      break;
    }

    broadcast(clisockfd, buffer);
  }

  remove_client(clisockfd);
  print_clients();
  close(clisockfd);

  return NULL;
}

int main(int argc, char *argv[]) {
  int sockfd = socket(AF_INET, SOCK_STREAM, 0);
  if (sockfd < 0)
    error("ERROR opening socket");

  struct sockaddr_in serv_addr;
  socklen_t slen = sizeof(serv_addr);
  memset((char *)&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = INADDR_ANY;
  // serv_addr.sin_addr.s_addr = inet_addr("192.168.1.171");
  serv_addr.sin_port = htons(PORT_NUM);

  int status = bind(sockfd, (struct sockaddr *)&serv_addr, slen);
  if (status < 0)
    error("ERROR on binding");

  listen(sockfd, 5); // maximum number of connections = 5

  while (1) {
    struct sockaddr_in cli_addr;
    socklen_t clen = sizeof(cli_addr);
    int newsockfd = accept(sockfd, (struct sockaddr *)&cli_addr, &clen);
    if (newsockfd < 0)
      error("ERROR on accept");

    char ip_str[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &(cli_addr.sin_addr), ip_str, INET_ADDRSTRLEN);
    printf("Connected: %s\n", ip_str);
    print_clients();

    // prepare ThreadArgs structure to pass client socket
    ThreadArgs *args = (ThreadArgs *)malloc(sizeof(ThreadArgs));
    if (args == NULL)
      error("ERROR creating thread argument");

    args->clisockfd = newsockfd;

    pthread_t tid;
    if (pthread_create(&tid, NULL, thread_main, (void *)args) != 0)
      error("ERROR creating a new thread");
  }

  return 0;
}
