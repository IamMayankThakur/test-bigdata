#include <arpa/inet.h>  //inet_addr
#include <fcntl.h>      // for o_rdonly
#include <pthread.h>    //for threading , link with lpthread
#include <semaphore.h>  //semaphore init wait and post
#include <stdio.h>
#include <stdlib.h>  //strlen
#include <string.h>  //strlen
#include <sys/socket.h>
#include <unistd.h>  //write

#define BUF_LEN 4096
int *new_sock;
pthread_t t_producer;
pthread_t t_consumer;

sem_t buf_mutex, empty_count, fill_count;
char buff[BUF_LEN];
int socket_desc, client_sock, c;
struct sockaddr_in server, client;

int produce() {
  char filename[256];
  read(client_sock, filename, 256);
  read(client_sock, filename, 256);
  printf("###%s", filename);
  int fd = open(filename, O_RDONLY);
  if (fd < 0) {
    printf("File open error");
    return 1;
  }
  int nread = read(fd, buff, 4096);
  close(fd);
  printf("in producer@@");
}

int consume() {
  sleep(3);
  int sock = *(int *)new_sock;
  write(sock, buff, 4096);
  sleep(10);
  printf("In consumer %s", buff);
}

void *producer(void *args) {
  sem_wait(&empty_count);
  sem_wait(&buf_mutex);
  produce();
  sem_post(&buf_mutex);
  sem_post(&fill_count);
  return NULL;
}

void *consumer(void *args) {
  sem_wait(&fill_count);
  sem_wait(&buf_mutex);
  consume();
  sem_post(&buf_mutex);
  sem_post(&empty_count);
  return NULL;
}

int main(void) {
    // Create socket
    socket_desc = socket(AF_INET, SOCK_STREAM, 0);
    if (socket_desc == -1) {
      printf("Could not create socket");
    }
    puts("Socket created");

    // Prepare the sockaddr_in structure
    server.sin_family = AF_INET;
    server.sin_addr.s_addr = INADDR_ANY;
    server.sin_port = htons(5050);

    // Bind
    if (bind(socket_desc, (struct sockaddr *)&server, sizeof(server)) < 0) {
      // print the error message
      perror("bind failed. Error");
      return 1;
    }
    puts("bind done");

    // Listen

    for (size_t i = 0; i < BUF_LEN; i++) {
      buff[i] = 0;
    }

    listen(socket_desc, 3);
    c = sizeof(struct sockaddr_in);

    // Accept and incoming connection
    puts("Waiting for incoming connections...");
    client_sock =
        accept(socket_desc, (struct sockaddr *)&client, (socklen_t *)&c);
    new_sock = malloc(1);
    *new_sock = client_sock;
    int err;
    sem_init(&buf_mutex, 0, 1);
    sem_init(&fill_count, 0, 0);

    sem_init(&empty_count, 0, BUF_LEN);

    err = pthread_create(&t_producer, NULL, producer, NULL);
    if (err != 0) {
      printf("Error creating producer ");
    } else {
      printf("Successfully created producer ");
    }

    err = pthread_create(&t_consumer, NULL, consumer, NULL);
    if (err != 0) {
      printf("Error creating consumer ");
    } else {
      printf("Successfully created consumer ");
    }

    pthread_join((t_producer), NULL);
    pthread_join((t_consumer), NULL);

    close(socket_desc);
    close(client_sock);
}
