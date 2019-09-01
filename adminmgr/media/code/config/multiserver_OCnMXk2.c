#include <arpa/inet.h>  //inet_addr
#include <pthread.h>    //for threading , link with lpthread
#include <stdio.h>
#include <stdlib.h>  //strlen
#include <string.h>  //strlen
#include <sys/socket.h>
#include <unistd.h>  //write
#include <semaphore.h> //semaphore init wait and post
#include <fcntl.h> // for o_rdonly

#define BUF_LEN 4096
int * new_sock;
pthread_t t_producer;
pthread_t t_consumer;

sem_t buf_mutex,empty_count,fill_count;
char buff[BUF_LEN];

int produce(){
  // TODO:  Use open() system call to open the file and write it to a character buffer "buff"
}

int consume(){
    sleep(3);
    int sock = *(int *)new_sock;
    write(sock, buff, BUF_LEN);
    printf("In consumer %s",buff);
    // TODO:  Read from the buffer written in produce() function and send it to client using write() system call, using the "sock" file descriptor.
}

void* producer(void *args){
  // TODO:  Use the three semaphores, use sem_wait() and sem_post()
  produce();
  return NULL;
}


void* consumer(void *args){
  // TODO:  Use the three semaphores, use sem_wait() and sem_post()
  consume();
  return NULL;
}

int main(void){
	int socket_desc, client_sock, c;
  struct sockaddr_in server, client;

  // Create socket
  socket_desc = socket(AF_INET, SOCK_STREAM, 0);
  if (socket_desc == -1) {
    printf("Could not create socket");
  }
  puts("Socket created");

  // Prepare the sockaddr_in structure
  server.sin_family = AF_INET;
  server.sin_addr.s_addr = INADDR_ANY;
  server.sin_port = htons(5052);

  // Bind
  if (bind(socket_desc, (struct sockaddr *)&server, sizeof(server)) < 0) {
    // print the error message
    perror("bind failed. Error");
    return 1;
  }
  puts("bind done");

  // Listen
  listen(socket_desc, 3);
  c = sizeof(struct sockaddr_in);

  // Accept and incoming connection
  puts("Waiting for incoming connections...");
  client_sock = accept(socket_desc, (struct sockaddr *)&client,
                               (socklen_t *)&c);
    new_sock = malloc(1);
    *new_sock = client_sock;
	sem_init(&buf_mutex,0,1);
	sem_init(&fill_count,0,0);

	sem_init(&empty_count,0,BUF_LEN);

    // TODO: Create 2 threads t_producer and t_consumer, with producer() and consumer() as their callback functions.
    // TODO: Join the 2 threads

    close(socket_desc);
    close(client_sock);
	return 0;
}

