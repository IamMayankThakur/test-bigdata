#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#define LIST "list"
#define GET "get"

int main(int argc, char **argv) {
  if (argc == 1) {
    printf("Please enter valid command line arguments, list or get. \n");
    exit(0);
  }
  int sockfd = 0;
  int bytesReceived = 0;
  char recvBuff[256];
  memset(recvBuff, 0, sizeof(recvBuff));
  struct sockaddr_in serv_addr;

  /* Create a socket first */
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Error : Could not create socket \n");
    return 1;
  }

  /* Initialize sockaddr_in data structure */
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(5050);  // port
  serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

  /* Attempt a connection */
  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    printf("\n Error : Connect Failed \n");
    return 1;
  }
  if (strcmp(LIST, argv[1]) == 0) {
    write(sockfd, LIST, 256);
    while ((bytesReceived = read(sockfd, recvBuff, 256)) > 0) {
      // printf("Bytes received %d\n", bytesReceived);
      // recvBuff[n] = 0;
      sleep(1);
      printf("%s \n", recvBuff);
    }

    if (bytesReceived < 0) {
      printf("\n Read Error \n");
    }
    return 0;
  }
  /* Create file where data will be stored */
  if (strcmp(GET, argv[1]) == 0) {
    write(sockfd, GET, 256);
    sleep(1);
    write(sockfd, argv[2], 256);
    printf("@@%s", argv[2]);
    int fd = open(argv[2], O_CREAT | O_WRONLY);
    if (fd < 0) {
      printf("Error opening file");
      return 1;
    }

    /* Receive data in chunks of 256 bytes */
    while ((bytesReceived = read(sockfd, recvBuff, 256)) > 0) {
      printf("Bytes received %d\n", bytesReceived);
      // recvBuff[n] = 0;
      sleep(1);
      write(fd, recvBuff, bytesReceived);
      printf("%s \n", recvBuff);
    }

    if (bytesReceived < 0) {
      printf("\n Read Complete \n");
    }
  }
  // close(fd)
  return 0;
}