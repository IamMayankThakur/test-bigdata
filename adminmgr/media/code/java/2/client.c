#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>

#define LIST "list"
#define GET "get"

int main(int argc, char **argv) {
  int sockfd = 0;
  int bytesReceived = 0;
  char recvBuff[256];
  memset(recvBuff, '0', sizeof(recvBuff));
  struct sockaddr_in serv_addr;

  /* Create a socket first */
  if ((sockfd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    printf("\n Error : Could not create socket \n");
    return 1;
  }

  /* Initialize sockaddr_in data structure */
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(5052);  // port
  serv_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

  /* Attempt a connection */
  if (connect(sockfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr)) < 0) {
    printf("\n Error : Connect Failed \n");
    return 1;
  }

  if (strcmp(LIST, argv[1]) == 0)
  {
    // TODO:  Send LIST to the server using the write() system call using the "sockfd" file descriptor
    // TODO:  Use read() system call within a loop to read from the server using the "sockfd" file descriptor
  }

  /*Create file where data will be stored */
  if (strcmp(GET, argv[1]) == 0){
    // TODO:  Send GET to the server using the "sockfd" file descriptor
    // TODO:  Send the filename to the server using the "sockfd" file descriptor
    // TODO: Create a file with the same name using the open() system call using appropriate flags

    /*TODO:  Receive data in chunks of 256 bytes */
    // TODO:  Use the read() system call within a loop to fetch the contents of the file from the server
    // TODO:  Use the write() system call to write those contents in the file which was created above.
    }
  return 0;
}