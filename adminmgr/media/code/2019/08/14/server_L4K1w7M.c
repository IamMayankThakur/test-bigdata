#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h> 

#define LIST "list"
#define GET "get"

int main(void)
{
  int listenfd = 0;
  int connfd = 0;
  char recvBuff[256];
  struct sockaddr_in serv_addr;
  char sendBuff[1024];
  int numrv;
  char filename[1024];

  listenfd = socket(AF_INET, SOCK_STREAM, 0);

  printf("Socket retrieve success\n");

  memset(&serv_addr, '0', sizeof(serv_addr));
  memset(sendBuff, '0', sizeof(sendBuff));

  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
  serv_addr.sin_port = htons(5050);

  bind(listenfd, (struct sockaddr *)&serv_addr, sizeof(serv_addr));

  if (listen(listenfd, 10) == -1)
  {
    printf("Failed to listen\n");
    return -1;
  }

  while (1)
  {
    connfd = accept(listenfd, (struct sockaddr *)NULL, NULL);

    // TODO:  Read if the client wants a GET or a LIST using a read() systemcall.

    // If client sends a GET
    if (strcmp(GET, recvBuff) == 0)
    {
      sleep(3);
      read(connfd, recvBuff, 256);
      // TODO:  Read the name of the file sent by the client using the connfd file descriptor.
      // TODO:  Open the file that we wish to transfer using open system call.

      /*TODO:  Read data from file and send it */
      while (1)
      {
        /*TODO:  First read file in chunks of 256 bytes */
        unsigned char buff[256] = {0};
        /*TODO:  If read was success, send data. to the client*/
    }

    // If client sends a LIST
    if (strcmp(LIST, recvBuff) == 0)
    { 
      // If list is sent by the client.
      DIR *d;
      struct dirent *dir;
      d = opendir(".");
      if (d)
      {
        while ((dir = readdir(d)) != NULL)
        {
          // TODO:  send the names of the file using the write() system call.
          // TODO:  Use man page of readdir to figure out how to use the "dir" variable
        }
        closedir(d);
      }
    }
    sleep(1);
  }
  return 0;
}