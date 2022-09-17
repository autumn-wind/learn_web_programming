#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<sys/socket.h>

void error_handling(char *message);

int main(int argc, char *argv[])
{
    int rcv_buf = -1, snd_buf = -1;
    int sock = socket(PF_INET, SOCK_STREAM, 0);

    socklen_t len = sizeof(rcv_buf);
    if (getsockopt(sock,SOL_SOCKET, SO_RCVBUF, (void*)&rcv_buf, &len))
	error_handling("getsockopt() error");

    len = sizeof(snd_buf);
    if (getsockopt(sock, SOL_SOCKET, SO_SNDBUF, (void*)&snd_buf, &len))
	error_handling("getsockopt() error");

    printf("Input buffer size: %d\n", rcv_buf);
    printf("Output buffer size: %d\n", snd_buf);

    close(sock);
    return 0;
}

void error_handling(char *message)
{
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(1);
}

