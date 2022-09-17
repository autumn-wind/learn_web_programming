#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<sys/socket.h>

void error_handling(char *message);

int main(int argc, char *argv[])
{
    int sock_type = -1;
    socklen_t optlen = sizeof(sock_type);

    int tcp_sock = socket(PF_INET, SOCK_STREAM, 0);
    int udp_sock = socket(PF_INET, SOCK_DGRAM, 0);
    printf("SOCK_STREAM: %d\n", SOCK_STREAM);
    printf("SOCK_DGRAM: %d\n", SOCK_DGRAM);

    if (getsockopt(tcp_sock, SOL_SOCKET, SO_TYPE, (void*)&sock_type, &optlen))
	error_handling("getsockopt() error!");
    printf("TCP socket type: %d\n", sock_type);

    if (getsockopt(udp_sock, SOL_SOCKET, SO_TYPE, (void*)&sock_type, &optlen))
	error_handling("getsockopt() error!");
    printf("UDP socket type: %d\n", sock_type);

    close(tcp_sock);
    close(udp_sock);

    return 0;
}

void error_handling(char *message)
{
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(1);
}

