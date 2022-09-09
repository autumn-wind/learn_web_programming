#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<sys/socket.h>

#define BUF_SIZE 30
#define ENDING_MSG "Thank you"
void error_handling(char *message);

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
	printf("Usage: %s <IP> <port>\n", argv[0]);
	exit(1);
    }

    FILE *fp = fopen("received.dat", "wb");
    int sd = socket(PF_INET, SOCK_STREAM, 0);

    struct sockaddr_in serv_adr;
    memset(&serv_adr, 0, sizeof(serv_adr));
    serv_adr.sin_family = AF_INET;
    serv_adr.sin_addr.s_addr = inet_addr(argv[1]);
    serv_adr.sin_port = htons(atoi(argv[2]));

    connect(sd, (struct sockaddr*)&serv_adr, sizeof(serv_adr));

    int read_cnt;
    char buf[BUF_SIZE];
    while((read_cnt = read(sd, buf, BUF_SIZE)) != 0)
	fwrite((void*)buf, 1, read_cnt, fp);

    puts("Received file data");
    write(sd, ENDING_MSG, sizeof(ENDING_MSG));

    fclose(fp);
    close(sd);
    return 0;
}
