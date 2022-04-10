#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<sys/socket.h>

#define MESSAGE_MAX_LENTH 1024
void error_handling(char *message);

int main(int argc, char *argv[])
{
    int serv_sock;
    int clnt_sock;

    struct sockaddr_in serv_addr;
    struct sockaddr_in clnt_addr;
    socklen_t clnt_addr_size;

    char clnt_msg[MESSAGE_MAX_LENTH], serv_msg[MESSAGE_MAX_LENTH];

    if (argc != 2)
    {
		printf("Usage : %s <port>\n", argv[0]);
		exit(1);
    }

    serv_sock = socket(PF_INET, SOCK_STREAM, 0);
    if(serv_sock == -1)
	error_handling("socket() error");

    memset(&serv_addr, 0, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
    serv_addr.sin_port = htons(atoi(argv[1]));

    if (bind(serv_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1)
	error_handling("bind() error");

    if (listen(serv_sock, 5) == -1)
	error_handling("listen() error");

    clnt_addr_size = sizeof(clnt_addr);
    clnt_sock = accept(serv_sock, (struct sockaddr*)&clnt_addr, &clnt_addr_size);
    if (clnt_sock == -1)
	error_handling("accept() error");

	for (int i = 0; i < 3; i++) {
		int str_len = 0;
		read(clnt_sock, &str_len, 4);
		int recv_len = 0;
		while (recv_len < str_len) {
			int recv_cnt = read(clnt_sock, clnt_msg + recv_len, MESSAGE_MAX_LENTH - recv_len - 1);
			recv_len += recv_cnt;
		}
		clnt_msg[recv_len] = '\0';
		printf("Message from client: %s\n", clnt_msg);

		if (clnt_msg[recv_len - 1] == '?')
			clnt_msg[recv_len - 1] = '!';

		int resp_len = strlen(clnt_msg);
		*(int*)serv_msg = resp_len;
		memcpy(serv_msg + 4, clnt_msg, resp_len);
		write(clnt_sock, serv_msg, 4 + resp_len);
		printf("Message sent to client: %s\n", clnt_msg);
		putchar('\n');
	}

    close(clnt_sock);
    close(serv_sock);
    return 0;
}

void error_handling(char *message)
{
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(1);
}
