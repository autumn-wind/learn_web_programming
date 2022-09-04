#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<sys/socket.h>

#define BUF_SIZE 30
void error_handling(char *message);

int main(int argc, char *argv[]) 
{
	if (argc != 2) {
		printf("Usage : %s <port>\n", argv[0]);
		exit(1);
	}

	int serv_sock = socket(PF_INET, SOCK_DGRAM, 0);
	if (serv_sock == -1)
		error_handling("UDP socket creation error");

	struct sockaddr_in serv_adr;
	memset(&serv_adr, 0, sizeof(serv_adr));
	serv_adr.sin_family = AF_INET;
	serv_adr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_adr.sin_port = htons(atoi(argv[1]));

	if (bind(serv_sock, (struct sockaddr*)&serv_adr, sizeof(serv_adr)) == -1)
		error_handling("bind() error");

	while (1)
	{
		struct sockaddr_in clnt_adr;
		socklen_t clnt_adr_sz = sizeof(clnt_adr);
		char message[BUF_SIZE];
		int str_len = recvfrom(serv_sock, message, sizeof(message) - 1, 0,
				(struct sockaddr*)&clnt_adr, &clnt_adr_sz);

		message[str_len] = 0;
		printf("Message from client: %s\n", message);

		fputs("Insert message(q to quit): ", stdout);
		fgets(message, sizeof(message), stdin);
		if (!strcmp(message, "q\n") || !strcmp(message, "Q\n"))
		    break;

		sendto(serv_sock, message, strlen(message), 0,
				(struct sockaddr*)&clnt_adr, clnt_adr_sz);
	}
	close(serv_sock);
	return 0;
}

void error_handling(char *message)
{
	fputs(message, stderr);
	fputc('\n', stderr);
	exit(1);
}
