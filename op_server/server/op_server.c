#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define BUF_SIZE 1024
#define OPSZ 4
int calculate(int opnum, int opnds[], char oprator);
void error_handling(char *message);

int main(int argc, char *argv[])
{
	if (argc != 2) {
		printf("Usage : %s <port>\n", argv[0]);
		exit(1);
	}

	int serv_sock;
	serv_sock = socket(PF_INET, SOCK_STREAM, 0);
	if (serv_sock == -1)
		error_handling("socket() error");

	struct sockaddr_in serv_addr;
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(atoi(argv[1]));

	if (bind(serv_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1)
		error_handling("bind() error");

	if (listen(serv_sock, 5) == -1)
		error_handling("listen() error");

	struct sockaddr_in clnt_addr;
	socklen_t clnt_addr_size = sizeof(clnt_addr);
	int clnt_sock;
	char opinfo[BUF_SIZE];
	int result, opnd_cnt;
	int recv_cnt, recv_len;
	for (int i = 0; i < 5; i++)
	{
		clnt_sock = accept(serv_sock, (struct sockaddr*)&clnt_addr, &clnt_addr_size);
		if (clnt_sock == -1)
			error_handling("accept() error");
		else
			printf("Connected client %d \n", i + 1);

		opnd_cnt = 0;
		read(clnt_sock, &opnd_cnt, 1); // TCP will automatically handle byte-order when transforming data

		recv_len = 0;
		while ((opnd_cnt * OPSZ + 1) > recv_len) // doesn't handle BUF_SIZE is not enough edge case
		{
			recv_cnt = read(clnt_sock, &opinfo[recv_len], BUF_SIZE - 1 - recv_len);
			recv_len += recv_cnt;
		}
		result = calculate(opnd_cnt, (int*)opinfo, opinfo[recv_len - 1]);
		write(clnt_sock, &result, sizeof(result));
		close(clnt_sock);
	}
	close(serv_sock);
	return 0;
}

int calculate(int opnum, int opnds[], char op)
{
	int result = opnds[0];
	switch (op) // no default handler
	{
		case '+':
			for (int i = 1; i < opnum; i++) result += opnds[i];
			break;
		case '-':
			for (int i = 1; i < opnum; i++) result -= opnds[i];
			break;
		case '*':
			for (int i = 1; i < opnum; i++) result *= opnds[i];
			break;
	}
	return result;
}

void error_handling(char *message)
{
	fputs(message, stderr);
	fputc('\n', stderr);
	exit(1);
}
