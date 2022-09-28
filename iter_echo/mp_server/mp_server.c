#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/wait.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define BUF_SIZE 1024
#define TRUE 1
#define FALSE 0

void error_handling(char *message);
void read_childproc(int sig);

int main(int argc, char *argv[])
{
	if (argc != 2) {
		printf("Usage : %s <port>\n", argv[0]);
		exit(1);
	}

	struct sigaction act;
	act.sa_handler = read_childproc;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	sigaction(SIGCHLD, &act, 0);

	int serv_sock = socket(PF_INET, SOCK_STREAM, 0);
	if (serv_sock == -1)
		error_handling("socket() error");

	/*int option = TRUE;*/
	/*setsockopt(serv_sock, SOL_SOCKET, SO_REUSEADDR, (void*)&option, sizeof(option));*/

	struct sockaddr_in serv_addr;
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);
	serv_addr.sin_port = htons(atoi(argv[1]));

	if (bind(serv_sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1)
		error_handling("bind() error");

	if (listen(serv_sock, 5) == -1)
		error_handling("listen() error");

	char message[BUF_SIZE];
	struct sockaddr_in clnt_addr;
	socklen_t clnt_addr_size = sizeof(clnt_addr);
	while (1)
	{
		int clnt_sock = accept(serv_sock, (struct sockaddr*)&clnt_addr, &clnt_addr_size);
		if (clnt_sock == -1)
			continue;
		else
			puts("new client connected...");

		pid_t pid = fork();
		if (pid == -1) // fork failure
		{
			close(clnt_sock);
			continue;
		}
		else if (pid == 0) // child process
		{
			close(serv_sock);
			int str_len;
			while ((str_len = read(clnt_sock, message, BUF_SIZE)) != 0)
				write(clnt_sock, message, str_len);
			close(clnt_sock);
			puts("client disconnected...");
			return 0;
		}
		else // father process
			close(clnt_sock);
	}
	close(serv_sock);
	return 0;
}

void read_childproc(int sig)
{
	int status;
	pid_t pid = waitpid(-1, &status, WNOHANG);
	printf("removed proc id: %d\n", pid);
}

void error_handling(char *message)
{
	fputs(message, stderr);
	fputc('\n', stderr);
	exit(1);
}
