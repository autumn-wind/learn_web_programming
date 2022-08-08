#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

#define BUF_SIZE 1024
#define FILE_NAME_MAX_LENGTH 256
#define SIZE_OF_INT 4
#define SIZE_OF_SIZE_T 8

const char *mock_file_content = "abcd";

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
	clnt_sock = accept(serv_sock, (struct sockaddr*)&clnt_addr, &clnt_addr_size);
	if (clnt_sock == -1)
		error_handling("accept() error");
	else
		printf("Connected client\n");

	int file_name_length;
	read(clnt_sock, &file_name_length, SIZE_OF_INT);  // receive file name length from client

	char file_name[FILE_NAME_MAX_LENGTH];
	int recv_len = 0;
	while (recv_len < file_name_length) 
	{
		int recv_cnt = read(clnt_sock, file_name + recv_len, FILE_NAME_MAX_LENGTH - 1 - recv_len); // receive file name
		recv_len += recv_cnt;
	}
	file_name[file_name_length] = '\0';
	printf("Required file %s by client\n", file_name);

	if (strcmp(file_name, "mock_file.txt")) {
		printf("No required file\n");
	}
	else {
		printf("Found required file, file content: %s\n", mock_file_content);
		char resp[BUF_SIZE];
		size_t file_content_len = sizeof(mock_file_content);
		*(size_t*)resp = file_content_len;
		memcpy(resp + SIZE_OF_SIZE_T, mock_file_content, file_content_len);
		write(clnt_sock, resp, SIZE_OF_SIZE_T + file_content_len); // send | file content length + file content | to client
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
