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

void error_handling(char *message);

int main(int argc, char *argv[])
{
	int sock;
	char file_name[FILE_NAME_MAX_LENGTH];
	char buf[BUF_SIZE];
	struct sockaddr_in serv_addr;

	if (argc != 3) {
		printf("Usage : %s <IP> <port>\n", argv[0]);
		exit(1);
	}

	sock = socket(PF_INET, SOCK_STREAM, 0);
	if (sock == -1)
		error_handling("socket() error");

	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = inet_addr(argv[1]);
	serv_addr.sin_port = htons(atoi(argv[2]));

	if (connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)) == -1)
		error_handling("connect() error");
	else
		puts("Connected...........");

	fputs("Please input file_name: ", stdout);
	scanf("%s", file_name);
	int file_name_length = strlen(file_name);
	*(int*)buf = file_name_length;
	memcpy(buf + SIZE_OF_INT, file_name, file_name_length);
	write(sock, buf, SIZE_OF_INT + file_name_length); // send | file name length (4 bytes) + file name | to server

	size_t file_content_length = 0;
	read(sock, &file_content_length, SIZE_OF_SIZE_T); // receive file content length
	if (file_content_length == 0) {
		printf("No file received\n");
	}
	else {
		int recv_len = 0;
		while (recv_len < file_content_length) {
			int recv_cnt = read(sock, buf + recv_len, BUF_SIZE - recv_len - 1); // receive file content
			recv_len += recv_cnt;
		}
		buf[recv_len] = '\0';
		printf("The file content from server: %s\n", buf);
	}
	close(sock);
	return 0;
}

void error_handling(char *message)
{
	fputs(message, stderr);
	fputc('\n', stderr);
	exit(1);
}
