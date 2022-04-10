#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<arpa/inet.h>
#include<sys/socket.h>

#define MESSAGE_MAX_LENTH 1024
const char *greeting[] = {"hello?", "hi?", "how are you?"};
void error_handling(char *);

int main(int argc, char* argv[])
{
    int sock;
    struct sockaddr_in serv_addr;
    char message[MESSAGE_MAX_LENTH];

    if (argc != 3)
    {
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
	error_handling("connect() error!");

	for (int i = 0; i < 3; i++) {
		int greeting_len = strlen(greeting[i]);
		*(int*)message = greeting_len;
		memcpy(message + 4, greeting[i], greeting_len);
		write(sock, message, 4 + greeting_len);
		printf("Message sent to server: %s\n", greeting[i]);

		int str_len = 0;
		read(sock, &str_len, 4); // 4 bytes are short enough that can always be read in one time?
		int recv_len = 0;
		while (recv_len < str_len) {
			int recv_cnt = read(sock, message + recv_len, MESSAGE_MAX_LENTH - recv_len - 1);
			recv_len += recv_cnt;
		}
		message[recv_len] = '\0';
		printf("Message from server: %s\n", message);
		putchar('\n');
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
