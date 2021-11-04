#include<stdio.h>
#include<stdlib.h>
#include<fcntl.h>
#include<unistd.h>

#define BUF_SIZE 1000

void error_handling(char *message);

int main(int argn, char* argc[])
{
    if (argn != 3)
	error_handling("correct usage: ./cp src_file dest_file");

    int src_fd = open(argc[1], O_RDONLY);
    if (src_fd == -1)
	error_handling("open src file error!");

    int dest_fd = open(argc[2], O_CREAT|O_WRONLY|O_TRUNC);
    if (dest_fd == -1)
	error_handling("open dest file error!");

    char buf[BUF_SIZE];
    ssize_t file_length;
    do
    {
	if ((file_length = read(src_fd, buf, sizeof(buf))) == -1)
	    error_handling("read src file error!");

	/*printf("file data: %s", buf);*/
	/*printf("file length: %d", file_length);*/

	if (write(dest_fd, buf, file_length) == -1)
	    error_handling("write dest file error!");

    } while (file_length == BUF_SIZE);
    close(src_fd);
    close(dest_fd);

    return 0;
}

void error_handling(char *message)
{
    fputs(message, stderr);
    fputc('\n', stderr);
    exit(1);
}
