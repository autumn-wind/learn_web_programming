#include<stdio.h>
#include<unistd.h>
#include<sys/wait.h>

int main(int argc, char *argv[])
{
	pid_t pid = fork();
	if (pid == 0) // child process
	{
		sleep(15);
		return 24;
	}
	else // parent process
	{
		printf("Child PID: %d\n", pid);
		int status;
		while (!waitpid(-1, &status, WNOHANG))
		{
			sleep(3);
			puts("sleep 3 sec.");
		}

		if(WIFEXITED(status))
			printf("Child send: %d\n", WEXITSTATUS(status));

		sleep(30);
	}

	return 0;
}
