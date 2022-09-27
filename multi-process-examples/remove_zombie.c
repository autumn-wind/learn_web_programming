#include<stdio.h>
#include<stdlib.h>
#include<unistd.h>
#include<signal.h>
#include<sys/wait.h>

void read_childproc(int sig)
{
	int status;
	pid_t id = waitpid(-1, &status, WNOHANG);
	if (WIFEXITED(status))
	{
		printf("Removed proc id: %d\n", id);
		printf("Child send %d\n", WEXITSTATUS(status));
	}
}

int main()
{
	struct sigaction act;
	act.sa_handler = read_childproc;
	sigemptyset(&act.sa_mask);
	act.sa_flags = 0;
	sigaction(SIGCHLD, &act, 0);

	pid_t pid = fork();
	if (pid == 0) // child process
	{
		puts("Hi! I'm a child process");
		sleep(10);
		return 12;
	}
	else // father process
	{
		printf("Child proc id: %d\n", pid);
		pid = fork();
		if (pid == 0) // child process 2
		{
			puts("Hi! I'm child process 2");
			sleep(10);
			return 24;
		}
		else //father process
		{
			printf("Child proc id: %d\n", pid);
			int i;
			for (i = 0; i < 5; i++)
			{
				puts("wait...");
				sleep(5);
			}
		}
	}
	return 0;
}
