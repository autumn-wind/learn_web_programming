#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<signal.h>

#define BUF_SIZE 30

void register_sigalrm_handler();

void keycontrol(int sig)
{
    if (sig == SIGINT)
    {
	signal(SIGALRM, SIG_IGN);
	fputs("Input message(Y to quit): ", stdout);
	char message[BUF_SIZE];
	fgets(message, BUF_SIZE, stdin);

	if (!strcmp(message, "y\n") || !strcmp(message, "Y\n"))
	    exit(1);
	else
	{
	    register_sigalrm_handler();
	    alarm(1);
	}
    }
}

void timeout(int sig)
{
    if (sig == SIGALRM)
	puts("1s passed...");
    alarm(1);
}

void register_sigint_handler()
{
    struct sigaction act;
    act.sa_handler = keycontrol;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGINT, &act, 0);
}

void register_sigalrm_handler()
{
    struct sigaction act;
    act.sa_handler = timeout;
    sigemptyset(&act.sa_mask);
    act.sa_flags = 0;
    sigaction(SIGALRM, &act, 0);
}

int main()
{
    register_sigint_handler();
    register_sigalrm_handler();
    alarm(1);

    while (1)
    {
	sleep(100);
    }
    return 0;
}
