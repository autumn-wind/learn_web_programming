#include<stdio.h>
#include<unistd.h>

int gval = 10;
int main(int argc, char *argv[])
{
	int lval = 20;
	gval++, lval += 5;

	pid_t pid = fork();
	if (pid == 0) // child process
		gval += 2, lval += 2;
	else // parent process
		gval -= 2, lval -=2;

	if (pid == 0)
		printf("Child Proc: [%d, %d]\n", gval, lval);
	else
		printf("Parent Proc: [%d, %d]\n", gval, lval);

	return 0;
}
