#include "apue.h"
#include "myerror.h"

int main()
{
    int lines = 4;
    char buf[lines];
    while (fgets(buf, lines, stdin) != NULL)
        if (fputs(buf, stdout) == EOF)
            err_sys("output error");
    if (ferror(stdin))
        err_sys("input error");
    exit(0);
}
