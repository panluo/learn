#include "apue.h"
#include "myerror.h"

int glob = 6;   /* external variable in initialized data */

int main(void)
{
    int var;    // automatic variable on the stack
    pid_t pid;

    var = 88;
    printf("before vfork\n");   // we don't flush stdio

    if ((pid = vfork()) < 0) {
        err_sys("vfork error");
    }else if (pid == 0) {
        glob++;     //modify parent's variables
        var++;
        _exit(0);   //_exit 并不执行标准IO缓冲的冲洗操作
    }

    /*
     * parent continues here
     */
    printf("pid = %d, glob = %d, var = %d\n",getpid(),glob,var);
    exit(0);
}
