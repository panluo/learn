/*
 * #include <unistd.h>
 * int link(const char *existingpath, const char *newpath);　if done return 0 else return -1
 * 创建一个指向现有文件的链接
 * int unlink(const char *pathname);
 * 删除一个现有的目录项，并将由pathname所引用文件的链接计数减１。
 *
 */

#include "apue.h"
#include <fcntl.h>
#include <myerror.h>
int main(void)
{
    if(open("tempfile",O_RDWR) < 0)
        err_sys("open error");
    if(unlink("tempfile") < 0)
        err_sys("unlink error");
    printf("file unlinked\n");
    sleep(10);
    printf("done\n");
    exit(0);
}
