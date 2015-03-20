/*
 * #include <utime.h>
 * int utime(const char *pathname,const struct utimbuf *times);
 * 修改一个文件的访问和修改时间
 *
 *
 * utimbuf 定义：
 * struct utimbuf{
 *      time_t actime;      // access time 
 *      time_t modtime;     // modification time
 * }
 */

#include "apue.h"
#include <fcntl.h>
#include <utime.h>
#include <myerror.h>  /*　/usr/local/ 下*/

int main(int argc, char *argv[])
{
    int i,fd;
    struct stat statbuf;
    struct utimbuf timebuf;

    for (i = 1; i < argc; i++) {
        if (stat(argv[i], &statbuf) < 0) {  // fetch current time
            err_ret("%s : stat error", argv[i]);
            continue;
        }

        if ((fd = open(argv[i], O_RDWR | O_TRUNC )) < 0 ) { //truncate
            err_ret("%s : open error",argv[i]);
            continue;
        }

        close(fd);

        timebuf.actime = statbuf.st_atime;
        timebuf.modtime = statbuf.st_mtime;

        if (utime(argv[i],&timebuf) < 0){   //reset time
            err_ret("%s : utime error",argv[i]);
            continue;
        }
    }
    exit(0);
}
