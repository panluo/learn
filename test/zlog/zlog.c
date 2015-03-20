#include <stdio.h>
#include "zlog.h"

int main(int argc, char** argv)
{
/*    int rc;
    zlog_category_t *c;

    rc = zlog_init("test_zlog.conf");
    if(rc) {
        printf("init failed\n");
        return -1;
    }

    c = zlog_get_category("my_cat");

    if(!c){
        printf("get cat failed\n");
        zlog_fini();
        return -2;
    }

    zlog_info(c,"hello,zlog");
    zlog_fini();
    return 0;
*/
    int rc;
    rc = dzlog_init("test_default.conf","my_cat");
    if(rc){
        printf("init failed\n");
        return -1;
    }

    dzlog_info("hello,zlog");
    zlog_fini();
    return 0;
}
