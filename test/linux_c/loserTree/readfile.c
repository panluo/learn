#include <stdio.h>
#include <stdlib.h>

#include <string.h>
#define n 10

typedef struct type
{
    char key[1024];
    int num;
} KeyType;
//typedef type KeyType;

void readlines(){
    FILE *tmp=fopen("/home/luo/domain","r");
    char buf[200];
    KeyType sort[n];
    //KeyType sort;
    char *result=NULL;
    int i=0;
    while(fgets(buf,200,tmp)!=NULL){
        //printf("1%s",buf);
        //(sort[i])->key=strtok(buf,"\t");
        strcpy(sort[i].key, strtok(buf, "\t"));
        sort[i].num = atoi(strtok(NULL, "\t"));
        //char *t = strtok(buf, "\t");
        //int d = atoi(strtok(NULL, "\t"));
        //printf("%s", t);
        //printf("%d", d);
        //strcpy(sort[i].key, t);
        //sort[i].num =d;
        //sort[i].num=atoi(strtok(NULL,"\t"));
        /*get next token*/
        i++;
        if(i>n)
            break;
    }
    for(i=0;i<n;i++)
       printf("%s++++%d",sort[i].key,sort[i].num);

}

void main(){
    readlines();
}
