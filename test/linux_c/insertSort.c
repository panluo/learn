#include<stdio.h>
void main()
{
    int a[10] = {34,36,18,25,37,63,17,8,59,39};

    int i,j,tmp;
    for(i=0;i<10;i++)
        printf("%d ",a[i]);
    printf("\n");

    for(i=1;i<10;i++)
    {
        j=i-1;
        tmp=a[i];
//        while(j>=0)
//        {
//            if(tmp<=a[j])
//            {
//                a[j+1]=a[j];
//                j--;
//            }
//            else
//            {
//                a[j+1]=tmp;
//                break;
//            }
//        }
//        if(j==-1)
//            a[0]=tmp;
        while(j>=0 && tmp<=a[j]){
            a[j+1] = a[j];
            j--;
        }
        if(j!=i-1)
            a[j+1]=tmp;
    }

    for(i=0;i<10;i++)
    {
        printf("%d ",a[i]);
    }
    printf("\n");
}
