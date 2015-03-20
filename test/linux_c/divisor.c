#include<stdio.h>
int divisor(int a, int b)
{
    if(a%b==0)
    {
        printf("common divisor is : %d",b);
    }
    else
    {
        divisor(b,a%b);
    }
}

int main()
{
    int a,b;
    printf("input a and b:\n");
    scanf("%d %d",&a,&b);
    divisor(a,b);
}
