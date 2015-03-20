#include<stdio.h>

int partition(int a[], int start, int end){
    int pivot = a[start];
    while(start<end){
        while(a[end]>pivot && end>start){
            end--;
        }
        if(end>start){
            a[start]=a[end];
            start++;
        }

        while(a[start]<=pivot && end>start){
            start++;
        }
        if(end>start){
            a[end]=a[start];
            end--;
        }
    }
    a[start] = pivot;
    return start;
}

void quickSort(int a[],int start, int end){
    if(end-start>1){
        int mid = partition(a,start,end);
      //  if(mid != start){   // carefull
            quickSort(a,start,mid);
            quickSort(a,mid+1,end);  // be care why is mid+1
        //}
    }
}

void main(){
    int a[] = {37,34,56,23,89,33,24,53,25,45,29,48,20,100,140,36};
    int len = sizeof(a)/sizeof(a[0]);
    int i=0;
    for(i=0;i<len;i++)
        printf("a[%d]=%d\n",i,a[i]);
    quickSort(a,0,len-1);
    for(i=0;i<len;i++)
        printf("a[%d]=%d\n",i,a[i]);
}

/*
void quickSort(int a[],int numsize)//a是整形数组，numsize是元素个数
{
    int i=0,j=numsize-1;
    int val=a[0];            //指定参考值val大小
    if(numsize>1)           //确保数组长度至少为2，否则无需排序
    {
        while(i<j)          //循环结束条件
        {
            //从后向前搜索比val小的元素，找到后填到a[i]中并跳出循环
            for(;j>i;j--)
                if(a[j]<val)
                {
                    a[i]=a[j];
                    break;
                }
            //从前往后搜索比val大的元素，找到后填到a[j]中并跳出循环
            for(;i<j;i++)
                if(a[i]>val)
                {
                    a[j]=a[i];
                    break;
                }
        }
        a[i]=val;           //将保存在val中的数放到a[i]中
        QuickSort(a,i);     //递归，对前i个数排序
        QuickSort(a+i+1,numsize-1-i);//对i+1到numsize-1这numsize-1-i个数排序
    }
}
*/
