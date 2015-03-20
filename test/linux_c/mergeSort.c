#include <stdio.h>
#include <malloc.h>
#include <limits.h>

void merge(int A[], int p, int q, int r){
    int *L,*R;
    int i,j,k;
    int lsize = q-p+1,rsize = r-q;
    L=(int *)malloc(sizeof(int)*lsize+1);
    R=(int *)malloc(sizeof(int)*rsize+1);
    for(i=0;i<lsize;i++){
        L[i] = A[i+p];
    }
    for(j=0;j<rsize;j++){
        R[j] = A[j+q+1];
    }

    L[i] = INT_MAX;
    R[j] = INT_MAX;
    i=0;j=0;
    for(k=p;k<=r;){
        if(L[i] <= R[j]){
            A[k] = L[i];
            k++;
            i++;
        }
        else{
            A[k] = R[j];
            k++;
            j++;
        }
       // printf("%d ",A[k]);
    }
}
void mergeSort(int A[], int begin, int end){
    int mid;
    if(end > begin){
        mid = (begin + end)/2;
        mergeSort(A,begin,mid);
        mergeSort(A,mid+1,end);
        merge(A,begin,mid,end);
    }
}

int main(){
    int i;
    int A[] = {13, 2, 69, 6, 1000, 32, 75, 15, 6, 29};
    //int arr[] =  {13, 2, 69, 6, 1000, 32, 75, 15, 6, 29, 531};
    printf("Original array:\n");
    for(i=0; i<10; i++)
        printf("%d ", A[i]);

    /*Merge Sort*/
    mergeSort(A, 0, 10);
    printf("\n\nArrar after Merge Sort: \n");
    for(i=0; i<10; i++)
        printf("%d ", A[i]);
    
    printf("\n");
}
