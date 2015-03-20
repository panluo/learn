/*
 *  Use an big array to implement heap
 *
 *  declare: heap[0] :total nodes in the heap
 *          for a node i, its children are i*2 and i*2+1(if exists)
 *          its parent is i/2 
 * */
#include <stdio.h>
#include <stdlib.h>

void percolate_up(int heap[]);
void percolate_down(int heap[]);
void swap(int *pa, int *pb);


void insert(int new, int heap[]){
    //int childIdx,parentIdx;
    heap[0] = heap[0] + 1;
    heap[heap[0]] = new;

    /* recover heap property */
    percolate_up(heap);
}

void percolate_up(int heap[]){
    int lightIdx,parentIdx;

    lightIdx = heap[0];
    parentIdx = lightIdx/2;

    /* lightIdx is root then swap */

    while((parentIdx > 0) && (heap[lightIdx] < heap[parentIdx])){
        swap(heap + lightIdx, heap + parentIdx);
        lightIdx = parentIdx;
        parentIdx = lightIdx/2;
    }
}

int delete_min(int heap[]){
    int min;
    if(heap[0]<1){
        printf("Error : delete_min from an empty heap.");
        exit(1);
    }
    // delete root move the last leaf to the root
    min = heap[1];
    swap(heap + 1, heap + heap[0]);
    heap[0] -= 1;

    //recover heap property
    percolate_down(heap);
    return min;
}
void percolate_down(int heap[]){
    int heavyIdx;
    int childIdx1,childIdx2, minIdx;
    int sign;   //state variable, 1:swap; 0:no swap

    heavyIdx = 1;
    do{
        sign = 0;
        childIdx1 = heavyIdx*2;
        childIdx2 = childIdx1 + 1;

        if(childIdx1 > heap[0]){
            // both children are null
            break;
        }else if(childIdx2 > heap[0]){
            //right children is null
            minIdx = childIdx1;
        }else{
            minIdx = (heap[childIdx1] < heap[childIdx2]) ? childIdx1 : childIdx2;
        }

        if(heap[heavyIdx] > heap[minIdx]){
            //swp with child
            swap(heap + heavyIdx, heap + minIdx);
            heavyIdx = minIdx;
            sign = 1;
        }
    }while(sign == 1);
}

void swap(int *pa, int *pb){
    int tmp;
    tmp = *pa;
    *pa = *pb;
    *pb = tmp;
}

void main(){
    
    int a[10] = {12,35,24,84,42,18,20,73,90,77};
    int b[10] = {0};
    int i=0;
    for(i=0;i<10;i++){
        printf("%4d",a[i]);
        insert(a[i],b);
    }
    printf("\n");
    
    for(i=0;i<10;i++){
        int bb = delete_min(b);
        printf("%4d",bb);
    }
    printf("\n");
}
