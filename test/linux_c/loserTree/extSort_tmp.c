#include<stdio.h>
#include<stdlib.h>
#include<string.h>

#define TRUE 1
#define FALSE 0
#define OK 1
#define ERROR 0
#define INFEASIBLE -1
#define MINKEY -1
#define MAXKEY 100

/* Status是函数的类型,其值是函数结果状态代码，如OK等 */ 
typedef int Status;

/* Boolean是布尔类型,其值是TRUE或FALSE */ 
typedef int Boolean;

/* 一个用作示例的小顺序表的最大长度 */ 
#define MAXSIZE 20

typedef int KeyType;

/* k路归并 */ 
#define k 3  

/* 设输出M个数据换行 */ 
#define M 10  

/* k+1个文件指针(fp[k]为大文件指针)，全局变量 */ 
FILE *fp[k + 1];  

/* 败者树是完全二叉树且不含叶子，可采用顺序存储结构 */ 
typedef int LoserTree[k];  
typedef KeyType ExNode, External[k+1];  

/* 全局变量 */ 
External b;  

/* 从第i个文件(第i个归并段)读入该段当前第1个记录的关键字到外结点 */ 
int input(int i, KeyType *a){ 
    int j = fscanf(fp[i], "%d ", a); 
    if(j > 0){ 
        printf("%d\n", *a); 
        return 1; 
    }else{ 
        return 0; 
    } 
} 

/* 将第i个文件(第i个归并段)中当前的记录写至输出归并段 */ 
void output(int i){ 
    fprintf(fp[k], "%d ", b[i]); 
} 

/* 沿从叶子结点b[s]到根结点ls[0]的路径调整败者树。*/ 
void Adjust(LoserTree ls, int s){  
    int i, t; 

    /* ls[t]是b[s]的双亲结点 */ 
    t = (s + k) / 2;  
    while(t > 0){ 
        /* s指示新的胜者 */ 
        if(b[s] > b[ls[t]]){ 
            i = s; 
            s = ls[t];  
            ls[t] = i; 
        } 
        t = t / 2; 
    } 
    ls[0] = s; 
} 
/**  
 * * 已知b[0]到b[k-1]为完全二叉树ls的叶子结点，存有k个关键字，沿从叶子  
 * * 到根的k条路径将ls调整成为败者树。 
 * */ 
void CreateLoserTree(LoserTree ls){  
    int i; 
    b[k] = MINKEY; 

    /* 设置ls中“败者”的初值 */ 
    for(i = 0; i < k; ++i){ 
        ls[i] = k;  
    } 

    /* 依次从b[k-1]，b[k-2]，…，b[0]出发调整败者 */ 
    for(i = k - 1; i >= 0; --i){ 
        Adjust(ls, i); 
    } 
} 

/**  
 * * 利用败者树ls将编号从0到k-1的k个输入归并段中的记录归并到输出归并段。  
 * * b[0]至b[k-1]为败者树上的k个叶子结点，分别存放k个输入归并段中当前记录的关键字。  
 * */ 
void K_Merge(LoserTree ls, External b){  
    int i, q; 

    /* 分别从k个输入归并段读人该段当前第一个记录的关键字到外结点 */ 
    for(i = 0; i < k; ++i) { 
        input(i, &b[i]); 
    } 

    /* 建败者树ls，选得最小关键字为b[ls[0]].key */ 
    CreateLoserTree(ls);  

    while(b[ls[0]] != MAXKEY){ 
        /* q指示当前最小关键字所在归并段 */ 
        q = ls[0];  

        /* 将编号为q的归并段中当前（关键字为b[q].key）的记录写至输出归并段 */ 
        output(q);  

        /* 从编号为q的输入归并段中读人下一个记录的关键字 */ 
        if(input(q, &b[q]) > 0){ 
            /* 调整败者树，选择新的最小关键字 */ 
            Adjust(ls,q);  
        }  
    } 

    /* 将含最大关键字MAXKEY的记录写至输出归并段 */ 
    output(ls[0]);  
} 


void show(KeyType t) { 
    printf("(%d)", t); 
} 

int main(){ 
    KeyType r; 
    int i, j; 
    char fname[k][4], fout[5] = "out", s[3]; 
    LoserTree ls; 

    /* 依次打开f0,f1,f2,…,k个文件 */ 
    for(i = 0; i < k; i++){  
        /* 生成k个文件名f0,f1,f2,… */ 
        /*itoa(i, s, 10); */
        sprintf(s,"%d",i);
        strcpy(fname[i], "f"); 
        strcat(fname[i], s); 

        /* 以读的方式打开文件f0,f1,… */ 
        fp[i] = fopen(fname[i], "r");  
        printf("有序子文件f%d的记录为:\n",i); 

        /* 依次将f0,f1,…的数据读入r */ 
        do{ 
            j = fscanf(fp[i], "%d ", &r); 
            /* 输出r的内容 */ 
            if(j == 1){ 
                show(r);  
            } 
        }while(j == 1); 
        printf("\n"); 

        /* 使fp[i]的指针重新返回f0,f1,…的起始位置，以便重新读入内存 */ 
        rewind(fp[i]);  
    } 

    /* 以写的方式打开大文件fout */ 
    fp[k] = fopen(fout, "w");  

    /* 利用败者树ls将k个输入归并段中的记录归并到输出归并段，即大文件fout */ 
    K_Merge(ls, b);  

    /* 关闭文件f0,f1,…和文件fout */ 
    for(i = 0; i <= k; i++){ 
        fclose(fp[i]);  
    } 

    /* 以读的方式重新打开大文件fout验证排序 */ 
    fp[k] = fopen(fout, "r");  
    printf("排序后的大文件的记录为:\n"); 

    i = 1; 
    do{ 
        /* 将fout的数据读入r */ 
        j = fscanf(fp[k], "%d ", &r); 

        /* 输出r的内容 */ 
        if(j == 1){ 
            show(r);  
        } 

        /* 换行 */ 
        if(i++ % M == 0){ 
            printf("\n");  
        } 
    }while(j == 1); 
    printf("\n"); 

    /* 关闭大文件fout */ 
    fclose(fp[k]);  
    return 0;
} 
