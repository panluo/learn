#include <stdio.h>
#include <stdlib.h>

typedef struct node *LIST;
typedef struct node *position;

/* node,节点 */
struct node {
    int element;
    position next;
};

/*
 * operations(stereotype)
 *
 */
LIST init_list(void);
void delete_list(LIST);
int is_null(LIST);
void insert_node(position,int);
void delete_node(LIST,position);
position find_last(LIST);
position find_value(LIST,int);
position find_previous(LIST,position);
void print_list(LIST);

/* for testing purpose */

void main()
{
    LIST L;
    position np;

    int i;
    /* elements to be put int the list */
    int a[] = {1,3,5,7,9};

    /* initiate a list */

    L = init_list();
    print_list(L);
}

/*
 * Traverse the list and print each element
 */

void print_list(LIST L)
{
    position np;
    if (is_null(L)){
        printf("Empty List \n\n");
        return;
    }

    np = L;
    while(np->next != NULL){
        np = np->next;
        printf("%p : %d \n",np,np->element);
    }
    printf("\n");
}


/*
 * initialize a linked list.This list has a head node
 * head node doesn't store valid element value
 */

LIST init_list(void)
{
    LIST L;
    L = (position) malloc(sizeof(struct node));
    L -> next = NULL;
    return L;
}

/*
 * Delete all nodes in a list
 */

void delete_list(LIST L)
{
    position np, next;

    np = L;
    do{
        next = np -> next;
        free(np);
        np = next;
    }while(next != NULL);
}

/*
 * if a list only has head node, then the list is null
 */
int is_null(LIST L)
{
    return ((L -> next) == NULL);
}

/*
 * insert a node after position np
 */

void insert_node(position np,int value)
{
    position new_one;
    new_one = (position) malloc(sizeof(struct node));
    new_one -> next = np -> next;
    np -> next = new_one;
    new_one -> element = value;
}

/*
 * delete node 
 */
void delete_node(LIST L,position np)
{
    position previous,next;
    next = np -> next;
    previous = find_previous(L,np);
    if(previous != NULL){
        previous -> next = next;
        free(np);
    }
    else{
        printf("Error : np not in the list");
    }
}


/*
 *  find the last node of the list
 *  寻找表的最后一个节点
 */
position find_last(LIST L)
{
    position np;
    np = L;
    while(np->next != NULL) {
        np = np->next;
    }
    return np;
}

/*
 *  This function serves for 2 purposes:
 *  1. find previous node 
 *  2. return NULL if the position isn't in the list
 *   寻找npTarget节点前面的节点
 */
position find_previous(LIST L, position npTarget)
{
    position np;
    np = L;
    while (np->next != NULL) {
        if (np->next == npTarget) return np; 
        np = np->next;
    } 
    return NULL;
}

/*
 *  find the first node with specific value
 */
position find_value(LIST L, int value) 
{
    position np;
    np = L;
    while (np->next != NULL) {
        np = np->next;
        if (np->element == value) 
            return np;
    }
    return NULL;
}
