/* use single-linked list to implement stack */
#include <stdio.h>
#include <stdlib.h>

typedef struct node *position;
typedef int ElementTP;

// point to the  head node of the list
typedef struct node *STACK;

struct node {
    ElementTP element;
    position next;
};

STACK init_stack(void);
void delete_stack(STACK);
ElementTP top(STACK);
void push(STACK, ElementTP);
ElementTP pop(STACK);
int is_null(STACK);

void main(void)
{
    ElementTP a;
    int i;
    STACK sk;
    sk = init_stack();
    push(sk, 1);
    push(sk, 2);
    push(sk, 8);
    printf("Stack is null? %d\n", is_null(sk));
    for (i=0; i<3; i++) {
        a = pop(sk);
        printf("pop: %d\n", a);
    }

    printf("Stack is null? %d\n", is_null(sk));    
    delete_stack(sk);
}

/*
 * initiate the stack
 * malloc the head node.
 * Head node doesn't store valid data
 * head->next is the top node
 */
STACK init_stack(void)
{
    position np;
    STACK sk;
    np = (position) malloc(sizeof(struct node));
    np->next = NULL;  // sk->next is the top node
    sk = np; 
    return sk;
}

/* pop out all elements 
 * and then delete head node
 */
void delete_stack(STACK sk)
{
    while(!is_null(sk)) {
        pop(sk);
    }
    free(sk);
}
/* 
 * View the top frame
 */
ElementTP top(STACK sk)
{
    return (sk->next->element);
}

/*
 * push a value into the stack
 */
void push(STACK sk, ElementTP value) 
{
    position np, oldTop;
    oldTop = sk->next;    

    np = (position) malloc(sizeof(struct node));
    np->element  = value;
    np->next     = sk->next;

    sk->next     = np; 
}

/* 
 * pop out the top value
 */
ElementTP pop(STACK sk)
{
    ElementTP element;
    position top, newTop;
    if (is_null(sk)) {
        printf("pop() on an empty stack");
        exit(1);
    } 
    else {
        top      = sk->next;
        element  = top->element;     
        newTop   = top->next;
        sk->next     = newTop;
        free(top);
        return element;
    } 
}

/* check whether a stack is empty*/
int is_null(STACK sk)
{
    return (sk->next == NULL);
}
