/* By Vamei */
/* use single-linked list to implement queue */
#include <stdio.h>
#include <stdlib.h>

typedef struct node *position;
typedef int ElementTP;

// point to the head node of the list
typedef struct HeadNode *QUEUE;

struct node {
    ElementTP element;
    position next;
};

/*
 *  CAUTIOUS: "HeadNode" is different from "node", 
 *  it's another struct
 *  end: points to the last value in the queue
 */
struct HeadNode {
    ElementTP element;
    position next;
    position end;
};


/*
 * Operations
 */
QUEUE init_queue(void);
void delete_queue(QUEUE);
void enqueue(QUEUE, ElementTP);
ElementTP dequeue(QUEUE);
int is_null(QUEUE);

/*
 *  * Test
 *   */
void main(void)
{
    ElementTP a;
    int i;
    QUEUE qu;
    qu = init_queue();

    enqueue(qu, 1);
    enqueue(qu, 2);
    enqueue(qu, 8);
    printf("Queue is null? %d\n", is_null(qu));
    for (i=0; i<3; i++) {
        a = dequeue(qu);
        printf("dequeue: %d\n", a);
    }

    printf("Queue is null? %d\n", is_null(qu));    
    delete_queue(qu);
}

/*
 *  * initiate the queue
 *   * malloc the head node.
 *    * Head node doesn't store valid data
 *     * head->next is the first node in the queue.
 *      */
QUEUE init_queue(void)
{
    QUEUE hnp;
    hnp = (QUEUE) malloc(sizeof(struct HeadNode));
    hnp->next = NULL;  // qu->next is the first node
    hnp->end  = NULL;
    return hnp;
}

/*
 *  * dequeue all elements 
 *   * and then delete head node
 *    */
void delete_queue(QUEUE qu)
{
    while(!is_null(qu)) {
        dequeue(qu);
    }
    free(qu);
}

/*
 *  * enqueue a value to the end of the queue 
 *   */
void enqueue(QUEUE qu, ElementTP value) 
{
    position np, oldEnd;
    oldEnd = qu->end;    

    np = (position) malloc(sizeof(struct node));
    np->element = value;
    np->next = NULL;

    /* if queue is empyt, then oldEnd is NULL */
    if (oldEnd) {
        oldEnd->next = np;
    }
    else {
        qu->next = np;
    }

    qu->end = np; 
}

/* 
 * dequeue the first value
 */
ElementTP dequeue(QUEUE qu)
{
    ElementTP element;
    position first, newFirst;
    if (is_null(qu)) {
        printf("dequeue() on an empty queue");
        exit(1);
    } 
    else {
        first = qu->next;
        element = first->element;     
        newFirst = first->next;
        qu->next = newFirst;
        free(first);
        return element;
    } 
}

/*
 * check: queue is empty?
 */
int is_null(QUEUE qu)
{
    return (qu->next == NULL);
}
