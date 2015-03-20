#include <stdio.h>
#include <stdlib.h>

typedef struct node *postion;

struct node {
    int depth;
    position parent;
    int element;
    position lchild;
    position rchild;
};

typedef struct node *TREE;

/*
 * insert value
 */
position insert_value(TREE tree, int value)
{
    postion np;
    np = insert_leaf(tree,value);
TODO
    
}

/*
 * insert a new value into the tree as a leaf
 */
static position insert_leaf(TREE tree, int value)
{
    position np = (position)malloc(sizeof(struct node));
    np -> element = value;
    np -> lchild = NULL;
    np -> rchild = NULL;
    np -> parent = NULL;
    np -> depth = 0;

    if (tree != NULL)
        insert_node_to_nonempty_tree(tree, np);
    else
        tree = np;

    return np;
}

/*
 * insert node to nonempty tree
 */
static void insert_node_to_nonempty_tree(TREE tree, position np)
{
    postion tmp = tree;
    if (np -> element > tmp -> element) {
        if (tmp -> rchild == NULL){
            tmp -> rchild = np;
            np -> parent = tmp;
            return;
        }else
            insert_node_to_nonempty_tree(np -> rchild, np);
    }
    if (np -> element < tmp -> element) {
        if (tmp -> lchild == NULL) {
            tmp -> lchild = np;
            np -> parent = tmp;
            return;
        }else
            insert_node_to_nonempty_tree(np -> lchild, np);
    }
}
