#include <stdio.h>
#include <stdlib.h>

typedef struct node *position;

struct node {
    position parent;
    position lchild;
    position rchild;
    int element;
};

typedef struct node *TREE;


void print_sorted_tree(TREE tree);
position find_min(TREE tree);
position find_max(TREE tree);
position find_value(TREE tree,int value);
int delete_node(position node);
position insert_value(TREE tree, int value);

static int delete_leaf(position);
static void insert_node_to_nonempty_tree(TREE, position);


/*
 * print values of the tree in sorted order
 */
void print_sorted_tree(TREE tree)
{
    if (tree == NULL)
        return;
    print_sorted_tree(tree->lchild);
    printf("%4d",tree->element);
    print_sorted_tree(tree->rchild);
}

/*
 * search for minimum value
 * traverse lchild
 */
position find_min(TREE tree)
{
    /*
       if (tree->lchild == NULL)
       return tree;
       find_min(tree->lchild); */

    if (tree == NULL)
        return NULL;
    position np = tree;
    while( np -> lchild != NULL){
        np = np->lchild;
    }
    return np;
}

/*
 * search for maximum value
 * traverse rchild
 */
position find_max(TREE tree)
{
    if (tree == NULL)
        return NULL;
    position np = tree;
    while( np->rchild != NULL)
        np = np -> rchild;
    return np;
}

/*
 * search for value
 */
position find_value(TREE tree,int value)
{
    if (tree == NULL)
        return NULL;
    if (tree -> element == value)
        return tree;
    else if (value < tree -> element)
        find_value(tree -> lchild,value);
    else if (value > tree -> element)
        find_value(tree -> rchild,value);
}

/*
 * delete node np
 */
int delete_node(position node)
{
    position replace;
    if (node -> rchild == NULL && node -> lchild == NULL){
        return delete_leaf(node);
    }else{
        replace = (node -> lchild != NULL) ? find_max(node -> lchild):find_min(node -> rchild);
        node -> element = replace -> element;
        return delete_leaf(node);
    }
}

/*
 * insert a value into the tree
 * return root address of tree
 */
position insert_value(TREE tree, int value)
{
    position node;
    node = (position) malloc(sizeof(struct node));
    node -> element = value;
    node -> parent = NULL;
    node -> rchild = NULL;
    node -> lchild = NULL;

    if (tree == NULL) 
        tree = node;
    else{
        insert_node_to_nonempty_tree(tree,node);
    }
    return tree;
}



static void insert_node_to_nonempty_tree(TREE tree,position node)
{
    if (tree -> element > node -> element)
        if (tree -> lchild != NULL)
            insert_node_to_nonempty_tree(tree -> lchild,node);
        else{
            tree -> lchild = node;
            node -> parent = tree;
        }
    else if ( tree -> rchild != NULL)
        insert_node_to_nonempty_tree(tree -> rchild,node);
    else{
        tree -> rchild = node;
        node -> parent = tree;
    }
}



static int delete_leaf(position node)
{
    int element = node -> element;
    if (node -> parent == NULL){
        free(node);
        return element;
    }

    position parent = node -> parent;
    if(parent -> lchild == node)
        parent -> lchild = NULL;
    if(parent -> rchild == node)
        parent -> rchild = NULL;
    free(node);
    return element;
}

void main(void) 
{
    TREE tr;
    position np;
    int element;
    tr = NULL;
    tr = insert_value(tr, 18);
    tr = insert_value(tr, 5);
    tr = insert_value(tr, 2); 
    tr = insert_value(tr, 8);
    tr = insert_value(tr, 81);
    tr = insert_value(tr, 101);
    printf("Original:\n");
    print_sorted_tree(tr); printf("\n");
    np = find_max(tr);
   
    printf("%d\n",np-> element);
    np = find_min(tr);
    printf("%d\n", np -> element);
    np = find_value(tr, 8);
    if(np != NULL) {
        delete_node(np);
        printf("After deletion:\n");
        print_sorted_tree(tr); printf("\n");
    }
}
