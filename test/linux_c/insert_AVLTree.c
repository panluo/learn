/*
 * AVL 树，树中任意一节点左子树和右子树深度相差不超过1（平衡二叉树）
 * 为实现平衡，所以在插入的时候需要“旋转”。
 */

#include <stdio.h>
#include <stdlib.h>

typedef struct node *position;
typedef struct node *TREE;

typedef struct node{
    int depth;
    int element;
    position lchild;
    position rchild;
    position parent;
};



/*
 * insert value
 */
postition insert_value(TREE tr, int value)
{
    position np = insert_leaf(tr,value);
    update_root_depth(np);
    if (tr == NULL)
        tr = np;
    else
        tr = recover_avl(tr,np);
    return tr;
}

//========================================
// static functions : for internal use


/*
 * traverse the path from new node to root node
 * make one rotation, recover AVL and stop
 */
static TREE recover_avl(TREE tr,position np)
{
    static TREE recover_avl(TREE tr, position np) 
    {
        int myDiff;
        while (np != NULL) {
            update_root_depth(np);
            myDiff = depth_diff(np);
            if (myDiff > 1 || myDiff < -1) {
                if (myDiff > 1) {
                    /* left rotate needed */
                    if(depth_diff(np->rchild) > 0) {
                        np = left_single_rotate(np);
                    }
                    else {
                        np = left_double_rotate(np);
                    }
                }
                if (myDiff < -1) {
                    if(depth_diff(np->lchild) < 0) {
                        np = right_single_rotate(np);
                    }
                    else {
                        np = right_double_rotate(np);
                    }
                }
                /* if rotation changes root node */
                if (np->parent == NULL) tr = np;
                break;
            }
            np = np->parent;
        }

        return tr;
    }
}

/*
 * left single rotation
 * return the new root
 */
static TREE left_single_rotate(TREE tr)
{
    TREE new_root,parent;
    parent = tr -> parent;
    new_root = tr -> rchild;

    if (new_root -> lchild != NULL)
        new_root -> lchild
}


/*
 *  * left double rotation
 *   * return
 *    */
static TREE left_double_rotate(TREE tr) 
{
    right_single_rotate(tr->rchild);
    return left_single_rotate(tr);
}

/*
 * right double rotation
 * return
 */
static TREE right_double_rotate(TREE tr) 
{
    left_single_rotate(tr->lchild);
    return right_single_rotate(tr);
}


/*
 *  * difference of rchild->depth and lchild->depth
 *   */
static int depth_diff(TREE tr) 
{
    if (tr == NULL) {
        return 0;
    }
    else {
        return depth(tr->rchild) - depth(tr->lchild);
    }
}
static int depth(TREE tr)
{
    if (tr == NULL)
        return 0;
    else
        return tr -> depth;
}
/*
 * assume lchild-> depth and rchild-> depth are correct
 */
static void update_root_depth(position np)
{
    int maxChildDepth;
    int depChild, depRChild;

    if(tr == NULL)
        return;
    else{
        depLChild = depth(tr->lchild);
        depRChild = depth(tr->rchild);
        maxChildDepth = depLChild > depRChild? depLChild : depRChild;
        tr->depth = maxChildDepth + 1;
    }
}
/*
 * insert a new value into the tree as a leaf
 * return address of the new node
 */
static position insert_leaf(TREE tr, int value)
{
    position np = (position) malloc(sizeof(struct node));
    np -> element = value;
    np -> parent = NULL;
    np -> lchild = NULL;
    np -> rchild = NULL;

    if(tr == NULL)
        tr = np;
    else
        insert_node_to_nonempty_tree(tr,np);
    return np;
}

/*
 * insert a node to a non-empty tree
 * called by insert_value()
 */
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
