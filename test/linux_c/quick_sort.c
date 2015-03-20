/*select pivot, put elements (<= pivot) to the left*/
void quick_sort(int a[], int ac)
{
    /*use swap*/

    /* pivot is a position, 
     *        all the elements before pivot is smaller or equal to pvalue */
    int pivot;
    /* the position of the element to be tested against pivot */
    int sample;

    /* select a pvalue.  
     *        Median is supposed to be a good choice, but that will itself take time.
     *               here, the pvalue is selected in a very simple wayi: a[ac/2] */
    /* store pvalue at a[0] */
    swap(a+0, a+ac/2);
    pivot = 1; 

    /* test each element */
    for (sample=1; sample<ac; sample++) {
        if (a[sample] < a[0]) {
            swap(a+pivot, a+sample);
            pivot++;
        }
    }
    /* swap an element (which <= pvalue) with a[0] */
    swap(a+0,a+pivot-1);

    /* base case, if only two elements are in the array,
     *        the above pass has already sorted the array */
    if (ac<=2) return;
    else {
        /* recursion */
        quick_sort(a, pivot);
        quick_sort(a+pivot, ac-pivot);
    }
}
/* By Vamei */
/* exchange the values pointed by pa and pb*/
void swap(int *pa, int *pb)
{
    int tmp;
    tmp = *pa;
    *pa = *pb;
    *pb = tmp;
}

void main(){
    int a[] = {23,34,12,78,29,46,71,9,89,67,53,20,55}
}

