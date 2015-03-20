#!/usr/bin/env python

# mergeSort 
# @time 2014-10-13
import random

def merge(listA, listB):
    merged = []
    while listA and listB:
        if listA[0] > listB[0]:
            merged.append(listB.pop(0))
        else:
            merged.append(listA.pop(0))

    merged.extend(listA)
    merged.extend(listB)
    return merged

def mergeSort(listSort):
    if len(listSort) <= 1:
        return listSort
    # random to avoid dead loop for special sequence
    r = listSort[random.randint(0, len(listSort) - 1)]
    left, mid, right = [],[],[]
    for i in listSort:
        if i < r:
            left.append(i)
        elif i == r:
            mid.append(i)
        else:
            right.append(i)

    left.extend(mid)
            
    listA = mergeSort(left)
    listB = mergeSort(right)
    merged = merge(listA,listB)
    return merged

if __name__ == "__main__":
    print "please input integer number array"
    lst = []
    while 1:
        try:
            n = raw_input(">")
        except:
            print ""
            break
    
    lst.extend([int(i) for i in n.split()])
    print "origin: ", lst
    print "sorted: ", mergeSort(lst)

