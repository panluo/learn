#!/usr/bin/env python
#

import heapq
import random

heap = []
heapq.heapify(heap)
for i in range(15):
    item = random.randint(10, 100)
    print "comeing ", item,
    if len(heap) >= 5:
        top_item = heap[0] # smallest in heap
        if top_item < item: # min heap
            top_item = heapq.heappop(heap)
            print index
            print "pop", top_item,
            heapq.heappush(heap, item)
            print "push", item,
    else:
        heapq.heappush(heap, item)
        print "push", item,
    pass
    print heap
pass
print heap

print "sort"
heap.sort()

print heap
