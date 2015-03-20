from heapq import heappush, heappop
from datetime import datetime 
from multiprocessing import Process, Queue, Pipe, current_process, freeze_support
import os

class file_heap:
    def __init__(self, dir, idx = 0, count = 1):
        files = os.listdir(dir)
        self.heap = []
        self.files = {}
        self.bulks = {}
        self.pre_element = None
        for i in range(len(files)):
            file = files[i]
            if hash(file) % count != idx: continue
            input = open(os.path.join(dir, file))
            self.files[i] = input
            self.bulks[i] = ''
            heappush(self.heap, (self.get_next_element_buffered(i), i))

    def get_next_element_buffered(self, i):
        if len(self.bulks[i]) < 256:
            if self.files[i] is not None:
                buf = self.files[i].read(65536)
                if buf:
                    self.bulks[i] += buf
                else:
                    self.files[i].close()
                    self.files[i] = None

                end_line = self.bulks[i].find('\n')
                if end_line == -1:
                    end_line = len(self.bulks[i])
                element = self.bulks[i][:end_line]
                self.bulks[i] = self.bulks[i][end_line+1:]
                return element

     def poppush_uniq(self):
         while True:
             element = self.poppush()
             if element is None:
                 return None
             if element != self.pre_element:
                 self.pre_element = element 
                 return element

    def poppush(self):
        try:
            element, index = heappop(self.heap)
        except IndexError:
            return None
        new_element = self.get_next_element_buffered(index)
        if new_element:
            heappush(self.heap, (new_element, index))
        return element


def heappoppush(dir, queue, idx = 0, count = 1):
    heap = file_heap(dir, idx, count)
    while True:
        d = heap.poppush_uniq()
        queue.put(d)
        if d is None: return

def heappoppush2(dir, queue, count = 1): 
    heap = []
    procs = []
    queues = []
    pre_element = None
    for i in range(count):
        q = Queue(1024)
        q_buf = queue_buffer(q)
        queues.append(q_buf)
        p = Process(target=heappoppush, args=(dir, q_buf, i, count))
        procs.append(p)
        p.start()

    queues = tuple(queues)

    for i in range(count):
        heappush(heap, (queues[i].get(), i))
    while True:
        try:
            d, i= heappop(heap)
        except IndexError:
            queue.put(None)
            for p in procs:
                p.join()
            return
        else:
            if d is not None:
                heappush(heap,(queues[i].get(), i))
                if d != pre_element:
                    pre_element = d
                    queue.put(d)

def merge_file(dir):
    heap = file_heap( dir )
    os.system('rm -f '+dir+'.merge')
    fmerge = open(dir+'.merge', 'a')
    element = heap.poppush_uniq() 
    fmerge.write(element+'\n')
    while element is not None:
        element = heap.poppush_uniq()
        fmerge.write(element+'\n')

class queue_buffer:
    def __init__(self, queue):
        self.q = queue
        self.rbuf = []
        self.wbuf = []

    def get(self):
        if len(self.rbuf) == 0:
            self.rbuf = self.q.get()
        r = self.rbuf[0]
        del self.rbuf[0]
        return r

    def put(self, d):
        self.wbuf.append(d)
        if d is None or len(self.wbuf) > 1024:
            self.q.put(self.wbuf)
            self.wbuf = []

def diff_file(file_old, file_new, file_diff, buf = 268435456):
    print 'buffer size', buf
    from file_split import split_sort_file
    os.system('rm -rf '+ os.path.splitext(file_old)[0] ) 
    os.system('rm -rf '+ os.path.splitext(file_new)[0] ) 
    t = datetime.now()
    split_sort_file(file_old,5,buf)
    split_sort_file(file_new,5,buf)
    print 'split elasped ', datetime.now() - t
    os.system('cat %s/* | wc -l'%os.path.splitext(file_old)[0])
    os.system('cat %s/* | wc -l'%os.path.splitext(file_new)[0])
    os.system('rm -f '+file_diff)
    t = datetime.now()
    zdiff = open(file_diff, 'a')

    old_q = Queue(1024)
    new_q = Queue(1024)
    old_queue = queue_buffer(old_q) 
    new_queue = queue_buffer(new_q) 
    h1 = Process(target=heappoppush2, args=(os.path.splitext(file_old)[0], old_queue, 3))
    h2 = Process(target=heappoppush2, args=(os.path.splitext(file_new)[0], new_queue, 3))
    h1.start(), h2.start()
    old = old_queue.get()
    new = new_queue.get()
    old_count, new_count = 0, 0
    while old is not None or new is not None:
        if old > new or old is None:
            zdiff.write('< '+new+'\n')
            new = new_queue.get()
            new_count +=1
        elif old < new or new is None:
            zdiff.write('> '+old+'\n')
            old = old_queue.get()
            old_count +=1
        else:
            old = old_queue.get()
            new = new_queue.get()
            
    print 'new_count:', new_count 
    print 'old_count:', old_count 
    print 'diff elasped ', datetime.now() - t
    h1.join(), h2.join()
