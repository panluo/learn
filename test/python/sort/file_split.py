import gzip
import os
from multiprocessing import Process, Queue, Pipe, current_process, freeze_support
from datetime import datetime
def sort_worker(input,output):
    while True:
        lines = input.get().splitlines()
        element_set = {}
        for line in lines:
            if line.strip() == 'STOP':
                return 
            try:
                element = line.split(' ')[0]
                if not element_set.get(element): element_set[element] = ''
            except:
                pass

        sorted_element = sorted(element_set)
        #print sorted_element
        output.put('\n'.join(sorted_element))


def write_worker(input, pre):
    os.system('mkdir %s'%pre)
    i = 0
    while True:
        content = input.get()
        if content.strip() == 'STOP':
            return
        write_sorted_bulk(content, '%s/%s'%(pre, i))
        i += 1

def write_sorted_bulk(content, filename):
    f = file(filename, 'w')
    f.write(content)
    f.close()

def split_sort_file(filename, num_sort = 3, buf_size = 65536*64*4):
    t = datetime.now()
    pre, ext = os.path.splitext(filename)
    if ext == '.gz':
        file_file = gzip.open(filename, 'rb')
    else:
        file_file = open(filename)
    bulk_queue = Queue(10)
    sorted_queue = Queue(10)
    NUM_SORT = num_sort
    sort_worker_pool = []
    for i in range(NUM_SORT):
        sort_worker_pool.append( Process(target=sort_worker, args=(bulk_queue, sorted_queue)) )         
        sort_worker_pool[i].start()

    NUM_WRITE = 1
    write_worker_pool = []
    for i in range(NUM_WRITE):
        write_worker_pool.append( Process(target=write_worker, args=(sorted_queue, pre))
        write_worker_pool[i].start()
        
    buf = file_file.read(buf_size)
    sorted_count = 0
    while len(buf):
        end_line = buf.rfind('\n')
        #print buf[:end_line+1]
        bulk_queue.put(buf[:end_line+1])
        sorted_count += 1
        if end_line != -1:
            buf = buf[end_line+1:] + file_file.read(buf_size)
        else:
            buf = file_file.read(buf_size)
            
    for i in range(NUM_SORT):
        bulk_queue.put('STOP')
    for i in range(NUM_SORT):
        sort_worker_pool[i].join()
        
    for i in range(NUM_WRITE):
        sorted_queue.put('STOP')
    
    for i in range(NUM_WRITE):
        write_worker_pool[i].join()
        
    print 'elasped ', datetime.now() - t 
    return sorted_count
