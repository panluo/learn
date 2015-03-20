#!/usr/bin/env python

import threading
from time import sleep,ctime

loops = [4,2]

def loop(nloop,nsec):
    print 'start loop',nloop,'at:',ctime()
    sleep(nsec)
    print 'loop',nloop,'done at:',ctime()


def main():
    print 'starting at:',ctime()
    threads = []
    nloops = range(len(loops))
    
    #所有线程都创建之后，再一起调用start（）函数启动
    for i in nloops:
        t = threading.Thread(target = loop,args=(i,loops[i]))
        threads.append(t)

    for i in nloops:
        threads[i].start()

    #程序挂起,直到线程结束;如果给了 timeout,则最多阻塞 timeout 秒
    for i in nloops:
        threads[i].join()

    print 'all DONE at:',ctime()

if __name__ == '__main__':
    main()
