#!/usr/bin/env python
import threading
from time import ctime,sleep

class Mythread(threading.Thread):
    def __init__(self,func,args,name=''):
        threading.Thread.__init__(self)
        self.name = name
        self.func = func
        self.args = args

    def getResult(self):
        return self.res
    
    def run(self):
        print 'starting',self.name,'at:',ctime()
        self.res = apply(self.func,self.args)
        print self.name,'finished at:',ctime()

def fib(x): 
    sleep(0.005)
    if x<2:
        return 1
    return (fib(x-2) + fib(x-1))

def fac(x):
    sleep(0.1)
    if x<2:
        return 1
    return (x*fac(x-1))

def sums(x):
    sleep(0.1)
    if x<2:
        return 1
    return (x+sums(x-1))

funcs = [fib,fac,sums]
n = 12

def main():
    nfuncs = range(len(funcs))
    print '*** single thread'

    for i in nfuncs:
        print 'starting',funcs[i].__name__,'at:',ctime()
        print funcs[i](n)
        print funcs[i].__name__,'finished at:',ctime()

    print '\n*** multiple threads'

    threads = []
    for i in nfuncs:
        t = Mythread(funcs[i],(n,),funcs[i].__name__)
        threads.append(t)

    for i in nfuncs:
        threads[i].start()

    for i in nfuncs:
        threads[i].join()

    print threads[i].getResult()

    print 'all DONE'

if __name__ == '__main__':
    main()
