#!/usr/bin/env python

#def log(text=' '):
#    def decorator(func):
#        def wrapper(*args,**kw):
#            print '%s %s():'%(text,func.__name__)
#            return func(*args,**kw)
#        return wrapper
#    return decorator

import functools

def log(func):
    @functools.wraps(func)
    def wrapper(*args,**kw):
        print 'begin call %s():'%func.__name__
        return_value = func(*args,**kw)
        print 'end call %s():'%func.__name__
        return return_value 
    return wrapper
    
#@log
#def now():
#    print 'now'
