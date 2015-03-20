#!/usr/bin/env python

import functools

def log(obj):
    if hasattr(obj,'__call__'):  #if isinstance(text,str) or text is None
        
        @functools.wraps(obj)
        def wrapper(*args,**kw):
            print 'begin call %s():'%obj.__name__
            obj(*args,**kw)
            print 'end call %s():' %obj.__name__
            return

        return wrapper

    else:
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args,**kw):
                print '%s %s():'%(obj,func.__name__)
                print 'begin call:%s'%func.__name__
                func(*args,**kw)
                print 'end call:%s'%func.__name__
                return

            return wrapper

        return decorator
