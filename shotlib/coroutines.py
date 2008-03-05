#
# Some fooling around with coroutines.
#

#
# Motivation:
#   I want a way to write a bot with multiple threads of control.
#   I want to ensure only one outbound web request is in progress at a time
#    (to limit how much like a bot we look like)
#

from twisted.python import log, failure
from twisted.internet import reactor, defer, threads
from twisted.python.threadable import getThreadID, isInIOThread
import sys

from shotlib.util import copyinfo

class Call(object):
    def __init__(self, f, *args, **kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs
    def invoke(self, step, errback):
        d = threads.deferToThread(self.f, *self.args, **self.kwargs)
        d.addCallback(step)
        d.addErrback(errback)

class Sleep(object):
    def __init__(self, t):
        self.t = t
    def invoke(self, step, errback):
        reactor.callLater(self.t, step)


class Fork(object):
    def __init__(self, f, *args, **kwargs):
        self.f = f
        self.args = args
        self.kwargs = kwargs
    def invoke(self, step, errback):
        d = threads.deferToThread(self.f, *self.args, **self.kwargs)
        d.addErrback(errback)
        reactor.callLater(0.0, step, d)

class Return(object):
    def __init__(self, val):
        self.val = val

    def invoke(self, step, errback):
        reactor.callLater(0.0, step)
        
class UnexpectedValue(Exception):
    pass

def coroutine(func):
    result = [None]
    def start(*args, **kwargs):
        try:
            g = func(*args, **kwargs)
        except:
            return defer.fail()
        if not hasattr(g, 'next'):
            # oop, it wasn't a coroutine
            return defer.succeed(g)
        d = defer.Deferred()
        def errback(f):
            try:
                g.throw(f.type, f.value, f.tb)
            except:
                info = sys.exc_info()
                if isinstance(info[0], GeneratorExit):
                    raise
                else:
                    d.errback(failure.Failure())
                    return                
            reactor.callLater(0.0, step)
            #d.errback(*val)
        def step(*val):
            try:
                if val:
                    r = g.send(*val)
                else:
                    r = g.next()
                if isinstance(r, Return):
                    result[0] = r.val
            except StopIteration:
                d.callback(result[0])
                return
            except:
                info = sys.exc_info()
                if isinstance(info[0], GeneratorExit):
                    raise
                else:
                    d.errback(failure.Failure())
                    return
            if isinstance(r, defer.Deferred):
                r.addCallback(step)
                r.addErrback(errback)
            elif not hasattr(r, 'invoke'):
               raise UnexpectedValue()
            else:
                r.invoke(step, errback)
        reactor.callLater(0.0, step)
        return d
    return start

def blocking(func):
    #
    # mark a function as blocking -- ensure it's not called in the I/O thread
    #
    # We'll experimentally auto-deferize it if it's called in the I/O thread
    @copyinfo(func)
    def _wrap(*args, **kw):
        if isInIOThread():
            return threads.deferToThread(func, *args, **kw)
        else:
            return func(*args, **kw)
    return _wrap

class Hodor(Exception):
    pass

@coroutine
def two():
    print getThreadID(), 'running two!'
    yield Return(5)

@coroutine
def one():
    yield Sleep(0.5)
    print getThreadID(), 'Back in one, calling two'
    result = (yield two())
    print getThreadID(), 'one has resumed: two returned ', result

@coroutine
def handler(q, r):
    while True:
        item = (yield q.get())
        if item is None:
            print 'Handler is shutting down'
            break
        print 'Handler is processing ',item
        r.put(item + 1)
        
@coroutine
def maker():
    q = defer.DeferredQueue()
    r = defer.DeferredQueue()
    handler(q, r)
    for i in range(5):
        q.put(i)
        print 'Result is ', (yield r.get())
    q.put(None)
    print 'Maker is shutting down'
@coroutine
def test_routine():
    @blocking
    def expensive(t):
        print 'Thread ',getThreadID(), ' is handling expensive computation ', t
        if t == 2:
            raise Hodor()
        return t + 5
    for i in range(10):
        try:
            v = (yield expensive(i))
            print getThreadID(), 'My expensive operation resulted in ', v
        except Hodor:
            print 'Oh noes, processing ',i, ' blew up'
            import traceback
            traceback.print_exc()
        yield Sleep(0.5)


if __name__ == '__main__':
    def complete(*args):
        print 'Completed ', args        
        reactor.stop()
    d = defer.DeferredList([test_routine(), one(), maker()])
    d.addBoth(complete)
    reactor.run()
