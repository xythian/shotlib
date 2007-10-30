
#
# Eventually someone will need to go back and rename a bunch of these modules.
#


#
# An event processing thread which will pull messages off a queue and process them, and
# easily allow delayed events.
#

from Queue import Queue, Empty
import threading
from threading import Thread
from sched import scheduler
import time
import logging
import asyncore
import socket
import os
import fcntl

#
# This design uses two threads -- one for the queue/processing and one for timers.
#

STOP = object()

ENTRY = logging.getLogger('shotlib.events.Entry')

class Entry(object):
    def __init__(self, delay, func, args, kwargs):
        self.delay = delay
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def invoke(self):
        try:
            self.func(*self.args, **self.kwargs)
            del self.func
            del self.args
            del self.kwargs
            del self.delay
        except:
            ENTRY.error('Exception processing scheduled event', exc_info=True)

class EventQueue(object):
    def __init__(self, maxsize, name='EventQueue', loggername='EventQueue'):
        self.name = name
        self.thread = Thread(name=name, target=self.run)        
        self._queue = Queue(maxsize)
        self._stop = False
        self.log = logging.getLogger(loggername)

    def start(self):
        self.log.debug('Starting %s', self.name)
        self.thread.start()

    def stop(self):
        self.log.debug('Queueing stop for %s', self.name)
        self._queue.put(STOP)

    def wrap(self, function):    
        def send(*args, **kwargs):
            self.queue(function, *args, **kwargs)
        return send

    def wrap_async(self, function):
        def invoke(callback, *args, **kwargs):
            callback(function(*args, **kwargs))
        return self.wrap(invoke)

    def run(self):
        while not self._stop:
            event = self._queue.get(True)
            if event is STOP:
                self._stop = True
                break
            elif event:
                try:
                    handle, args, kwargs = event
                    handle(*args, **kwargs)
                except:
                    self.log.error('Exception for %s handler', self.name, exc_info=True)
        self.log.debug('%s STOPPED', self.name)

class Scheduler(EventQueue):
    def __init__(self, maxsize=0, name='Scheduler'):
        super(Scheduler, self).__init__(maxsize=maxsize,
                                        name='Scheduler <%s>' % name,
                                        loggername='Scheduler.%s' % name)
        self.sched = scheduler(time.time, self.delay)

    def schedule(self, delay, func, *args, **kwargs):
        self._queue.put(Entry(delay, func, args, kwargs))

    def delay(self, t):
        now = time.time()
        begin = now
        then = now + t
        while now <= then:
            try:
                entry = self._queue.get(True, then - now)
            except Empty:
                break
            if entry is STOP:
                self._stop = True
            elif entry:
                self.sched.enter(entry.delay, 1, entry.invoke, ())
                break
            now = time.time()
            
    def run(self):
        self.log.info('%s started', self.name)
        while not self._stop:
            self.sched.run()
            if self._stop:
                break
            entry = self._queue.get(True)
            if entry is STOP:
                self._stop = True
                break
            elif entry:
                self.sched.enter(entry.delay, 1, entry.invoke, ())
        self.log.info('%s stopped', self.name)
        
class EventProcessor(EventQueue):
    "An event processing loop with support for delayed event injection"
    def __init__(self, maxsize=0, name='EventProcessor', loggername='EventProcessor'):
        super(EventProcessor, self).__init__(maxsize=maxsize, name=name, loggername=loggername)
        self.scheduler = Scheduler()

    def start(self):
        super(EventProcessor, self).start()
        self.scheduler.start()

    def stop(self):
        super(EventProcessor, self).stop()
        self.scheduler.stop()

    def queue(self, func, *args, **kwargs):
#        self.log.debug('Queue: %s(*%s, **%s)', func, args, kwargs)
        self._queue.put((func, args, kwargs))

    def schedule(self, delay, func, *args, **kwargs):
        self.scheduler.schedule(delay, self.queue, func, *args, **kwargs)

    def delay_wrap(self, function):
        def schedule(delay, *args, **kwargs):
            self.schedule(delay, function, *args, **kwargs)
        return schedule

    def wrap_delay_async(self, function):
        return self.delay_wrap(self.wrap_async(function))
        


def nonblockify(fd):
    flags = fcntl.fcntl(fd, fcntl.F_GETFL, 0)
    flags = flags | os.O_NONBLOCK
    fcntl.fcntl(fd, fcntl.F_SETFL, flags)
    

class WakeupChannel(asyncore.dispatcher):
    def __init__(self):
        r, w = os.pipe()
        nonblockify(r)
        nonblockify(w)
        self._fileno = r
        self._writefd = w
        self.wakeupn = 0

    def wakeup(self):
        os.write(self._writefd, ' ')

    def readable(self):
        return True

    def writable(self):
        return False

    def close(self):
        self.del_channel()
        os.close(self._fileno)
        os.close(self._writefd)

    def handle_read_event(self):
        os.read(self._fileno, 1)

class EventIOProcessor(EventProcessor):
    def __init__(self, *args, **kwargs):
        super(EventIOProcessor, self).__init__(*args, **kwargs)
        self.fdmap = {}
    
    def start(self):
        self.wakeupchannel = WakeupChannel()
        self.wakeupchannel.add_channel(map=self.fdmap)
        super(EventIOProcessor, self).start()

    def queue(self, func, *args, **kwargs):
        super(EventIOProcessor, self).queue(func, *args, **kwargs)
        self._wakeup()
        

    def _wakeup(self):
        self.wakeupchannel.wakeup()

    def stop(self):
        super(EventIOProcessor, self).stop()
        self._wakeup()

    def run(self):
        self.log.info('%s starting', self.name)
        while not self._stop:
            try:
                event = self._queue.get(True, 0.05)
                while event:
                    if event is STOP:
                        self._stop = True
                        raise Empty
                    elif event:
                        try:
                            handle, args, kwargs = event
                            handle(*args, **kwargs)
                        except:
                            self.log.error('Exception for %s handler', self.name, exc_info=True)
                    event = self._queue.get(False)
            except Empty:
                pass
            asyncore.poll(timeout=30.0, map=self.fdmap)
        self.log.info('%s stopping', self.name)
        

if __name__ == '__main__':
    import sys
    logger = logging.getLogger()
    hndlr = logging.StreamHandler(sys.stdout)
    hndlr.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
    logger.addHandler(hndlr)
    logger.setLevel(logging.DEBUG)
    processor = EventProcessor()
    processor.start()
    def out(msg):
        logger.info(msg)

    def runout(processor):
        qout = processor.wrap(out)
        sout = processor.delay_wrap(out)
        sout(1.5, 'Hello hodor 3!')
        sout(.5, 'Hello hodor 1!')
        sout(1.0, 'Hello hodor 2!')
        qout('Pants')
        processor.schedule(1.0, sout, .7, 'Hello hodor 4!')        
        processor.schedule(3.5, processor.stop)
        qout('Ugly')
        processor.thread.join()

    runout(processor)
    eioprocessor = EventIOProcessor(name='EIOProcessor')
    eioprocessor.start()
    runout(eioprocessor)


    for t in threading.enumerate():
        print t
