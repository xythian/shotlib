from shotlib.service import Service
from shotlib import events

import uuid
import asyncore
import logging
import spread
from xmlrpclib import Fault, dumps, loads

LOG = logging.getLogger('shotlib.spreadutil')

class SpreadedService(Service):
    """A Service that also connects an mbox to a spread group"""

    def option_parser(self):
        parser = super(SpreadedService, self).option_parser()
        parser.add_option("-u", "--spreaduser", action="store",
                          default=None, dest="spreaduser",
                          help="Spread username")
        parser.add_option("-s", "--server", action="store",
                          default="4803@localhost", dest="server",
                          help="Spread server to connect to")
        return parser

class SpreadChannel(asyncore.dispatcher):
    "Wraps a Spread mbox in an asyncore channel"
    
    debug = 0
    connected = 0
    accepting = 0
    closing = 0
    addr = None

    def __init__(self, mbox, receive, map=None):
        self.mbox = mbox
        self.connected = 1
        self.receive = receive
        self._fileno = mbox.fileno()
        self.add_channel(map=map)
        asyncore.dispatcher.__init__(self, map=map)

    def __repr__(self):
        status = [self.__class__.__module__+"."+self.__class__.__name__]
        status.append('Spread mbox: ')
        status.append(repr(self.mbox))
        # On some systems (RH10) id() can be a negative number. 
        # work around this.
        MAX = 2L*sys.maxint+1
        return '<%s at %#x>' % (' '.join(status), id(self)&MAX)

    def readable(self):
        return True

    def writable(self):
        return False

    def handle_read_event(self):
        try:
            message = self.mbox.receive()
            self.receive(message)
        except:
            LOG.error('Uncaptured SpreadChannel exception', exc_info=True)

    def close(self):
        self.del_channel()
        self.mbox.disconnect()

    def log(self, message):
        LOG.info(message)

    def log_info(self, message, type='info'):
        LOG.info(message)        

class QueuedProxy(object):
    def __init__(self, target, processor, methods=()):
        self.__target = target
        self.__processor = processor
        for method in methods:
            f = getattr(self.__target, method)
            setattr(self, method, processor.wrap(f))


class SpreadedIOService(SpreadedService):
    def signal_term(self, signum, frame):
        self.quit = True
        self.processor.stop()

    signal_int = signal_term
    
    def run(self):
        self.log('Connect to %s as %s...', self.options.server, self.options.spreaduser)
        if self.options.spreaduser:
            self.mbox = spread.connect(self.options.server, self.options.spreaduser)
        else:
            self.mbox = spread.connect(self.options.server)            
        self.log('Initializing event loop...')
        self.processor = events.EventIOProcessor(name=self.name)
        self.chan = SpreadChannel(self.mbox, self.receive_message, map=self.processor.fdmap)
        self.endpoint = SpreadMessageEndpoint(QueuedProxy(self.mbox, self.processor, methods=('join',
                                                                                              'leave',
                                                                                              'multicast',
                                                                                              'multigroup_multicast')))
        self.private_group = self.mbox.private_group
        self.endpoint.private_group = self.private_group
        self.processor.start()
        self.processor.queue(self.handle_startup)
        self.processor.thread.join()
        self.chan.close()
        self.handle_cleanup()
        
    def receive_message(self, message):
        self.processor.queue(self.endpoint.receive, message)

    def handle_cleanup(self):
        self.mbox.disconnect()
        del self.mbox
        

    def handle_startup(self):
        pass

class SpreadMessageReceiver(object):
    def __init__(self, mbox):
        self.mbox = mbox
        self.handlers = {}

    def send(self, group, message, type=0):
        if isinstance(group, tuple):
            self.mbox.multigroup_multicast(spread.SAFE_MESS, group, message, type)
        else:
            self.mbox.multicast(spread.SAFE_MESS, group, message, type)            

    def register(self, type, handler):
        self.handlers[type] = handler

    def remove(self, type):
        del self.handlers[type]
        
    def receive(self, msg):
        handler = self.handlers.get(msg.msg_type)
        if handler:
            handler(msg)
            return True
        else:
            return False

class SpreadMessageGroup(SpreadMessageReceiver):
    def __init__(self, mbox):
        super(SpreadMessageGroup, self).__init__(mbox)
        self.members = ()

    def joined(self, members):
        pass

    def left(self, members):
        pass

    def update_members(self, members):
        self.members = members

    def receive_membership(self, msg):
        if msg.reason == 0:
            # transitional message?
            pass
        elif msg.reason == spread.CAUSED_BY_JOIN:
            self.joined(msg.extra)
        elif msg.reason == spread.CAUSED_BY_LEAVE:
            self.left(msg.extra)
        elif msg.reason == spread.CAUSED_BY_DISCONNECT:
            self.left(msg.extra)
        elif msg.reason == spread.CAUSED_BY_NETWORK:
            # don't do anything for this yet
            pass
        self.update_members(msg.members)            

class SpreadMessageEndpoint(SpreadMessageReceiver):
    def __init__(self, mbox):
        super(SpreadMessageEndpoint, self).__init__(mbox)
        self.groups = {}

    def join(self, groupname):
        LOG.info('Joining %s', groupname)
        group = SpreadMessageGroup(self.mbox)
        self.groups[groupname] = group
        self.mbox.join(groupname)
        return group

    def leave(self, groupname):
        del self.groups[groupname]
        self.mbox.leave(groupname)

    def receive_membership(self, msg):
        group = self.groups.get(msg.group)
        if group:
            group.receive_membership(msg)

    def receive(self, msg):
        if hasattr(msg, 'reason') and (msg.reason & spread.MEMBERSHIP_MESS):
            self.receive_membership(msg)
            return
        for group in msg.groups:
            if group == self.private_group:
                if super(SpreadMessageEndpoint, self).receive(msg):
                    return
            else:
                g = self.groups.get(group)
                if g:
                    if g.receive(msg):
                        return
            if super(SpreadMessageEndpoint, self).receive(msg):
                return


SPREADXMLRPC = 1000
SPREADXMLRPC_R = 1001

class SpreadXMLRPCServer(object):
    def __init__(self, processor, endpoint):
        self.endpoint = endpoint
        self.processor = processor
        self.endpoint.register(SPREADXMLRPC, self.receive)
        self.handlers = {}        

    def reply(self, sender, msgid, result):
        self.endpoint.send(sender, msgid + dumps((result,), methodresponse=1), type=SPREADXMLRPC_R)

    def receive(self, msg):
        msgid = msg.message[:36]
        body = msg.message[36:]
        sender = msg.sender
        def respond(data):
            self.reply(sender, msgid, data)
        params, methodname = loads(body)
        handler = self.handlers.get(methodname)
        if handler:
            handler(respond, *params)
        else:
            respond(Fault('No such method ' + methodname))
        return True
        
    def register_handler(self, name, method):
        def wrapper(respond, *args):
            respond(method(*args))
        self.handlers[name] = wrapper

    def register_async(self, name, method):
        self.handlers[name] = method

class Timeout(Fault):
    def __init__(self):
        Fault.__init__(self, 500, "Timeout")

class SpreadXMLRPCClientRequest(object):
    def timeout(self):
        try:
            self.send_error(Timeout(), None)
        finally:
            self.finish()

    def receive(self, msg):
        body = msg.message[36:]
        try:
            data, methodname = loads(body)
            self.send_result(data)                
        except:
            t, v = sys.exc_type, sys.exc_value
            self.send_error(t, v)
        self.finish()

class SpreadXMLRPCScatterRequest(object):
    def timeout(self):
        try:
            self.send_result(tuple(self.results), tuple(self.errors))
            del self.results
            del self.errors
        finally:
            self.finish()

    def receive(self, msg):
        body = msg.message[36:]
        try:
            data, methodname = loads(body)
            self.results.append((msg.sender, data))
        except:
            t, v = sys.exc_type, sys.exc_value
            self.errors.append((msg.sender, t, v))

def generate_uuid():
    return str(uuid.uuid4())

class SpreadXMLRPCClient(object):
    def __init__(self, processor, endpoint):
        self.endpoint = endpoint
        self.processor = processor
        self.endpoint.register(SPREADXMLRPC_R, self.receive)
        self.pending_requests = {}

    def receive(self, msg):
        msgid = msg.message[:36]
        request = self.pending_requests.get(msgid)
        if request:
            request.receive(msg)

    def send_request(self, groups, request, data, timeout):
        def finish():
            try:
                del self.pending_requests[request.msgid]
            except KeyError:
                pass
        request.msgid = generate_uuid()
        self.pending_requests[request.msgid] = request
        request.finish = finish
        self.processor.schedule(timeout, request.timeout)
        self.endpoint.send(groups, request.msgid + data, type=SPREADXMLRPC)

    def invoke(self, callback, e_callback, groups, methodname, params, timeout=5.0):
        request = SpreadXMLRPCClientRequest()
        request.send_error = e_callback
        request.send_result = callback
        data = dumps(params, methodname)
        self.send_request(groups, request, data, timeout)

    def scatter(self, callback, groups, methodname, params, timeout=1.0):
        request = SpreadXMLRPCScatterRequest()
        request.send_result = callback
        request = self.init_request(request)
        data = dumps(params, methodname)
        self.send_request(groups, request, data, timeout)

    def make_proxy(self, channel, name, timeout=5.0, scatter=0.5):
        def invoke(callback, e_callback, *args):
            self.invoke(callback, e_callback, channel, name, args, timeout=timeout)
        def scatter(callback, *args):
            self.scatter(callback, channel, name, args, timeout=scatter)
        invoke.scatter = scatter
        return invoke
        
if __name__ == '__main__':
    class TestSpreadedIOService(SpreadedIOService):
        def handle_startup(self):
            self.xmlrpc = SpreadXMLRPCServer(self.processor, self.endpoint)
            self.xmlrpc.register_handler('testSpread.echo', self.echo)
            self.xmlrpc.register_async('testSpread.async', self.async)
            self.client = SpreadXMLRPCClient(self.processor, self.endpoint)
            self.endpoint.join('teh.awesome')
            def c1(response):
                LOG.info('response %s', response[0])
            def e1(e, v):
                LOG.info('error %s, %s', e, v)
            echoproxy = self.client.make_proxy('teh.awesome', 'testSpread.echo')
            asyncproxy = self.client.make_proxy('teh.awesome', 'testSpread.async')            
            echoproxy(c1, e1, 'one two three four five')
            asyncproxy(c1, e1, 0.8)
            asyncproxy(c1, e1, 6.0)
            self.processor.schedule(7.0, self.processor.stop)
               
        def echo(self, message):                
            return 'echoing ' + message

        def async(self, respond, t1):
            self.processor.schedule(t1, respond, 'respond')
        
    service = TestSpreadedIOService()
    service.main()




