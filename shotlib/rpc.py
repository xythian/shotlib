
#
# Simple message-transport-based RPC
#
import inspect, sys, traceback
from functools import partial

class ExportedMethod(object):
    def __init__(self, func):
        self.func = func
        self.argspec = inspect.getargspec(func)
        args = list(self.argspec.args)
        defs = self.argspec.defaults
        # kwargs are the last n args, where n is the length of defs
        kwargs = []
        if defs:
            kwargs = args[-len(defs):]
            args = args[:-len(defs)]
        if args and args[0] == 'self':
            del args[0]
        self.args = tuple(args)
        self.kwargs = tuple(kwargs)

export = ExportedMethod

def generate_dispatch(spec, wrapper=None):
    def dispatch(self, datum):
        method = datum.get('.method')
        if not method:
            return {'.code' : 400, '.message' : 'Missing .method'}
        target = spec.get(method)
        if not target:
            return {'.code' : 404, '.message' : '.method %s not found' % method}
        args = [self] + [datum.get(arg) for arg in target.args]
        kwargs = dict((arg, datum.get(arg)) for arg in target.kwargs)
        try:
            if wrapper and kwargs:
                return {'.code' : 200, '.result' : wrapper(self, partial(target.func, *args, **kwargs))}
            elif wrapper:
                return {'.code' : 200, '.result' : wrapper(self, partial(target.func, *args))}                
            if kwargs:
                return {'.code' : 200, '.result' : target.func(*args, **kwargs)}
            else:
                return {'.code' : 200, '.result' : target.func(*args)}
        except:
            return {'.code' : 500, '.message' : 'Exception in call of %s' % method,
                    '.traceback' : traceback.format_exc()}
    return dispatch

class RPCException(Exception):
    pass

class Client(object):
    def __init__(self, transport):
        self.transport = transport

    def invoke(self, datum):
        result = self.transport.send(datum)
        code = result.get('.code', 500)
        if code == 200:
            return result['.result']
        elif code == 400:
            raise RPCException('Message missing .method')
        elif code == 404:
            raise NameError(result['.message'])
        elif code == 500:
            raise RPCException('Remote exception: %s\n%s' % (result['.message'], result['.traceback']))
        else:
            raise RPCException('Unknown result code: %d' % code)

def generate_client(name, spec):
    output = []
    output.append("class %sClient(Client):" % name)
    for key, func in spec.items():
        output.append("   def %s%s:" % (key, inspect.formatargspec(*func.argspec)))
        mname = repr(key)
        margs = ",".join("%s : %s" % (repr(arg), arg) for arg in func.args + (func.kwargs if func.kwargs else ()))
        if margs:
            margs = "," + margs
        output.append("      return self.invoke({'.method' : %s%s})" % (mname, margs))
    lcs = {'Client' : Client}
    exec "\n".join(output) in lcs
    return lcs[name + 'Client']

class EndpointMeta(type):
    @classmethod
    def process_properties(cls, name, bases, dct):
        spec = {}
        #print cls, name, bases, dct
        for key, val in dct.items():
            if isinstance(val, ExportedMethod):
                spec[key] = val
                # the endpoint represents the server-side, leave the bare method
                dct[key] = val.func
                # we will define a "dispatch" method on the server class
                # and generate a client class
        if spec:
            # generate a dispatch method            
            dct['dispatch'] = generate_dispatch(spec, wrapper=dct.get('dispatch'))
            # generate a client class
            dct['Client'] = generate_client(name, spec)
    def __new__(cls, name, bases, dict):
        cls.process_properties(name, bases, dict)
        return super(EndpointMeta, cls).__new__(cls, name, bases, dict)


class Endpoint(object):
    __metaclass__ = EndpointMeta
    __slots__ = []

    def __init__(self):
        # so super calls don't fail
        pass

class LocalTransport(object):
    def __init__(self, target, echo=False):
        self.target = target
        self.echo = echo

    def send(self, datum):
        if self.echo:
            print 'IN: ', datum
        result = self.target.dispatch(datum)
        if self.echo:
            print 'OUT: ', result
        return result

try:
    import zmq
    import eventlet, uuid
    from eventlet.event import Event
    from eventlet.timeout import Timeout

    def zmqproxy_client(sock, timeout=600):
        work = {}
        def _sendrecv(msg):
            key = uuid.uuid4().bytes
            work[key] = Event()
            sock.send_multipart([key, '', msg])
            try:
                with Timeout(timeout):
                    return work[key].wait()
            finally:
                work.pop(key, None)
            
        def _dispatch_responses():
            while True:
                msg = sock.recv_multipart()
                key = msg[0]
                body = msg[-1]
                evt = work.pop(key, None)
                if evt:
                    evt.send(body)
        eventlet.spawn_n(_dispatch_responses)
        return _sendrecv


    import msgpack
    class ZMQTransportClient(object):
        def __init__(self, reqsock=None):
            self._sock = reqsock

        def send(self, datum):
            self._sock.send(msgpack.packb(datum))
            return msgpack.unpackb(self._sock.recv())

    class ZMQTransportServer(object):
        def __init__(self, service, repsock=None):
            self._sock = repsock
            self.target = service

        def run(self):
            while True:
                self._sock.send(msgpack.packb(self.target.dispatch(msgpack.unpackb(self._sock.recv()))))


    def zmqpush_client(sock, result=None):
        def _push(msg):
            sock.send(msg)
            return result
        return _push

    def zmqproxy_server(sock, pool, handle_request):
        while True:
            message = sock.recv_multipart()        
            msg = message[-1]
            routing = message[:-1]
            def send(resp):
                sock.send_multipart(routing + [resp])
            pool.spawn_n(handle_request, msg, send)

    class EventletZMQTransportClient(object):
        def __init__(self, xreqsock=None, pushsock=None, timeout=600):
            if xreqsock:
                self._sender = zmqproxy_client(xreqsock, timeout=timeout)
            elif pushsock:
                self._sender = zmqpush_client(pushsock, result=msgpack.packb(0))
            else:
                raise Exception('No socket specified')

        def send(self, datum):
            return msgpack.unpackb(self._sender(msgpack.packb(datum)))

    class EventletZMQTransportServer(object):
        def __init__(self, service, xrepsock=None, pool=None, workers=50):
            self.service = service
            if not pool:
                pool = eventlet.GreenPool(workers)
            eventlet.spawn_n(zmqproxy_server, xrepsock, pool, self.handle_request)

        def handle_request(self, message, reply):
            reply(msgpack.packb(self.service.dispatch(msgpack.unpackb(message))))

except ImportError:
    pass

#
# regular/greenthread transports
# HTTP/zmq REQ/REP transports
#

def run_tests():
    class MyService(Endpoint):
        @export
        def some_method(self, foo, bar, zot=None):
            return foo + bar

        @export
        def fail(self):
            return 1 / 0

    svc = MyService()
    client = MyService.Client(LocalTransport(svc))
    if client.some_method(1, 2, 3) != 3:
        raise Exception("This should be 3")
    try:
        print client.fail()
        raise Exception('This should fail')
    except RPCException, e:
        pass

if __name__ == '__main__':
    # tests!
    run_tests()
