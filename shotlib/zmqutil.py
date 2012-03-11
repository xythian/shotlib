
#
# misc support/utility code for zmq
# 

import zmq

def logging_proxy(frontend, backend, target):
    def _sender():
        while True:
            backend.send_multipart(frontend.recv_multipart())
    eventlet.spawn_n(_sender)
    while True:
        chunks = backend.recv_multipart()
        target.send(chunks[-1])
        frontend.send_multipart(chunks)



def request_pipeline(pool, work, pull, push):
    "Receive requests to fetch from a PULL socket.  Send results to PUSH socket."
    while True:
        msg = msgpack.unpackb(pull.recv())
        pool.spawn_n(work, push, msg)

def log_pipeline(pull, push, target):
    while True:
        msg = pull.recv()
        target.send(msg)
        push.send(msg)

def iterify(val):
    return val if isinstance(val, (list, tuple)) else (val,)

def create_sock(ctx, kind, connect=(), bind=(), subscribe=()):
    connect = iterify(connect)
    bind = iterify(bind)
    subscribe = iterify(subscribe)
    sock = ctx.socket(kind)
    for addr in bind:
        sock.bind(addr)
    for addr in connect:
        sock.connect(addr)
    for val in subscribe:
        sock.setsockopt(zmq.SUBSCRIBE, val)
    return sock

