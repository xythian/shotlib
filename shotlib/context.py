from __future__ import with_statement
__all__ = ['RequestContext', 'enter', 'exit', 'context', 'push', 'pop']

from threading import local
from contextlib import contextmanager

class RequestContext(object):
    def __init__(self, user=None, request=None, **kw):
        self.request = request        
        self.user = user
        for key, val in kw.items():
            setattr(self, key, val)

    def url(self, path):
        if self.request:
            return self.request.full_url(path)
        else:
            return path
        
__context = local()

def enter(context):
    __context.ctx = context


def exit():
    del __context.ctx


def context():
    try:
        return __context.ctx
    except AttributeError:
        return None

def push(ctx):
    if hasattr(__context, 'ctx'):
        if not hasattr(__context, 'stack'):
            __context.stack = []
        __context.stack.append(__context.ctx)
    __context.ctx = ctx

def pop():
    if hasattr(__context, 'stack'):
        __context.ctx = __context.stack.pop()
    else:
        del __context.ctx

class _PrincipalContext(object):
    def __init__(self, user):
        self.user = user
        self.perms = []

@contextmanager
def with_context(ctx, func, *args, **kwargs):
    push(ctx)
    try:
        return func(*args, **kwargs)
    finally:
        pop()

@contextmanager
def with_user(user, func, *args, **kwargs):
    ctx = context()
    if ctx:
        oldu = ctx.user
        ctx.user = user
        try:
            return func(*args, **kwargs)
        finally:
            ctx.user = oldu
    else:
        return with_context(_PrincipalContext(user), func, *args, **kwargs)
