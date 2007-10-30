__all__ = ['RequestContext', 'enter', 'exit', 'context']

from threading import local

class RequestContext(object):
    def __init__(self, request):
        self.request = request
        self.user = None
        self.storage = None

    def url(self, path):
        "If path is server relative, return a app-root rooted url, if it's a full url then return it"
        # TODO: implement me
        return self.request.full_url(path)
        
__context = local()

def enter(context):
    __context.ctx = context


def exit():
    del __context.ctx


def context():
    return __context.ctx
