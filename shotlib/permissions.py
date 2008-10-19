from shotlib.context import context
from shotlib.enums import create_enum
from shotlib.util import copyinfo

class PermissionDenied(Exception):
    pass

class Permission(object):
    def __init__(self):
        self.name = 'unknown'

    def _create_perm(self):
        def execute(target, user=None):
            return self.test(target, user=user)
        execute.require = self.require
        execute.verify = self.verify
        execute.test = self.test
        return execute

    def __call__(self, func):
        self.__base = func
        self.name = func.__name__     
        return self._create_perm()

    @property
    def context(self):
        return context()

    def test(self, target, user=None):
        assert target is not None
        if user is None:
            user = self.context.user
        try:
            tgt = getattr(target, '_permission_' + self.name)
        except AttributeError:
            return self.__base(target, user)
        return tgt(user)
    
    def require(self, target):
        if target is None:
            raise PermissionDenied("%s cannot %s %s" % (self.context.user, self.name, target))
        if not self.test(target):
            raise PermissionDenied("%s cannot %s %s" % (self.context.user, self.name, target))
        return target

    def verify(self, func):
        @copyinfo(func)
        def _verify(*args, **kwargs):
            return self.require(func(*args, **kwargs))
        return _verify

    def requires(self, func):
        @copyinfo(func)
        def wrapped(*args, **kwargs):
            self.require(args[0])
            return func(*args, **kwargs)
        return wrapped
