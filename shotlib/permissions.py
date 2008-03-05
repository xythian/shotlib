__all__ = ['PermissionDenied', 'Roles', 'Rule', 'Permission', 'Principal', 'all']

from shotlib.context import context
from shotlib.util import copyinfo
from shotlib.enums import create_enum
from contextlib import contextmanager

import logging

LOG = logging.getLogger('shotlib.permissions')

class PermissionDenied(Exception):
    pass

Roles = create_enum('Role',
                    'NONE',
                    'READ',
                    'WRITE',
                    'MODERATE',
                    'ADMIN',
                    'SUPERUSER')

Roles.roles = Roles.values
Roles.implied = {}
for role in Roles.roles:
    Roles.implied[role] = tuple(Roles.roles[:role]) + (role,)

all = []

def _type_compare(first, second):
    ft = isinstance(first.target, type)
    st = isinstance(second.target, type)
    if ft and not st:
        return -1
    elif st and not ft:
        return 1
    elif not ft and not st:
        return 0

    if first.target in second.target.__mro__:
        return 1
    elif second.target in first.target.__mro__:
        return -1
    else:
        return cmp(len(first.target.__mro__), len(second.target.__mro__))

def _pred_compare(first, second):
    fp = bool(first.predicate)
    sp = bool(second.predicate)
    if fp == sp:
        return 0
    elif fp and not sp:
        return 1
    else:
        return -1
    
def rule_compare(first, second):
    r = _type_compare(first, second)
    if r == 0:
        return _pred_compare(first, second)
    else:
        return r
        
class Rule(object):
    def __init__(self, target, predicate, rule):
        self.target = target
        if not predicate:
            self.predicate = lambda x: True
        else:
            self.predicate = predicate
        self.rule = rule

    def __call__(self, target, user):
        return self.rule(target, user)

    def match(self, target):
        if self.target == target:
            return self.predicate(target)
        elif isinstance(self.target, type):
            return isinstance(target, self.target) and self.predicate(target)
        else:
            return False
    def __str__(self):
        return "<rule: %s %s>" % (str(self.target),
                                  str(self.predicate))

    __repr__ = __str__


class Permission(object):
    def __init__(self):
        self.rules = []
        all.append(self._create_perm())

    def _create_perm(self):
        def execute(target):
            return self.test(target)
        execute.rule = self.rule
        execute.require = self.require
        execute.contextrule = self.contextrule
        return execute


    def __call__(self, func):
        self.__base = func
        self.name = func.__name__
        return self._create_perm()

    @property
    def context(self):
        return context()

    def test(self, target):
        assert target is not None
        LOG.debug("Testing %s for %s on %s", self.name, self.context.user, target)
        for rule in self.rules:            
            code = rule.rule.func_code
            filename, lineno = code.co_filename, code.co_firstlineno
            if rule.match(target):
                result = rule(target, self.context.user)
                LOG.debug("Rule %s:%s matched (%s)", filename, lineno, result)
                return result
            else:
                LOG.debug("   Rule %s:%s did not match", filename, lineno)
        return self.__base(target, self.context.user)

    def rule(self, clazz, predicate=None):
        def _add(func):
            self.rules.append(Rule(clazz, predicate, func))
            self.rules.sort(rule_compare, reverse=True)
            return func
        return _add

    def contextrule(self, instance):
        def _add(func):
            heappush(context().perms[self], Rule(instance, predicate, func))
            return func
        return _add

    def require(self, target):
        if not self.test(target):
            raise PermissionDenied("%s cannot %s %s" % (self.context.user, self.name, target))
        return target

    def requires(self, func):
        @copyinfo(func)
        def wrapped(*args, **kwargs):
            self.require(args[0])
            return func(*args, **kwargs)
        return wrapped

class Principal(object):
    level = Roles.NONE
    @property
    def roles(self):
        return Roles.implied[self.level]

if __name__ == '__main__':
    from shotlib.context import with_user

    def go():
        class MyContext(object):
            def __init__(self, user):
                self.user = user

        class Frobazz(object):    pass
        class Hork(Frobazz):      pass
        class Snort(Hork):      pass    
        class Barfle(object):     pass

        f = Frobazz()
        h = Hork()
        b = Barfle()
        s = Snort()
        s2 = Snort()

        bob = 'bob'
        sue = 'sue'

        @Permission()
        def poke(target, user):
            return False

        @Permission()
        def prod(target, user):
            return False

        @poke.rule(Hork)
        def __bob1(target, user):
            return user is bob

        def test_sue():
            assert not poke(h)
        def test_bob():
            assert (not poke(f))
            assert (poke(h))
            assert (poke(s))
            assert (not poke(b))
        with_user(sue, test_sue)
        with_user(bob, test_bob)

        def test_sue2():
            assert not poke(h)
            assert not poke(s2)
            assert poke(s)

        @poke.rule(s)
        def __sue1(target, user):
            return user is sue
        with_user(sue, test_sue2)
    go()
