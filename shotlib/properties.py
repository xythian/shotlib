"""This module has slowly been accumulating various utility functions."""

__all__ = ['AutoPropertyMeta',
           'ViewMeta',
           'trace',
           'demand_property',
           'config_property',
           'delegate_property',
           'RecordBodyMeta',
           'PackedRecord']

from shotlib.util import demand_property

import sys
from ConfigParser import ConfigParser
import logging
from struct import pack, unpack, calcsize
from datetime import tzinfo, datetime, timedelta
import time as _time

LOG = logging.getLogger('shotlib.properties')
LOG_TRACE = logging.getLogger('shotlib.trace')


class AutoPropertyMeta(type):
    def process_properties(cls, name, bases, dict):
        for key, val in dict.items():
            if key.startswith('_get_') and callable(val):
                pname = key[5:]
                if dict.has_key(pname):
                    continue
                getter = val
                if dict.has_key('_set_' + pname):
                    setter = dict['_set_' + pname]
                    prop = property(getter, setter)
                else:
                    prop = property(getter)
                dict[pname] = prop
            elif key.startswith('_load_') and callable(val):
                pname = key[6:]
                if dict.has_key(pname):
                    continue
                dict[pname] = demand_property(pname, val)
    process_properties = classmethod(process_properties)
    def __new__(cls, name, bases, dict):
        cls.process_properties(name, bases, dict)
        return super(AutoPropertyMeta, cls).__new__(cls, name, bases, dict)
    

class ViewMeta(AutoPropertyMeta):
    def process_properties(cls, name, bases, dict):
        super(ViewMeta, cls).process_properties(name, bases, dict)

        def any_has(name):
            for b in bases:
                if hasattr(b, name):
                    return True
            return False
        def process_of(c):
            if c == type:
                return
            for key, val in c.__dict__.items():
                if key.startswith('_'):
                    continue
                elif any_has(name):
                    continue
                elif dict.has_key(key):
                    continue
                def make_getter(key):
                    def getter(self):
                        return getattr(self._of, key)
                    return getter
                dict[key] = property(make_getter(key))
            for b in c.__bases__:
                process_of(b)
        try:
            view_of = dict['__of__']
            process_of(view_of)                        
        except KeyError:
            pass
    process_properties = classmethod(process_properties)    


def trace(v, *args):
    if LOG_TRACE.isEnabledFor(logging.DEBUG):
        LOG_TRACE.info(v, *args)

def wrap_printexc(func):
    def wrap_func(*args, **kw):
        trace("wrap_func: %s(%s)", func, args)
        try:
            return func(*args, **kw)
        except:
            import traceback
            traceback.print_exc()
            return None
    return wrap_func

def config_property(key, section='DEFAULT', get='get'):
    def get_config_property(self):
        trace('config_property %s.%s [%s]', self, key, section)
        return getattr(self.config, get)(section, key)
    return property(get_config_property)

def delegate_property(name, propname):
    def _delegate_get(self):
        trace('delegate_get %s.%s.%s', self, name, propname)
        o = getattr(self, name)
        if o:
            return getattr(o, propname)
        else:
            return o

    def _delegate_set(self, v):
        o = getattr(self, name)
        if o:
            setattr(o, propname, v)

    def _delegate_del(self):
        o = getattr(self, name)
        if o:
            delattr(o, propname)

    return property(_delegate_get, _delegate_set, _delegate_del)

class RecordBodyMeta(type):
    def __new__(cls, name, bases, classdict):
        try:
            fields = classdict['__fields__']
        except KeyError:
            return type.__new__(cls, name, bases, classdict)
        try:
            fmt = classdict['_format']
            if fmt:
                classdict['_length'] = calcsize(fmt)
        except KeyError:
            pass
        for i, columnname in enumerate(fields):
            def makeproperty(i, name, classdict):
                def getter(self):
                    return self._fields[i]
                def setter(self, v):
                    self._fields[i] = v
                classdict[columnname] = property(getter, setter, None)
            if classdict.has_key(columnname):
                raise TypeError, ("RecordBody class can't define %s" % columnname,)
            makeproperty(i, columnname, classdict)
        classdict['fieldCount'] = len(fields)        
        return type.__new__(cls, name, bases, classdict)

class PackedRecord(object):
    __metaclass__ = RecordBodyMeta
    __slots__ = ['_fields', '_length']

    _format = ''

    def __init__(self, _data='', **kwargs):
        self._fields = [0] * self.fieldCount        
        if _data:
            self._fields = self.unpack(_data)
        elif kwargs:
            for name, kval in kwargs.items():
                setattr(self, name, kval)
        else:
            self._fields = [0] * self.fieldCount

    @classmethod
    def read(cls, data, offset, saveraw=False):
        d = data[offset:offset+cls._length]
        o = cls(_data=d)
        if saveraw:
            o._raw = d
        return offset + cls._length, o

    def unpack(self, data):
        return list(unpack(self._format, data))

    def pack(self):
        return pack(self._format, *self._fields) 
