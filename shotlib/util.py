__all__ = ['demand_property',
           'demandprop',
           'copyinfo']

from functools import wraps

copyinfo = wraps

def demand_property(name, loadfunc):
    def _get_demand_property(self):
        try:
            return getattr(self, '_%s_value' % name)
        except AttributeError:
            v = loadfunc(self)
            setattr(self, '_%s_value' % name, v)
            return v
    def _flush_demand_property(self):
        try:
            delattr(self, '_%s_value' % name)
        except AttributeError:
            # ok, not loaded
            pass
        
    return property(_get_demand_property, None, _flush_demand_property, doc=loadfunc.__doc__)

def demandprop(func):
    return demand_property(func.__name__, func)

def zip_attrs(t, row, *names):
    for i, name in enumerate(names):
        setattr(t, name, row[i])
