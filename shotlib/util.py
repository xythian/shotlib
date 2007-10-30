__all__ = ['columnproperty',
           'demand_property',
           'demandprop',
           'copyinfo']

def columnproperty(idx, col, doc=None):
    def get(self):
        return col.from_sql(self._data[idx])
    def set(self, v):
        self._data[idx] = col.to_sql(v)
    def delete(self):
        self._data[idx] = None
    return property(get, set, delete, doc=doc)

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

def copyinfo(func):
    def do_wrap(wrapped):
        wrapped.__doc__ = func.__doc__
        wrapped.__name__ = func.__name__
        wrapped.__dict__ = func.__dict__
        return wrapped
    return do_wrap

def zip_attrs(t, row, *names):
    for i, name in enumerate(names):
        setattr(t, name, row[i])
