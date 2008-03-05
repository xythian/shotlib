__all__ = ['create_enum', 'create_bitfield']

def _create_enum(name, values, bitfield=False):
    class EnumValue(int):
        def __new__(cls, v, name):
            x = super(EnumValue, cls).__new__(cls, v)
            x.name = name
            return x
        def __str__(self):
            return '<%s:%d>' % (self.name, self)
    EnumValue.__name__ = name
    class EnumCollection(object):
        pass
    EnumCollection.__name__ = name + 's'
    v = EnumCollection()
    v._type_ = EnumValue
    vals = []
    names = {}
    for i, name in enumerate(values):
        val = i
        if bitfield:
            val = 2**i
        ev = EnumValue(val, name)
        setattr(v, name, ev)
        vals.append(ev)
        names[name] = ev
    v.values = tuple(vals)
    v.names = names
    return v
    
def create_enum(name, *values):
    return _create_enum(name, values)

def create_bitfield(name, *values):
    return _create_enum(name, values, bitfield=True)
