__all__ = ['dumps', 'loads']

try:    
    from cjson import encode, decode

    def dumps(v):        
        return encode(v)

    def loads(v):
        return decode(v, all_unicode=True)

except ImportError:
    from simplejson import dumps, loads


def load(f):
    return loads(f.read())
