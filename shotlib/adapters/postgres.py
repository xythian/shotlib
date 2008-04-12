import psycopg2
from psycopg2.extensions  import adapt

def register_numpy_adapters():
    import numpy
    def adapt_numpy_num(v):
        if v is None:
            return psycopg2.extensions.AsIs('NULL')
        else:
            return psycopg2.extensions.AsIs(str(v))
    for t in (numpy.int64, numpy.int32, numpy.int16, numpy.int8, numpy.float64, numpy.float32):
        psycopg2.extensions.register_adapter(t, adapt_numpy_num)
        
    def adapt_numpy_array(v):
        return psycopg2.extensions.AsIs("'{%s}'" % (",".join(adapt(i).getquoted() for i in list(v))))
    psycopg2.extensions.register_adapter(numpy.ndarray, adapt_numpy_array)

#
# Future: write a custom type representing a numpy array in the db so it comes out that way, too
# or maybe, if numpy is available, return ALL arrays in the db as numpy arrays
