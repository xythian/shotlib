#
# Utility functions for working with avro files
#
from avro import protocol, datafile, schema, io
from contextlib import contextmanager

@contextmanager
def avro_writer(path, schema):
    tmppath = path + '.tmp'
    success = False
    with open(tmppath, 'wb') as writer:
        with closing(datafile.DataFileWriter(writer, io.DatumWriter(), writers_schema=schema, codec='deflate')) as dfw:
            try:
                yield dfw
                success = True
            finally:
                if success:
                    os.rename(tmppath, path)


@contextmanager
def avro_reader(path):
    with open(path, 'rb') as reader:
        dfr = datafile.DataFileReader(reader, io.DatumReader())
        try:
            yield dfr
        finally:
            dfr.close()

