from __future__ import with_statement
from contextlib import contextmanager
from shotlib.util import *
import re
from itertools import ifilter
import time
from threading import local
import logging

LOG = logging.getLogger("shotlib.sql")

__context = local()

def current_storage():
    return __context.storage

def current_itemcache():
    return __context.itemcache

@contextmanager
def set_current_storage(storage):
    __context.storage = storage
    __context.itemcache = {}
    try:
        yield storage
    finally:
        del __context.storage
        del __context.itemcache
    

def withdb(func):
    @copyinfo(func)
    def _wrap_withdb(*args, **kwargs):
        return func(current_storage(), *args, **kwargs)
    return _wrap_withdb

def dbmethod(func):
    @copyinfo(func)
    def _wrap(self, *args, **kwargs):
        return func(self, current_storage(), *args, **kwargs)
    return _wrap

def cursormethod(func):
    @copyinfo(func)
    def _wrap(self, *args, **kwargs):
        return func(self, current_storage().cursor, *args, **kwargs)
    return _wrap

def columnproperty(idx, col, doc=None):
    def get(self):
        return self._data[idx]
    def set(self, v):
        self._data[idx] = v
    def delete(self):
        self._data[idx] = None
    return property(get, set, delete, doc=doc)


class Column(str):
    #__slots__ = ('colname', 'select', 'update', 'insert', 'name', 'idx')
    def __new__(cls, name, idx=None, colname=None, virtual=False, select=True, update=True, insert=True, rowid=False):
        if colname is None:
            colname = name
        self = super(Column, cls).__new__(cls, colname)
        if virtual:
            select = update = insert = False
        
        self.select = select
        self.update = update
        self.insert = insert
        self.name = name
        self.idx = idx
        self.rowid = rowid
        self.colname = colname
        return self

    def to_sql(self, v):
        return v

    def from_sql(self, v):
        return v

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, repr(self.name))

class QueryInfo(object):
    __slots__ = ('columns', 'table', 'rowid')
    def __init__(self, columns=None, table=None, rowid=None):
        if columns is not None:
            self.columns = tuple(columns)
            for idx, col in enumerate(self.columns):
                col.idx = idx
            f = [r for r in columns if r.rowid]
            if len(f) == 1:
                self.rowid = f[0]
            else:
                self.rowid = None
        else:
            self.columns = []
            self.rowid = None            
        self.table = table

    def defaults(self):
        return [None] * len(self.columns)        

class QInfo(QueryInfo):
    def __init__(self, table=None):
        super(QInfo, self).__init__(table=table)
        
    def all(self, *entries):
        # ewwww
        d = sys._getframe(1).f_locals        
        for entry in entries:
            if isinstance(entry, tuple):
                name, colname = entry
            else:
                name = entry
                colname = entry
            d[name] = self.column(name=name, colname=colname)            
    def column(self, name='', colname=None, rowid=False, **kw):
        def do_wrap(func):            
            doc = func.__doc__
            idx = len(self.columns)
            if not name:
                name = func.__name__
            if not colname:
                colname = name
            col = Column(idx=idx, name=name, colname=colname, **kw)
            self.columns.append(col)
            return columnproperty(idx, doc)
        return do_wrap


def to_column(col):
    if not isinstance(col, Column):
        if col == 'id':
            return Column(col, insert=False, update=False, rowid=True)
        else:
            return Column(col)
    else:
        return col

class RecordMeta(type):
    def process_properties(cls, name, bases, dict):
        qinfo = dict.get('q')
        if not qinfo:
            tbl = dict.get('_table')
            cols = dict.get('_columns')
            if cols:
                qinfo = QueryInfo(columns=[to_column(col) for col in cols], table=tbl)
                dict['q'] = qinfo
                if dict.get('_table'):
                    del dict['_table']
                del dict['_columns']
        if qinfo and qinfo.columns:
            newcols = []
            for i, col in enumerate(qinfo.columns):
                dict[col.name] = columnproperty(i, col)
                newcols.append(col)
            qinfo.columns = newcols
            if not qinfo.rowid:
                f = [r for r in qinfo.columns if r.rowid]
                if len(f) == 1:
                    qinfo.rowid = f[0]
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
        return super(RecordMeta, cls).__new__(cls, name, bases, dict)

class Record(object):
    __metaclass__ = RecordMeta
    __slots__ = ('_data',)
    def __init__(self, **kw):
        self._data = self.q.defaults()
        if kw:
            for key, val in kw.items():
                setattr(self, key, val)

    @property
    def is_new(self):
        return getattr(self, self.q.rowid.name) is None

    @classmethod
    def from_row(cls, row):
        r = cls()
        r._data = [col.from_sql(v) for col, v in zip(cls.q.columns, row)]
        return r

    def to_row(self):
        return [col.to_sql(v) for col, v in zip(self.q.columns, self._data)]

    @property
    def _storage(self):
        return current_storage()

    @classmethod
    def row_wrap(cls, func):
        @copyinfo(func)
        def _wrapped_row_wrap(*args, **kwargs):
            return [cls.from_row(row) for row in func(*args, **kwargs)]
        return _wrapped_row_wrap

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__,
                           ",".join("%s=%s" % (k.name, repr(v)) for k, v in zip(self.q.columns, self._data)))

RE_S = re.compile("\s+", re.I|re.MULTILINE)
RE_ARG = re.compile("[{](?P<name>[^}]+)[}]", re.I|re.MULTILINE)

def posarg(n):
    return '%s'

def compile_query(query, **environ):
    args = []
    def sub(m):
        name = m.group('name')
        if name in environ:
            return environ[name]
        else:
            args.append(name)
            return posarg(len(args) - 1)
    query = RE_S.sub(" ", query)
    query = RE_ARG.sub(sub, query)
    return query, tuple(args)

def take_first(func):
    @copyinfo(func)
    def _wrapped_take_first(*args, **kwargs):
        result = func(*args, **kwargs)
        if len(result) > 0:
            return result[0]
        else:
            return None
    return _wrapped_take_first

class FilterQuery(object):
    def __init__(self, q, f):
        self.__q = q
        self.__count = q.count()
        self.__f = f

    def __getitem__(self, item):
        if isinstance(item, slice):
            start, stop, step = item.indices(self.__count)
            length = stop - start
            result = self.__q.__getitem__(item)
            return filter(self.__f, result)
        else:
            result = self.__q.__getitem__(item)
            if self.__f(result):
                return result
            else:
                return None

    def __iter__(self):
        return ifilter(self.__f, iter(self.__q))

    def __len__(self):
        return self.__count

DECORATOR_REGISTRY = {'first' : take_first,
                      'scalar' : lambda func: take_first(take_first(func))}

def paginated_query(csql, dsql, cls=None, result_filter=None):
    cfunc = query_func(csql, scalar=True)
    qfunc = query_func(dsql, cls=cls)
    class PaginatedQueryFilter(object):
        def __init__(self, args):
            self.__count = cfunc(**args)
            self.__args = args
        def __len__(self):
            return self.__count
        if result_filter is not None:
            def __iter__(self):
                return ifilter(result_filter,
                               self[0:self.__count])            
            def __getitem__(self, item):
                if isinstance(item, slice):
                    start, stop, step = item.indices(self.__count)
                    limit = stop - start
                    offset = start
                    res = qfunc(limit=limit, offset=offset, **self.__args)
                    return filter(result_filter, res)
                else:
                    offset = item
                    limit = 1
                    result = qfunc(limit=limit, offset=offset, **kwargs)
                    if not result:
                        return None
                    elif result_filter(result[0]):
                        return result[0]
                    else:
                        return None
        else:
            def __iter__(self):
                return self[0:self.__count]
            def __getitem__(self, item):
                if isinstance(item, slice):
                    start, stop, step = item.indices(self.__count)
                    limit = stop - start
                    offset = start
                    return qfunc(limit=limit, offset=offset, **self.__args)
                else:
                    offset = item
                    limit = 1
                    result = qfunc(limit=limit, offset=offset, **self.__args)
                    if not result:
                        return None
                    else:
                        return result[0]
    def execute(**kwargs):
        return PaginatedQueryFilter(kwargs)
    return execute

def query_func(sql, func=None, decorators=(), **kwargs):
    decorators = list(decorators)
    cls = None
    for kw, arg in kwargs.items():
        if kw == 'self':
            decorators.append(func)
        elif arg is True:
            decorators.append(DECORATOR_REGISTRY[kw])
        elif arg is False:
            continue
        elif kw == 'cls' and arg is not None:
            decorators.append(arg.row_wrap)
            cls = arg
        else:
            raise Exception('Unexpected keyword argument to query_func: %s' % kw)
    if cls is not None:
        rowfunc = cls.from_row
        if cls.q.table:
            table = cls.q.table
            cols = ",".join('%s.%s' % (table, col.colname) for col in cls.q.columns)
        else:
            table = ''
            cols = ",".join(cls.q.columns)
    else:
        cols = ''
        table = ''
    qry, args = compile_query(sql, columns=cols, table=table)
    def execute_query(db, **kwargs):
        cursor = db.cursor
        argvals = tuple(kwargs[arg] for arg in args)
        t = time.time()
        cursor.execute(qry, argvals)
        LOG.debug("QUERY: %.2fms: %s %r",
                  (time.time() - t)*1000.0,
                  qry,
                  argvals)
        return cursor.fetchall()
    # TODO: build a better ordering mechansim for decorators
    #decorators.reverse()
    for decorator in decorators:
        execute_query = decorator(execute_query)
    return withdb(execute_query)

def query(func=None, **kwargs):
    if func is None:
        def _wrap(func):            
            return query_func(func.__doc__, **kwargs)
        return _wrap
    else:
        return query_func(func.__doc__, **kwargs)

class DatabaseMetaCommon(object):
    def __init__(self, tables=None):
        if tables is not None:
            self._tables = tables
            for k, v in tables.items():
                setattr(self, k, v)
    
    def last_rowid(self, cursor, record):
        return cursor.getconnection().last_insert_rowid()

    # TODO: grok multi-column primary keys .. maybe

    def compose_insert_query(self, record):
        collist = [col.colname for col in record.q.columns if col.insert]
        values = ["%s" for v in collist]
        query = "INSERT INTO %s (%s) VALUES (%s)" % (record.q.table,
                                                     ",".join(collist),
                                                     ",".join(values))
        return query
    
    def to_insert_args(self, record):
        return [col.to_sql(getattr(record, col.name)) for col in record.q.columns if col.insert]

    @cursormethod
    def insert(self, cursor, record):
        cursor.execute(self.compose_insert_query(record), self.to_insert_args(record))
        setattr(record, record.q.rowid.name, self.last_rowid(cursor, record))
        return record

    @cursormethod
    def insertmany(self, cursor, records, prototype=None):
        "note: this doesn't update records' ids"
        if prototype is None:
            prototype = records[0]
        cursor.executemany(self.compose_insert_query(prototype), (self.to_insert_args(record) for record in records))


    def to_update_args(self, record):
        args = [col.to_sql(getattr(record, col.name)) for col in record.q.columns if col.update]
        args.append(record.q.rowid.to_sql((getattr(record, record.q.rowid.name))))
        return args
        
    def compose_update_query(self, record):
        columns = ["%s = %%s" % col.colname for col in record.q.columns if col.update]
        query = "UPDATE %s SET %s WHERE %s = %%s" % (record.q.table,
                                                    ",".join(columns),
                                                    record.q.rowid.colname)
        return query

    @cursormethod
    def update(self, cursor, record):
        qry = self.compose_update_query(record)
        cursor.execute(self.compose_update_query(record), self.to_update_args(record))
        return record

    @cursormethod
    def updatemany(self, cursor, records, prototype=None):
        records = list(records)
        if prototype is None:
            prototype = records[0]        
        cursor.executemany(self.compose_update_query(prototype), (self.to_update_args(record) for record in records))

    @cursormethod
    def delete(self, cursor, record):
        cursor.execute("DELETE FROM %s WHERE %s = %%s" % (record.q.table,
                                                          record.q.rowid.colname),
                       record.q.rowid.to_sql(getattr(record, record.q.rowid.name)))

    def save(self, record):
        if record.is_new:
            self.insert(record)
        else:
            self.update(record)

    @cursormethod
    def savemany(self, cursor, records, prototype=None):
        records = list(records) # gross, but pretty much necessary
        if prototype is None:
            prototype = records[0]
        new_records = [record for record in records if record.is_new]
        old_records = [record for record in records if not record.is_new]
        del records
        if new_records:
            self.insertmany(cursor, new_records)
        if old_records:
            self.updatemany(cursor, old_records)

    @cursormethod
    def commit(self, cursor):
        cursor.connection.commit()

def defer_props(target, o, *names):
    for name in names:
        setattr(target, name, getattr(o, name))

class StorageSession(object):
    def __init__(self, db, meta, memcache=None):
        self.db = db
        self.cursor = db.cursor()
        self.meta = meta
        self.memcache = memcache
        defer_props(self, meta,
                    'insert',
                    'insertmany',
                    'update',
                    'delete',
                    'save',
                    'savemany',
                    'commit')

    @classmethod
    @contextmanager
    def create(cls, db, meta):
        with set_current_storage(cls(db, meta)) as session:
            yield session
    
class StorageContextManager(object):
    def __init__(self, meta, storage, dbconn):
        self.meta = meta
        self.storage = storage
        self.dbconn = dbconn

    @contextmanager
    def __call__(self):
        with self.dbconn() as db:
            with self.storage.create(db, self.meta) as session:
                yield session

class RowCache(object):
    def __init__(self, kw='id', cls=None, empty=None):
        self.data = {}
        self._missing = object()
        self._kw = kw
        if cls:
            self.__fromrow = cls.from_row
            self.__torow = lambda x:x.to_row()
        else:
            self.__fromrow = lambda x:x
            self.__torow = lambda x:x
        self._empty = empty

    def __call__(self, func):
        self.__load = func
        to_row = self.__torow
        from_row = self.__fromrow
        @copyinfo(func)
        def __get(*args, **kwargs):
            id = kwargs[self._kw]
            row = self.data.get(id)            
            if row is self._missing:            
                return self._empty
            elif row:
                return from_row(row)
            else:
                result = self.__load(*args, **kwargs)
                if result:
                    self.data[id] = to_row(result)
                    return result
                else:
                    self.data[id] = self._missing 
                    return self._empty
        __get.cache = self
        __get.flush = self.flush
        return __get

    def get(self, id):
        row = self.data(id)
        if row is self._missing or row is None:
            return None
        else:
            return self.__fromrow(row)

    def flush(self, id=None):
        if id is None:
            self.data = {}
        elif self.data.has_key(id):
            del self.data[id]

    def update(self, data):
        self.data.update(data)

    def load(self, rows):
        for row in rows:
            self.data[row[0]] = row

class ItemCache(RowCache):
    def _get_data(self):
        try:
            return current_itemcache()[self._key]        
        except KeyError:
            val = {}
            current_itemcache()[self._key] = val
            return val

    def _set_data(self, v):
        pass

    data = property(_get_data, _set_data)

    @property
    def _key(self):
        return id(self)

    def __call__(self, func):
        self.__load = func
        @copyinfo(func)
        def __get(*args, **kwargs):
            id = kwargs[self._kw]
            row = self.data.get(id)            
            if row is self._missing:            
                return self._empty
            elif row:
                return row
            else:
                result = self.__load(*args, **kwargs)
                if result:
                    self.data[id] = result
                    return result
                else:
                    self.data[id] = self._missing 
                    return self._empty
        def __put(key, val):
            self.data[key] = val
        __get.cache = self
        __get.flush = self.flush
        __get.put = __put
        return __get

