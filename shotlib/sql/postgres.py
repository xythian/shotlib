from itertools import groupby
from shotlib.sql.common import Record, Column, DatabaseMetaCommon

# sigh, pg_tables doesn't include the oid, so we need pg_class

Q_COLUMNS = """SELECT t.tablename, a.attnum, a.attname, a.attndims, y.typname	
FROM pg_tables t 
INNER JOIN pg_class c ON c.relname = t.tablename AND c.relkind = 'r'::char
INNER JOIN pg_attribute a ON a.attrelid = c.oid 
INNER JOIN pg_type y ON y.oid = a.atttypid
WHERE t.schemaname = %(schema)s AND a.attnum > 0
ORDER BY t.schemaname, t.tablename, a.attnum"""

Q_TYPES =  """SELECT y.oid, y.typname	
FROM pg_type y ON y.oid = a.atttypid
"""

class DatabaseMeta(DatabaseMetaCommon):
    def last_rowid(self, cursor, record):
        if hasattr(record.q, 'rowid_seq'):
            seq = record.q.rowid_seq
        else:
            seq = '%s_%s_seq' % (record.q.table, record.q.rowid.colname)
        cursor.execute("SELECT currval(%s)", (seq,))
        return cursor.fetchone()[0]

def generate_tableclasses(db, schema, adapters=None):
    cursor = db.cursor()
    cursor.execute(Q_COLUMNS, {'schema' : schema})
    rows = cursor.fetchall()
    result = {}
    if not adapters:
        adapters = {}
    for k, g in groupby(rows, lambda x:x[0]):
        cols = list(g)
        tablename = cols[0][0]
        rcols = []
        for _, idx, name, dims, typname in cols:
            # for now we assume coercion is the domain of psycopg2
            colcls = adapters.get(typname)
            if colcls is not None:
                rcols.append(colcls(name))
            else:
                rcols.append(name)
        class RowAdapter(Record):
            __slots__ = ()
            _columns = rcols
            _table = tablename
        RowAdapter.__name__ = str(tablename)
        result[tablename] = RowAdapter
    del cursor
    return DatabaseMeta(result)

def generate_rowclass(db, name, query, args=None, kwargs=None):
    cursor = db.cursor()

    if args:
        cursor.execute(query, args)
    elif kwargs:
        cursor.execute(query, kwargs)
    else:
        cursor.execute(query)
    cols = [Column(r[0], idx=i) for i, r in enumerate(cursor.description)]
    del cursor
    class RowAdapter(Record):
        __slots__ = ()
        _columns = cols
    RowAdapter.__name__ = str(name)
    return RowAdapter
        
