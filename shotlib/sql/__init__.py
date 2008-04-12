
#
# ShotSQL
#   Attempt to collect all my various attempts of making a database convenience layer.
#   Note: not an *abstraction* layer.
#

if __name__ == '__main__':
    from shotlib.sql.postgres import generate_tableclasses, generate_rowclass
    import psycopg2
    db = psycopg2.connect(database='wordshot')
    db.cursor().execute("SET search_path TO wordshot")
    hodor = generate_tableclasses(db, 'wordshot')
    for k, v in hodor._tables.items():
        print k
        for col in v._columns:
            print '   ',col
            generate_rowclass(db, "SELECT * FROM view_user_section")
