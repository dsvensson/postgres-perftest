import time, sys
import psycopg2
from psycopg2.extensions import adapt, register_adapter

class DB(object):
    def __init__(self, dbapi):
        self.field_order = ('key', 'a0', 'a1', 'a2')
        self.connection = None
        self.dbapi = dbapi

    def connect(self, **kwargs):
        self.connection = self.dbapi.connect("dbname='%(dbname)s' user='%(user)s' host='%(host)s'" % kwargs)
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.cursor.execute("""
        CREATE TABLE item (
            key MACADDR PRIMARY KEY,
            a0 VARCHAR,
            a1 VARCHAR,
            a2 VARCHAR
        )
        """)
        self.cursor.execute("""
        CREATE TEMPORARY TABLE temp_item (
            key MACADDR PRIMARY KEY,
            a0 VARCHAR,
            a1 VARCHAR,
            a2 VARCHAR
        ) ON COMMIT DELETE ROWS
        """)

    def drop_table(self):
        self.cursor.execute("""
        DROP TABLE item CASCADE
        """)
    
    def create_function(self):
        self.cursor.execute("""
        CREATE OR REPLACE FUNCTION replace_item(items item[]) RETURNS VOID AS $$
        BEGIN
            FOR i IN COALESCE(array_lower(items,1),0) .. COALESCE(array_upper(items,1),-1) LOOP
               UPDATE item SET a0=items[i].a0,a1=items[i].a1,a2=items[i].a2 WHERE key=items[i].key;
            END LOOP;
        END;
        $$ LANGUAGE plpgsql
        """)

    def drop_function(self):
        self.cursor.execute("""
        DROP FUNCTION replace_item(data item[]) CASCADE
        """)
    
    def drop_rule(self):
        self.cursor.execute("""
        DROP RULE insert_or_replace ON item CASCADE
        """)
    
    def create_rule(self):
        self.cursor.execute("""
        CREATE RULE "insert_or_replace" AS
            ON INSERT TO "item"
            WHERE
              EXISTS(SELECT 1 FROM item WHERE key=NEW.key LIMIT 1)
            DO INSTEAD
               (UPDATE item SET a0=NEW.a0,a1=NEW.a1,a2=NEW.a2 WHERE key=NEW.key);
        """)

    def truncate(self):
        self.cursor.execute("TRUNCATE item")
        self.cursor.execute("VACUUM FULL ANALYZE")

    def begin(self):
        self.cursor.execute("BEGIN")

    def commit(self):
        self.cursor.execute("COMMIT")

    def rollback(self):
        self.cursor.execute("ROLLBACK")

    def insert_items(self, items):
        q = "INSERT INTO item (key, a0, a1, a2) VALUES ("
        q += "), (".join((", ".join(str(adapt(item[f])) for f in self.field_order)) for item in items)
        q += ")"
        self.cursor.execute(q)

    def update_items_merlin83(self, items):
        q = "INSERT INTO temp_item (key, a0, a1, a2) VALUES ("
        q += "), (".join((", ".join(str(adapt(item[f])) for f in self.field_order)) for item in items)
        q += ")"
        self.cursor.execute(q)
        q  = "DELETE FROM item USING temp_item WHERE item.key=temp_item.key"
        self.cursor.execute(q)
        q  = "INSERT INTO item (key, a0, a1, a2) SELECT key, a0, a1, a2 FROM temp_item"
        self.cursor.execute(q)

    def update_items_merlin83_2(self, items):
        q = "INSERT INTO temp_item (key, a0, a1, a2) VALUES ("
        q += "), (".join((", ".join(str(adapt(item[f])) for f in self.field_order)) for item in items)
        q += ")"
        self.cursor.execute(q)
        q  = "UPDATE item SET a0=temp_item.a0, a1=temp_item.a1, a2=temp_item.a2 FROM temp_item WHERE item.key=temp_item.key"
        self.cursor.execute(q)

    def update_items_andres(self, items):
        q  = "UPDATE item SET a0=i.a0, a1=i.a1, a2=i.a2 FROM "
        q += "(VALUES (" + "), (".join(", ".join(str(adapt(item[f])) for f in self.field_order) for item in items) + ")) AS i(key, a0, a1, a2)"
        q += " WHERE item.key=i.key::macaddr"
        self.cursor.execute(q)

    def update_items(self, items):
        q = "UPDATE item SET a0=%(a0)s,a1=%(a1)s,a2=%(a2)s WHERE key=%(key)s"
        self.cursor.executemany(q, items)

    def replace_items(self, items):
        q = "SELECT replace_item(ARRAY[("
        q += ")::item, (".join((", ".join(str(adapt(item[f])) for f in self.field_order)) for item in items)
        q += ")::item])"
        self.cursor.execute(q)

class Batch(object):
    def __init__(self, limit):
        self.batch = []
        self.limit = limit

    def append(self, sub):
        self.batch.append(sub)
        return len(self.batch) >= self.limit

    def drain(self):
        b = self.batch
        self.batch = []
        return b

def populate(batch, n, batch_receiver):
    handled_items = 0

    for i in xrange(n):
        item = { 'key': '00:00:00:%02x:%02x:%02x' % ((i >> 16) & 0xff, (i >> 8) & 0xff, i & 0xff), 'a0': 'a0 - %06x' % i, 'a1': 'a1 - %06x' % i, 'a2': 'a2 - %06x' % i}
        if batch.append(item):
            batch_receiver(batch.drain())
        handled_items += 1
    
    items = batch.drain()
    if items:
        batch_receiver(items)

    return handled_items

def run_test(item_count, batch_size, db, fn):
    affected_items = 0
    try:
        start_time = time.time()
        db.begin()
        affected_items = populate(Batch(batch_size), item_count, fn)
        db.commit()
        elapsed = time.time() - start_time
        return affected_items, elapsed
    except:
        db.rollback()
        raise

def run_test_no_transaction(item_count, batch_size, db, fn):
    affected_items = 0
    try:
        start_time = time.time()
        affected_items = populate(Batch(batch_size), item_count, fn)
        elapsed = time.time() - start_time
        return affected_items, elapsed
    except:
        db.rollback()
        raise

def transaction_wrap(db, fn):
    def f(*args, **kwargs):
        db.begin()
        fn(*args, **kwargs)
        db.commit()
    return fn

def test_insert(item_count, batch_size, db):
    truncate(db)
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.insert_items))
    print "Multi-row insert           : %d items inserted in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def test_update(item_count, batch_size, db):
    prepopulate(item_count, batch_size, db)
    #affected_items, elapsed = run_test(item_count, batch_size, db, db.update_items)
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.update_items))
    print "executemany() update       : %d items updated  in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def test_update_andres(item_count, batch_size, db):
    prepopulate(item_count, batch_size, db)
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.update_items_andres))
    print "update_andres              : %d items updated  in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def test_update_merlin83(item_count, batch_size, db):
    prepopulate(item_count, batch_size, db)
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.update_items_merlin83))
    print "update_merlin83 (i/d/i)    : %d items updated  in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def test_update_merlin83_2(item_count, batch_size, db):
    prepopulate(item_count, batch_size, db)
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.update_items_merlin83_2))
    print "update_merlin83 (i/u)      : %d items updated  in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def test_replace(item_count, batch_size, db):
    prepopulate(item_count, batch_size, db)
    db.create_function()
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.replace_items))
    db.drop_function()
    print "replace_item() procedure   : %d items replaced in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def test_rule_insert(item_count, batch_size, db):
    truncate(db)
    db.create_rule()
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.insert_items))
    db.drop_rule()
    print "Multi-row insert_or_replace: %d items inserted in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def test_rule_replace(item_count, batch_size, db):
    prepopulate(item_count, batch_size, db)
    db.create_rule()
    affected_items, elapsed = run_test_no_transaction(item_count, batch_size, db, transaction_wrap(db, db.insert_items))
    db.drop_rule()
    print "Multi-row insert_or_replace: %d items replaced in %5.2f seconds averaging %8.2f items/s" %(affected_items, elapsed, affected_items / elapsed)

def truncate(db):
    db.truncate()
    
def prepopulate(item_count, batch_size, db):
    try:
        db.truncate()
        db.begin()
        populate(Batch(batch_size), item_count, db.insert_items)
        db.commit()
    except:
        db.rollback()
        raise

def run():
    db = DB(psycopg2)
    db.connect(dbname='psm', host='localhost', user='psm')
    try:
        db.begin()
        db.drop_table()
        db.commit()
    except psycopg2.ProgrammingError, e:
        print >>sys.stderr, "Removing old tables:", e
        db.rollback()
        

    db.create_table()

    test_insert(50000, 500, db)
    test_update(50000, 500, db)
    test_update_andres(50000, 500, db)
    test_update_merlin83(50000, 500, db)
    test_update_merlin83_2(50000, 500, db)
    test_replace(50000, 500, db)
    test_rule_insert(50000, 500, db)
    test_rule_replace(50000, 500, db)

    db.drop_table()

if __name__ == '__main__':
    run()
