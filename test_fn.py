# this example triggered by DB trigger

import os
import time

cur = DB.cursor()
cur.execute("SELECT input FROM test_source where id = %s", ([ID]))
input = cur.fetchall()
if len(input) == 0:
    print("data is not yet available for ID#", ID)
    time.sleep(1)

cur.execute("SELECT input FROM test_source where id = %s", ([ID]))
input = cur.fetchall()

if len(input) > 0:
    key = 'test_source'
    ETCD.put(key, "/etcd")
    val, meta = ETCD.get(key)
    v = val.decode('UTF-8')
    cur.execute("insert into test_sink (data) values (%s||'='||%s||'_'||%s)", ( input[0], v, os.environ.get('TEST_ERV', 'NA')))
    DB.commit()

print("tested stdout on example, #", ID, len(input))
