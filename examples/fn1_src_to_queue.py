# Step 1 of the pipeline: fires on every INSERT into public.bench_src via
# yaxaha's CfgTabl.on_commit_python, i.e. after the CLUSTER commit, not on a
# per-row PostgreSQL trigger.
#
# Bound locals (see rppd README): DB, ETCD, TABLE, TRIG, and one upper-cased
# local per PK column - here ID. CH is bound too when serve_clickhouse is on,
# but this step deliberately does not use it: the point is to hand off through
# the etcd queue.
#
# It reads the row it was told about and puts it on the etcd queue that fn2
# watches. /q/ is what marks a key as a queue for the Rust etcd implementation.

import json

cur = DB.cursor()
cur.execute("SELECT id, bid, amount, note FROM public.bench_src WHERE id = %s", ([ID]))
rows = cur.fetchall()
# End the transaction even though this function only reads. psycopg2 opens one
# on the first execute, and rppd reuses the connection across calls, so without
# this the backend sits "idle in transaction" holding ACCESS SHARE on the table
# - enough to block any DDL or TRUNCATE on it indefinitely.
DB.commit()
if len(rows) == 0:
    # the cluster commit landed but this node has not materialised the row yet
    print("fn1: no row for ID", ID)
else:
    r = rows[0]
    payload = json.dumps({"id": r[0], "bid": r[1], "amount": r[2], "note": r[3]})
    # One queue entry per source row. The path shape matters: the Rust etcd
    # queue routes /q/<name>/producer/<n> to the watchers it registered as
    # /q/<name>/consumer/<node-uuid>. A key that is not under producer/ is not
    # delivered as a queue message.
    ETCD.put("/q/analytics/producer/{}".format(r[0]), payload)
    print("fn1: queued id={} bid={} amount={}".format(r[0], r[1], r[2]))
