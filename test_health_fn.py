# this example triggered by DB trigger - test_fn.py with a health / test check
#
# Two parts, one file:
#   - the body (below the definitions): runs on every event, as in test_fn.py;
#     it never calls test()
#   - test(level): runs on a health / test check only, never on an event
#
# On a check rppd runs the definitions only - the top-level imports, defs, classes
# and literal constants - then calls test(level); the body is skipped. So test()
# uses nothing the body computes (cur, input): it sets up what it needs itself,
# from what rppd hands a call (DB, ETCD, TOPIC, TABLE, SECRET). Imports stay plain
# at the top - one under a top-level if or try is skipped on a check.
#
# input: level, optional, default 0 - the deeper, the more it touches:
#   0  cheap, no side effects: DB answers, the tables exist (safe on a timer)
#   1  and its dependencies answer: etcd reads the key the body writes
#   2  and a round trip with test data: a row written and read back, rolled back;
#      an etcd key put, read back and deleted
# return: 0 ok; any other number is a failure, its code:
#   1 test_source missing, 2 test_sink missing, 3 etcd answers wrong, 4 round trip
# exception: a failure, with its text
#
# config changes: rppd.yaml:
# rppd_function - all new columns null by default:
#  test_level: null - do not test, any value - test on deploy
#  test_timestamp: null - not tested, if test_level is not null, mean run immediately
# rppd_function_log:
#  test_level: null is a normal non test exec, any than null - tested
# rppd_cron
#  test_level: null - do not run (disable or not used)
#
#

import os
import time

SOURCE = "test_source"
SINK = "test_sink"
ETCD_KEY = "test_source"
PROBE_KEY = "test_source/health"


def test(level: int = 0) -> int:
    cur = DB.cursor()
    try:
        # level 0: the database answers, and the tables the body reads and writes exist
        cur.execute("select to_regclass(%s), to_regclass(%s)", ("public." + SOURCE, "public." + SINK))
        source, sink = cur.fetchone()
        if source is None:
            return 1
        if sink is None:
            return 2
        if level < 1:
            return 0

        # level 1: etcd answers - a read of the key the body writes, nothing written
        ETCD.get(ETCD_KEY)
        if level < 2:
            return 0

        # level 2: a round trip with test data, nothing left behind
        cur.execute("insert into " + SINK + " (data) values (%s) returning id", ("health check",))
        row_id = cur.fetchone()[0]
        cur.execute("select data from " + SINK + " where id = %s", (row_id,))
        back = cur.fetchone()
        DB.rollback()
        if back is None or back[0] != "health check":
            return 4
        ETCD.put(PROBE_KEY, "ok")
        val, meta = ETCD.get(PROBE_KEY)
        ETCD.delete(PROBE_KEY)
        if val is None or val.decode("UTF-8") != "ok":
            return 3
        return 0
    finally:
        # whatever the check left open is not committed
        DB.rollback()
        cur.close()


# ── the body: every event ────────────────────────────────────────────────────────

cur = DB.cursor()
cur.execute("SELECT input FROM test_source where id = %s", ([ID]))
input = cur.fetchall()
if len(input) == 0:
    print("data is not yet available for ID#", ID)
    time.sleep(1)

cur.execute("SELECT input FROM test_source where id = %s", ([ID]))
input = cur.fetchall()

if len(input) > 0:
    ETCD.put(ETCD_KEY, "/etcd")
    val, meta = ETCD.get(ETCD_KEY)
    v = val.decode('UTF-8')
    cur.execute("insert into test_sink (data) values (%s||'='||%s||'_'||%s)", ( input[0], v, os.environ.get('TEST_ERV', 'NA')))
    DB.commit()

print("tested stdout on example, #", ID, len(input))
