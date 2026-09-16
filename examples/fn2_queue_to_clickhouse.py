# Step 2 of the pipeline: fires on each etcd queue entry under /q/analytics
# (rppd_function.schema_table = '/q/analytics'), so it is driven by fn1, not by
# the database.
#
# Bound locals: KEY and VALUE for a queue event, plus DB, ETCD and - the piece
# this test exercises - CH, the clickhouse_connect client that yaxaha binds
# when serve_clickhouse is on (see CfgTabl.on_commit_python).
#
# It joins the original pgbench row with the live cluster ping status and lands
# both in a ClickHouse analytics table.

import json

payload = json.loads(VALUE.decode('UTF-8') if isinstance(VALUE, bytes) else VALUE)

# Live cluster ping status. yt_info() returns JSON, not a record set, so expand
# it with json_array_elements rather than a column definition list. The 'E'
# selector carries one entry per node, keyed by node UUID, e.g.
#   b0..d504cf:192.168.1.210:7421, Good, Ping: uptime: 280, rank 4, load 0, rtt 8, cerr 0
# alongside non-node entries (cluster, term, health.*) that are filtered out.
cur = DB.cursor()
cur.execute("SELECT e->>'name', e->>'value' FROM json_array_elements(yt_info('E')) e")
entries = cur.fetchall()
# Read-only, but still end the transaction - see the note in fn1: a reused
# connection left "idle in transaction" pins locks for as long as rppd keeps it.
DB.commit()


def is_node_uuid(name):
    return name is not None and len(name) == 36 and name.count('-') == 4


def field(line, key):
    # "... rank 4, load 0, rtt 12, cerr 0" -> the integer after `key`
    for part in line.split(','):
        part = part.strip()
        if part.startswith(key + ' '):
            try:
                return int(part.split(' ')[1])
            except (IndexError, ValueError):
                return 0
    return 0


CH.command("""
CREATE TABLE IF NOT EXISTS public.bench_analytics (
    src_id     Int32,
    bid        Int32,
    amount     Int32,
    note       Nullable(String),
    node_uuid  String,
    node_host  String,
    node_state String,
    rtt        Int32,
    load       Int32,
    cerr       Int32,
    seen_at    DateTime DEFAULT now()
) ENGINE = MergeTree ORDER BY (src_id, node_uuid)
""")

rows = []
for name, value in entries:
    if not is_node_uuid(name) or not value:
        continue
    parts = [p.strip() for p in value.split(',')]
    # parts[0] is "<uuid8>:<host>:<port>"; parts[1] is the peer state
    addr = parts[0].split(':')
    host = addr[1] if len(addr) > 2 else parts[0]
    state = parts[1] if len(parts) > 1 else ''
    rows.append([
        payload["id"], payload["bid"], payload["amount"], payload["note"],
        name, host, state,
        field(value, 'rtt'), field(value, 'load'), field(value, 'cerr'),
    ])

if rows:
    CH.insert(
        'public.bench_analytics', rows,
        column_names=['src_id', 'bid', 'amount', 'note',
                      'node_uuid', 'node_host', 'node_state', 'rtt', 'load', 'cerr'])

print("fn2: key={} src_id={} nodes={}".format(KEY, payload["id"], len(rows)))
