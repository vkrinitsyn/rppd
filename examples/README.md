# Examples

## etcd queue straight into ClickHouse — no Python, no PostgreSQL

`etcd_ch_queue_test.py` needs only an etcd client. A queue named
**`/q/ch:<table>/`** or **`/queue/clickhouse:<table>/`** is consumed by the etcd
server itself: each message's JSON value is written into that ClickHouse table
as JSONEachRow, batched, with no watcher and no consumer process in between.

```bash
python3 etcd_ch_queue_test.py --etcd HOST:7421 --ch HOST:8123 \
                              --table etcd_events --count 200
```

```python
c = etcd3.client(host=..., port=7421)
c.put("/q/ch:etcd_events/producer/1", json.dumps({"id": 1, "source": "etcd-client"}))
# ... and the row is in ClickHouse default.etcd_events
```

`ch:events` uses the configured database, `ch:analytics.events` names its own.
Requires the `clickhouse` cargo feature on etcds (ytserv turns it on with its
own `clickhouse` feature) and `clickhouse_url` configured. Without an endpoint
the queue behaves like an ordinary one rather than dropping messages, and an
insert that fails leaves the rows queued for the next attempt.

Measured 2026-09-15: **200 of 200** messages produced by the Python client
landed in ClickHouse; both queue-name spellings work.

## A two-stage Python pipeline

Run against a 3-node yaxaha cluster on 2026-09-15:

```
INSERT ──on_commit_python──▶ fn1_src_to_queue.py ──etcd /q/──▶ fn2_queue_to_clickhouse.py ──▶ ClickHouse
```

`fn1_src_to_queue.py` reads the row it was notified about and puts it on the
etcd queue. `fn2_queue_to_clickhouse.py` consumes the queue entry, reads live
cluster status, and writes both through **`CH`** — the `clickhouse_connect`
client bound alongside `DB` and `ETCD`.

Full setup, the pgbench driver and measured results live in the `yaxaha-tests`
repo under `pipeline/`.

## Three things these examples encode

**A node must find itself in `rppd_config`, and the master must be set.** With
no self row, `start_bg` gets `id = 0`, its `select master … where id = $1`
returns nothing, `started` is never set, and every event is then refused with a
misleading `[py] Internal error "not a master"`. Embedded in yaxaha this is now
maintained automatically: ytserv mirrors the cluster roster into the table each
heartbeat and points `master` at the elected cluster leader, calling
`set_master()` so the running instance follows a handover without a restart.
Standalone, set it yourself — the column is `bool unique`, so exactly one row
may be true.

**A read-only function must still end its transaction.** psycopg2 opens one on
the first `execute`, and rppd reuses the connection across calls, so without a
`DB.commit()` (or `rollback()`) the backend sits `idle in transaction` holding
ACCESS SHARE on whatever it read — enough to block `TRUNCATE` or DDL on that
table indefinitely. Both examples commit after reading.

**Under yaxaha, `yt_info()` returns JSON, not a record set.** So this fails:

```sql
SELECT value FROM yt_info('E') AS x(name text, value text, module text)
-- ERROR: a column definition list is only allowed for functions returning "record"
```

Expand it instead:

```sql
SELECT e->>'name', e->>'value' FROM json_array_elements(yt_info('E')) e
```

## Executor notes (both fixed here)

**Drive-token leak.** The executor is token driven: every completed execution
sends `None`, which picks the next queued item. Both paths that shelve an event
because the context pool is at capacity returned without putting a token back,
so each such event destroyed one. Once the in-flight work drained, the count hit
zero and the node stopped executing while its queue still held work — until a
restart, which is why the first runs after a restart always looked healthy.
`respawn_drive()` now guarantees one comes back, with a single outstanding retry
so a burst of shelved events is not a burst of wake-ups.

**Blocking Python on the async runtime.** `invoke()` runs the function
synchronously and was called straight from an async task. On a one-core node
that blocks the only runtime worker for the whole call, starving the completions
that return contexts and send the next token, the gRPC server, and anything else
sharing the runtime. Now wrapped in `tokio::task::block_in_place`.

Worth knowing if you embed rppd: it is easy to read the symptom as a queue or
delivery problem. It is not — throughput simply tracks `max_db_connections` and
then stops.

## Verified

One pgbench run through the whole chain, counted at every hop: 13 queue puts ->
13 dispatched -> 13 acknowledged -> 13 executed -> 13 ClickHouse rows. The queue,
the acknowledge path and the executor are lossless.
