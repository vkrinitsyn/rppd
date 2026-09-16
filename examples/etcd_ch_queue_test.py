#!/usr/bin/env python3
"""etcd queue -> ClickHouse, with nothing in between.

A queue named `/q/ch:<table>/` (or `/queue/clickhouse:<table>/`) is consumed by
the etcd server itself: each message's JSON value is written straight into that
ClickHouse table as JSONEachRow. No watcher, no Python function, no PostgreSQL
round trip -- this script only needs an etcd client.

Requires ytserv built with the `clickhouse` feature (which turns on the etcds
`clickhouse` feature) and `clickhouse_url` configured on the node.

    python3 etcd_ch_queue_test.py [--etcd HOST:PORT] [--ch HOST:PORT]
                                  [--table NAME] [--count N]

Exit code 0 when every produced row is found in ClickHouse.
"""

import argparse
import json
import sys
import time
import urllib.parse
import urllib.request

import etcd3


def ch_query(ch, sql):
    # POST with the statement in the body: ClickHouse treats a GET as read-only,
    # so DDL over GET comes back as 500
    req = urllib.request.Request("http://{}/".format(ch), data=sql.encode())
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.read().decode().strip()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--etcd", default="192.168.1.70:7421")
    ap.add_argument("--ch", default="192.168.1.212:8123")
    ap.add_argument("--table", default="etcd_events")
    ap.add_argument("--count", type=int, default=25)
    ap.add_argument("--timeout", type=int, default=60, help="seconds to wait for the rows")
    a = ap.parse_args()

    host, _, port = a.etcd.partition(":")
    cli = etcd3.client(host=host, port=int(port or 2379))

    # the queue name carries the target table; /q/<name>/producer/<n> is the
    # producer path the dispatcher routes from
    queue = "ch:{}".format(a.table)
    base = int(time.time()) % 100000
    # tag every row of THIS run: ids are time-derived and ranges from earlier
    # runs overlap, so counting by id range over-reports
    tag = "etcd-client-{}".format(base)

    ch_query(a.ch, "CREATE TABLE IF NOT EXISTS default.{} ("
                   "id Int32, source String, payload Nullable(String), "
                   "ts DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY id"
                   .format(a.table))
    before = int(ch_query(a.ch, "SELECT count() FROM default.{}".format(a.table)))

    print("producing {} message(s) to /q/{}/producer/* on {}".format(a.count, queue, a.etcd))
    ids = []
    for i in range(a.count):
        rid = base + i
        ids.append(rid)
        cli.put("/q/{}/producer/{}".format(queue, rid),
                json.dumps({"id": rid, "source": tag,
                            "payload": "message {} of {}".format(i + 1, a.count)}))

    mine = "SELECT count() FROM default.{} WHERE source = '{}'".format(a.table, tag)
    deadline = time.time() + a.timeout
    found = 0
    while time.time() < deadline:
        time.sleep(2)
        found = int(ch_query(a.ch, mine))
        if found >= a.count:
            break

    got = int(ch_query(a.ch, "SELECT count() FROM default.{}".format(a.table)))
    print("ClickHouse default.{}: {} row(s) total, {} of {} from this run"
          .format(a.table, got, found, a.count))

    if found == a.count:
        sample = ch_query(a.ch, "SELECT id, source, payload FROM default.{} "
                                "WHERE source = '{}' ORDER BY id LIMIT 3 FORMAT TSV"
                                .format(a.table, tag))
        print("sample:\n" + sample)
        print("PASS")
        return 0

    print("FAIL: {} of {} rows arrived".format(found, a.count), file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
