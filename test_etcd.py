#!/usr/bin/python3
# this is a script to run RPPD etcd test by inserting into queue
import etcd3
import time

def run():
    ETCD = etcd3.client("localhost", 8881)
    key = '/q/test/input/112'

    x = ETCD.put(key, 'test etcd queue 3')
    print(f"put with previous: {key}={x}")

    time.sleep(1)

    val, _ = ETCD.get(key)
    print(f"should be cleaned: {key}={val}")


if __name__ == '__main__':
    run()
