#!/usr/bin/python3
import etcd3

def run():
    ETCD = etcd3.client("localhost", 8881)
    key = '/q/test/input/111'

    val, _ = ETCD.get(key)
    print(f"before put: {key}={val}")

    x = ETCD.put(key, 'test etcd queue 2')
    # val, meta = ETCD.get(key)
    # v = val.decode('UTF-8') + " /e"
    print(f"got: {x}")


if __name__ == '__main__':
    run()
