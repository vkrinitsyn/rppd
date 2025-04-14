#!/usr/bin/python3
import etcd3

def run():
    ETCD = etcd3.client("localhost", 8881)
    ID = 1
    key = 'nodes'
    ETCD.put(key, '111')
    val, meta = ETCD.get(key)
    v = val.decode('UTF-8') + " /e"
    print(f"got: {key}={v}")

# this is a function code start>
#     cur = DB.cursor()
#     cur.execute("SELECT input FROM test_source where id = %s", ([ID]))
#     input = cur.fetchall()
#     if len(input) > 0:
#         cur.execute("insert into test_sink (data) values (%s)", ( input[0] ))
#         DB.commit()

if __name__ == '__main__':
    run()
