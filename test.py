#!/usr/bin/python3
import psycopg2

def run():
    DB = psycopg2.connect(host="localhost", database="$USER", user="$USER", password="")
    PK = 1
# this is a function code start>
    cur = DB.cursor()
    cur.execute("SELECT input FROM test_source where id = %s", ([PK]))
    input = cur.fetchall()
    if len(input) > 0:
        cur.execute("insert into test_sink (data) values (%s)", ( input[0] ))
        DB.commit()

if __name__ == '__main__':
    run()