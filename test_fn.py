cur = DB.cursor()
cur.execute("SELECT input FROM test_source where id = %s", ([PK]))
input = cur.fetchall()
if len(input) > 0:
    cur.execute("insert into test_sink (data) values (%s)", ( input[0] ))
    DB.commit()
