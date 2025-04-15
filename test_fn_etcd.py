cur = DB.cursor()
cur.execute("insert into test_sink (data) values (%s||'='||%s)", ( KEY, VALUE.decode('UTF-8')))
DB.commit()
