# RPPD - Rust Python Postgres Discovery

## Build
> [!IMPORTANT]
The target platform is Linux! The key component is pxrg, a rust postgres trigger engine that does not support windows.

> [!NOTE]
https://grpc.io/docs/protoc-installation/


### Build
```shell
./make.sh
```


### FAQ
> [!NOTE]

IF
> Mismatched rust versions: cargo-pgrx Mismatched rust versions: cargo-pgrx

Then (this takes time)
```shell
cargo install cargo-pgrx
cargo pgrx init 
```

## Overall architecture

- Postgres extension with a trigger to notify backend service about a insert or update or delete event.
- The service is a one to many instances with first act as master and run or forward event execution to the actor nodes.
- All nodes tried to register on master and participate in performance.
- Function trigger created on a target table and relay on topic/queue definition. This might be a column to pass a row id into function.
- The executed code is a python function. 
- The action also available by schedule to implement a cron job.
- All configuration store in db table created by extension.
- If main service shutting down of unavailable than one ot service become a master. 
- No cluster leader election, but whoever be able to safe and undoubted updated self as master on configuration table. 

![topics](rppd%20schema.png)

## Usage

1. Create postgres extension: will copy rppd.so and create few tables in desired schema
2. Run as many service as required:
 - a text file in any format OR a dir with a file: rppd.rppd_config names are: 'schema', 'url', 'bind'
 - three optional args, in any order: config schema, db connection url (starts with 'postgres://', binding IP and port default is 'localhost:8881'
 - use PGPASSWORD env variable (or from file) to postgres connection
 - use PGUSER env variable (or read from file) to postgres connection
 - priority is: env (if used), if no use or not set, than app args param, than file, than default 
3. Perform insert or update to configure a callback AND fire a function 


## Python samples

### variables - kwargs

```python 
DB -- database connection
TOPIC -- fc.fns.topic
TABLE -- fc.fns.schema_table
TRIG -- UPDATE = 0;  INSERT = 1;  DELETE = 2;  TRUNCATE = 3;
"COLUMN_NAME" --i.e. "ID" = see trig_value (up to 3)
```

### python example:
```python

sql = "SELECT * FROM {} where a = %s".format(TABLE)
print(sql)

cur = DB.cursor()
cur.execute(sql, (PK))
print(cur.fetchall())

cur.execute("insert into test (b) values (%s)", ( "{}-{}".format(TOPIC, TRIG),))
DB.commit()
```

### python example2
```python
cur = DB.cursor()
cur.execute("SELECT * FROM {} ".format(TABLE))
print(cur.fetchall())
```

### test example:
```sql
create table if not exists test_source (id serial primary key, input text);
create table if not exists test_sink (id serial primary key, data text);
\set code `cat test_fn.py`
insert into rppd_function (code, checksum, schema_table, topic) values (:'code', 'na', 'public.test_source', '.id');
CREATE TRIGGER test_src_event AFTER INSERT OR UPDATE OR DELETE ON test_source FOR EACH ROW EXECUTE PROCEDURE rppd_event();
insert into test_source (input) values ('test input');
select * from test_sink;
```

### python test example
for the test above
```python
cur = DB.cursor()
cur.execute("SELECT input FROM test_source where id = %s", ([ID]))
input = cur.fetchall()
if len(input) > 0:
    cur.execute("insert into test_sink (data) values (%s)", ( input[0] ))
    DB.commit()
```


## TODO

### Core features and improvements 
- Python function DB connection pool
- Connect to multiple DB (rw/ro - replica)
- Python function code sign, approve and verify on call
- Monitoring cadence and hardware with email notification

### Cloud integration 
- RESTapi/PubSub/ServiceBus to trigger events
- OpenTelemetry logging integration like Appins  
- Keyvault integration for a Database connections


todo: test multiple nodes, save logs, restore incomplete functions
todo impl: queue/topic, cleanup saved logs, add debug function

