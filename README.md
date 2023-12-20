# RPPD - Rust Python Postgres Discovery

## Build

target platform: Linux, pxrg, as rust postgres trigger engine does not support windows.
https://grpc.io/docs/protoc-installation/

### Trigger build

cargo install cargo-pgrx
cargo pgrx init

cargo build --lib --release -F pg14
cargo pgrx package

v=$(pg_config --version)
echo ${v:11:2}
sudo cp target/release/rppd-pg14/usr/share/postgresql/14/extension/rppd* /usr/share/postgresql/14/extension/
sudo cp target/release/rppd-pg14/usr/lib/postgresql/14/lib/rppd.so /usr/lib/postgresql/14/lib
psql -c "create extension rppd" 


### Server build

cargo install sqlx-cli
cargo sqlx prepare

### FAQ

IF
> Mismatched rust versions: cargo-pgrx Mismatched rust versions: cargo-pgrx

Then (this takes time)
> cargo install cargo-pgrx
> cargo pgrx init 


## Overall architecture

- Postgres extension with a trigger to notify service about a insert or update event.
- The service is a one to many instances with first act as master and forward event execution to the actor nodes or execute.
- All nodes tried to register on master and participate in.
- Function trigger on a target table and possible column named id
- The execution code is a python function. 
- The action also available by schedule to implement a queue.
- All configuration store in db table created by extension creation.
- If main service shutting down of unavailable than one ot service become a master. 
- No election, but whoever first will be able to safe and undoubted update configuration table. 

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

### test code:

> sql = "SELECT * FROM {} where a = %s".format(TABLE)
> print(sql)
>
> cur = DB.cursor()
> cur.execute(sql, (PK))
> print(cur.fetchall())
>
> cur.execute("insert into test (b) values (%s)", ( "{}-{}".format(TOPIC, TRIG),))
> DB.commit()


### test code2
> cur = DB.cursor()
> cur.execute("SELECT * FROM {} ".format(TABLE))
> print(cur.fetchall())



## TODO

### Core features and improvements 
- Python function DB connection to secondary pool i.e. follower DB for handle heavy readonly load
- Python function code sign, approve and verify on call
- Monitoring cadence and hardware with email notification

### Cloud integration 
- RESTapi/PubSub/ServiceBus to trigger events
- OpenTelemetry logging integration like Appins  
- Keyvault integration for a Database connections
