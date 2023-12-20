rppd-name = RPPD
rppd-about = Rust Python Postgres Discovery

rppd-usage = 1. Create postgres extension: copy rppd.so, sql, control files, than perform create SQL, this will also create few tables in desired schema
    2. Run as many service as wanted. One of them will be master
    3. Create trigger in desired table. Perform insert or update to fire a function

rppd-default-args = Up to three optional args, in any order: extention schema, where config table located (i.e. extention installed) ended with dot, db connection url, binding IP and port separeted with :
err-too-many-args = "Too many args {$count}"
err-wrong-argument = "Wrong {$string}"

error = Error
err-wrong-port-format = Wrong port format: {$value} {$string}
err-wrong-schema-format = Wrong schema format: {$string}