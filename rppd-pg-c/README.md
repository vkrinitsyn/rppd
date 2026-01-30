# RPPD PostgreSQL Extension (C Implementation)

PostgreSQL trigger extension for RPPD (Remote Python Procedure Daemon) - C implementation.

## Dependencies

- PostgreSQL development headers (postgresql-server-dev-XX)
- libcurl with HTTP/2 support
- protobuf-c (optional, for proto file generation)

### Ubuntu/Debian

```bash
sudo apt-get install postgresql-16 postgresql-client-common postgresql-server-dev-16 libcurl4-openssl-dev libprotobuf-c-dev protobuf-c-compiler
```

### RHEL/CentOS/Fedora

```bash
sudo dnf install postgresql-devel libcurl-devel protobuf-c-devel
```

## Building

```bash
make
sudo make install
```

## Usage

Enable the extension in your PostgreSQL database:

```sql
CREATE EXTENSION rppd;
```

The extension provides:

- `rppd_event()` - Trigger function for table change notifications
- `rppd_info()` - Status query functions (multiple overloads)

## Configuration

Set `rppd.node = 'local'` to disable trigger processing (useful for local development).

## Files

- `src/rppd.c` - Main PostgreSQL extension code
- `src/rppd.h` - Header file with type definitions
- `src/grpc_client.c` - gRPC client using libcurl HTTP/2
- `src/protobuf.c` - Manual protobuf encoding/decoding
- `rppd.control` - Extension control file
- `rppd--0.2.2.sql` - SQL installation script
- `pg_setup.sql` - Tables and triggers setup
