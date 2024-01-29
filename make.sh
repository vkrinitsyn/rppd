#!/bin/bash
set -e
#/*%LPH%*/

pgv=$(pg_config --version)
v=${pgv:11:2}

echo "Check prerequisites 'cargo-pgrx' for a build for Postgres# "
cargo pgrx info version $v || { cargo install cargo-pgrx; cargo pgrx init; }

echo "Building ..."
psql -c "drop extension if exists rppd;" || echo "USE this for a manual drop override: psql -c \"drop extension if exists rppd cascade;\""

cargo build --release -F pg$v
#cargo build --lib --release -F pg$v

echo "Prepare extension files to copy"
cargo pgrx package

echo "Copy extension files into .../postgresql/$v/... (required sudo)"
sudo cp target/release/rppd-pg$v/usr/share/postgresql/$v/extension/rppd* /usr/share/postgresql/$v/extension/
sudo cp target/release/rppd-pg$v/usr/lib/postgresql/$v/lib/rppd.so /usr/lib/postgresql/$v/lib

psql -c "create extension if not exists rppd"
psql -c "\d rppd_config"

#cargo build --bin rppd --release -F pg$v

echo "Release build completed OK. Use 'target/release/rppd' to run a server, see README test example for a next step"
