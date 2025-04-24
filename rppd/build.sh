#!/bin/bash
set -e
cargo build
echo
echo  lib-embedded,etcd-provided
cargo build --features lib-embedded,etcd-provided --no-default-features --lib
echo
echo etcd-provided
cargo build --features etcd-provided --no-default-features --lib
echo
echo etcd-external
cargo build --features etcd-external --no-default-features
