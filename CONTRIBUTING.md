# Contributing to rppd

Thank you for helping. rppd - Rust Python Postgres Discovery - is licensed MIT; a
contribution is accepted under the same terms (see [LICENSE](LICENSE)).

## Issues

Open an issue on GitHub for a bug or a feature request. For a bug, give the rppd version,
the features it is built with (`etcd-embeded`, `etcd-provided`, `etcd-external`,
`lib-embedded`, `tracer`, `datafusion`), whether it runs as the `rppd` service or
embedded (`rppde`, as YT Cluster's ytserv-all does), the PostgreSQL and Python versions,
the function's code and its `rppd_function` row, and the smallest sequence that shows it:
what you did, what you expected, and what you got.

A security problem is not reported in a public issue: write to
**support@dbinvent.com** instead.

## Pull requests

1. Fork the repository and branch from `main`.
2. Keep a change to one subject; describe what it changes and why.
3. Build and test it (below); add tests for what you change.
4. Messages a user reads - errors, log lines, the help - go through `tr!("module.name")`
   and the locale files `rppd/locales/<language>.yml` (`rppd-common/locales/` for the
   generated gRPC clients): de, en, fr, ja, ru, zh, one file each. Add a new key in every
   file, on the same line in each, with the same `%{placeholders}`; an error's message
   starts with its code, `RPPD-Ennn`, the same in every language. The unit tests check it.
5. A change to the tables rppd keeps goes into `rppd.yaml`, which creates and alters them;
   a change to the wire into `rppd-common/protos/`.
6. Open the pull request against `main`.

## Build and test

`protoc` is needed (`rppd-common/build.rs`), and Python's headers for the embedded
interpreter (pyo3): `apt install protobuf-compiler python3-dev`. The PostgreSQL extension in
`rppd-pg-c/` builds with PGXS (`pg_config`): see its [README](rppd-pg-c/README.md).

```
cargo build                              # the rppd service and the rppde library
cargo test -p rppd --lib                 # the unit tests, Python functions among them
cargo test -p rppd --lib --features tracer   # OpenTelemetry
cargo test -p rppd-common                # the gRPC types, their messages and the language
```

Before a pull request, run the checks as well (`cargo make audit`, `cargo make geiger`):

```
cargo audit                                    # known vulnerabilities in the dependencies
cargo geiger --manifest-path rppd/Cargo.toml   # unsafe code in rppd and its dependencies
```

rppd-common forbids unsafe code of its own (`#![forbid(unsafe_code)]`); rppd keeps one
block, the Python objects dropped under the interpreter's lock (`rppd/src/py.rs`).

## Code of conduct

Taking part in this project means following its
[Code of Conduct](CODE_OF_CONDUCT.md).
