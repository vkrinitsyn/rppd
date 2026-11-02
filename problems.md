# rppd - known problems

Problems of rppd, most severe first, on the scale severe / warning / low: what each did,
and its fix. None is open.

## R1 - low: a standalone rppd did not watch a queue again after the server cancelled it - fixed in 0.2.17

**What.** The embedded etcd cancels the watch of a queue consumer that does not take a
message in time (ETCD-E052). rppd 0.2.16, built against etcds 0.21.2 - whose server never
sent that cancel - had no re-watch, so with a newer etcds a queue whose watch was
cancelled stopped being consumed.

**Fix.** rppd 0.2.17, with etcds 0.23.0 (both on crates.io since 2026-11-01), watches
such a queue again: at once the first time, after a short wait when it is cancelled
again (`rppd/src/rd_etcd.rs` and its unit tests).

