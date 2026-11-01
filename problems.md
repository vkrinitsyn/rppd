# rppd - known problems

Open problems of rppd, most severe first, on the scale severe / warning / low. Each says
what, when, what it does, the workaround, and what a fix needs.

## R1 - low: a standalone rppd does not watch a queue again after the server cancels it

**What.** The embedded etcd cancels the watch of a queue consumer that does not take a
message in time (ETCD-E052), and rppd watches such a queue again. The published rppd
(0.2.16) is built against etcds 0.21.2 from crates.io, whose server never sends that
cancel, and has no such re-watch; this checkout has both, but neither is published.

**Effect.** None for the published pair. An embedder that takes a newer etcds with the
published rppd gets a queue that stops being consumed once its watch is cancelled.

**Workaround.** Use rppd and etcds from the same release - or from these checkouts.

**Prepared, not published.** etcds is 0.21.4 in its checkout; rppd is 0.2.17 and asks
`etcds ^0.21.4`, and its workspace builds against the sibling checkout through
`[patch.crates-io]` until that is published. Left for the owner:

1. In `etcd`: `cargo publish --dry-run`, then `cargo publish`.
2. In `rppd`: take the `[patch.crates-io]` block out of the workspace `Cargo.toml`,
   `cargo update -p etcds`, `cargo test --lib` and `cargo check --bins` in `rppd/`.
3. `cargo publish -p rppd-common` if its version on crates.io is not the one in its
   `Cargo.toml`, then `cargo publish -p rppd` (its `rppd-common` path dependency needs a
   `version` for that, as for any release).

