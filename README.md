# srt-rs

[![codecov][codecov badge]][codecov] [![Rust](https://github.com/russelltg/srt-rs/actions/workflows/rust.yml/badge.svg)](https://github.com/russelltg/srt-rs/actions/workflows/rust.yml)


> NOTE: THIS IS NOT PRODUCTION READY.

Pure rust implementation of SRT (Secure Reliable Transport), without unsafe code.

Reference implementation is available at https://github.com/haivision/srt

# Features

- Fast (heap allocations are rare, uses async IO)
- Full safety guarantees of rust

# What works

- [x] Listen server connecting
- [x] Client (connect) connecting
- [x] Rendezvous connecting
- [x] Receiving
- [x] Sending
- [x] Special SRT packets (partial)
- [x] Actual SRT (TSBPD)
- [x] Timestamp drift recovery (not throughly tested)
- [x] Congestion control
- [x] Encryption
- [x] Bidirectional

# Thread Efficiency

The reference implementation of SRT requires 3 threads per sender and 5 threads per receiver. 

With srt-rs, many connections can be driven from a single thread using
[compio's][compio] completion-based (io_uring/IOCP) async runtime; run one runtime per core to scale out. This should allow for handing of many more connections.

# Examples

## Generate and send SRT packets

```
cargo run --example sender
```

## Receive SRT packets

```
cargo run --example receiver
```

# Structure

This repository is structured into 5 crates:
* `srt-protocol`: State machines for the SRT protocol, with no dependencies on futures or an async runtime. Someday, I would like this to be a no-std crate. I expect this to have frequent breaking changes.
* `srt-compio`: [compio][compio] elements written on top of the protocol, expected to be a relatively stable API.
* `srt-transmit`: A srt-live-tranmsit replacement written ontop of `srt-tokio`
* `srt-c`: Experimental C bindings to this crate, intended to be both API and ABI compatiable with the reference implementation
* `srt-c-unittests`: The unit tests from the reference implementation that are ran against `srt-c`. Many of these do not pass yet.

[codecov]: https://codecov.io/gh/russelltg/srt-rs
[codecov badge]: https://codecov.io/gh/russelltg/srt-rs/branch/main/graph/badge.svg
[compio]: https://github.com/compio-rs/compio
