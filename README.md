# Golem reqwest

[![crates.io](https://img.shields.io/crates/v/reqwest.svg)](https://crates.io/crates/reqwest)
[![Documentation](https://docs.rs/reqwest/badge.svg)](https://docs.rs/reqwest)
[![MIT/Apache-2 licensed](https://img.shields.io/crates/l/reqwest.svg)](./LICENSE-APACHE)

Started as a fork of [reqwest](https://docs.rs/reqwest) to add a WASI-HTTP backend, now it only contains this new
backend while trying to keep the API as close to the original as possible.

To be used in [Golem](https://golem.cloud) components, or any other WASM environment that provides the WASI HTTP 0.2
host API.