# rusty-scylla-tests
rusty-scylla-tests is a collection of Scylla tests written in Rust.


# Example usage:
* Print help:
```bash
cargo run --release -- --help
```
* Run test with default values:
```bash
cargo run --release -- many-views-partitions-delete
```
* See test arguments:
```bash
cargo run --release -- many-views-partitions-delete --help
```
* Run test with arguments:
```bash
cargo run --release -- many-views-partitions-delete --nodes 10.0.1.3,10.0.1.4
```

All tests require a running scylla cluster, you can start a cluster using [ccm](https://github.com/scylladb/scylla-ccm).
