# AGENTS.md

## Project Overview

This project provides native Hadoop and HDFS clients for Rust and Python. The Cargo workspace separates Hadoop-wide foundations from the HDFS implementation, and the Python package is built with maturin.

## Repository Structure

- `rust/hadoop-native/`: Hadoop-wide Rust foundations, including configuration, security, protobuf, and RPC support. This crate has no HDFS-specific APIs.
- `rust/hdfs-native/`: Native Rust HDFS client, including NameNode, DataNode, encryption, and glob functionality. Depends on `hadoop-native`.
- `python/`: Python bindings, CLI, and fsspec integration. Built with maturin.
- `docs/`: Documentation sources.
- `wheels/`: Pre-built Python wheel files for various platforms.

## Building and Development

### Rust
- Use `cargo build --workspace` to build all Rust components.
- Run Rust tests with `cargo test --workspace`, or scope them with `cargo test -p hadoop-native` or `cargo test -p hdfs-native`.
- Run a single Rust test with `cargo test -p <crate> <testname>`.
- Run HDFS integration tests with `cargo test -p hdfs-native --features integration-test`. This requires Java and Maven to be installed.

### Python
- Use the Python venv at `python/.venv` for building and running tests.
- Build Python wheels with `maturin build` (see `python/README.md` for details). Include `-E devel` to install development tools.
- Run Python tests with `pytest` in the `python/` directory.

## Agents and Automation

- Agents can automate builds, tests, and packaging for both Rust and Python.
- Use CI/CD to ensure code quality and cross-platform compatibility.
- Rust releases are independent: pushing `hadoop-v<version>` publishes `hadoop-native`, while `hdfs-v<version>` publishes `hdfs-native`.
- An `hdfs-v<version>` tag also publishes the Python package. Its version must match `hdfs-native`.
- See `README.md` and `python/README.md` for more detailed instructions.

## Contribution Guidelines

- Follow code style and linting rules for Rust and Python.
- Write tests for new features and bug fixes.
- Document public APIs and major changes.

## Additional Resources

- [README.md](./README.md): Main project overview and usage.
- [python/README.md](./python/README.md): Python-specific instructions.
