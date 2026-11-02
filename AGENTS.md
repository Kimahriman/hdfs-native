# AGENTS.md

## Project Overview

This project provides native HDFS (Hadoop Distributed File System) bindings and utilities for Rust and Python. It includes a Rust crate, Python bindings (via maturin), and tools for interacting with HDFS in a performant, cross-platform way.

## Repository Structure

- `rust/`: Core Rust library for HDFS interaction.
- `python/`: Python bindings, CLI, and fsspec integration. Built with maturin.
- `docs/`: Documentation sources.
- `wheels/`: Pre-built Python wheel files for various platforms.

## Building and Development

### Rust
- Use `cargo build` to build Rust components.
- Run Rust tests with `cargo test`.
- Run a single Rust test with `cargo test <testname>`
- Run Rust integration tests with cargo test --features integration-test. This requires Java and Maven to be installed.

#### Rust benchmarks
- Run the I/O benchmarks from the repository root. They start a MiniDFS cluster, so Java, Maven, and Hadoop's `hadoop` command must be available.
- Include the generated MiniDFS config directory in both `HADOOP_CONF_DIR` and at the beginning of `CLASSPATH`. The libhdfs comparison needs these configs to connect to MiniDFS:
  ```bash
  export HADOOP_CONF_DIR="$PWD/rust/target/test"
  export CLASSPATH="$PWD/rust/target/test:$(hadoop classpath)"
  ```
- Run the full I/O benchmark suite with `cargo bench -p hdfs-native --features benchmark --bench io`.
- Pass a Criterion filter after `--` to select a subset. For example, `cargo bench -p hdfs-native --features benchmark --bench io -- write` runs the `write` and `write-ec` groups.

### Python
- Use the Python venv at `python/.venv` for building and running tests.
- Build Python wheels with `maturin build` (see `python/README.md` for details). Include `-E devel` to install development tools.
- Run Python tests with `pytest` in the `python/` directory.

## Agents and Automation

- Agents can automate builds, tests, and packaging for both Rust and Python.
- Use CI/CD to ensure code quality and cross-platform compatibility.
- See `README.md` and `python/README.md` for more detailed instructions.

## Contribution Guidelines

- Follow code style and linting rules for Rust and Python.
- Write tests for new features and bug fixes.
- Document public APIs and major changes.

## Additional Resources

- [README.md](./README.md): Main project overview and usage.
- [python/README.md](./python/README.md): Python-specific instructions.
