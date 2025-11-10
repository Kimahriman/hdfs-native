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

### Python
- Build Python wheels with `maturin build` (see `python/README.md` for details).
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
