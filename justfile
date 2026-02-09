# Default recipe: list available recipes
default:
    @just --list

# Build in debug mode
build:
    cargo build

# Build in release mode
release:
    cargo build --release

# Run all tests
test:
    cargo test

# Run clippy lints
lint:
    cargo clippy -- -D warnings

# Format code
fmt:
    cargo fmt

# Check formatting without modifying files
fmt-check:
    cargo fmt --check

# Run all CI checks
ci: fmt-check lint build test

# Run qontrol with arguments
run *ARGS:
    cargo run -- {{ARGS}}

# Run qontrol with debug logging
run-debug *ARGS:
    cargo run -- -vv {{ARGS}}

# Remove build artifacts
clean:
    cargo clean
