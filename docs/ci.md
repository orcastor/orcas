# Builds and releases

GitHub Actions builds the CLI on native Linux amd64 and arm64 runners with
Go 1.27.3 and CGO enabled for SQLite. Semaphore uses the same `scripts/ci.sh`
entry point on Linux amd64. Dependencies are downloaded and verified without
updating `go.mod` or `go.sum`; the CLI is written to `dist/orcas` so its output
does not collide with the `cmd/` source directory.

The build compiles and vets every test package, then runs race-enabled SDK and regression
tests for persisted compression formats, SMB/FUSE metadata, encryption key
switching, and chunk buffer/flush safety. Run the same checks locally with:

```sh
bash scripts/ci.sh
```

This is a focused release gate, not a claim that the entire legacy test suite
passes. The wider suite includes filesystem mounts, long-running performance
tests and existing failures in core defragmentation, encryption and references,
S3 fixture authentication, batch writer races, and bucket statistics concurrency. To investigate it
separately, run `go test -count=1 -timeout=10m ./...` with the required filesystem
fixtures available. Mount tests additionally require FUSE.

To publish a release, push a new semantic version tag such as `v0.1.0` at the
tested commit. The Release workflow reruns both architecture builds and all
release regressions before publishing the two CLI archives and `SHA256SUMS`.
Each archive includes the executable, license, READMEs and example configuration.
Consumers of the Go module should require the release tag.
