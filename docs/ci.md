# Builds and releases

GitHub Actions builds the CLI on native Linux amd64 and arm64 runners with
Go 1.27.3 and CGO enabled for SQLite. Semaphore uses the same `scripts/ci.sh`
entry point on Linux amd64. Dependencies are downloaded and verified without
updating `go.mod` or `go.sum`; the CLI is written to `dist/orcas` so its output
does not collide with the `cmd/` source directory.

The build compiles and vets every test package, then runs race-enabled SDK and regression
tests for authentication, authorization revocation, request limits, CORS, safe
download paths, private storage permissions, and persisted compression formats, SMB/FUSE metadata, encryption key
switching, chunk buffer/flush safety, concurrent batch persistence, budgeted cache
ownership, journal overlap indexing, SQLite query plans, and pooled deduplication. Run the same checks locally with:

```sh
bash scripts/ci.sh
```

This is a focused release gate, not a claim that the entire legacy test suite
passes. The wider suite includes filesystem mounts, long-running performance
tests and existing failures in core defragmentation, encryption and references,
S3 fixture authentication and bucket statistics concurrency. To investigate it
separately, run `ORCAS_ADMIN_PASSWORD=orcas go test -count=1 -timeout=10m ./...` with the required filesystem
fixtures available. Mount tests additionally require FUSE.

To publish a release, commit release notes at `docs/releases/<tag>.md`, then push
a new semantic version tag such as `v0.2.0` at the tested commit. The Release
workflow reruns both architecture builds and all
release regressions before publishing the two CLI archives and `SHA256SUMS`.
Each archive includes the executable, license, READMEs and example configuration.
The GitHub release description uses the committed notes for that tag. Consumers
of the Go module should require the release tag.

The module now requires Go 1.26.10 or newer and selects the patched Go 1.27.3
toolchain. The SDK integration fixtures explicitly use `ORCAS_ADMIN_PASSWORD=orcas`
inside the test process; production initialization has no default password.
Security regressions set their own temporary credentials and databases.

For a dependency security scan using the same toolchain:

```sh
go run golang.org/x/vuln/cmd/govulncheck@v1.9.0 ./...
```
