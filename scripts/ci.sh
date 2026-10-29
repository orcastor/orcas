#!/usr/bin/env bash
set -euo pipefail

# SQLite requires CGO. Build on a native runner for each release architecture.
export CGO_ENABLED=1

go mod download
go mod verify
go build -mod=readonly ./...

# Compile and vet every test package. The legacy integration/stress suite is
# separate from these release regressions; see docs/ci.md for its limitations.
go test -mod=readonly -run '^$' ./...
go test -mod=readonly -race -short -count=1 -timeout=5m ./sdk
go test -mod=readonly -race -count=1 -timeout=5m ./core -run '^TestCompression'
go test -mod=readonly -race -count=1 -timeout=5m ./vfs \
  -run '^(TestGetattr|TestLookupReturnsValidLinkCount|TestStatxReportsObjectType|TestQuiesceWrites|TestSetEndecKeySafe|TestHasPendingWriteState|TestGetEndecKeyForFS|TestReleaseChunkBuffer|TestTmpRenameDetection|TestChunkedFileWriterFlushSkips)'

mkdir -p dist
go build -mod=readonly -trimpath -o dist/orcas ./cmd
dist/orcas -h

# A dependency update must not silently change the committed module graph.
git diff --exit-code -- go.mod go.sum
