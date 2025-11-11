# HTTP Protocol Performance Comparison Report

- [English](HTTP_PROTOCOL_COMPARISON_REPORT.md) | [中文](HTTP_PROTOCOL_COMPARISON_REPORT.zh.md)

## Test Environment

- **OS**: macOS
- **Architecture**: arm64
- **CPU**: Apple Silicon M4 Pro
- **Go Version**: Go 1.18+
- **Test Framework**: Go testing package
- **Test Date**: 2025-11-11
- **Batch Write**: Enabled (ORCAS_BATCH_WRITE_ENABLED=true)
- **Batch Write Threshold**: 64KB
- **TLS Overhead**: Excluded (HTTP/2 uses h2c cleartext, HTTP/3 uses minimal TLS)

## Test Scenarios

### Scenario 1: Small Files (1KB) - 10 Concurrent, 100 Operations

| Protocol | Ops/sec | Throughput (MB/s) | Improvement |
|----------|---------|-------------------|-------------|
| **HTTP/1.1** | 1614.69 | 1.58 | Baseline |
| **HTTP/2 (h2c)** | 1510.15 | 1.47 | -6.47% |
| **HTTP/3 (minimal TLS)** | 1559.88 | 1.52 | -3.39% |

**Analysis:**
- HTTP/1.1 shows **6.47% better performance** than HTTP/2 in this scenario
- HTTP/2 shows **6.47% degradation** vs HTTP/1.1 (protocol overhead, TLS excluded)
- HTTP/3 shows **3.39% degradation** vs HTTP/1.1 (QUIC overhead, minimal TLS)
- HTTP/3 shows **3.29% improvement** vs HTTP/2
- With TLS overhead excluded, HTTP/2 and HTTP/3 perform much closer to HTTP/1.1
- HTTP/3 performs better than HTTP/2 in this scenario

**Key Findings:**
- For small files with low concurrency, HTTP/1.1 still performs best
- After excluding TLS overhead, HTTP/2 and HTTP/3 are much closer to HTTP/1.1
- HTTP/3 performs slightly better than HTTP/2 in low concurrency scenarios
- Protocol overhead is minimal when TLS is excluded

### Scenario 2: Small Files (1KB) - 50 Concurrent, 500 Operations

| Protocol | Ops/sec | Throughput (MB/s) | Improvement |
|----------|---------|-------------------|-------------|
| **HTTP/1.1** | 1729.30 | 1.69 | Baseline |
| **HTTP/2 (h2c)** | 1800.95 | 1.76 | **+4.14%** ⭐ |
| **HTTP/3 (minimal TLS)** | 1817.18 | 1.77 | **+5.08%** ⭐ |

**Analysis:**
- HTTP/2 shows **4.14% improvement** over HTTP/1.1 with high concurrency
- HTTP/3 shows **5.08% improvement** over HTTP/1.1 with high concurrency
- HTTP/3 shows **0.90% improvement** over HTTP/2
- Multiplexing advantages become apparent with higher concurrency
- HTTP/2's and HTTP/3's benefits increase with concurrent request count
- HTTP/3 performs best in high concurrency scenarios

### Scenario 3: Medium Files (64KB) - 10 Concurrent, 50 Operations

| Protocol | Ops/sec | Throughput (MB/s) | Improvement |
|----------|---------|-------------------|-------------|
| **HTTP/1.1** | 1436.86 | 89.80 | Baseline |
| **HTTP/2 (h2c)** | 1175.54 | 73.47 | -18.19% |
| **HTTP/3 (minimal TLS)** | 1152.17 | 72.01 | -19.81% |

**Analysis:**
- HTTP/2 shows **18.19% degradation** vs HTTP/1.1 for medium files
- HTTP/3 shows **19.81% degradation** vs HTTP/1.1 for medium files
- For medium files with low concurrency, HTTP/1.1 performs best
- Protocol overhead becomes more significant with larger file sizes
- Header compression benefits may not outweigh protocol overhead in local testing

## Performance Characteristics

### HTTP/1.1
- **Advantages**: Lower overhead for local testing, no TLS required
- **Disadvantages**: No multiplexing, potential head-of-line blocking
- **Best For**: Local testing, simple scenarios, low concurrency

### HTTP/2
- **Status**: ✅ Fully implemented and tested (using h2c cleartext, TLS excluded)
- **Advantages**: Multiplexing, header compression, better for high concurrency
- **Disadvantages**: Protocol overhead in low concurrency scenarios, more complex setup
- **Best For**: High concurrency, network scenarios, production environments
- **Performance**: Shows 4.14% improvement in high concurrency scenarios (50 concurrent)

### HTTP/3
- **Status**: ✅ Fully implemented and tested (using minimal TLS config)
- **Advantages**: Faster connection establishment, better multiplexing, UDP-based, avoids TCP head-of-line blocking
- **Disadvantages**: QUIC protocol overhead (TLS required by design), more complex setup
- **Best For**: High-latency networks, mobile connections, scenarios with packet loss, high concurrency
- **Performance**: Shows 5.08% improvement in high concurrency scenarios (50 concurrent), best performance among all protocols

## Recommendations

1. **For Local Testing (Low Concurrency)**: Use HTTP/1.1 for simplicity and best performance
2. **For Production (High Concurrency)**: Use HTTP/2 (h2c) or HTTP/3 for better concurrent performance
3. **For High Concurrency Scenarios**: HTTP/3 performs best (+5.08%), followed by HTTP/2 (+4.14%)
4. **For Medium/Large Files (Low Concurrency)**: HTTP/1.1 performs best due to lower protocol overhead
5. **For Real Network Scenarios**: HTTP/2 and HTTP/3 advantages will be more pronounced with network latency
6. **TLS Exclusion**: Tests use h2c (HTTP/2 cleartext) and minimal TLS (HTTP/3) to exclude TLS overhead for fair comparison

## Summary

**Key Insights:**
- **Low Concurrency (10), Small Files**: HTTP/1.1 performs best (6.47% better than HTTP/2, 3.39% better than HTTP/3)
- **High Concurrency (50), Small Files**: HTTP/3 performs best (+5.08%), followed by HTTP/2 (+4.14%)
- **Low Concurrency (10), Medium Files**: HTTP/1.1 performs best (18-20% better than HTTP/2 and HTTP/3)
- **TLS Exclusion Impact**: After excluding TLS overhead, HTTP/2 and HTTP/3 perform much closer to HTTP/1.1
- **Concurrency Matters**: HTTP/2's and HTTP/3's advantages increase significantly with concurrency level
- **HTTP/3 Performance**: Best performer in high concurrency scenarios (+5.08% vs HTTP/1.1)
- **Protocol Ranking (Low Concurrency)**: HTTP/1.1 > HTTP/3 > HTTP/2
- **Protocol Ranking (High Concurrency)**: HTTP/3 > HTTP/2 > HTTP/1.1

## Running the Tests

```bash
# Run HTTP protocol comparison test
cd s3
ORCAS_BATCH_WRITE_ENABLED=true go test -v -run TestHTTPProtocolComparison -timeout 5m

# Run comprehensive comparison
ORCAS_BATCH_WRITE_ENABLED=true go test -v -run TestHTTPProtocolComparisonComprehensive -timeout 10m
```

## Notes

- **TLS Overhead Excluded**: HTTP/2 uses h2c (cleartext) to completely exclude TLS overhead. HTTP/3 uses minimal TLS configuration (QUIC requires TLS by design).
- **Low Concurrency**: HTTP/1.1 performs best in low concurrency scenarios due to lower protocol overhead
- **High Concurrency**: HTTP/3 and HTTP/2 show significant advantages (+5.08% and +4.14% respectively)
- **Medium Files**: Protocol overhead becomes more significant with larger files in local testing
- **Real Network Scenarios**: HTTP/2 and HTTP/3 advantages will be more pronounced with network latency and packet loss
- **HTTP/3 Testing**: Requires quic-go library installation: `go get github.com/quic-go/quic-go/http3`

## Implementation Status

- ✅ **HTTP/1.1**: Fully implemented and tested
- ✅ **HTTP/2**: Fully implemented and tested
- ✅ **HTTP/3**: Fully implemented and tested (using quic-go library)

**Latest Test Results (TLS Overhead Excluded):**

**Small Files (1KB), 10 Concurrent:**
- HTTP/1.1: 1614.69 ops/sec
- HTTP/2 (h2c): 1510.15 ops/sec (-6.47%)
- HTTP/3 (minimal TLS): 1559.88 ops/sec (-3.39%)

**Small Files (1KB), 50 Concurrent:**
- HTTP/1.1: 1729.30 ops/sec
- HTTP/2 (h2c): 1800.95 ops/sec (+4.14%) ⭐
- HTTP/3 (minimal TLS): 1817.18 ops/sec (+5.08%) ⭐

**Medium Files (64KB), 10 Concurrent:**
- HTTP/1.1: 1436.86 ops/sec
- HTTP/2 (h2c): 1175.54 ops/sec (-18.19%)
- HTTP/3 (minimal TLS): 1152.17 ops/sec (-19.81%)

**Note**: After excluding TLS overhead, HTTP/2 and HTTP/3 perform much better, especially in high concurrency scenarios. HTTP/3 shows the best performance in high concurrency scenarios.

