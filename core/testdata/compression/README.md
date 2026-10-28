These deterministic fixtures were generated with archiver/v3 v3.5.1 before
removing that dependency. Each decodes to 256 copies of the UTF-8 string
`Orcas legacy compression interoperability: 0123456789\n` (a newline at the end).
They contain no user data or encryption keys.

The regression test reads these old streams with the new implementation and
reads new output with independent codec APIs. Persisted DATA_CMPR bit values
and the Snappy framing, Zstandard, gzip and Brotli formats stay unchanged.
No legacy archive library is needed to run the tests.
