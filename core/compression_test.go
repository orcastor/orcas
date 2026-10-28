package core

import (
	"bytes"
	"compress/gzip"
	"errors"
	"io"
	"os"
	"testing"

	"github.com/andybalholm/brotli"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
)

func TestCompressionLegacyInteroperability(t *testing.T) {
	want := bytes.Repeat([]byte("Orcas legacy compression interoperability: 0123456789\n"), 256)
	for _, tc := range []struct {
		name string
		flag uint32
	}{
		{"snappy", DATA_CMPR_SNAPPY}, {"zstd", DATA_CMPR_ZSTD},
		{"gzip", DATA_CMPR_GZIP}, {"brotli", DATA_CMPR_BR},
	} {
		t.Run(tc.name, func(t *testing.T) {
			legacy, err := os.ReadFile("testdata/compression/legacy." + tc.name)
			if err != nil {
				t.Fatal(err)
			}
			got, err := DecompressData(legacy, CreateDecompressor(tc.flag))
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("old data unreadable: %v", err)
			}
			kind := tc.flag
			encoded, err := CompressData(want, CreateCompressor(tc.flag, 6), &kind)
			if err != nil || kind != tc.flag {
				t.Fatalf("compression failed or changed persisted flags: %v", err)
			}
			var r io.Reader
			switch tc.name {
			case "snappy":
				r = snappy.NewReader(bytes.NewReader(encoded))
			case "zstd":
				z, e := zstd.NewReader(bytes.NewReader(encoded))
				if e != nil {
					t.Fatal(e)
				}
				defer z.Close()
				r = z
			case "gzip":
				z, e := gzip.NewReader(bytes.NewReader(encoded))
				if e != nil {
					t.Fatal(e)
				}
				defer z.Close()
				r = z
			case "brotli":
				r = brotli.NewReader(bytes.NewReader(encoded))
			}
			got, err = io.ReadAll(r)
			if err != nil || !bytes.Equal(got, want) {
				t.Fatalf("new output incompatible with independent codec reader: %v", err)
			}
			if _, err := DecompressData([]byte("invalid encoded stream"), CreateDecompressor(tc.flag)); err == nil {
				t.Fatal("corrupt stream accepted")
			}
			if err := CreateCompressor(tc.flag, 6).Compress(bytes.NewReader(want), failingCompressionWriter{}); !errors.Is(err, errCompressionWrite) {
				t.Fatalf("output failure hidden: %v", err)
			}
		})
	}
}

var errCompressionWrite = errors.New("compression destination failed")

type failingCompressionWriter struct{}

func (failingCompressionWriter) Write([]byte) (int, error) { return 0, errCompressionWrite }

func TestCompressionDisabledAndEmpty(t *testing.T) {
	if CreateCompressor(0, 0) != nil || CreateDecompressor(0) != nil {
		t.Fatal("disabled compression must preserve plaintext path")
	}
	kind := uint32(0)
	want := []byte("uncompressed")
	got, err := CompressData(want, nil, &kind)
	if err != nil || !bytes.Equal(want, got) {
		t.Fatal("plaintext changed")
	}
	got, err = DecompressData(nil, CreateDecompressor(DATA_CMPR_GZIP))
	if err != nil || len(got) != 0 {
		t.Fatal("empty data no longer supported")
	}
}
