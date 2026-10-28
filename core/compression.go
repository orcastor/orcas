package core

import (
	"io"

	"github.com/andybalholm/brotli"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/klauspost/pgzip"
)

// Compressor and Decompressor describe streams, not archive extraction. Keep
// the persisted DATA_CMPR flags and underlying codecs compatible with existing
// buckets without importing a general-purpose archive reader.
type Compressor interface {
	Compress(io.Reader, io.Writer) error
}

type Decompressor interface {
	Decompress(io.Reader, io.Writer) error
}

type compressFunc func(io.Reader, io.Writer) error

func (f compressFunc) Compress(r io.Reader, w io.Writer) error { return f(r, w) }

type decompressFunc func(io.Reader, io.Writer) error

func (f decompressFunc) Decompress(r io.Reader, w io.Writer) error { return f(r, w) }

func copyAndClose(w io.WriteCloser, r io.Reader) error {
	_, err := io.Copy(w, r)
	closeErr := w.Close()
	if err != nil {
		return err
	}
	return closeErr
}

func snappyCompressor() Compressor {
	return compressFunc(func(r io.Reader, w io.Writer) error {
		return copyAndClose(snappy.NewBufferedWriter(w), r)
	})
}

func snappyDecompressor() Decompressor {
	return decompressFunc(func(r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, snappy.NewReader(r))
		return err
	})
}

func zstdCompressor(level int) Compressor {
	return compressFunc(func(r io.Reader, w io.Writer) error {
		z, err := zstd.NewWriter(w, zstd.WithEncoderLevel(zstd.EncoderLevelFromZstd(level)))
		if err != nil {
			return err
		}
		return copyAndClose(z, r)
	})
}

func zstdDecompressor() Decompressor {
	return decompressFunc(func(r io.Reader, w io.Writer) error {
		z, err := zstd.NewReader(r)
		if err != nil {
			return err
		}
		defer z.Close()
		_, err = io.Copy(w, z)
		return err
	})
}

func gzipCompressor(level int) Compressor {
	return compressFunc(func(r io.Reader, w io.Writer) error {
		z, err := pgzip.NewWriterLevel(w, level)
		if err != nil {
			return err
		}
		return copyAndClose(z, r)
	})
}

func gzipDecompressor() Decompressor {
	return decompressFunc(func(r io.Reader, w io.Writer) error {
		z, err := pgzip.NewReader(r)
		if err != nil {
			return err
		}
		_, err = io.Copy(w, z)
		closeErr := z.Close()
		if err != nil {
			return err
		}
		return closeErr
	})
}

func brotliCompressor(quality int) Compressor {
	return compressFunc(func(r io.Reader, w io.Writer) error {
		return copyAndClose(brotli.NewWriterLevel(w, quality), r)
	})
}

func brotliDecompressor() Decompressor {
	return decompressFunc(func(r io.Reader, w io.Writer) error {
		_, err := io.Copy(w, brotli.NewReader(r))
		return err
	})
}
