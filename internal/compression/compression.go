package compression

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"io"
	"sync"
)

const (
	None = iota
	Gzip
	Zlib
	Flate
	LZW
	threshold = 50
)

var (
	gzipReaderPool = sync.Pool{
		New: func() interface{} {
			return &gzip.Reader{}
		},
	}
	zlibReaderPool = sync.Pool{
		New: func() interface{} {
			r, _ := zlib.NewReader(nil)
			return r
		},
	}
	flateReaderPool = sync.Pool{
		New: func() interface{} {
			return flate.NewReader(nil)
		},
	}
	lzwReaderPool = sync.Pool{
		New: func() interface{} {
			return lzw.NewReader(nil, lzw.LSB, 8)
		},
	}
	bufferPool = sync.Pool{
		New: func() interface{} {
			return make([]byte, 0, 1024)
		},
	}
)

func CompressData(data []byte) []byte {
	if len(data) < threshold {
		result := make([]byte, len(data)+1)
		result[0] = None
		copy(result[1:], data)
		return result
	}

	compressors := []struct {
		id   byte
		comp func([]byte) ([]byte, error)
	}{
		{Gzip, compressGzip},
		{Zlib, compressZlib},
		{Flate, compressFlate},
		{LZW, compressLZW},
	}

	best := data
	bestType := byte(None)

	for _, c := range compressors {
		if compressed, err := c.comp(data); err == nil && len(compressed) < len(best) {
			best = compressed
			bestType = c.id
		}
	}

	result := make([]byte, len(best)+1)
	result[0] = bestType
	copy(result[1:], best)
	return result
}

func compressLZW(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lzw.NewWriter(&buf, lzw.LSB, 8)
	_, err := writer.Write(data)
	writer.Close()
	return buf.Bytes(), err
}

func compressFlate(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, _ := flate.NewWriter(&buf, flate.DefaultCompression)
	_, err := writer.Write(data)
	writer.Close()
	return buf.Bytes(), err
}

func compressZlib(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, _ := zlib.NewWriterLevel(&buf, zlib.DefaultCompression)
	_, err := writer.Write(data)
	writer.Close()
	return buf.Bytes(), err
}

func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, _ := gzip.NewWriterLevel(&buf, gzip.DefaultCompression)
	_, err := writer.Write(data)
	writer.Close()
	return buf.Bytes(), err
}

func DecompressData(data []byte) []byte {
	if len(data) == 0 {
		return data
	}

	if len(data) > 0 && (data[0] == 0 || data[0] == 1) {
		if data[0] == 1 {
			reader := gzipReaderPool.Get().(*gzip.Reader)
			defer gzipReaderPool.Put(reader)

			if err := reader.Reset(bytes.NewReader(data[1:])); err == nil {
				buf := bufferPool.Get().([]byte)
				defer bufferPool.Put(buf[:0])

				if result, err := io.ReadAll(reader); err == nil {
					reader.Close()
					return result
				}
				reader.Close()
			}
		}
		return data[1:]
	}

	if len(data) > 0 && data[0] <= LZW {
		compressionType := data[0]
		compressedData := data[1:]

		switch compressionType {
		case None:
			return compressedData
		case Gzip:
			reader := gzipReaderPool.Get().(*gzip.Reader)
			defer gzipReaderPool.Put(reader)

			if err := reader.Reset(bytes.NewReader(compressedData)); err == nil {
				if result, err := io.ReadAll(reader); err == nil {
					reader.Close()
					return result
				}
				reader.Close()
			}
		case Zlib:
			reader := zlibReaderPool.Get().(io.ReadCloser)
			defer zlibReaderPool.Put(reader)

			if zlibReader, ok := reader.(interface{ Reset(io.Reader, []byte) error }); ok {
				if err := zlibReader.Reset(bytes.NewReader(compressedData), nil); err == nil {
					if result, err := io.ReadAll(reader); err == nil {
						reader.Close()
						return result
					}
					reader.Close()
				}
			} else {
				if reader, err := zlib.NewReader(bytes.NewReader(compressedData)); err == nil {
					defer reader.Close()
					if result, err := io.ReadAll(reader); err == nil {
						return result
					}
				}
			}
		case Flate:
			reader := flateReaderPool.Get().(io.ReadCloser)
			defer flateReaderPool.Put(reader)

			if flateReader, ok := reader.(flate.Resetter); ok {
				flateReader.Reset(bytes.NewReader(compressedData), nil)
				if result, err := io.ReadAll(reader); err == nil {
					reader.Close()
					return result
				}
				reader.Close()
			} else {
				reader := flate.NewReader(bytes.NewReader(compressedData))
				defer reader.Close()
				if result, err := io.ReadAll(reader); err == nil {
					return result
				}
			}
		case LZW:
			if result, err := io.ReadAll(lzw.NewReader(bytes.NewReader(compressedData), lzw.LSB, 8)); err == nil {
				return result
			}
		}
	}

	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		reader := gzipReaderPool.Get().(*gzip.Reader)
		defer gzipReaderPool.Put(reader)

		if err := reader.Reset(bytes.NewReader(data)); err == nil {
			if result, err := io.ReadAll(reader); err == nil {
				reader.Close()
				return result
			}
			reader.Close()
		}
	}

	return data
}
