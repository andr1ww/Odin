package compression

import (
	"bytes"
	"compress/flate"
	"compress/gzip"
	"compress/lzw"
	"compress/zlib"
	"io"
)

const (
	None = iota
	Gzip
	Zlib
	Flate
	LZW
	threshold = 50
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
		{LZW, compressLZW},
		{Flate, compressFlate},
		{Zlib, compressZlib},
		{Gzip, compressGzip},
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
	writer, _ := flate.NewWriter(&buf, flate.BestCompression)
	_, err := writer.Write(data)
	writer.Close()
	return buf.Bytes(), err
}

func compressZlib(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, _ := zlib.NewWriterLevel(&buf, zlib.BestCompression)
	_, err := writer.Write(data)
	writer.Close()
	return buf.Bytes(), err
}

func compressGzip(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer, _ := gzip.NewWriterLevel(&buf, gzip.BestCompression)
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
			if gzReader, err := gzip.NewReader(bytes.NewReader(data[1:])); err == nil {
				defer gzReader.Close()
				if result, err := io.ReadAll(gzReader); err == nil {
					return result
				}
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
			if reader, err := gzip.NewReader(bytes.NewReader(compressedData)); err == nil {
				defer reader.Close()
				if result, err := io.ReadAll(reader); err == nil {
					return result
				}
			}
		case Zlib:
			if reader, err := zlib.NewReader(bytes.NewReader(compressedData)); err == nil {
				defer reader.Close()
				if result, err := io.ReadAll(reader); err == nil {
					return result
				}
			}
		case Flate:
			reader := flate.NewReader(bytes.NewReader(compressedData))
			defer reader.Close()
			if result, err := io.ReadAll(reader); err == nil {
				return result
			}
		case LZW:
			reader := lzw.NewReader(bytes.NewReader(compressedData), lzw.LSB, 8)
			defer reader.Close()
			if result, err := io.ReadAll(reader); err == nil {
				return result
			}
		}
	}

	if len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b {
		if gzReader, err := gzip.NewReader(bytes.NewReader(data)); err == nil {
			defer gzReader.Close()
			if result, err := io.ReadAll(gzReader); err == nil {
				return result
			}
		}
	}

	return data
}
