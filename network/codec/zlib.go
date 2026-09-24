package codec

import (
	"bytes"
	"compress/gzip"
	"io"
)

type Gzip struct {
	level          int
	minSize        int
	maxDecodedSize int
}

type GzipOptions struct {
	Level          int
	MinSize        int
	MaxDecodedSize int
}

var _ Transform = (*Gzip)(nil)

func NewGzip(options GzipOptions) (*Gzip, error) {
	if options.Level != 0 && (options.Level < gzip.HuffmanOnly || options.Level > gzip.BestCompression) {
		return nil, ErrInvalidOptions
	}
	if options.MinSize < 0 || options.MaxDecodedSize < 0 {
		return nil, ErrInvalidOptions
	}
	level := options.Level
	if level == 0 {
		level = gzip.DefaultCompression
	}
	return &Gzip{level: level, minSize: options.MinSize, maxDecodedSize: options.MaxDecodedSize}, nil
}

func (z *Gzip) Flag() uint8 { return FlagCompressed }

func (z *Gzip) Encode(data []byte) ([]byte, bool, error) {
	if len(data) < z.minSize {
		return data, false, nil
	}
	var buf bytes.Buffer
	w, err := gzip.NewWriterLevel(&buf, z.level)
	if err != nil {
		return nil, false, err
	}
	_, err = w.Write(data)
	if err != nil {
		return nil, false, err
	}
	if err := w.Close(); err != nil {
		return nil, false, err
	}
	return buf.Bytes(), true, nil
}

func (z *Gzip) Decode(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer r.Close()
	var reader io.Reader = r
	if z.maxDecodedSize > 0 {
		reader = io.LimitReader(r, int64(z.maxDecodedSize)+1)
	}
	decoded, err := io.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	if z.maxDecodedSize > 0 && len(decoded) > z.maxDecodedSize {
		return nil, ErrDecodedTooLarge
	}
	return decoded, nil
}
