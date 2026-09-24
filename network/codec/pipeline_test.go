package codec

import (
	"bytes"
	"errors"
	"testing"
)

type testTransform struct {
	flag      uint8
	encodeErr error
	decodeErr error
}

func (t *testTransform) Flag() uint8 { return t.flag }
func (t *testTransform) Encode(data []byte) ([]byte, bool, error) {
	return data, true, t.encodeErr
}
func (t *testTransform) Decode(data []byte) ([]byte, error) {
	return data, t.decodeErr
}

func TestGzipPipelineRoundTrip(t *testing.T) {
	compressor, err := NewGzip(GzipOptions{MinSize: 16, MaxDecodedSize: 4096})
	if err != nil {
		t.Fatalf("NewGzip: %v", err)
	}
	pipeline, err := New(compressor)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	original := bytes.Repeat([]byte("game-connection-payload-"), 32)
	encoded, flags, err := pipeline.Encode(original)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if flags&FlagCompressed == 0 || len(encoded) >= len(original) {
		t.Fatalf("compression not applied: flags=%08b", flags)
	}
	if !bytes.HasPrefix(encoded, []byte{0x1f, 0x8b}) {
		t.Fatalf("encoded payload is not gzip: %x", encoded[:min(len(encoded), 2)])
	}
	decoded, err := pipeline.Decode(encoded, flags)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if !bytes.Equal(decoded, original) {
		t.Fatal("round trip payload mismatch")
	}
}

func TestPipelineValidationAndDecodedLimit(t *testing.T) {
	compressor, err := NewGzip(GzipOptions{MinSize: 1, MaxDecodedSize: 4096})
	if err != nil {
		t.Fatalf("NewGzip: %v", err)
	}
	if _, err = New(compressor, compressor); !errors.Is(err, ErrDuplicateFlag) {
		t.Fatalf("duplicate flag error: %v", err)
	}
	compressed, applied, err := compressor.Encode(bytes.Repeat([]byte("x"), 1024))
	if err != nil || !applied {
		t.Fatalf("Encode: applied=%v err=%v", applied, err)
	}
	decoder, err := NewGzip(GzipOptions{MaxDecodedSize: 128})
	if err != nil {
		t.Fatalf("NewGzip decoder: %v", err)
	}
	if _, err = decoder.Decode(compressed); !errors.Is(err, ErrDecodedTooLarge) {
		t.Fatalf("decoded limit error: %v", err)
	}
}

func TestPipelineAndGzipErrorPaths(t *testing.T) {
	if _, err := New(nil); !errors.Is(err, ErrInvalidFlag) {
		t.Fatalf("nil transform error = %v, want %v", err, ErrInvalidFlag)
	}
	if _, err := New(&testTransform{flag: FlagCompressed | FlagEncrypted}); !errors.Is(err, ErrInvalidFlag) {
		t.Fatalf("multi-bit flag error = %v, want %v", err, ErrInvalidFlag)
	}

	empty, err := New()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("payload")
	encoded, flags, err := empty.Encode(payload)
	if err != nil || flags != 0 || !bytes.Equal(encoded, payload) {
		t.Fatalf("empty encode = %q, %08b, %v", encoded, flags, err)
	}
	if _, err = empty.Decode(payload, FlagCompressed); !errors.Is(err, ErrUnknownFlags) {
		t.Fatalf("unknown flags error = %v, want %v", err, ErrUnknownFlags)
	}

	wantEncode := errors.New("encode failed")
	encoder, err := New(&testTransform{flag: FlagEncrypted, encodeErr: wantEncode})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = encoder.Encode(payload); !errors.Is(err, wantEncode) {
		t.Fatalf("encode error = %v, want %v", err, wantEncode)
	}
	wantDecode := errors.New("decode failed")
	decoder, err := New(&testTransform{flag: FlagEncrypted, decodeErr: wantDecode})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = decoder.Decode(payload, FlagEncrypted); !errors.Is(err, wantDecode) {
		t.Fatalf("decode error = %v, want %v", err, wantDecode)
	}

	invalidOptions := []GzipOptions{
		{Level: 100},
		{MinSize: -1},
		{MaxDecodedSize: -1},
	}
	for _, options := range invalidOptions {
		if _, err = NewGzip(options); !errors.Is(err, ErrInvalidOptions) {
			t.Fatalf("NewGzip(%#v) error = %v, want %v", options, err, ErrInvalidOptions)
		}
	}
	gzipCodec, err := NewGzip(GzipOptions{MinSize: 32})
	if err != nil {
		t.Fatal(err)
	}
	if got, applied, err := gzipCodec.Encode([]byte("small")); err != nil || applied || string(got) != "small" {
		t.Fatalf("small encode = %q, %v, %v", got, applied, err)
	}
	if _, err = gzipCodec.Decode([]byte("not-gzip")); err == nil {
		t.Fatal("invalid gzip payload decoded successfully")
	}
}
