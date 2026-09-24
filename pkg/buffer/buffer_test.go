package buffer

import (
	"bytes"
	"errors"
	"io"
	"math/rand"
	"strings"
	"testing"
)

func TestNew_DefaultInitialCapacity(t *testing.T) {
	b := New(0)
	if b == nil {
		t.Fatal("buffer should not be nil")
	}
	if got, want := b.Cap(), defaultInitialCap; got != want {
		t.Fatalf("unexpected capacity, got=%d want=%d", got, want)
	}
	if got := b.Len(); got != 0 {
		t.Fatalf("new buffer should be empty, got len=%d", got)
	}
}

func TestBuffer_WriteReadPeekSkipReset(t *testing.T) {
	b := New(64)

	n, err := b.Write([]byte("hello world"))
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}
	if n != 11 {
		t.Fatalf("unexpected write bytes, got=%d want=11", n)
	}

	peek, err := b.Peek(5)
	if err != nil {
		t.Fatalf("peek failed: %v", err)
	}
	if string(peek) != "hello" {
		t.Fatalf("unexpected peek data, got=%q", string(peek))
	}

	if err = b.Skip(6); err != nil {
		t.Fatalf("skip failed: %v", err)
	}
	if got := b.String(); got != "world" {
		t.Fatalf("unexpected readable string after skip, got=%q want=%q", got, "world")
	}

	dst := make([]byte, 5)
	n, err = b.Read(dst)
	if err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if n != 5 || string(dst) != "world" {
		t.Fatalf("unexpected read result, n=%d data=%q", n, string(dst))
	}

	if _, err = b.ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("read empty buffer should return EOF, got=%v", err)
	}

	b.Reset()
	if b.Len() != 0 {
		t.Fatalf("reset should clear buffer, got len=%d", b.Len())
	}
}

func TestBuffer_ReadableReturnsCopy(t *testing.T) {
	b := New(64)
	_, _ = b.Write([]byte("abc"))

	readable := b.Readable()
	readable[0] = 'z'

	view := b.View()
	if string(view) != "abc" {
		t.Fatalf("readable should return copy, got underlying=%q", string(view))
	}
}

func TestBuffer_WriteByteAndReadByte(t *testing.T) {
	b := New(8)

	if err := b.WriteByte('a'); err != nil {
		t.Fatalf("WriteByte failed: %v", err)
	}
	if err := b.WriteByte('b'); err != nil {
		t.Fatalf("WriteByte failed: %v", err)
	}

	c, err := b.ReadByte()
	if err != nil || c != 'a' {
		t.Fatalf("unexpected first byte, got=%q err=%v", c, err)
	}
	c, err = b.ReadByte()
	if err != nil || c != 'b' {
		t.Fatalf("unexpected second byte, got=%q err=%v", c, err)
	}
}

func TestBuffer_PeekAndSkipEdgeCases(t *testing.T) {
	b := New(16)
	_, _ = b.Write([]byte("abc"))

	if peek, err := b.Peek(0); err != nil || len(peek) != 0 {
		t.Fatalf("peek(0) should succeed with empty slice, err=%v len=%d", err, len(peek))
	}

	if err := b.Skip(0); err != nil {
		t.Fatalf("skip(0) should not fail: %v", err)
	}

	if _, err := b.Peek(4); err == nil {
		t.Fatal("peek beyond readable length should fail")
	}
	if err := b.Skip(4); err == nil {
		t.Fatal("skip beyond readable length should fail")
	}
}

func TestBuffer_WriteReader(t *testing.T) {
	b := New(64)
	reader := strings.NewReader("hello-reader")

	n, err := b.WriteReader(reader)
	if err != nil {
		t.Fatalf("WriteReader failed: %v", err)
	}
	if n != len("hello-reader") {
		t.Fatalf("unexpected bytes read, got=%d", n)
	}
	if got := b.String(); got != "hello-reader" {
		t.Fatalf("unexpected buffer content, got=%q", got)
	}
}

type errReader struct {
	data []byte
	read bool
}

func (r *errReader) Read(p []byte) (int, error) {
	if !r.read {
		r.read = true
		n := copy(p, r.data)
		return n, errors.New("reader boom")
	}
	return 0, io.EOF
}

func TestBuffer_WriteReaderReturnsPartialAndError(t *testing.T) {
	b := New(64)
	r := &errReader{data: []byte("partial")}

	n, err := b.WriteReader(r)
	if err == nil {
		t.Fatal("WriteReader should return error when reader fails")
	}
	if n != len("partial") {
		t.Fatalf("unexpected partial bytes, got=%d", n)
	}
	if got := b.String(); got != "partial" {
		t.Fatalf("unexpected buffer content, got=%q", got)
	}
}

func TestBuffer_WriteReaderOverLimit(t *testing.T) {
	b := New(64)
	reader := strings.NewReader(strings.Repeat("a", maxBufferCap+1))
	n, err := b.WriteReader(reader)
	if !errors.Is(err, ErrBufferOverLimit) {
		t.Fatalf("expected ErrBufferOverLimit, got=%v", err)
	}
	if n != maxBufferCap {
		t.Fatalf("unexpected partial bytes, got=%d want=%d", n, maxBufferCap)
	}
	if b.Len() != maxBufferCap {
		t.Fatalf("buffer length mismatch, got=%d want=%d", b.Len(), maxBufferCap)
	}
}

func TestBuffer_GrowKeepsData(t *testing.T) {
	b := New(4)

	if n, err := b.Write([]byte("abcd")); err != nil || n != 4 {
		t.Fatalf("unexpected first write, n=%d err=%v", n, err)
	}
	n, err := b.Write([]byte("efghij"))
	if err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	if n != 6 {
		t.Fatalf("second write should write all bytes, got=%d want=6", n)
	}

	if got := string(b.Bytes()); got != "abcdefghij" {
		t.Fatalf("buffer should keep all data after grow, got=%q", got)
	}
	if b.Cap() < 10 {
		t.Fatalf("capacity should grow to fit data, got=%d", b.Cap())
	}
}

func TestNew_ClampToMaxBufferCap(t *testing.T) {
	b := New(maxBufferCap + 1024)
	if got, want := b.Cap(), maxBufferCap; got != want {
		t.Fatalf("initial capacity should be clamped, got=%d want=%d", got, want)
	}
}

func TestBuffer_WriteOverLimit(t *testing.T) {
	b := New(8)
	data := make([]byte, maxBufferCap+1)
	n, err := b.Write(data)
	if !errors.Is(err, ErrBufferOverLimit) {
		t.Fatalf("expected ErrBufferOverLimit, got=%v", err)
	}
	if n != 0 {
		t.Fatalf("write over limit should not write data, got n=%d", n)
	}
}

func TestBuffer_WriteByteOverLimit(t *testing.T) {
	b := New(maxBufferCap)
	if _, err := b.Write(make([]byte, maxBufferCap)); err != nil {
		t.Fatalf("prepare data failed: %v", err)
	}
	if err := b.WriteByte('x'); !errors.Is(err, ErrBufferOverLimit) {
		t.Fatalf("expected ErrBufferOverLimit, got=%v", err)
	}
}

func TestBuffer_CompactAfterReadThenWrite(t *testing.T) {
	b := New(8)
	_, _ = b.Write([]byte("123456"))

	readPart := make([]byte, 4)
	if _, err := b.Read(readPart); err != nil {
		t.Fatalf("read failed: %v", err)
	}
	if string(readPart) != "1234" {
		t.Fatalf("unexpected read data, got=%q", string(readPart))
	}

	if n, err := b.Write([]byte("7890")); err != nil || n != 4 {
		t.Fatalf("write after read failed, n=%d err=%v", n, err)
	}
	if got := string(b.Bytes()); got != "567890" {
		t.Fatalf("unexpected data after compact+write, got=%q", got)
	}
}

func TestBuffer_RandomReadWriteModelConsistency(t *testing.T) {
	b := New(16)
	model := make([]byte, 0, 64)
	rng := rand.New(rand.NewSource(42))

	for i := 0; i < 300; i++ {
		if len(model) == 0 || rng.Intn(100) < 65 {
			// write path
			n := rng.Intn(12) + 1
			data := make([]byte, n)
			for j := 0; j < n; j++ {
				data[j] = byte(rng.Intn(26) + 'a')
			}
			written, err := b.Write(data)
			if err != nil {
				t.Fatalf("write failed at step %d: %v", i, err)
			}
			if written != len(data) {
				t.Fatalf("partial write at step %d: got=%d want=%d", i, written, len(data))
			}
			model = append(model, data...)
			continue
		}

		// read path
		maxRead := len(model)
		n := rng.Intn(maxRead) + 1
		dst := make([]byte, n)
		readN, err := b.Read(dst)
		if err != nil {
			t.Fatalf("read failed at step %d: %v", i, err)
		}
		if readN != n {
			t.Fatalf("partial read at step %d: got=%d want=%d", i, readN, n)
		}
		if !bytes.Equal(dst, model[:n]) {
			t.Fatalf("read data mismatch at step %d: got=%q want=%q", i, string(dst), string(model[:n]))
		}
		model = model[n:]
	}

	if !bytes.Equal(b.Bytes(), model) {
		t.Fatalf("final state mismatch: got=%q want=%q", string(b.Bytes()), string(model))
	}
}
