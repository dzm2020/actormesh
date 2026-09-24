package buffer

import (
	"bytes"
	"io"
	"testing"
)

func benchmarkWrite(b *testing.B, size int) {
	payload := bytes.Repeat([]byte("a"), size)
	buf := New(4096)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		if _, err := buf.Write(payload); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkBuffer_Write_256B(b *testing.B) { benchmarkWrite(b, 256) }
func BenchmarkBuffer_Write_4KB(b *testing.B)  { benchmarkWrite(b, 4*1024) }
func BenchmarkBuffer_Write_64KB(b *testing.B) { benchmarkWrite(b, 64*1024) }
func BenchmarkBuffer_Write_1MB(b *testing.B)  { benchmarkWrite(b, 1024*1024) }
func BenchmarkBuffer_Write_4MB(b *testing.B)  { benchmarkWrite(b, 4*1024*1024) }
func BenchmarkBuffer_Write_10MB(b *testing.B) { benchmarkWrite(b, 10*1024*1024) }

func benchmarkWriteReader(b *testing.B, size int) {
	payload := bytes.Repeat([]byte("b"), size)
	buf := New(4096)

	b.SetBytes(int64(size))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		buf.Reset()
		reader := bytes.NewReader(payload)
		n, err := buf.WriteReader(reader)
		if err != nil {
			b.Fatalf("WriteReader failed: %v", err)
		}
		if n != size {
			b.Fatalf("WriteReader bytes mismatch, got=%d want=%d", n, size)
		}
	}
}

func BenchmarkBuffer_WriteReader_4KB(b *testing.B)  { benchmarkWriteReader(b, 4*1024) }
func BenchmarkBuffer_WriteReader_64KB(b *testing.B) { benchmarkWriteReader(b, 64*1024) }
func BenchmarkBuffer_WriteReader_1MB(b *testing.B)  { benchmarkWriteReader(b, 1024*1024) }

func BenchmarkBuffer_WriteRead_Interleaved_4KB(b *testing.B) {
	const chunk = 4 * 1024
	payload := bytes.Repeat([]byte("c"), chunk)
	readDst := make([]byte, chunk)
	buf := New(4096)

	b.SetBytes(int64(chunk * 2))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := buf.Write(payload); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
		if _, err := io.ReadFull(buf, readDst); err != nil {
			b.Fatalf("ReadFull failed: %v", err)
		}
	}
}
