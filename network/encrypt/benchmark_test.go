package encrypt

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"
	"net"
	"testing"
)

const benchmarkPayloadSize = 4 * 1024

var benchmarkSink []byte

func benchmarkPayload() []byte {
	p := make([]byte, benchmarkPayloadSize)
	for i := range p {
		p[i] = byte(i)
	}
	return p
}

func benchmarkAEAD(b *testing.B) cipher.AEAD {
	b.Helper()
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i + 1)
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		b.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		b.Fatal(err)
	}
	return aead
}

func BenchmarkEncrypt4K_AES256GCM_CounterNonce_ReuseBuffer(b *testing.B) {
	payload := benchmarkPayload()
	aead := benchmarkAEAD(b)
	nonce := make([]byte, aead.NonceSize())
	dst := make([]byte, 0, len(payload)+aead.Overhead())
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(nonce[len(nonce)-8:], uint64(i))
		benchmarkSink = aead.Seal(dst[:0], nonce, payload, nil)
	}
}

func BenchmarkEncrypt4K_AES256GCM_RandomNonce_ReuseBuffer(b *testing.B) {
	payload := benchmarkPayload()
	aead := benchmarkAEAD(b)
	nonce := make([]byte, aead.NonceSize())
	dst := make([]byte, 0, len(payload)+aead.Overhead())
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := rand.Read(nonce); err != nil {
			b.Fatal(err)
		}
		benchmarkSink = aead.Seal(dst[:0], nonce, payload, nil)
	}
}

func BenchmarkEncrypt4K_AES256GCM_CounterNonce_Allocate(b *testing.B) {
	payload := benchmarkPayload()
	aead := benchmarkAEAD(b)
	nonce := make([]byte, aead.NonceSize())
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		binary.BigEndian.PutUint64(nonce[len(nonce)-8:], uint64(i))
		benchmarkSink = aead.Seal(nil, nonce, payload, nil)
	}
}

func BenchmarkEncrypt4K_XOR_ReuseBuffer(b *testing.B) {
	payload := benchmarkPayload()
	key := []byte("0123456789abcdef0123456789abcdef")
	dst := make([]byte, len(payload))
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i, v := range payload {
			dst[i] = v ^ key[i&31]
		}
	}
	benchmarkSink = dst
}

func BenchmarkEncrypt4K_XOR_Allocate(b *testing.B) {
	payload := benchmarkPayload()
	key := []byte("0123456789abcdef0123456789abcdef")
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		dst := make([]byte, len(payload))
		for i, v := range payload {
			dst[i] = v ^ key[i&31]
		}
		benchmarkSink = dst
	}
}

func BenchmarkEncrypt4K_CurrentECDHTransform(b *testing.B) {
	left, err := NewECDHCipher()
	if err != nil {
		b.Fatal(err)
	}
	right, err := NewECDHCipher()
	if err != nil {
		b.Fatal(err)
	}
	if err := left.GenerateSharedKey(right.PublicKey()); err != nil {
		b.Fatal(err)
	}
	payload := benchmarkPayload()
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkSink, err = left.Encrypt(payload)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecrypt4K_AES256GCM_ReuseBuffer(b *testing.B) {
	payload := benchmarkPayload()
	aead := benchmarkAEAD(b)
	nonce := make([]byte, aead.NonceSize())
	sealed := aead.Seal(nil, nonce, payload, nil)
	dst := make([]byte, 0, len(payload))
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchmarkSink, err = aead.Open(dst[:0], nonce, sealed, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecrypt4K_AES256GCM_Allocate(b *testing.B) {
	payload := benchmarkPayload()
	aead := benchmarkAEAD(b)
	nonce := make([]byte, aead.NonceSize())
	sealed := aead.Seal(nil, nonce, payload, nil)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		benchmarkSink, err = aead.Open(nil, nonce, sealed, nil)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkDecrypt4K_XOR_ReuseBuffer(b *testing.B) {
	payload := benchmarkPayload()
	key := []byte("0123456789abcdef0123456789abcdef")
	sealed := make([]byte, len(payload))
	for i, v := range payload {
		sealed[i] = v ^ key[i&31]
	}
	dst := make([]byte, len(payload))
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i, v := range sealed {
			dst[i] = v ^ key[i&31]
		}
	}
	benchmarkSink = dst
}

func BenchmarkDecrypt4K_CurrentECDHTransform(b *testing.B) {
	left, err := NewECDHCipher()
	if err != nil {
		b.Fatal(err)
	}
	right, err := NewECDHCipher()
	if err != nil {
		b.Fatal(err)
	}
	if err := left.GenerateSharedKey(right.PublicKey()); err != nil {
		b.Fatal(err)
	}
	payload := benchmarkPayload()
	sealed, err := left.Encrypt(payload)
	if err != nil {
		b.Fatal(err)
	}
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkSink, err = left.Decrypt(sealed)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// benchmarkTCP4K measures the full local send path: encrypt then one Write per
// 4096-byte payload over loopback TCP, with the peer draining the socket.
func benchmarkTCP4K(b *testing.B, seal func([]byte) []byte) {
	b.Helper()
	payload := benchmarkPayload()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		b.Fatal(err)
	}
	defer ln.Close()

	done := make(chan struct{})
	go func() {
		defer close(done)
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = io.Copy(io.Discard, conn)
	}()

	conn, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()
	if tcp, ok := conn.(*net.TCPConn); ok {
		_ = tcp.SetNoDelay(true)
	}

	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := conn.Write(seal(payload)); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	_ = conn.Close()
	<-done
}

func BenchmarkTCPSend4K_NoEncryption(b *testing.B) {
	benchmarkTCP4K(b, func(p []byte) []byte { return p })
}

func BenchmarkTCPSend4K_XOR_ReuseBuffer(b *testing.B) {
	key := []byte("0123456789abcdef0123456789abcdef")
	dst := make([]byte, benchmarkPayloadSize)
	benchmarkTCP4K(b, func(p []byte) []byte {
		for i, v := range p {
			dst[i] = v ^ key[i&31]
		}
		return dst
	})
}

func BenchmarkTCPSend4K_XOR_Allocate(b *testing.B) {
	key := []byte("0123456789abcdef0123456789abcdef")
	benchmarkTCP4K(b, func(p []byte) []byte {
		dst := make([]byte, len(p))
		for i, v := range p {
			dst[i] = v ^ key[i&31]
		}
		return dst
	})
}

func BenchmarkTCPSend4K_CurrentECDHTransform(b *testing.B) {
	left, err := NewECDHCipher()
	if err != nil {
		b.Fatal(err)
	}
	right, err := NewECDHCipher()
	if err != nil {
		b.Fatal(err)
	}
	if err := left.GenerateSharedKey(right.PublicKey()); err != nil {
		b.Fatal(err)
	}
	benchmarkTCP4K(b, func(p []byte) []byte {
		dst, err := left.Encrypt(p)
		if err != nil {
			b.Fatal(err)
		}
		return dst
	})
}

func BenchmarkTCPSend4K_AES256GCM_ReuseBuffer(b *testing.B) {
	aead := benchmarkAEAD(b)
	nonce := make([]byte, aead.NonceSize())
	dst := make([]byte, 0, benchmarkPayloadSize+aead.Overhead())
	var counter uint64
	benchmarkTCP4K(b, func(p []byte) []byte {
		counter++
		binary.BigEndian.PutUint64(nonce[len(nonce)-8:], counter)
		return aead.Seal(dst[:0], nonce, p, nil)
	})
}

func BenchmarkTCPSend4K_AES256GCM_Allocate(b *testing.B) {
	aead := benchmarkAEAD(b)
	nonce := make([]byte, aead.NonceSize())
	var counter uint64
	benchmarkTCP4K(b, func(p []byte) []byte {
		counter++
		binary.BigEndian.PutUint64(nonce[len(nonce)-8:], counter)
		return aead.Seal(nil, nonce, p, nil)
	})
}
