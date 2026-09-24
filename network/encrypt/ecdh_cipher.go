package encrypt

import (
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"errors"
)

const (
	aesKeySize = 32
	salt       = "frth454564hgj1"
	ecdhInfo   = "game-server"
)

var (
	ErrInvalidKey = errors.New("network: invalid shared key")
)

func NewECDHCipher() (*ECDHCipher, error) {
	privateKey, err := ecdh.X25519().GenerateKey(rand.Reader)
	if err != nil {
		return nil, err
	}
	return &ECDHCipher{
		privateKey: append([]byte(nil), privateKey.Bytes()...),
		publicKey:  append([]byte(nil), privateKey.PublicKey().Bytes()...),
	}, nil
}

type ECDHCipher struct {
	privateKey []byte
	publicKey  []byte
	sharedKey  []byte
}

func (p *ECDHCipher) PublicKey() []byte {
	return p.publicKey
}

func (p *ECDHCipher) GenerateSharedKey(peerPublicKey []byte) error {
	key, err := ecdh.X25519().NewPrivateKey(p.privateKey)
	if err != nil {
		return err
	}
	peerKey, err := ecdh.X25519().NewPublicKey(peerPublicKey)
	if err != nil {
		return err
	}

	secret, err := key.ECDH(peerKey)
	if err != nil {
		return err
	}

	sharedKey, err := hkdf.Key(sha256.New, secret, []byte(salt), ecdhInfo, aesKeySize)
	if err != nil {
		return err
	}
	p.sharedKey = sharedKey
	return nil
}

// Encrypt applies the custom reversible byte transform:
//
//	A[i] = K[i mod 32]
//	B[i] = K[(i + 13) mod 32]
//	C[i] = (((P[i] + A[i]) mod 256) XOR B[i])
func (p *ECDHCipher) Encrypt(plaintext []byte) ([]byte, error) {
	return p.crypt(plaintext, false)
}

// Decrypt reverses Encrypt:
//
//	P[i] = (((C[i] XOR B[i]) - A[i]) mod 256)
func (p *ECDHCipher) Decrypt(ciphertext []byte) ([]byte, error) {
	return p.crypt(ciphertext, true)
}

func (p *ECDHCipher) crypt(input []byte, decrypt bool) ([]byte, error) {
	if len(p.sharedKey) != aesKeySize {
		return nil, ErrInvalidKey
	}
	output := make([]byte, len(input))
	for i, value := range input {
		a := int(p.sharedKey[i%aesKeySize])
		b := p.sharedKey[(i+13)%aesKeySize]
		if decrypt {
			value = byte((int(value^b) - a + 256) % 256)
		} else {
			value = byte((int(value)+a)%256) ^ b
		}
		output[i] = value
	}
	return output, nil
}
