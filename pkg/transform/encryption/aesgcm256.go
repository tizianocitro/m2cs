package encryption

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"io"
)

func EncryptAesgcm(key string, plaintext io.Reader) (io.Reader, error) {
	hashedKey := sha256.Sum256([]byte(key))

	block, err := aes.NewCipher(hashedKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	plainBytes, err := io.ReadAll(plaintext)
	if err != nil {
		return nil, fmt.Errorf("failed to read input: %w", err)
	}

	ciphertext := aead.Seal(nil, nonce, plainBytes, nil)

	final := append(nonce, ciphertext...)
	return bytes.NewReader(final), nil
}

func DecryptAesgcm(key string, encrypted io.Reader) (io.Reader, error) {
	hashedKey := sha256.Sum256([]byte(key))

	block, err := aes.NewCipher(hashedKey[:])
	if err != nil {
		return nil, fmt.Errorf("failed to create cipher: %w", err)
	}

	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	cipherBytes, err := io.ReadAll(encrypted)
	if err != nil {
		return nil, fmt.Errorf("failed to read encrypted input: %w", err)
	}

	if len(cipherBytes) < aead.NonceSize() {
		return nil, fmt.Errorf("invalid ciphertext: too short")
	}

	nonce := cipherBytes[:aead.NonceSize()]
	ciphertext := cipherBytes[aead.NonceSize():]

	plaintext, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("decryption failed: %w", err)
	}

	return bytes.NewReader(plaintext), nil
}
