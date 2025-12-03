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

type AESGCMEncrypt struct {
	Key string
}

func (a *AESGCMEncrypt) Name() string { return "aesgcm-encrypt" }

func (a *AESGCMEncrypt) Apply(reader io.Reader) (io.Reader, io.Closer, error) {
	if a.Key == "" {
		return nil, nil, fmt.Errorf("aesgcm: missing key")
	}

	// Derive 32-byte AES key from passphrase (SHA-256).
	key := sha256.Sum256([]byte(a.Key))

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, nil, fmt.Errorf("aesgcm: new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, fmt.Errorf("aesgcm: new GCM: %w", err)
	}

	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, nil, fmt.Errorf("aesgcm: nonce: %w", err)
	}

	plain, err := io.ReadAll(reader)
	if err != nil {
		return nil, nil, fmt.Errorf("aesgcm: read input: %w", err)
	}

	ct := aead.Seal(nil, nonce, plain, nil)

	out := make([]byte, 0, len(nonce)+len(ct))
	out = append(out, nonce...)
	out = append(out, ct...)

	return bytes.NewReader(out), io.NopCloser(bytes.NewReader(nil)), nil
}

type AESGCMDecrypt struct {
	Key string // passphrase; internally derived to a 32-byte key via SHA-256
}

func (AESGCMDecrypt) Name() string { return "aesgcm-decrypt" }

func (t AESGCMDecrypt) Apply(rc io.ReadCloser) (io.ReadCloser, error) {
	if t.Key == "" {
		_ = rc.Close()
		return nil, fmt.Errorf("aesgcm: missing key")
	}

	cipherBytes, err := io.ReadAll(rc)
	_ = rc.Close()
	if err != nil {
		return nil, fmt.Errorf("aesgcm: read input: %w", err)
	}

	key := sha256.Sum256([]byte(t.Key))

	block, err := aes.NewCipher(key[:])
	if err != nil {
		return nil, fmt.Errorf("aesgcm: new cipher: %w", err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("aesgcm: new GCM: %w", err)
	}

	if len(cipherBytes) < aead.NonceSize() {
		return nil, fmt.Errorf("aesgcm: invalid ciphertext (too short)")
	}

	nonce := cipherBytes[:aead.NonceSize()]
	ciphertext := cipherBytes[aead.NonceSize():]

	plain, err := aead.Open(nil, nonce, ciphertext, nil)
	if err != nil {
		return nil, fmt.Errorf("aesgcm: decryption failed: %w", err)
	}

	// Wrap plaintext in a new ReadCloser for the next pipeline step / caller.
	return io.NopCloser(bytes.NewReader(plain)), nil
}
