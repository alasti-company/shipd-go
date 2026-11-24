package shipd

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"net/http"
)

// verifySnapshotSignature runs only when downloading configuration, never while evaluating.
func verifySnapshotSignature(body []byte, headers http.Header, keys map[string]string) error {
	id := headers.Get("X-Shipd-Snapshot-Key-Id")
	keyPEM, ok := keys[id]
	if !ok {
		return errors.New("shipd: snapshot signing key untrusted")
	}
	block, _ := pem.Decode([]byte(keyPEM))
	if block == nil {
		return errors.New("shipd: invalid snapshot verification key")
	}
	parsed, err := x509.ParsePKIXPublicKey(block.Bytes)
	if err != nil {
		return errors.New("shipd: invalid snapshot verification key")
	}
	key, ok := parsed.(ed25519.PublicKey)
	if !ok {
		return errors.New("shipd: snapshot verification requires Ed25519")
	}
	signature, err := base64.RawURLEncoding.DecodeString(headers.Get("X-Shipd-Snapshot-Signature"))
	if err != nil || !ed25519.Verify(key, body, signature) {
		return errors.New("shipd: snapshot signature verification failed")
	}
	return nil
}
