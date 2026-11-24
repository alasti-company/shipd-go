package shipd

import (
	"crypto/ed25519"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type snapshotVerificationKeys map[string]ed25519.PublicKey

func parseSnapshotVerificationKeys(keys map[string]string) (snapshotVerificationKeys, error) {
	if keys == nil {
		return nil, nil
	}
	parsed := make(snapshotVerificationKeys, len(keys))
	for id, keyPEM := range keys {
		block, rest := pem.Decode([]byte(keyPEM))
		if id == "" || block == nil || strings.TrimSpace(string(rest)) != "" {
			return nil, fmt.Errorf("shipd: invalid snapshot verification key %q", id)
		}
		public, err := x509.ParsePKIXPublicKey(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("shipd: invalid snapshot verification key %q", id)
		}
		key, ok := public.(ed25519.PublicKey)
		if !ok {
			return nil, fmt.Errorf("shipd: snapshot verification key %q must be Ed25519", id)
		}
		parsed[id] = key
	}
	return parsed, nil
}

// verifySnapshotSignature is the PEM convenience boundary; clients compile keys in New.
func verifySnapshotSignature(body []byte, headers http.Header, keys map[string]string) error {
	parsed, err := parseSnapshotVerificationKeys(keys)
	if err != nil {
		return err
	}
	return verifySnapshotWithKeys(body, headers, parsed)
}

func verifySnapshotWithKeys(body []byte, headers http.Header, keys snapshotVerificationKeys) error {
	id := headers.Get("X-Shipd-Snapshot-Key-Id")
	raw := headers.Get("X-Shipd-Snapshot-Signature")
	if id == "" || raw == "" {
		return errors.New("shipd: snapshot signature missing")
	}
	key, ok := keys[id]
	if !ok {
		return errors.New("shipd: snapshot signing key untrusted")
	}
	signature, err := base64.RawURLEncoding.DecodeString(raw)
	if err != nil || !ed25519.Verify(key, body, signature) {
		return errors.New("shipd: snapshot signature verification failed")
	}
	return nil
}

func snapshotSignatureFromHeaders(headers http.Header) *SnapshotSignature {
	keyID, value := headers.Get("X-Shipd-Snapshot-Key-Id"), headers.Get("X-Shipd-Snapshot-Signature")
	if keyID == "" || value == "" {
		return nil
	}
	return &SnapshotSignature{KeyID: keyID, Value: value}
}

func snapshotSignatureHeaders(signature *SnapshotSignature) http.Header {
	headers := http.Header{}
	if signature != nil {
		headers.Set("X-Shipd-Snapshot-Key-Id", signature.KeyID)
		headers.Set("X-Shipd-Snapshot-Signature", signature.Value)
	}
	return headers
}
