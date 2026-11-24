package shipd

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"testing"
)

func TestSnapshotSignature(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	spki, _ := x509.MarshalPKIXPublicKey(public)
	keys := map[string]string{"test": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spki}))}
	body := []byte(`{"version":2,"flags":{"checkout":{"releaseControl":{"codeOwned":true,"rollout":{"steps":[1,100]}}}}}`)
	headers := http.Header{}
	headers.Set("X-Shipd-Snapshot-Key-Id", "test")
	headers.Set("X-Shipd-Snapshot-Signature", base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, body)))
	if err := verifySnapshotSignature(body, headers, keys); err != nil {
		t.Fatal(err)
	}
	if err := verifySnapshotSignature(append(body, ' '), headers, keys); err == nil {
		t.Fatal("accepted changed snapshot bytes")
	}
	headers.Del("X-Shipd-Snapshot-Signature")
	if err := verifySnapshotSignature(body, headers, keys); err == nil {
		t.Fatal("accepted unsigned snapshot")
	}
}
