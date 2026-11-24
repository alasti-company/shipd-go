package shipd

import (
	"context"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"encoding/pem"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
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

func TestPinnedSnapshotCacheRejectsUnverifiableEntries(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	spki, _ := x509.MarshalPKIXPublicKey(public)
	keyPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spki}))
	body := []byte(wire(4, true))
	proof := &SnapshotSignature{KeyID: "old", Value: base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, body))}
	for _, test := range []struct {
		name   string
		cached CachedSnapshot
		keys   map[string]string
	}{
		{"legacy unsigned", CachedSnapshot{Snapshot: body, ETag: `"four"`}, map[string]string{"old": keyPEM}},
		{"tampered", CachedSnapshot{Snapshot: []byte(wire(4, false)), ETag: `"four"`, Signature: proof}, map[string]string{"old": keyPEM}},
		{"removed signing key", CachedSnapshot{Snapshot: body, ETag: `"four"`, Signature: proof}, map[string]string{"new": keyPEM}},
	} {
		t.Run(test.name, func(t *testing.T) {
			cache := &memoryCache{data: &test.cached}
			client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("If-None-Match") != "" {
					t.Error("unverified cache supplied an ETag")
				}
				w.WriteHeader(http.StatusNotModified)
			}, Options{Cache: cache, SnapshotVerificationKeys: test.keys, DisableEvents: true})
			select {
			case <-client.attempted:
			case <-time.After(time.Second):
				t.Fatal("fetch did not finish")
			}
			if _, ok := client.SnapshotVersion(); ok {
				t.Fatal("unverified cache was applied")
			}
			if client.IsEnabled("new-checkout", EvaluationContext{}) {
				t.Fatal("unverified cache overrode defaults")
			}
			if client.LastError() == nil {
				t.Fatal("accepted 304 without a verified cached snapshot")
			}
		})
	}
}

func TestPinnedSnapshotCachePersistsProofAndWorksOffline(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	spki, _ := x509.MarshalPKIXPublicKey(public)
	keys := map[string]string{"test": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spki}))}
	body := []byte(wire(4, true))
	proof := base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, body))
	cache := &memoryCache{}
	first, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Shipd-Snapshot-Key-Id", "test")
		w.Header().Set("X-Shipd-Snapshot-Signature", proof)
		w.Header().Set("ETag", `"four"`)
		w.Write(body)
	}, Options{Cache: cache, SnapshotVerificationKeys: keys, DisableEvents: true})
	waitReady(t, first)
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	if cache.data.Signature == nil || cache.data.Signature.KeyID != "test" || cache.data.Signature.Value != proof {
		t.Fatal("cache lost signing proof")
	}
	cache.mu.Unlock()
	var requests atomic.Int32
	offline, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-None-Match") != `"four"` {
			t.Error("verified cache did not supply its ETag")
		}
		if requests.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
		} else {
			w.WriteHeader(http.StatusNotModified)
		}
	}, Options{Cache: cache, SnapshotVerificationKeys: keys, DisableEvents: true})
	waitReady(t, offline)
	if !offline.IsEnabled("new-checkout", EvaluationContext{}) {
		t.Fatal("verified cache unavailable offline")
	}
	if err := offline.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if version, ok := offline.SnapshotVersion(); !ok || version != 4 {
		t.Fatal("304 lost verified cache")
	}
}

func TestPinnedRefreshRejectsUnsignedAndTamperedResponses(t *testing.T) {
	public, private, _ := ed25519.GenerateKey(rand.Reader)
	spki, _ := x509.MarshalPKIXPublicKey(public)
	keys := map[string]string{"test": string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spki}))}
	var stage atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if stage.Load() > 0 && r.Header.Get("If-None-Match") != `"four"` {
			t.Error("failed verification changed ETag")
		}
		switch stage.Load() {
		case 0:
			body := []byte(wire(4, true))
			w.Header().Set("X-Shipd-Snapshot-Key-Id", "test")
			w.Header().Set("X-Shipd-Snapshot-Signature", base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, body)))
			w.Header().Set("ETag", `"four"`)
			w.Write(body)
		case 1:
			w.Header().Set("X-Shipd-Snapshot-Key-Id", "test")
			w.Header().Set("X-Shipd-Snapshot-Signature", base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte(wire(5, true)))))
			w.Header().Set("ETag", `"five"`)
			w.Write([]byte(wire(5, false)))
		default:
			w.Header().Set("ETag", `"six"`)
			w.Write([]byte(wire(6, false)))
		}
	}, Options{SnapshotVerificationKeys: keys, DisableEvents: true})
	waitReady(t, c)
	for _, next := range []int32{1, 2} {
		stage.Store(next)
		if err := c.Refresh(context.Background()); err == nil {
			t.Fatal("accepted unsigned or changed response")
		}
		if version, ok := c.SnapshotVersion(); !ok || version != 4 {
			t.Fatal("verification failure replaced snapshot")
		}
		if !c.IsEnabled("new-checkout", EvaluationContext{}) {
			t.Fatal("verification failure changed value")
		}
	}
}

func TestNewRejectsInvalidSnapshotKeys(t *testing.T) {
	p256, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	spki, _ := x509.MarshalPKIXPublicKey(&p256.PublicKey)
	for _, key := range []string{"invalid PEM", string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: spki}))} {
		c, err := New(Options{SDKKey: "shipd_sdk_server_test", SnapshotVerificationKeys: map[string]string{"test": key}})
		if err == nil {
			c.Close()
			t.Fatal("accepted invalid verification key configuration")
		}
	}
}
