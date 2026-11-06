package shipd

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestPublishedSDKParity(t *testing.T) {
	data, err := os.ReadFile("testdata/published-sdk.json.gz")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Package  string          `json:"package"`
		Snapshot json.RawMessage `json:"snapshot"`
		Cases    []struct {
			Key, Method string
			Context     struct {
				UserID     *string `json:"userId"`
				Attributes map[string]any
				SegmentIDs []string
			}
			Expected any
		}
	}
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if err := json.NewDecoder(reader).Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	if fixture.Package != "shipdit@0.6.0" {
		t.Fatal(fixture.Package)
	}
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, Options{Bootstrap: fixture.Snapshot, DisableEvents: true})
	for i, tc := range fixture.Cases {
		ctx := EvaluationContext{Attributes: tc.Context.Attributes, SegmentIDs: tc.Context.SegmentIDs}
		if tc.Context.UserID != nil {
			ctx.UserID = *tc.Context.UserID
			ctx.UserIDPresent = true
		}
		var actual any
		switch tc.Method {
		case "isEnabled":
			actual = c.IsEnabled(tc.Key, ctx)
		case "getVariant":
			actual = c.GetVariant(tc.Key, ctx)
		case "getNumber":
			actual = c.GetNumber(tc.Key, ctx)
		case "getJson":
			actual = c.GetJSON(tc.Key, ctx)
		default:
			t.Fatal(tc.Method)
		}
		if !reflect.DeepEqual(actual, tc.Expected) {
			t.Fatalf("case %d %s %s context %+v: got %#v, want %#v", i, tc.Method, tc.Key, ctx, actual, tc.Expected)
		}
	}
}

type memoryCache struct {
	mu               sync.Mutex
	data             *CachedSnapshot
	loadErr, saveErr error
	saves            int
}

func (m *memoryCache) Load(context.Context) (*CachedSnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.data, m.loadErr
}
func (m *memoryCache) Save(_ context.Context, data CachedSnapshot) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.saves++
	m.data = &data
	return m.saveErr
}
func TestCacheLifecycle(t *testing.T) {
	cache := &memoryCache{data: &CachedSnapshot{Snapshot: []byte(wire(4, true)), ETag: `"four"`}}
	var requests atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			if r.Header.Get("If-None-Match") != `"four"` {
				t.Error("missing cached ETag")
			}
			w.WriteHeader(503)
			return
		}
		w.Header().Set("ETag", `"five"`)
		fmt.Fprint(w, wire(5, false))
	}, Options{Cache: cache, DisableEvents: true})
	waitReady(t, c)
	if !c.IsEnabled("new-checkout", EvaluationContext{}) || c.Revision() != 1 {
		t.Fatal("cached snapshot not applied")
	}
	var notified atomic.Int32
	unsubscribe := c.Subscribe(func() { notified.Add(1); _ = c.IsEnabled("new-checkout", EvaluationContext{}) })
	c.Subscribe(func() { panic("listener panic must be contained") })
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.IsEnabled("new-checkout", EvaluationContext{}) || c.Revision() != 2 || notified.Load() != 1 {
		t.Fatal("new snapshot not published")
	}
	unsubscribe()
	unsubscribe()
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	if c.Revision() != 2 || notified.Load() != 1 {
		t.Fatal("same version emitted a change")
	}
	cache.mu.Lock()
	defer cache.mu.Unlock()
	if cache.saves != 2 || cache.data.ETag != `"five"` {
		t.Fatal("cache not saved")
	}
}
func TestCacheValidationAndErrors(t *testing.T) {
	for _, cache := range []*memoryCache{{data: &CachedSnapshot{Snapshot: []byte(`{}`)}}, {loadErr: errors.New("disk unavailable")}, {saveErr: errors.New("disk full")}} {
		errorsSeen := make(chan error, 10)
		c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, wire(2, true)) }, Options{Cache: cache, DisableEvents: true, OnError: func(err error) { errorsSeen <- err }})
		waitReady(t, c)
		if v, ok := c.SnapshotVersion(); !ok || v != 2 {
			t.Fatal("cache error prevented network recovery")
		}
		select {
		case <-errorsSeen:
		default:
			t.Fatal("missing cache error")
		}
	}
	cache := &memoryCache{data: &CachedSnapshot{Snapshot: []byte(wire(1, false))}}
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, Options{Cache: cache, Bootstrap: []byte(wire(9, true)), DisableEvents: true})
	waitReady(t, c)
	if v, _ := c.SnapshotVersion(); v != 9 {
		t.Fatal("cache rolled bootstrap back")
	}
}
func TestContextCorrelationAndDiagnostics(t *testing.T) {
	attrs := map[string]any{"plan": "pro", "zero": 0, "nil": nil, "bad": math.NaN(), "object": map[string]any{"x": 1}}
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Request-ID") == "" || r.Header.Get("X-Correlation-ID") != r.Header.Get("X-Request-ID") || r.Header.Get("X-Shipd-Stream-Protocol") != "1" {
			t.Error("missing compatibility headers")
		}
		w.Header().Set("X-Shipd-Auto-Traits", "country=us&city=San+Francisco&timezone=America%2FLos_Angeles")
		fmt.Fprint(w, wire(1, true))
	}, Options{Context: EvaluationContext{UserID: "base", Attributes: attrs, SegmentIDs: []string{"base"}}, AnonymousID: "anon", AutomaticTraits: true, AutomaticTraitSeed: map[string]string{"city": "seed"}, FlushAt: -1, FlushInterval: -1})
	attrs["plan"] = "free"
	waitReady(t, c)
	ctx := mergeContext(c.options.Context, EvaluationContext{UserIDPresent: true, Attributes: map[string]any{"zero": false}, SegmentIDs: []string{}})
	if ctx.UserID != "" || ctx.Attributes["plan"] != "pro" || ctx.Attributes["zero"] != false || len(ctx.SegmentIDs) != 0 {
		t.Fatalf("bad merged context: %+v", ctx)
	}
	c.IsEnabled("new-checkout", EvaluationContext{Attributes: map[string]any{"plan": "enterprise"}})
	if err := c.Identify(Identity{UserID: "login", AnonymousID: "session", Traits: map[string]any{"plan": "pro"}}); err != nil {
		t.Fatal(err)
	}
	c.Reset()
	if c.AnonymousID() != "anon" {
		t.Fatal("server reset changed correlation")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	event := c.events[len(c.events)-2]
	traits := event["traits"].(map[string]any)
	if event["anonymousId"] != "anon" || event["userId"] != "base" || traits["plan"] != "enterprise" || traits["country"] != "US" || traits["city"] != "seed" {
		t.Fatalf("bad event: %v", event)
	}
	if _, ok := traits["nil"]; !ok {
		t.Fatal("nil trait omitted")
	}
	if _, ok := traits["bad"]; ok {
		t.Fatal("invalid trait emitted")
	}
	if _, ok := traits["object"]; ok {
		t.Fatal("object trait emitted")
	}
	if c.events[len(c.events)-1]["anonymousId"] != "session" {
		t.Fatal("identify correlation omitted")
	}
}
func TestStreamResyncAndLag(t *testing.T) {
	var snapshots atomic.Int32
	subscribed := make(chan map[string]any, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/sdk/v1/stream" || r.URL.Query().Get("ticket") != "short-lived" || r.Header.Get("Authorization") != "" {
			t.Error("incorrect stream authentication")
		}
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := wsjson.Write(ctx, conn, map[string]any{"protocolVersion": 1, "type": "connected", "connectionId": "c", "environmentId": "e", "snapshotVersion": 1}); err != nil {
			return
		}
		var subscription map[string]any
		if err := wsjson.Read(ctx, conn, &subscription); err != nil {
			return
		}
		subscribed <- subscription
		for _, frame := range []map[string]any{
			{"protocolVersion": 1, "type": "snapshot.updated", "environmentId": "other", "version": 100},
			{"protocolVersion": 1, "type": "snapshot.updated", "environmentId": "e", "version": 1},
			{"protocolVersion": 1, "type": "snapshot.updated", "environmentId": "e", "version": 5},
		} {
			if err := wsjson.Write(ctx, conn, frame); err != nil {
				return
			}
		}
		_, _, _ = conn.Read(ctx)
	}))
	defer gateway.Close()
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sdk/v1/snapshot":
			n := snapshots.Add(1)
			version := 1
			if n >= 4 {
				version = 5
			}
			fmt.Fprint(w, wire(version, true))
		case "/sdk/v1/stream-ticket":
			if r.Method != "POST" {
				t.Error("ticket requires POST")
			}
			fmt.Fprint(w, `{"ticket":"short-lived","ttlSeconds":300}`)
		default:
			w.WriteHeader(202)
		}
	}, Options{Stream: true, Overrides: Overrides{StreamEndpoint: gateway.URL}, DisableEvents: true})
	waitReady(t, c)
	select {
	case subscription := <-subscribed:
		if subscription["lastKnownVersion"] != float64(1) || subscription["protocolVersion"] != float64(1) {
			t.Fatal(subscription)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no subscription")
	}
	eventually(t, func() bool { v, _ := c.SnapshotVersion(); return v == 5 })
	if snapshots.Load() != 4 {
		t.Fatalf("got %d fetches, want initial and three lag retries", snapshots.Load())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
}
func TestStreamValidationAndTicketRedaction(t *testing.T) {
	for _, endpoint := range []string{"ftp://example.com", "https://user:pass@example.com", "https://example.com?secret=1"} {
		if _, err := New(Options{SDKKey: "key", Stream: true, Overrides: Overrides{StreamEndpoint: endpoint}}); err == nil {
			t.Fatal("accepted", endpoint)
		}
	}
	url, err := streamURL("https://example.com/base/", "a+b&c")
	if err != nil || url != "wss://example.com/base/sdk/v1/stream?ticket=a%2Bb%26c" {
		t.Fatal(url, err)
	}
	seen := make(chan error, 8)
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "stream-ticket") {
			fmt.Fprint(w, `{"ticket":"secret-ticket","ttlSeconds":300}`)
		} else {
			fmt.Fprint(w, wire(1, true))
		}
	}, Options{Stream: true, DisableEvents: true, Overrides: Overrides{StreamEndpoint: "http://127.0.0.1:1"}, OnError: func(err error) { seen <- err }})
	waitReady(t, c)
	select {
	case err := <-seen:
		if strings.Contains(err.Error(), "secret-ticket") {
			t.Fatal("ticket leaked", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("missing connection error")
	}
}

func TestReadyTimeoutAndDisabledFetchTimeout(t *testing.T) {
	c, err := New(Options{SDKKey: "key", ReadyTimeout: 5 * time.Millisecond, DisablePolling: true, DisableEvents: true, DisableFetchTimeout: true, HTTPClient: httpClientFunc(func(r *http.Request) (*http.Response, error) {
		if _, ok := r.Context().Deadline(); ok {
			t.Error("fetch timeout was not disabled")
		}
		<-r.Context().Done()
		return nil, r.Context().Err()
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	if err := c.Ready(context.Background()); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Ready = %v", err)
	}
}
func TestStreamReconnectReusesValidTicket(t *testing.T) {
	var connections, tickets atomic.Int32
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		n := connections.Add(1)
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		_ = wsjson.Write(ctx, conn, map[string]any{"protocolVersion": 1, "type": "connected", "connectionId": "c", "environmentId": "e", "snapshotVersion": n})
		var subscription map[string]any
		if err := wsjson.Read(ctx, conn, &subscription); err != nil {
			return
		}
		if n == 1 {
			return
		}
		_, _, _ = conn.Read(ctx)
	}))
	defer gateway.Close()
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "stream-ticket") {
			tickets.Add(1)
			fmt.Fprint(w, `{"ticket":"reusable","ttlSeconds":300}`)
			return
		}
		version := 1
		if connections.Load() > 1 {
			version = 2
		}
		fmt.Fprint(w, wire(version, true))
	}, Options{Stream: true, Overrides: Overrides{StreamEndpoint: gateway.URL}, DisableEvents: true})
	waitReady(t, c)
	eventually(t, func() bool { v, _ := c.SnapshotVersion(); return v == 2 })
	if tickets.Load() != 1 || connections.Load() != 2 {
		t.Fatalf("connections=%d tickets=%d", connections.Load(), tickets.Load())
	}
}
func TestErrorCallbackAndFlushFailureDoesNotRequeueErrors(t *testing.T) {
	var callbacks atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) }, Options{FlushAt: -1, FlushInterval: -1, OnError: func(err error) { callbacks.Add(1); panic("contained") }})
	<-c.attempted
	before := bufferedEvents(c)
	if before != 2 {
		t.Fatalf("want fetch timing and error, got %d", before)
	}
	if err := c.Flush(context.Background()); err == nil {
		t.Fatal("missing flush error")
	}
	if bufferedEvents(c) != before || callbacks.Load() != 2 {
		t.Fatal("flush error recursively entered event buffer")
	}
}

func TestStreamForcedResyncAndProtocolRejection(t *testing.T) {
	var snapshots atomic.Int32
	errorsSeen := make(chan error, 8)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		_ = wsjson.Write(ctx, conn, map[string]any{"protocolVersion": 1, "type": "connected", "connectionId": "c", "environmentId": "e", "snapshotVersion": 1})
		var subscription map[string]any
		if err := wsjson.Read(ctx, conn, &subscription); err != nil {
			return
		}
		_ = wsjson.Write(ctx, conn, map[string]any{"protocolVersion": 1, "type": "snapshot.resync", "reason": "server_request"})
		_ = wsjson.Write(ctx, conn, map[string]any{"protocolVersion": 2, "type": "snapshot.updated", "environmentId": "e", "version": 999})
		_, _, _ = conn.Read(ctx)
	}))
	defer gateway.Close()
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "stream-ticket") {
			fmt.Fprint(w, `{"ticket":"ticket","ttlSeconds":300}`)
			return
		}
		version := snapshots.Add(1)
		fmt.Fprint(w, wire(int(version), true))
	}, Options{Stream: true, DisableEvents: true, Overrides: Overrides{StreamEndpoint: gateway.URL}, OnError: func(err error) { errorsSeen <- err }})
	waitReady(t, c)
	select {
	case err := <-errorsSeen:
		if !strings.Contains(err.Error(), "unsupported stream frame") {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("unsupported protocol not rejected")
	}
	if v, _ := c.SnapshotVersion(); v != 2 || snapshots.Load() != 2 {
		t.Fatalf("forced resync failed: version=%d fetches=%d", v, snapshots.Load())
	}
}

func TestSnapshotWithoutETagClearsPreviousValidator(t *testing.T) {
	cache := &memoryCache{}
	var requests atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		n := requests.Add(1)
		switch n {
		case 1:
			w.Header().Set("ETag", `"one"`)
		case 2:
			if r.Header.Get("If-None-Match") != `"one"` {
				t.Error("missing initial validator")
			}
		case 3:
			if validator := r.Header.Get("If-None-Match"); validator != "" {
				t.Errorf("stale validator: %s", validator)
			}
		}
		fmt.Fprint(w, wire(int(n), true))
	}, Options{Cache: cache, DisableEvents: true})
	waitReady(t, c)
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	cache.mu.Lock()
	etag := cache.data.ETag
	cache.mu.Unlock()
	if etag != "" {
		t.Fatal("cache retained stale validator", etag)
	}
	if err := c.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
}
func TestStreamUsesCustomHTTPClientAndTimeoutPolicy(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			subscribed := make(chan struct{}, 1)
			gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
				defer cancel()
				_ = wsjson.Write(ctx, conn, map[string]any{"protocolVersion": 1, "type": "connected", "connectionId": "c", "environmentId": "e", "snapshotVersion": 1})
				var message map[string]any
				if err := wsjson.Read(ctx, conn, &message); err != nil {
					return
				}
				subscribed <- struct{}{}
				_, _, _ = conn.Read(ctx)
			}))
			defer gateway.Close()
			var handshakes atomic.Int32
			transport := httpClientFunc(func(r *http.Request) (*http.Response, error) {
				if strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
					handshakes.Add(1)
					_, hasDeadline := r.Context().Deadline()
					if hasDeadline == disabled {
						t.Errorf("handshake deadline=%v, DisableFetchTimeout=%v", hasDeadline, disabled)
					}
				}
				return http.DefaultClient.Do(r)
			})
			c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "stream-ticket") {
					fmt.Fprint(w, `{"ticket":"custom","ttlSeconds":300}`)
					return
				}
				fmt.Fprint(w, wire(1, true))
			}, Options{Stream: true, DisableFetchTimeout: disabled, HTTPClient: transport, DisableEvents: true, Overrides: Overrides{StreamEndpoint: gateway.URL}})
			waitReady(t, c)
			select {
			case <-subscribed:
			case <-time.After(5 * time.Second):
				t.Fatal("custom transport did not establish stream")
			}
			if handshakes.Load() != 1 {
				t.Fatal("custom HTTPClient was bypassed")
			}
			ctx, cancel := c.requestContext(c.ctx)
			defer cancel()
			if _, hasDeadline := ctx.Deadline(); hasDeadline == disabled {
				t.Error("subscription context uses incorrect timeout policy")
			}
		})
	}
}
