package shipd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func wire(version int, enabled bool) string {
	return fmt.Sprintf(`{"schemaVersion":1,"projectId":"p","environmentId":"e","environmentKey":"production","version":%d,"generatedAt":"2026-10-09T00:00:00Z","flags":{"new-checkout":{"key":"new-checkout","type":"boolean","enabled":%t,"default":false,"version":1,"rules":[],"fallthrough":{"kind":"boolean","value":true}}}}`, version, enabled)
}

type httpClientFunc func(*http.Request) (*http.Response, error)

func (f httpClientFunc) Do(r *http.Request) (*http.Response, error) { return f(r) }

// eventually polls cond until it holds, failing the test after five seconds.
func eventually(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("condition not met within 5s")
		}
		time.Sleep(time.Millisecond)
	}
}
func bufferedEvents(c *Client) int { c.mu.RLock(); defer c.mu.RUnlock(); return len(c.events) }
func waitReady(t *testing.T, c *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Ready(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.attempted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
func newTestClient(t *testing.T, handler http.HandlerFunc, options Options) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	options.SDKKey = "shipd_sdk_server_test"
	options.Overrides.UnsafeEndpoint = server.URL
	options.DisablePolling = true
	c, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c, server
}
func TestSnapshotLifecycle(t *testing.T) {
	var stage atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer shipd_sdk_server_test" || r.Header.Get("X-Shipd-Snapshot-Schema") != "1" {
			t.Error("missing SDK headers")
		}
		switch stage.Load() {
		case 0:
			w.Header().Set("ETag", `"v2"`)
			fmt.Fprint(w, wire(2, true))
		case 1:
			if r.Header.Get("If-None-Match") != `"v2"` {
				t.Error("missing ETag")
			}
			w.WriteHeader(304)
		case 2:
			w.Header().Set("ETag", `"v1"`)
			fmt.Fprint(w, wire(1, false))
		case 3:
			fmt.Fprint(w, strings.Replace(wire(3, true), `"rules":[]`, `"rules":[{"conditions":[{"kind":"unknown"}],"serve":{"kind":"boolean","value":false}}]`, 1))
		case 4:
			w.WriteHeader(401)
		case 5:
			fmt.Fprint(w, strings.Replace(wire(3, true), `"schemaVersion":1`, `"schemaVersion":2`, 1))
		case 6:
			fmt.Fprint(w, wire(3, false))
		}
	}, Options{DisableEvents: true})
	waitReady(t, c)
	for i := 1; i <= 6; i++ {
		stage.Store(int32(i))
		err := c.Refresh(context.Background())
		if (i >= 3 && i <= 5) != (err != nil) {
			t.Fatalf("stage %d error %v", i, err)
		}
		expected := i != 6
		if got := c.IsEnabled("new-checkout", EvaluationContext{}); got != expected {
			t.Fatalf("stage %d value %v", i, got)
		}
		if i >= 2 && i <= 5 && c.etag != `"v2"` {
			t.Fatal("invalid/older snapshot changed ETag")
		}
	}
	if c.LastError() != nil {
		t.Fatal("successful refresh did not clear error")
	}
}
func TestDefaultsReadyTimeoutAndClose(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, Options{DisableEvents: true, Defaults: map[string]any{"new-checkout": true, "max-retries": 3, "payload": map[string]any{"mode": "strict"}}})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	start := time.Now()
	if err := c.Ready(ctx); err == nil || !strings.Contains(err.Error(), "HTTP 503") {
		t.Fatalf("Ready should return the initial fetch error, got %v", err)
	}
	if time.Since(start) > 500*time.Millisecond {
		t.Fatal("Ready waited for the deadline after the initial fetch failed")
	}
	if !c.IsEnabled("new-checkout", EvaluationContext{}) || c.GetNumber("max-retries", EvaluationContext{}) != 3 || c.GetVariant("missing", EvaluationContext{}) != "control" {
		t.Fatal("incorrect defaults")
	}
	payload := c.GetJSON("payload", EvaluationContext{}).(map[string]any)
	payload["mode"] = "changed"
	if c.GetJSON("payload", EvaluationContext{}).(map[string]any)["mode"] != "strict" {
		t.Fatal("default mutated")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	_ = c.Close()
	if !errors.Is(c.Refresh(context.Background()), ErrClosed) {
		t.Fatal("refresh after close")
	}
	if !c.IsEnabled("new-checkout", EvaluationContext{}) {
		t.Fatal("defaults after close")
	}
}
func TestReadyRespectsContextWhileFetchIsPending(t *testing.T) {
	c, _ := newTestClient(t, func(_ http.ResponseWriter, r *http.Request) { <-r.Context().Done() }, Options{DisableEvents: true})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if !errors.Is(c.Ready(ctx), context.DeadlineExceeded) {
		t.Fatal("Ready should respect its context while the first fetch is in flight")
	}
}

func TestReadyReturnsErrClosedAfterSnapshotLoaded(t *testing.T) {
	for _, bootstrap := range []bool{false, true} {
		t.Run(fmt.Sprintf("bootstrap=%t", bootstrap), func(t *testing.T) {
			options := Options{DisableEvents: true}
			if bootstrap {
				options.Bootstrap = []byte(wire(1, true))
			}
			c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, wire(1, true))
			}, options)
			waitReady(t, c)
			if err := c.Close(); err != nil {
				t.Fatal(err)
			}
			if err := c.Ready(context.Background()); !errors.Is(err, ErrClosed) {
				t.Fatalf("Ready after Close = %v, want ErrClosed", err)
			}
		})
	}
}

func TestReadyReturnsErrClosedDuringInitialFetch(t *testing.T) {
	started := make(chan struct{})
	c, _ := newTestClient(t, func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
	}, Options{DisableEvents: true})
	<-started
	result := make(chan error, 1)
	go func() { result <- c.Ready(context.Background()) }()
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; !errors.Is(err, ErrClosed) {
		t.Fatalf("Ready interrupted by Close = %v, want ErrClosed", err)
	}
}

func TestReturnedSnapshotVersionCannotMutateTelemetry(t *testing.T) {
	posted := make(chan []map[string]any, 1)
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet {
			fmt.Fprint(w, wire(9, true))
			return
		}
		var body struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		posted <- body.Events
		w.WriteHeader(http.StatusAccepted)
	}, Options{FlushAt: -1, FlushInterval: -1})
	waitReady(t, c)
	for _, key := range []string{"new-checkout", "missing"} {
		r := c.Evaluate(key, EvaluationContext{})
		if r.SnapshotVersion == nil {
			t.Fatal("expected loaded snapshot version")
		}
		*r.SnapshotVersion = 999
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	events := <-posted
	if len(events) != 4 {
		t.Fatalf("got %d events, want fetch timing, two evaluations and an unregistered flag", len(events))
	}
	for _, event := range events {
		if version := event["snapshotVersion"]; version != float64(9) {
			t.Fatalf("%s event snapshotVersion = %v, want 9", event["type"], version)
		}
	}
}

func TestBootstrap(t *testing.T) {
	targeted := `{"schemaVersion":1,"projectId":"p","environmentId":"e","environmentKey":"test","version":1,"generatedAt":"2026-10-10T00:00:00Z","flags":{"new-checkout":{"key":"new-checkout","type":"boolean","enabled":true,"default":false,"rules":[{"id":"pro","conditions":[{"kind":"attribute","attribute":"plan","operator":"eq","value":"pro"}],"serve":{"kind":"boolean","value":true}}],"fallthrough":{"kind":"boolean","value":false}}}}`
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, Options{DisableEvents: true, Bootstrap: []byte(targeted)})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Ready(ctx); err != nil {
		t.Fatalf("bootstrap should make the client ready: %v", err)
	}
	pro := EvaluationContext{Attributes: map[string]any{"plan": "pro"}}
	if !c.IsEnabled("new-checkout", pro) || c.IsEnabled("new-checkout", EvaluationContext{}) {
		t.Fatal("bootstrap targeting rules were not evaluated")
	}
	if v, ok := c.SnapshotVersion(); !ok || v != 1 {
		t.Fatal("bootstrap version not reported")
	}
	if _, err := New(Options{SDKKey: "test", Bootstrap: []byte(`{"flags":1}`)}); err == nil {
		t.Fatal("invalid bootstrap accepted")
	}
}
func TestDefaultEndpoint(t *testing.T) {
	var host atomic.Value
	c, err := New(Options{SDKKey: "test", DisableEvents: true, DisablePolling: true, HTTPClient: httpClientFunc(func(r *http.Request) (*http.Response, error) {
		host.Store(r.URL.Scheme + "://" + r.URL.Host)
		return nil, errors.New("offline")
	})})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_ = c.Ready(ctx)
	if host.Load() != DefaultEndpoint {
		t.Fatalf("requested %v, want %s", host.Load(), DefaultEndpoint)
	}
}
func TestShutdownFlushesThenCloses(t *testing.T) {
	var delivered atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		var batch struct {
			Events []any `json:"events"`
		}
		_ = json.NewDecoder(r.Body).Decode(&batch)
		delivered.Add(int32(len(batch.Events)))
		w.WriteHeader(202)
	}, Options{FlushAt: -1, FlushInterval: time.Hour})
	waitReady(t, c)
	c.IsEnabled("new-checkout", EvaluationContext{})
	c.IsEnabled("new-checkout", EvaluationContext{})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := c.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if delivered.Load() != 3 {
		t.Fatalf("delivered %d events before close, want 3", delivered.Load())
	}
	if !errors.Is(c.Refresh(ctx), ErrClosed) {
		t.Fatal("client should be closed after Shutdown")
	}
	if err := c.Shutdown(ctx); err != nil {
		t.Fatalf("repeated Shutdown: %v", err)
	}
}
func TestShutdownReportsFlushErrorAndStillCloses(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		w.WriteHeader(503)
	}, Options{FlushAt: -1, FlushInterval: time.Hour})
	waitReady(t, c)
	c.IsEnabled("new-checkout", EvaluationContext{})
	if err := c.Shutdown(context.Background()); err == nil {
		t.Fatal("Shutdown should report the failed flush")
	}
	if !errors.Is(c.Refresh(context.Background()), ErrClosed) {
		t.Fatal("client should be closed even when the flush fails")
	}
}
func TestCloseCancelsInflightRequest(t *testing.T) {
	started := make(chan struct{})
	c, _ := newTestClient(t, func(_ http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done() }, Options{DisableEvents: true})
	<-started
	done := make(chan struct{})
	go func() { _ = c.Close(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close did not cancel fetch")
	}
}
func TestPollingRecovers(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, wire(1, true))
	}))
	defer server.Close()
	c, err := New(Options{SDKKey: "test", Overrides: Overrides{UnsafeEndpoint: server.URL}, RefreshInterval: 10 * time.Millisecond, DisableEvents: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if c.Ready(ctx) == nil {
		t.Fatal("Ready should report the failed initial fetch")
	}
	for _, ok := c.SnapshotVersion(); !ok; _, ok = c.SnapshotVersion() {
		if ctx.Err() != nil {
			t.Fatal("polling did not recover")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if !c.IsEnabled("new-checkout", EvaluationContext{}) || c.Ready(ctx) != nil {
		t.Fatal("polling did not recover")
	}
}
func TestConcurrentEvaluationRefreshAndClose(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, wire(1, true)) }, Options{DisableEvents: true})
	waitReady(t, c)
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				c.IsEnabled("new-checkout", EvaluationContext{UserID: "usr_123"})
				_ = c.Refresh(context.Background())
			}
		}()
	}
	_ = c.Close()
	wg.Wait()
}
func TestEventRetryAndIdentify(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	var batches []map[string]any
	var mu sync.Mutex
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		var batch map[string]any
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Error(err)
		}
		if fail.Load() {
			w.WriteHeader(503)
			return
		}
		mu.Lock()
		batches = append(batches, batch)
		mu.Unlock()
		w.WriteHeader(202)
	}, Options{})
	waitReady(t, c)
	c.IsEnabled("new-checkout", EvaluationContext{})
	if err := c.Identify(Identity{UserID: "usr_123", Traits: map[string]any{"plan": "pro"}}); err != nil {
		t.Fatal(err)
	}
	if c.Flush(context.Background()) == nil {
		t.Fatal("expected flush error")
	}
	fail.Store(false)
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	events := batches[0]["events"].([]any)
	if events[0].(map[string]any)["type"] != "timing" {
		t.Fatal("missing snapshot timing")
	}
	events = events[1:]
	if len(events) != 2 {
		t.Fatalf("events lost: %v", events)
	}
	event := events[0].(map[string]any)
	if _, ok := event["ruleId"]; ok {
		t.Fatal("empty ruleId rejected by edge")
	}
	if _, ok := event["userId"]; ok {
		t.Fatal("empty userId rejected by edge")
	}
	if err := c.Identify(Identity{UserID: "usr_123", Traits: map[string]any{"bad": []string{"x"}}}); err == nil {
		t.Fatal("invalid traits accepted")
	}
}
func TestEventBufferBound(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, wire(1, true)) }, Options{FlushAt: -1})
	waitReady(t, c)
	for i := 0; i < 250; i++ {
		c.IsEnabled("new-checkout", EvaluationContext{UserID: fmt.Sprint(i)})
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.events) != 200 || c.events[0]["userId"] != "50" || c.dropped != 51 {
		t.Fatal("buffer should retain latest 200 events and count drops")
	}
}
func TestSizeTriggeredFlush(t *testing.T) {
	var delivered atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		var batch struct {
			Events []any `json:"events"`
		}
		_ = json.NewDecoder(r.Body).Decode(&batch)
		delivered.Add(int32(len(batch.Events)))
		w.WriteHeader(202)
	}, Options{FlushAt: 10, FlushInterval: time.Hour})
	waitReady(t, c)
	// Far more than the 200-event buffer: none may be dropped while the edge is
	// healthy. Each round stays below the buffer, then waits for size-triggered
	// flushes to drain the backlog below FlushAt.
	for i := 0; i < 1000; i++ {
		c.IsEnabled("new-checkout", EvaluationContext{UserID: fmt.Sprint(i)})
		if i%100 == 99 {
			eventually(t, func() bool { return bufferedEvents(c) < 10 })
		}
	}
	if c.DroppedEvents() != 0 {
		t.Fatalf("dropped %d events with a healthy edge", c.DroppedEvents())
	}
	// The remainder below FlushAt waits for the interval; deliver it explicitly.
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := delivered.Load(); got != 1001 || c.DroppedEvents() != 0 {
		t.Fatalf("delivered %d events, dropped %d", got, c.DroppedEvents())
	}
}
func TestSizeTriggeredFlushBacksOffAfterFailure(t *testing.T) {
	var posts atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		posts.Add(1)
		w.WriteHeader(503)
	}, Options{FlushAt: 5, FlushInterval: time.Hour})
	waitReady(t, c)
	for i := 0; i < 5; i++ {
		c.IsEnabled("new-checkout", EvaluationContext{})
	}
	// Wait for the first flush to fail and for any wake queued during it to be
	// consumed. From then on, record() sends no wake while the flush is failing.
	eventually(t, func() bool {
		c.mu.RLock()
		defer c.mu.RUnlock()
		return posts.Load() == 1 && c.flushFailing && len(c.flushNow) == 0
	})
	for i := 0; i < 300; i++ {
		c.IsEnabled("new-checkout", EvaluationContext{})
	}
	if len(c.flushNow) != 0 {
		t.Fatal("a failing client must not request size-triggered flushes")
	}
	if n := posts.Load(); n != 1 {
		t.Fatalf("a failing edge received %d flushes; want 1 until the next interval", n)
	}
	if c.DroppedEvents() == 0 {
		t.Fatal("overflow while the edge is down should be counted")
	}
}
func TestSnapshotVersion(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, wire(9, true)) }, Options{FlushAt: -1})
	waitReady(t, c)
	if v, ok := c.SnapshotVersion(); !ok || v != 9 {
		t.Fatalf("SnapshotVersion() = %d, %v", v, ok)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.events) != 1 || c.events[0]["type"] != "timing" {
		t.Fatalf("SnapshotVersion must leave only the initial snapshot fetch timing event; got %v", c.events)
	}
}
func TestFetchTimeoutAndConfiguration(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }, Options{FetchTimeout: 10 * time.Millisecond, DisableEvents: true})
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("expected timeout")
	}
	for _, opts := range []Options{{}, {SDKKey: "test", Overrides: Overrides{UnsafeEndpoint: "ftp://example.com"}}, {SDKKey: "test", Overrides: Overrides{UnsafeEndpoint: "https://user:secret@example.com"}}, {SDKKey: "test", RefreshInterval: -1}, {SDKKey: "test", FlushAt: 201}, {SDKKey: "test", FetchTimeout: -1}, {SDKKey: "test", Defaults: map[string]any{"bad": make(chan int)}}} {
		if _, err := New(opts); err == nil {
			t.Fatal("invalid options accepted")
		}
	}
}

func TestEventBatchLimitsAndPartialRetry(t *testing.T) {
	var calls atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		var batch struct {
			Events []map[string]any `json:"events"`
		}
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			t.Error(err)
		}
		if len(batch.Events) > 100 {
			t.Error("batch exceeds edge limit")
		}
		call := calls.Add(1)
		if call == 2 {
			w.WriteHeader(503)
			return
		}
		w.WriteHeader(202)
	}, Options{FlushAt: -1})
	waitReady(t, c)
	for i := 0; i < 200; i++ {
		c.IsEnabled("new-checkout", EvaluationContext{UserID: fmt.Sprint(i)})
	}
	if err := c.Flush(context.Background()); err == nil {
		t.Fatal("second batch should fail")
	}
	c.mu.RLock()
	n := len(c.events)
	first := c.events[0]["userId"]
	c.mu.RUnlock()
	if n != 100 || first != "100" {
		t.Fatal("successful batch must not be requeued")
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatal("expected two batches and one retry")
	}
}
func TestOversizedEvaluationAndIdentify(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, wire(1, true)) }, Options{})
	waitReady(t, c)
	c.record(map[string]any{"type": "evaluation", "key": "large", "reason": "fallthrough", "value": strings.Repeat("x", 300*1024), "snapshotVersion": 1})
	c.mu.RLock()
	_, hasValue := c.events[0]["value"]
	c.mu.RUnlock()
	if hasValue {
		t.Fatal("oversized evaluation value should be omitted")
	}
	traits := map[string]any{}
	for i := 0; i < 100; i++ {
		traits[fmt.Sprint(i)] = strings.Repeat("界", 1024)
	}
	if err := c.Identify(Identity{UserID: "usr_123", Traits: traits}); err == nil {
		t.Fatal("oversized identify should fail validation")
	}
}

func TestEventsRespectByteLimit(t *testing.T) {
	var requests atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		data, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if len(data) > 256*1024 {
			t.Errorf("batch body exceeds edge limit: %d", len(data))
		}
		requests.Add(1)
		w.WriteHeader(202)
	}, Options{})
	waitReady(t, c)
	traits := map[string]any{}
	for i := 0; i < 80; i++ {
		traits[fmt.Sprint(i)] = strings.Repeat("x", 1024)
	}
	for i := 0; i < 4; i++ {
		if err := c.Identify(Identity{UserID: fmt.Sprint(i), Traits: traits}); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatal("large events should split into two requests")
	}
}
func TestDefaultClientRejectsRedirect(t *testing.T) {
	var followed atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { followed.Store(true); fmt.Fprint(w, wire(1, true)) }))
	defer target.Close()
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) }, Options{DisableEvents: true})
	if err := c.Refresh(context.Background()); err == nil {
		t.Fatal("redirect should be rejected")
	}
	if followed.Load() {
		t.Fatal("SDK followed redirect")
	}
}

func TestTypedGettersUseConfiguredDefaultsForWrongServedType(t *testing.T) {
	for _, test := range []struct {
		name     string
		fallback any
		kind     string
		value    string
		check    func(*Client) any
	}{
		{"boolean", true, "variant", `"semantic"`, func(c *Client) any { return c.IsEnabled("new-checkout", EvaluationContext{}) }},
		{"variant", "safe", "boolean", `true`, func(c *Client) any { return c.GetVariant("new-checkout", EvaluationContext{}) }},
		{"number", float64(3), "boolean", `true`, func(c *Client) any { return c.GetNumber("new-checkout", EvaluationContext{}) }},
	} {
		t.Run(test.name, func(t *testing.T) {
			body := strings.Replace(wire(1, true), `"fallthrough":{"kind":"boolean","value":true}`, fmt.Sprintf(`"fallthrough":{"kind":%q,"value":%s}`, test.kind, test.value), 1)
			c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }, Options{DisableEvents: true, Defaults: map[string]any{"new-checkout": test.fallback}})
			waitReady(t, c)
			if got := test.check(c); got != test.fallback {
				t.Fatalf("got %v, want %v", got, test.fallback)
			}
		})
	}
}
func TestJSONResultsAreDetachedFromEventsAndSnapshot(t *testing.T) {
	snapshot := `{"schemaVersion":1,"projectId":"p","environmentId":"e","environmentKey":"test","version":1,"generatedAt":"2026-10-10T00:00:00Z","flags":{"payload":{"key":"payload","type":"json","enabled":true,"default":{},"rules":[],"fallthrough":{"kind":"json","value":{"mode":"strict"}}}}}`
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, Options{Bootstrap: []byte(snapshot), FlushAt: -1, FlushInterval: -1})
	first := c.GetJSON("payload", EvaluationContext{}).(map[string]any)
	first["mode"] = "mutated"
	if got := c.GetJSON("payload", EvaluationContext{}).(map[string]any)["mode"]; got != "strict" {
		t.Fatalf("snapshot mutated through a result: %v", got)
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	if got := c.events[0]["value"].(map[string]any)["mode"]; got != "strict" {
		t.Fatalf("queued event mutated through a result: %v", got)
	}
}
func TestNegativeFlushIntervalDisablesTimedFlush(t *testing.T) {
	var posts atomic.Int32
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		posts.Add(1)
		w.WriteHeader(202)
	}, Options{FlushAt: -1, FlushInterval: -1})
	waitReady(t, c)
	c.IsEnabled("new-checkout", EvaluationContext{})
	if bufferedEvents(c) != 2 || posts.Load() != 0 {
		t.Fatal("events should wait for an explicit flush")
	}
	if err := c.Flush(context.Background()); err != nil || posts.Load() != 1 {
		t.Fatalf("explicit flush: err=%v posts=%d", err, posts.Load())
	}
}
func TestIdentifyErrorsNameTheProblem(t *testing.T) {
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, wire(1, true)) }, Options{FlushAt: -1})
	traits := map[string]any{}
	for i := 0; i < 101; i++ {
		traits[fmt.Sprint(i)] = i
	}
	for want, identity := range map[string]Identity{
		"requires UserID":              {},
		"UserID exceeds 256":           {UserID: strings.Repeat("u", 257)},
		"Name exceeds 256":             {UserID: "u", Name: strings.Repeat("n", 257)},
		"at most 100 traits":           {UserID: "u", Traits: traits},
		`trait "bad" must be a scalar`: {UserID: "u", Traits: map[string]any{"bad": []int{1}}},
	} {
		if err := c.Identify(identity); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("want error containing %q, got %v", want, err)
		}
	}
}
func TestShutdownDeliversEventsRecordedDuringBackgroundFlush(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var delivered []string
	c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			fmt.Fprint(w, wire(1, true))
			return
		}
		var batch struct {
			Events []map[string]any `json:"events"`
		}
		_ = json.NewDecoder(r.Body).Decode(&batch)
		mu.Lock()
		first := len(delivered) == 0
		for _, e := range batch.Events {
			if e["type"] == "evaluation" {
				delivered = append(delivered, fmt.Sprint(e["userId"]))
			}
		}
		mu.Unlock()
		if first {
			close(entered)
			<-release
		}
		w.WriteHeader(202)
	}, Options{FlushAt: -1, FlushInterval: -1})
	waitReady(t, c)
	c.IsEnabled("new-checkout", EvaluationContext{UserID: "before-flush"})
	go func() { _ = c.Flush(context.Background()) }() // stands in for a background flush
	<-entered
	c.IsEnabled("new-checkout", EvaluationContext{UserID: "during-flush"})
	done := make(chan error, 1)
	go func() { done <- c.Shutdown(context.Background()) }()
	eventually(t, func() bool { c.mu.RLock(); defer c.mu.RUnlock(); return c.shuttingDown })
	c.IsEnabled("new-checkout", EvaluationContext{UserID: "after-shutdown"})
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(delivered, ",") != "before-flush,during-flush" {
		t.Fatalf("delivered %v; want events recorded before Shutdown, and none after", delivered)
	}
}
func TestStringValuesAreBoundedByLength(t *testing.T) {
	short := map[string]any{"type": "evaluation", "value": strings.Repeat("v", 1024)}
	long := map[string]any{"type": "evaluation", "value": strings.Repeat("v", 64*1024)}
	if needsSizeCheck(short) || !needsSizeCheck(long) {
		t.Fatal("only strings that could exceed the event limit should be measured")
	}
	huge := strings.Repeat("x", 300*1024)
	snapshot := `{"schemaVersion":1,"projectId":"p","environmentId":"e","environmentKey":"test","version":1,"generatedAt":"2026-10-10T00:00:00Z","flags":{"v":{"key":"v","type":"variant","enabled":true,"default":"control","rules":[],"fallthrough":{"kind":"variant","value":"` + huge + `"}}}}`
	c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(503) }, Options{Bootstrap: []byte(snapshot), FlushAt: -1, FlushInterval: -1})
	if c.GetVariant("v", EvaluationContext{}) != huge {
		t.Fatal("caller should still receive the full value")
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	evaluations := 0
	for _, event := range c.events {
		if event["type"] != "evaluation" {
			continue
		}
		evaluations++
		if _, ok := event["value"]; ok {
			t.Fatal("oversized evaluation value retained")
		}
	}
	if evaluations != 1 {
		t.Fatal("an oversized string value should be dropped from its event")
	}
}
func TestUnsafeEndpointIsTrimmedAndPlaintextWarns(t *testing.T) {
	var logs bytes.Buffer
	log.SetOutput(&logs)
	defer log.SetOutput(os.Stderr)
	for endpoint, warns := range map[string]bool{
		"  https://edge.example.com/ ": false,
		"http://LOCALHOST:8787":        false,
		"http://127.0.0.1:8787":        false,
		"http://edge.internal:8787":    true,
	} {
		logs.Reset()
		c, err := New(Options{SDKKey: "test", DisableEvents: true, DisablePolling: true, Overrides: Overrides{UnsafeEndpoint: endpoint}, HTTPClient: httpClientFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })})
		if err != nil {
			t.Fatalf("%q: %v", endpoint, err)
		}
		_ = c.Close()
		if got := strings.Contains(logs.String(), "unencrypted"); got != warns {
			t.Errorf("%q: warned=%v, want %v", endpoint, got, warns)
		}
		if strings.HasSuffix(c.endpoint, "/") || strings.HasPrefix(c.endpoint, " ") {
			t.Errorf("%q: endpoint not normalized: %q", endpoint, c.endpoint)
		}
	}
}

func TestUnregisteredEventFollowsEvaluation(t *testing.T) {
	s, err := parseSnapshot([]byte(wire(1, true)))
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{snapshot: s, options: Options{FlushAt: -1}}
	c.Evaluate("missing", EvaluationContext{})
	if len(c.events) != 2 || c.events[0]["type"] != "evaluation" || c.events[1]["type"] != "unregistered_flag" {
		t.Fatalf("wrong event order: %v", c.events)
	}
}

func TestEvaluationTelemetryOmitsInvalidUserIDs(t *testing.T) {
	c := &Client{options: Options{FlushAt: -1}}
	for _, id := range []string{"", strings.Repeat("u", 256), strings.Repeat("u", 257), strings.Repeat("😀", 128), strings.Repeat("😀", 129)} {
		c.Evaluate("new-checkout", EvaluationContext{UserID: id, UserIDPresent: true})
	}
	if len(c.events) != 5 {
		t.Fatalf("want 5 evaluations, got %v", c.events)
	}
	for i, event := range c.events {
		_, present := event["userId"]
		if present != (i == 1 || i == 3) {
			t.Fatalf("event %d has invalid attribution: %v", i, event)
		}
	}
}

func TestEndpointRejectsEmptyQueryOrFragment(t *testing.T) {
	for _, endpoint := range []string{"https://edge.example.com?", "https://edge.example.com#", "https://edge.example.com/path?#", "   "} {
		t.Run(endpoint, func(t *testing.T) {
			_, err := New(Options{SDKKey: "test", Overrides: Overrides{UnsafeEndpoint: endpoint}})
			if err == nil {
				t.Fatal("invalid endpoint accepted")
			}
		})
	}
}
