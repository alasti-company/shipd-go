package shipd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestFlushSingleEventByteBoundary(t *testing.T) {
	for _, size := range []int{maxEventBytes, maxEventBytes + 1} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			var requests atomic.Int32
			c, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodGet {
					fmt.Fprint(w, wire(1, true))
					return
				}
				requests.Add(1)
				data, _ := io.ReadAll(r.Body)
				var batch struct{ Events []map[string]any }
				if err := json.Unmarshal(data, &batch); err != nil || len(batch.Events) != 1 || len(data) > 256*1024 {
					t.Errorf("invalid batch: %d bytes, %d events, error %v", len(data), len(batch.Events), err)
				}
				w.WriteHeader(http.StatusAccepted)
			}, Options{FlushAt: -1, FlushInterval: -1})
			waitReady(t, c)
			event := map[string]any{"type": "evaluation", "key": "large", "reason": "fallthrough", "value": "", "ts": "2026-01-02T00:00:00Z"}
			base, _ := json.Marshal(event)
			event["value"] = strings.Repeat("x", size-len(base))
			// Exercise the exact serialized boundary without clock-dependent timestamp length.
			c.mu.Lock()
			c.events = []map[string]any{event}
			c.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			err := c.Flush(ctx)
			if size == maxEventBytes {
				if err != nil || requests.Load() != 1 || bufferedEvents(c) != 0 {
					t.Fatalf("boundary event was not delivered: %v, requests %d", err, requests.Load())
				}
			} else if err == nil || requests.Load() != 0 || bufferedEvents(c) != 1 {
				t.Fatalf("oversized event must fail without empty requests: %v, requests %d", err, requests.Load())
			}
		})
	}
}

func TestBoundedStringUTF16AndAllocations(t *testing.T) {
	for _, tc := range []struct {
		value    string
		max      int
		nonempty bool
		want     bool
	}{
		{"", 0, false, true}, {"", 256, true, false},
		{"a", 1, true, true}, {"ab", 1, true, false},
		{"界", 1, true, true}, {"😀", 1, true, false}, {"😀", 2, true, true},
		{strings.Repeat("😀", 128), 256, true, true},
		{strings.Repeat("😀", 129), 256, true, false},
	} {
		if got := boundedString(tc.value, tc.max, tc.nonempty); got != tc.want {
			t.Errorf("boundedString(%q, %d, %v) = %v", tc.value, tc.max, tc.nonempty, got)
		}
	}
	large := strings.Repeat("😀", 1<<18)
	if allocations := testing.AllocsPerRun(100, func() {
		if boundedString(large, 256, true) {
			t.Fatal("oversized ID accepted")
		}
	}); allocations != 0 {
		t.Fatalf("oversized ID validation allocated %v times", allocations)
	}
}

func TestFlushAfterClose(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, wire(1, true)) }, Options{DisableEvents: disabled})
			waitReady(t, c)
			_ = c.Close()
			if err := c.Flush(context.Background()); !errors.Is(err, ErrClosed) {
				t.Fatalf("want ErrClosed, got %v", err)
			}
		})
	}
}

func TestEmptyFlushClearsLastError(t *testing.T) {
	for _, disabled := range []bool{false, true} {
		t.Run(fmt.Sprint(disabled), func(t *testing.T) {
			c, _ := newTestClient(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) }, Options{DisableEvents: disabled})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := c.Ready(ctx); err == nil || c.LastError() == nil {
				t.Fatal("expected initial fetch failure")
			}
			c.mu.Lock()
			c.events = nil
			c.mu.Unlock() // This test exercises an empty flush after fetch diagnostics.
			if err := c.Flush(ctx); err != nil {
				t.Fatal(err)
			}
			if err := c.LastError(); err != nil {
				t.Fatalf("successful empty flush left stale error: %v", err)
			}
		})
	}
}

func TestFlushAndShutdownDeadlineWhileBackgroundFlushRuns(t *testing.T) {
	for _, shutdown := range []bool{false, true} {
		t.Run(fmt.Sprint(shutdown), func(t *testing.T) {
			started := make(chan struct{})
			c, err := New(Options{SDKKey: "test", DisablePolling: true, FlushAt: 1, FlushInterval: -1, FetchTimeout: 5 * time.Second, HTTPClient: httpClientFunc(func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodGet {
					return &http.Response{StatusCode: http.StatusOK, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(wire(1, true)))}, nil
				}
				close(started)
				<-r.Context().Done()
				return nil, r.Context().Err()
			})})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			waitReady(t, c)
			c.Evaluate("new-checkout", EvaluationContext{})
			select {
			case <-started:
			case <-time.After(time.Second):
				t.Fatal("background flush did not start")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			result := make(chan error, 1)
			go func() {
				if shutdown {
					result <- c.Shutdown(ctx)
				} else {
					result <- c.Flush(ctx)
				}
			}()
			select {
			case err := <-result:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("want deadline error, got %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("flush wait exceeded its deadline")
			}
			if shutdown {
				if err := c.Ready(context.Background()); !errors.Is(err, ErrClosed) {
					t.Fatalf("Shutdown did not close client: %v", err)
				}
			}
		})
	}
}
