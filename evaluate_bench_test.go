package shipd

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type benchmarkFixture struct {
	Snapshot json.RawMessage     `json:"snapshot"`
	Contexts []EvaluationContext `json:"contexts"`
	Cases    []struct {
		Key                 string `json:"key"`
		ExpectedReason      string `json:"expectedReason"`
		ExpectedRuleID      string `json:"expectedRuleId"`
		ExpectedValue       *bool  `json:"expectedValue"`
		RequireBothOutcomes bool   `json:"requireBothOutcomes"`
	} `json:"cases"`
}

type benchmarkUpdatingHTTP struct {
	data    map[string]json.RawMessage
	version atomic.Int64
}

func (transport *benchmarkUpdatingHTTP) Do(req *http.Request) (*http.Response, error) {
	transport.data["version"], _ = json.Marshal(transport.version.Add(1))
	body, err := json.Marshal(transport.data)
	return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(body))}, err
}

// Opt-in instrumented latency pass; throughput comes from the native benchmark.
func TestBenchmarkLatency(t *testing.T) {
	output := os.Getenv("BENCH_LATENCY_OUTPUT")
	if output == "" {
		t.Skip("run through pnpm bench:sdk")
	}
	data, err := os.ReadFile(os.Getenv("BENCH_FIXTURE"))
	if err != nil {
		t.Fatal(err)
	}
	var fixture benchmarkFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	count := 50000
	if os.Getenv("BENCH_SMOKE") == "1" {
		count = 1000
	}
	rows := []map[string]any{}
	for _, scenario := range fixture.Cases {
		for _, updates := range []bool{false, true} {
			transport := &benchmarkUpdatingHTTP{}
			if err := json.Unmarshal(fixture.Snapshot, &transport.data); err != nil {
				t.Fatal(err)
			}
			var version int64
			if err := json.Unmarshal(transport.data["version"], &version); err != nil {
				t.Fatal(err)
			}
			transport.version.Store(version)
			client, err := New(Options{SDKKey: "benchmark-not-a-real-key", Bootstrap: fixture.Snapshot, DisablePolling: true, DisableEvents: true, HTTPClient: transport})
			if err != nil {
				t.Fatal(err)
			}
			<-client.attempted
			for i := 0; i < 1000; i++ {
				benchmarkSink = client.IsEnabled(scenario.Key, fixture.Contexts[i%len(fixture.Contexts)])
			}
			stop, done := make(chan struct{}), make(chan struct{})
			var replacements atomic.Int64
			if updates {
				go func() {
					defer close(done)
					for {
						select {
						case <-stop:
							return
						default:
						}
						if err := client.Refresh(context.Background()); err != nil {
							return
						}
						replacements.Add(1)
						time.Sleep(time.Millisecond)
					}
				}()
			}
			times := make([]float64, count)
			for i := range times {
				ctx := fixture.Contexts[i%len(fixture.Contexts)]
				start := time.Now()
				value := client.IsEnabled(scenario.Key, ctx)
				times[i] = float64(time.Since(start).Nanoseconds()) / 1000
				benchmarkSink = value
			}
			if updates {
				close(stop)
				<-done
			}
			if err := client.Close(); err != nil {
				t.Fatal(err)
			}
			sort.Float64s(times)
			mode := "sdk-events-off"
			if updates {
				mode = "sdk-replacements-1ms"
			}
			rows = append(rows, map[string]any{"scenario": scenario.Key, "mode": mode, "samples": count,
				"instrumentedP50Us": times[(count*50+99)/100-1], "instrumentedP95Us": times[(count*95+99)/100-1], "instrumentedP99Us": times[(count*99+99)/100-1], "snapshotReplacements": replacements.Load()})
		}
	}
	encoded, err := json.MarshalIndent(rows, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(output, append(encoded, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

type benchmarkHTTP struct{}

func (benchmarkHTTP) Do(req *http.Request) (*http.Response, error) {
	status := http.StatusNoContent
	if strings.HasSuffix(req.URL.Path, "/snapshot") {
		status = http.StatusNotModified
	}
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(""))}, nil
}

var benchmarkSink bool

// BenchmarkFlagEvaluation uses the same snapshot and 64 contexts as TypeScript.
// EventsBuffered measures recording into a saturated buffer, not event delivery.
func BenchmarkFlagEvaluation(b *testing.B) {
	path := os.Getenv("BENCH_FIXTURE")
	if path == "" {
		path = "testdata/workloads.json"
	}
	data, err := os.ReadFile(path)
	if err != nil {
		b.Fatal(err)
	}
	var fixture benchmarkFixture
	if err := json.Unmarshal(data, &fixture); err != nil {
		b.Fatal(err)
	}
	s, err := parseSnapshot(fixture.Snapshot)
	if err != nil {
		b.Fatal(err)
	}
	for _, scenario := range fixture.Cases {
		b.Run(scenario.Key, func(b *testing.B) {
			expected := make([]bool, len(fixture.Contexts))
			trueCount := 0
			for i, ctx := range fixture.Contexts {
				r := evaluate(s, scenario.Key, ctx, nil)
				value, ok := r.Value.(bool)
				if !ok || (scenario.ExpectedReason != "" && r.Reason != scenario.ExpectedReason) || (scenario.ExpectedRuleID != "" && r.RuleID != scenario.ExpectedRuleID) {
					b.Fatalf("unexpected result: %+v", r)
				}
				if scenario.ExpectedValue != nil && value != *scenario.ExpectedValue {
					b.Fatal("unexpected boolean value")
				}
				expected[i] = value
				if value {
					trueCount++
				}
			}
			if scenario.RequireBothOutcomes && (trueCount == 0 || trueCount == len(expected)) {
				b.Fatal("need both rollout outcomes")
			}
			for _, mode := range []string{"Evaluator", "SDKEventsOff", "SDKEventsBuffered"} {
				b.Run(mode, func(b *testing.B) {
					call := func(ctx EvaluationContext) bool { return evaluate(s, scenario.Key, ctx, nil).Value.(bool) }
					if mode != "Evaluator" {
						client, err := New(Options{SDKKey: "benchmark-not-a-real-key", Bootstrap: fixture.Snapshot,
							DisablePolling: true, DisableEvents: mode == "SDKEventsOff", FlushAt: -1, FlushInterval: -1, HTTPClient: benchmarkHTTP{}})
						if err != nil {
							b.Fatal(err)
						}
						<-client.attempted
						b.Cleanup(func() {
							if err := client.Close(); err != nil {
								b.Error(err)
							}
						})
						call = func(ctx EvaluationContext) bool { return client.IsEnabled(scenario.Key, ctx) }
					}
					for i, ctx := range fixture.Contexts {
						if call(ctx) != expected[i] {
							b.Fatal("SDK/evaluator mismatch")
						}
					}
					b.ReportAllocs()
					b.ResetTimer()
					var value bool
					for i := 0; i < b.N; i++ {
						value = call(fixture.Contexts[i%len(fixture.Contexts)])
					}
					b.StopTimer()
					benchmarkSink = value
					b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "eval/s")
				})
			}
		})
	}
}
