package openfeature_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	shipd "github.com/alasti-company/shipd-go"
	shipdof "github.com/alasti-company/shipd-go/openfeature"
	of "github.com/open-feature/go-sdk/openfeature"
)

func snapshot(version int, enabled bool) string {
	return fmt.Sprintf(`{
		"schemaVersion": 1, "version": %d, "projectId": "p", "environmentId": "e", "environmentKey": "test",
		"generatedAt": "2026-10-09T00:00:00Z",
		"flags": {
			"checkout": {
				"type": "boolean", "enabled": true, "default": false,
				"fallthrough": {"kind": "boolean", "value": %t},
				"rules": [{"id": "pro-audience", "conditions": [
					{"kind": "attribute", "attribute": "userId", "operator": "eq", "value": "user-1"},
					{"kind": "segment", "segmentId": "pro"}
				], "serve": {"kind": "boolean", "value": true}}]
			},
			"disabled": {"type": "boolean", "enabled": false, "default": true, "rules": [],
				"fallthrough": {"kind": "boolean", "value": false}},
			"search": {"type": "variant", "enabled": true, "rules": [],
				"fallthrough": {"kind": "variant", "value": "ranked"}},
			"retries": {"type": "number", "enabled": true, "rules": [],
				"fallthrough": {"kind": "number", "value": 5}},
			"ratio": {"type": "number", "enabled": true, "rules": [],
				"fallthrough": {"kind": "number", "value": 1.5}},
			"too-big": {"type": "number", "enabled": true, "rules": [],
				"fallthrough": {"kind": "number", "value": 9223372036854775808}},
			"payload": {"type": "json", "enabled": true, "rules": [],
				"fallthrough": {"kind": "json", "value": {"steps": ["verify", 2]}}},
			"array": {"type": "json", "enabled": true, "rules": [],
				"fallthrough": {"kind": "json", "value": [true, null]}},
			"primitive": {"type": "json", "enabled": true, "rules": [],
				"fallthrough": {"kind": "json", "value": true}},
			"null": {"type": "json", "enabled": true, "rules": [],
				"fallthrough": {"kind": "json", "value": null}},
			"bad-serve": {"type": "boolean", "enabled": true, "rules": [],
				"fallthrough": {"kind": "variant", "value": "ranked"}}
		},
		"segments": {"pro": {"conditions": [
			{"kind": "attribute", "attribute": "plan", "operator": "eq", "value": "pro"},
			{"kind": "attribute", "attribute": "score", "operator": "gte", "value": 10},
			{"kind": "attribute", "attribute": "employee", "operator": "eq", "value": false}
		]}}
	}`, version, enabled)
}

func optionsFor(t *testing.T, handler http.HandlerFunc) shipd.Options {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	t.Cleanup(func() {
		of.Shutdown()
		if err := of.SetProviderAndWait(of.NoopProvider{}); err != nil {
			t.Error(err)
		}
		of.SetEvaluationContext(of.EvaluationContext{})
	})
	return shipd.Options{
		SDKKey:         "shipd_sdk_server_test",
		Overrides:      shipd.Overrides{UnsafeEndpoint: server.URL},
		ReadyTimeout:   time.Second,
		DisablePolling: true,
		DisableEvents:  true,
	}
}

func TestResolutionThroughOpenFeature(t *testing.T) {
	options := optionsFor(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, snapshot(7, false))
	})
	options.Context = shipd.EvaluationContext{UserID: "user-1", Attributes: map[string]any{"plan": "pro"}}
	if err := of.SetProviderAndWait(shipdof.NewProvider(options)); err != nil {
		t.Fatal(err)
	}
	if name := of.ProviderMetadata().Name; name != "shipd" {
		t.Fatalf("provider name = %q", name)
	}

	ctx := context.Background()
	client := of.NewClient("checkout")
	of.SetEvaluationContext(of.NewEvaluationContext("global-user", map[string]any{"plan": "free", "score": uintptr(10)}))
	client.SetEvaluationContext(of.NewEvaluationContext("client-user", map[string]any{"employee": false}))
	user := of.NewEvaluationContext("user-1", map[string]any{"plan": "pro", "nullable": nil})

	checkout, err := client.BooleanValueDetails(ctx, "checkout", false, user)
	if err != nil || !checkout.Value || checkout.Reason != of.TargetingMatchReason || checkout.Variant != "" {
		t.Fatalf("targeted checkout = %+v, %v", checkout, err)
	}
	if version, err := checkout.FlagMetadata.GetInt("snapshotVersion"); err != nil || version != 7 {
		t.Fatalf("snapshot version = %d, %v", version, err)
	}
	if id, err := checkout.FlagMetadata.GetString("ruleId"); err != nil || id != "pro-audience" {
		t.Fatalf("rule ID = %q, %v", id, err)
	}
	noUser := of.NewTargetlessEvaluationContext(map[string]any{"plan": "pro"})
	defaulted, err := client.BooleanValueDetails(ctx, "checkout", true, noUser)
	if err != nil || defaulted.Value || defaulted.Reason != of.DefaultReason || len(defaulted.FlagMetadata) != 1 {
		t.Fatalf("fallthrough = %+v, %v", defaulted, err)
	}
	disabled, err := client.BooleanValueDetails(ctx, "disabled", false, user)
	if err != nil || !disabled.Value || disabled.Reason != of.DisabledReason {
		t.Fatalf("disabled flag = %+v, %v", disabled, err)
	}
	if value, err := client.StringValue(ctx, "search", "control", user); err != nil || value != "ranked" {
		t.Fatalf("variant = %q, %v", value, err)
	}
	if value, err := client.FloatValue(ctx, "ratio", 0, user); err != nil || value != 1.5 {
		t.Fatalf("float = %g, %v", value, err)
	}
	if value, err := client.IntValue(ctx, "retries", 0, user); err != nil || value != 5 {
		t.Fatalf("integer = %d, %v", value, err)
	}
	for key, want := range map[string]any{
		"payload":   map[string]any{"steps": []any{"verify", float64(2)}},
		"array":     []any{true, nil},
		"primitive": true,
		"null":      nil,
	} {
		value, err := client.ObjectValue(ctx, key, "fallback", user)
		if err != nil || !reflect.DeepEqual(value, want) {
			t.Fatalf("%s = %v, %v; want %v", key, value, err, want)
		}
		if payload, ok := value.(map[string]any); ok {
			payload["steps"].([]any)[0] = "changed"
			if again := client.Object(ctx, key, nil, user); !reflect.DeepEqual(again, want) {
				t.Fatalf("caller mutated the next evaluation: %v", again)
			}
		}
	}

	var requests sync.WaitGroup
	for i := 0; i < 32; i++ {
		requests.Add(1)
		go func() {
			defer requests.Done()
			requestCtx := user
			want := i%2 == 0
			if !want {
				requestCtx = noUser
			}
			if got, err := client.BooleanValue(ctx, "checkout", false, requestCtx); err != nil || got != want {
				t.Errorf("concurrent checkout = %t, %v; want %t", got, err, want)
			}
		}()
	}
	requests.Wait()
}

func TestCallerDefaultsAndErrors(t *testing.T) {
	options := optionsFor(t, func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, snapshot(1, true))
	})
	options.Defaults = map[string]any{"missing": true}
	if err := of.SetProviderAndWait(shipdof.NewProvider(options)); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	user := of.EvaluationContext{}
	client := of.NewClient("checkout")
	for key, code := range map[string]of.ErrorCode{
		"missing": of.FlagNotFoundCode, "search": of.TypeMismatchCode,
		"primitive": of.TypeMismatchCode, "bad-serve": of.TypeMismatchCode,
	} {
		details, err := client.BooleanValueDetails(ctx, key, false, user)
		if err == nil || details.Value || details.ErrorCode != code || details.Reason != of.ErrorReason || details.ErrorMessage == "" {
			t.Errorf("%s = %+v, %v; want %s", key, details, err, code)
		}
	}
	missing, err := client.StringValueDetails(ctx, "missing", "caller-specific", user)
	if err == nil || missing.Value != "caller-specific" || missing.ErrorCode != of.FlagNotFoundCode {
		t.Fatalf("missing variant = %+v, %v", missing, err)
	}
	for _, key := range []string{"ratio", "too-big"} {
		details, err := client.IntValueDetails(ctx, key, -9, user)
		if err == nil || details.Value != -9 || details.ErrorCode != of.TypeMismatchCode {
			t.Errorf("%s = %+v, %v", key, details, err)
		}
	}
	object, err := client.ObjectValueDetails(ctx, "search", nil, user)
	if err == nil || object.Value != nil || object.ErrorCode != of.TypeMismatchCode {
		t.Fatalf("non-JSON flag = %+v, %v", object, err)
	}
	for key, value := range map[string]any{
		"userId": "use targetingKey", of.TargetingKey: 123, "nested": []string{"pro"},
		"date": time.Now(), "infinite": math.Inf(1), "nan": math.NaN(),
	} {
		badContext := of.NewTargetlessEvaluationContext(map[string]any{key: value})
		details, err := client.BooleanValueDetails(ctx, "checkout", false, badContext)
		if err == nil || details.Value || details.ErrorCode != of.InvalidContextCode {
			t.Errorf("invalid context %s = %+v, %v", key, details, err)
		}
	}
}

func TestProviderRecoveryAndUpdates(t *testing.T) {
	var stage atomic.Int32
	options := optionsFor(t, func(w http.ResponseWriter, _ *http.Request) {
		switch stage.Load() {
		case 1:
			fmt.Fprint(w, snapshot(1, true))
		case 2:
			fmt.Fprint(w, snapshot(2, false))
		default:
			w.WriteHeader(http.StatusServiceUnavailable)
		}
	})
	options.DisablePolling = false
	options.RefreshInterval = 10 * time.Millisecond
	failures := make(chan error, 8)
	options.OnError = func(err error) {
		select {
		case failures <- err:
		default:
		}
	}
	client := of.NewClient("checkout")
	startupError := recordEvent(t, nil, of.ProviderError)
	recovered := recordEvent(t, client, of.ProviderReady)
	changed := recordEvent(t, client, of.ProviderConfigChange)
	if err := of.SetProvider(shipdof.NewProvider(options)); err != nil {
		t.Fatal(err)
	}
	if event := receive(t, startupError); !strings.Contains(event.Message, "503") {
		t.Fatalf("startup event = %+v", event)
	}
	ctx, user := context.Background(), of.EvaluationContext{}
	details, err := client.BooleanValueDetails(ctx, "checkout", true, user)
	if err == nil || !details.Value || details.ErrorCode != of.ProviderNotReadyCode {
		t.Fatalf("before recovery = %+v, %v", details, err)
	}

	stage.Store(1)
	receive(t, recovered)
	if client.State() != of.ReadyState || !client.Boolean(ctx, "checkout", false, user) {
		t.Fatal("provider should serve the recovered snapshot")
	}
	stage.Store(2)
	receive(t, changed)
	details, err = client.BooleanValueDetails(ctx, "checkout", true, user)
	if version, _ := details.FlagMetadata.GetInt("snapshotVersion"); err != nil || details.Value || version != 2 {
		t.Fatalf("updated checkout = %+v, %v", details, err)
	}
	for len(failures) > 0 {
		<-failures
	}
	stage.Store(3)
	receive(t, failures)
	if client.State() != of.ReadyState || client.Boolean(ctx, "checkout", true, user) {
		t.Fatal("failed refresh should keep serving the last snapshot")
	}
	select {
	case event := <-changed:
		t.Fatalf("unexpected configuration event: %+v", event)
	default:
	}
}

func TestInitializationRequiresServerKey(t *testing.T) {
	var requests atomic.Int32
	options := optionsFor(t, func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		fmt.Fprint(w, snapshot(1, true))
	})
	options.SDKKey = "shipd_sdk_client_test"
	err := of.SetProviderAndWait(shipdof.NewProvider(options))
	var initError *of.ProviderInitError
	if !errors.As(err, &initError) || initError.ErrorCode != of.ProviderFatalCode || requests.Load() != 0 {
		t.Fatalf("registration with a client key = %v (%d requests)", err, requests.Load())
	}
}

func TestStartupTimeoutRecoversWhenFetchFinishes(t *testing.T) {
	release := make(chan struct{})
	options := optionsFor(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			fmt.Fprint(w, snapshot(1, true))
		case <-r.Context().Done():
		}
	})
	options.ReadyTimeout = 20 * time.Millisecond
	options.FetchTimeout = 5 * time.Second
	client := of.NewClient("checkout")
	startupError := recordEvent(t, nil, of.ProviderError)
	recovered := recordEvent(t, client, of.ProviderReady)
	if err := of.SetProvider(shipdof.NewProvider(options)); err != nil {
		t.Fatal(err)
	}
	if event := receive(t, startupError); !strings.Contains(event.Message, context.DeadlineExceeded.Error()) {
		t.Fatalf("startup event = %+v", event)
	}
	close(release)
	receive(t, recovered)
	if !client.Boolean(context.Background(), "checkout", false, of.EvaluationContext{}) {
		t.Fatal("provider should serve the snapshot after its pending fetch finishes")
	}
}

func TestOfflineCacheAndShutdown(t *testing.T) {
	flushed := make(chan []map[string]any, 1)
	fetchStarted := make(chan struct{})
	fetchCanceled := make(chan struct{})
	options := optionsFor(t, func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/sdk/v1/snapshot":
			close(fetchStarted)
			<-r.Context().Done()
			close(fetchCanceled)
		case "/sdk/v1/events":
			var body struct{ Events []map[string]any }
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Error(err)
			}
			flushed <- body.Events
			w.WriteHeader(http.StatusAccepted)
		default:
			t.Errorf("unexpected request to %s", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	})
	options.Cache = offlineCache{snapshot: []byte(snapshot(9, true))}
	options.DisableEvents = false
	options.FlushAt, options.FlushInterval = -1, -1
	provider := shipdof.NewProvider(options)
	if err := of.SetProviderAndWait(provider); err != nil {
		t.Fatal(err)
	}
	receive(t, fetchStarted)
	ctx := context.Background()
	client := of.NewClient("checkout")
	user := of.NewEvaluationContext("user-2", map[string]any{"plan": "pro"})
	if !client.Boolean(ctx, "checkout", false, user) {
		t.Fatal("provider should serve the cached snapshot before the fetch finishes")
	}
	of.Shutdown()
	receive(t, fetchCanceled)
	events := receive(t, flushed)
	for _, event := range events {
		if event["type"] == "evaluation" && event["key"] == "checkout" && event["userId"] == "user-2" && event["value"] == true {
			if err := provider.ShutdownWithContext(ctx); err != nil {
				t.Fatal(err)
			}
			if err := provider.Init(of.EvaluationContext{}); err == nil {
				t.Fatal("closed provider was initialized again")
			}
			return
		}
	}
	t.Fatalf("shutdown did not deliver the evaluation event: %v", events)
}

type offlineCache struct{ snapshot []byte }

func (c offlineCache) Load(context.Context) (*shipd.CachedSnapshot, error) {
	return &shipd.CachedSnapshot{Snapshot: c.snapshot}, nil
}

func (offlineCache) Save(context.Context, shipd.CachedSnapshot) error { return nil }

func recordEvent(t *testing.T, client *of.Client, eventType of.EventType) <-chan of.EventDetails {
	t.Helper()
	events := make(chan of.EventDetails, 8)
	handler := func(details of.EventDetails) {
		if details.ProviderName != "shipd" {
			return
		}
		select {
		case events <- details:
		default:
		}
	}
	add, remove := of.AddHandler, of.RemoveHandler
	if client != nil {
		add, remove = client.AddHandler, client.RemoveHandler
	}
	add(eventType, &handler)
	t.Cleanup(func() { remove(eventType, &handler) })
	return events
}

func receive[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for provider activity")
		var zero T
		return zero
	}
}
