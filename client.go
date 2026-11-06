package shipd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// HTTPClient is implemented by *http.Client.
type HTTPClient interface {
	Do(*http.Request) (*http.Response, error)
}

// ErrClosed is returned by Ready, Refresh, Flush, and Identify after Close or
// Shutdown. Evaluation keeps working after close.
var ErrClosed = errors.New("shipd: client is closed")

// Client is safe for concurrent use. Keep one client per environment per process.
type Client struct {
	options   Options
	endpoint  string
	http      HTTPClient
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
	ready     chan struct{}
	readyOnce sync.Once
	// attempted closes after the first fetch attempt; attemptErr is its result.
	attempted  chan struct{}
	attemptErr error
	closeOnce  sync.Once
	refreshMu  sync.Mutex
	flushGate  chan struct{}
	mu         sync.RWMutex
	snapshot   *snapshot
	etag       string
	lastError  error
	closed     bool
	// shuttingDown stops recording so Shutdown\'s flush sees every earlier event.
	shuttingDown bool
	events       []map[string]any
	// flushNow wakes the background loop once FlushAt events are buffered.
	flushNow     chan struct{}
	flushFailing bool
	dropped      uint64
	revision     uint64
	listeners    map[uint64]func()
	nextListener uint64
	streamDone   chan struct{}
	automatic    map[string]any
}

// New starts an initial asynchronous fetch and background polling. Construction
// errors concern configuration only; network errors are available via LastError.
func New(options Options) (*Client, error) {
	if strings.TrimSpace(options.SDKKey) == "" {
		return nil, errors.New("shipd: SDKKey is required")
	}
	if !boundedString(options.AnonymousID, 256, false) {
		return nil, errors.New("shipd: AnonymousID exceeds 256 characters")
	}
	endpoint := DefaultEndpoint
	if options.Overrides.UnsafeEndpoint != "" {
		endpoint = jsTrim(options.Overrides.UnsafeEndpoint)
	}
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || strings.ContainsAny(endpoint, "?#") {
		return nil, errors.New("shipd: Overrides.UnsafeEndpoint must be an HTTP(S) URL without credentials, query, or fragment")
	}
	endpoint = strings.TrimRight(endpoint, "/")
	// Same rule as the TypeScript SDK: plaintext is acceptable only to loopback hosts.
	if u.Scheme == "http" && !isLoopback(u.Hostname()) {
		log.Printf("shipd: Overrides.UnsafeEndpoint (%s) sends your SDK key unencrypted. Use it only for local shipd development.", endpoint)
	}
	if options.RefreshInterval < 0 || options.FetchTimeout < 0 {
		return nil, errors.New("shipd: RefreshInterval and FetchTimeout cannot be negative")
	}
	if options.FlushAt > maxBufferedEvents {
		return nil, fmt.Errorf("shipd: FlushAt cannot exceed %d", maxBufferedEvents)
	}
	if options.FlushAt == 0 {
		options.FlushAt = 50
	}
	if options.FlushInterval == 0 {
		options.FlushInterval = 5 * time.Second
	}
	if options.RefreshInterval == 0 {
		options.RefreshInterval = 30 * time.Second
	}
	if options.ReadyTimeout == 0 {
		options.ReadyTimeout = 5 * time.Second
	}
	if options.FetchTimeout == 0 {
		options.FetchTimeout = 10 * time.Second
	}
	options.Context = copyContext(options.Context)
	seed := make(map[string]string, len(options.AutomaticTraitSeed))
	for k, v := range options.AutomaticTraitSeed {
		seed[k] = v
	}
	options.AutomaticTraitSeed = seed
	if options.Stream {
		if _, err := streamURL(options.Overrides.StreamEndpoint, "validation"); err != nil {
			return nil, err
		}
	}
	defaults, err := json.Marshal(options.Defaults)
	if err != nil {
		return nil, errors.New("shipd: Defaults must contain JSON-compatible values")
	}
	if err = json.Unmarshal(defaults, &options.Defaults); err != nil {
		return nil, err
	}
	transport := options.HTTPClient
	if transport == nil {
		// Do not forward SDK keys through HTTP redirects.
		transport = &http.Client{CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
	}
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{options: options, endpoint: endpoint, http: transport, ctx: ctx, cancel: cancel, done: make(chan struct{}), ready: make(chan struct{}), attempted: make(chan struct{}), flushNow: make(chan struct{}, 1), flushGate: make(chan struct{}, 1), listeners: make(map[uint64]func()), streamDone: make(chan struct{}), automatic: automaticTraits(options.AutomaticTraitSeed)}
	if options.Bootstrap != nil {
		bootstrap, err := parseSnapshot(options.Bootstrap)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("shipd: Bootstrap: %w", err)
		}
		c.snapshot = bootstrap
		c.readyOnce.Do(func() { close(c.ready) })
	}
	go c.run()
	return c, nil
}
func (c *Client) run() {
	defer close(c.done)
	c.loadCache()
	c.attemptErr = c.Refresh(c.ctx)
	close(c.attempted)
	if c.options.Stream {
		go c.runStream()
	} else {
		close(c.streamDone)
	}
	polling := time.NewTimer(c.options.RefreshInterval)
	defer polling.Stop()
	// A negative FlushInterval disables timed flushes; a nil channel never fires.
	var events <-chan time.Time
	if c.options.FlushInterval > 0 {
		ticker := time.NewTicker(c.options.FlushInterval)
		defer ticker.Stop()
		events = ticker.C
	}
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-polling.C:
			if !c.options.DisablePolling {
				_ = c.Refresh(c.ctx)
			}
			polling.Reset(c.options.RefreshInterval)
		case <-events:
			if !c.options.DisableEvents {
				_ = c.Flush(c.ctx)
			}
		case <-c.flushNow:
			// A wake queued during a flush that then failed waits for the interval.
			c.mu.RLock()
			failing := c.flushFailing
			c.mu.RUnlock()
			if !failing {
				_ = c.Flush(c.ctx)
			}
		}
	}
}

// Ready waits for the initial snapshot fetch. It returns nil once a valid
// snapshot (or Bootstrap) is loaded, or the initial fetch error as soon as that
// attempt fails; evaluation then serves defaults and polling may still recover.
// A bounded context is recommended.
func (c *Client) Ready(ctx context.Context) (err error) {
	if c.options.ReadyTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.options.ReadyTimeout)
		defer cancel()
	}
	// Readiness and cancellation may both be signaled; closing takes precedence.
	defer func() {
		c.mu.RLock()
		defer c.mu.RUnlock()
		if c.closed {
			err = ErrClosed
		}
	}()
	if c.options.ReadyTimeout < 0 {
		return nil
	}
	select {
	case <-c.ready:
		return nil
	default:
	}
	select {
	case <-c.ready:
		return nil
	case <-c.attempted:
		select {
		case <-c.ready:
			return nil
		default:
		}
		if c.attemptErr != nil {
			return c.attemptErr
		}
		return errors.New("shipd: initial snapshot fetch returned no snapshot")
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return ErrClosed
	}
}

// LastError reports the most recent background/request failure. A successful
// refresh or flush clears it. Evaluation never requires this method to succeed.
func (c *Client) LastError() error { c.mu.RLock(); defer c.mu.RUnlock(); return c.lastError }

// SnapshotVersion reports the loaded snapshot version without recording an
// evaluation. ok is false until the first valid snapshot arrives.
func (c *Client) SnapshotVersion() (version int64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if c.snapshot == nil {
		return 0, false
	}
	return c.snapshot.Version, true
}

// DroppedEvents reports how many diagnostic events were discarded because the
// buffer was full, for example while the edge was unreachable.
func (c *Client) DroppedEvents() uint64 { c.mu.RLock(); defer c.mu.RUnlock(); return c.dropped }
func (c *Client) setError(err error)    { c.mu.Lock(); c.lastError = err; c.mu.Unlock() }
func (c *Client) request(ctx context.Context, method, path string, body io.Reader) (*http.Response, error) {
	requestCtx, cancel := context.WithCancel(ctx)
	if !c.options.DisableFetchTimeout {
		cancel()
		requestCtx, cancel = context.WithTimeout(ctx, c.options.FetchTimeout)
	}
	req, err := http.NewRequestWithContext(requestCtx, method, c.endpoint+path, body)
	if err != nil {
		cancel()
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+c.options.SDKKey)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("X-Shipd-SDK-Version", SDKVersion)
	req.Header.Set("X-Shipd-Snapshot-Schema", "1")
	req.Header.Set("X-Shipd-Control-Plane-API", "1")
	req.Header.Set("X-Shipd-Stream-Protocol", "1")
	var correlation [16]byte
	if _, err := rand.Read(correlation[:]); err == nil {
		id := hex.EncodeToString(correlation[:])
		req.Header.Set("X-Request-ID", id)
		req.Header.Set("X-Correlation-ID", id)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if method == http.MethodGet && path == "/sdk/v1/snapshot" {
		c.mu.RLock()
		etag := c.etag
		c.mu.RUnlock()
		if etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
	}
	response, err := c.http.Do(req)
	if err != nil {
		cancel()
		return nil, err
	}
	response.Body = &cancelBody{ReadCloser: response.Body, cancel: cancel}
	return response, nil
}

type cancelBody struct {
	io.ReadCloser
	cancel context.CancelFunc
}

func (b *cancelBody) Close() error { defer b.cancel(); return b.ReadCloser.Close() }

// Refresh fetches a snapshot now. Requests are serialized so an old ETag cannot
// race a newer snapshot. Failures preserve the complete last valid snapshot.
func (c *Client) Refresh(ctx context.Context) (err error) {
	c.refreshMu.Lock()
	defer c.refreshMu.Unlock()
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	defer func() {
		c.setError(err)
		if err != nil {
			c.reportError(err, true)
		}
	}()
	started := time.Now()
	defer func() { c.recordTiming("snapshot_fetch", time.Since(started), nil) }()
	fetchCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	response, err := c.request(fetchCtx, http.MethodGet, "/sdk/v1/snapshot", nil)
	if err != nil {
		return fmt.Errorf("shipd: snapshot fetch: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode == http.StatusNotModified {
		c.applyAutomaticHeader(response.Header.Get("X-Shipd-Auto-Traits"))
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.snapshot == nil {
			return errors.New("shipd: received 304 without a snapshot")
		}
		if etag := response.Header.Get("ETag"); etag != "" {
			c.etag = etag
		}
		return nil
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("shipd: snapshot fetch: HTTP %d", response.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(response.Body, 8*1024*1024+1))
	if err != nil {
		return fmt.Errorf("shipd: snapshot read: %w", err)
	}
	if len(data) > 8*1024*1024 {
		return errors.New("shipd: snapshot exceeds 8 MiB")
	}
	next, err := parseSnapshot(data)
	if err != nil {
		return err
	}
	c.applyAutomaticHeader(response.Header.Get("X-Shipd-Auto-Traits"))
	c.applySnapshot(next, data, response.Header.Get("ETag"), fetchCtx, true)
	return nil
}

// Evaluate returns details and records a bounded diagnostic event including
// the user ID and up to 100 scalar traits. DisableEvents suppresses telemetry.
func (c *Client) Evaluate(key string, ctx EvaluationContext) Result { return c.evaluate(key, ctx, nil) }
func (c *Client) evaluate(key string, ctx EvaluationContext, fallback any) Result {
	c.mu.RLock()
	s := c.snapshot
	ctx = mergeContext(c.options.Context, ctx)
	if c.options.AutomaticTraits {
		ctx = mergeContext(EvaluationContext{Attributes: c.automatic}, ctx)
	}
	if v, ok := c.options.Defaults[key]; ok {
		fallback = v
	}
	c.mu.RUnlock()
	r := evaluate(s, key, ctx, fallback)
	if c.options.DisableEvents {
		r.Value = detach(r.Value)
		return r
	}
	var snapshotVersion any
	if r.SnapshotVersion != nil {
		snapshotVersion = *r.SnapshotVersion
	}
	// The event keeps the snapshot or default value, which the client never
	// mutates; the caller gets its own copy of JSON values and version data.
	event := map[string]any{"type": "evaluation", "key": key, "reason": r.Reason, "value": r.Value, "snapshotVersion": snapshotVersion}
	if traits := telemetryTraits(ctx.Attributes); traits != nil {
		event["traits"] = traits
	}
	if c.options.AnonymousID != "" {
		event["anonymousId"] = c.options.AnonymousID
	}
	r.Value = detach(r.Value)
	if r.RuleID != "" {
		event["ruleId"] = r.RuleID
	}
	if boundedString(ctx.UserID, 256, true) {
		event["userId"] = ctx.UserID
	}
	if boundedString(key, 256, true) && (r.RuleID == "" || boundedString(r.RuleID, 256, true)) {
		c.record(event)
	}
	if r.Reason == "not_found" && boundedString(key, 256, true) {
		c.record(map[string]any{"type": "unregistered_flag", "key": key, "snapshotVersion": snapshotVersion})
	}
	return r
}

// detach copies JSON objects and arrays; scalars are immutable and returned as is.
func detach(v any) any {
	switch v.(type) {
	case map[string]any, []any:
		return cloneJSON(v)
	}
	return v
}

func cloneJSON(v any) any {
	data, err := json.Marshal(v)
	if err != nil {
		return nil
	}
	var result any
	_ = json.Unmarshal(data, &result)
	return result
}

// IsEnabled evaluates a boolean flag for ctx. It never makes a network request.
// Before a snapshot loads, for an unknown key, or when the flag serves a
// non-boolean value, it returns the configured default or false.
func (c *Client) IsEnabled(key string, ctx EvaluationContext) bool {
	v, ok := c.evaluate(key, ctx, false).Value.(bool)
	if !ok {
		v, _ = c.options.Defaults[key].(bool)
	}
	return v
}

// GetVariant evaluates a variant flag for ctx. It falls back to the configured
// default, then to "control".
func (c *Client) GetVariant(key string, ctx EvaluationContext) string {
	v, ok := c.evaluate(key, ctx, "control").Value.(string)
	if !ok {
		if fallback, ok := c.options.Defaults[key].(string); ok {
			return fallback
		}
		return "control"
	}
	return v
}

// GetNumber evaluates a number flag for ctx. JSON numbers are float64. It falls
// back to the configured default, then to 0.
func (c *Client) GetNumber(key string, ctx EvaluationContext) float64 {
	v, ok := c.evaluate(key, ctx, float64(0)).Value.(float64)
	if !ok {
		v, _ = c.options.Defaults[key].(float64)
	}
	return v
}

// GetJSON evaluates a JSON flag for ctx and returns a copy the caller may
// modify. Objects decode as map[string]any and numbers as float64. It falls back
// to the configured default, then to an empty object.
func (c *Client) GetJSON(key string, ctx EvaluationContext) any {
	return c.evaluate(key, ctx, map[string]any{}).Value
}

// Shutdown flushes pending events within ctx, then closes the client. It
// returns the flush error, if any; the client is closed either way. Events
// recorded after Shutdown starts are not delivered. Use it for graceful
// shutdown with a bounded context. Repeated calls are safe.
func (c *Client) Shutdown(ctx context.Context) error {
	// Stop accepting events first. Flush then waits
	// for any background flush, so the final batch includes everything recorded
	// before Shutdown, including events requeued by a failed background send.
	c.mu.Lock()
	c.shuttingDown = true
	c.mu.Unlock()
	err := c.Flush(ctx)
	if errors.Is(err, ErrClosed) {
		err = nil
	}
	_ = c.Close()
	return err
}

// Close stops polling, cancels requests, and waits for background work without
// flushing events; use Shutdown to deliver them first. Evaluation after Close
// still serves the last snapshot/defaults. Close implements io.Closer.
func (c *Client) Close() error {
	c.closeOnce.Do(func() { c.mu.Lock(); c.closed = true; c.mu.Unlock(); c.cancel() })
	<-c.done
	<-c.streamDone
	c.refreshMu.Lock()
	c.refreshMu.Unlock()
	c.flushGate <- struct{}{}
	<-c.flushGate
	return nil
}

// isLoopback matches the TypeScript SDK's rule: localhost, *.localhost, and
// loopback IP addresses.
func isLoopback(host string) bool {
	host = strings.ToLower(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
