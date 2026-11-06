package shipd

import (
	"context"
	"fmt"
	"math"
	"net/url"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"
	_ "time/tzdata" // Keep automatic timezone traits valid in minimal containers.
)

func copyContext(ctx EvaluationContext) EvaluationContext {
	if ctx.Attributes != nil {
		a := make(map[string]any, len(ctx.Attributes))
		for k, v := range ctx.Attributes {
			a[k] = v
		}
		ctx.Attributes = a
	}
	if ctx.SegmentIDs != nil {
		ctx.SegmentIDs = append([]string{}, ctx.SegmentIDs...)
	}
	return ctx
}
func mergeContext(base, override EvaluationContext) EvaluationContext {
	result := base
	if override.UserID != "" || override.UserIDPresent {
		result.UserID = override.UserID
		result.UserIDPresent = true
	}
	if override.Attributes != nil {
		if len(base.Attributes) == 0 {
			result.Attributes = override.Attributes
		} else {
			result.Attributes = make(map[string]any, len(base.Attributes)+len(override.Attributes))
			for k, v := range base.Attributes {
				result.Attributes[k] = v
			}
			for k, v := range override.Attributes {
				result.Attributes[k] = v
			}
		}
	}
	if override.SegmentIDs != nil {
		result.SegmentIDs = override.SegmentIDs
	}
	return result
}

// Revision is the monotonic generation of snapshot or automatic trait changes.
func (c *Client) Revision() uint64 { c.mu.RLock(); defer c.mu.RUnlock(); return c.revision }

// Subscribe registers a change listener and returns an idempotent unsubscribe.
// Listeners run outside the state lock, may evaluate or unsubscribe, and must
// return promptly. Do not call blocking lifecycle methods from a listener.
// A callback already copied for delivery can run after unsubscribe returns.
func (c *Client) Subscribe(listener func()) func() {
	if listener == nil {
		return func() {}
	}
	c.mu.Lock()
	c.nextListener++
	id := c.nextListener
	if c.listeners == nil {
		c.listeners = map[uint64]func(){}
	}
	c.listeners[id] = listener
	c.mu.Unlock()
	var once sync.Once
	return func() { once.Do(func() { c.mu.Lock(); delete(c.listeners, id); c.mu.Unlock() }) }
}
func safeCallback(fn func()) { defer func() { _ = recover() }(); fn() }

// changeLocked publishes the new generation atomically with the state that it
// describes. Delivery runs after unlocking and must not wait for cache I/O.
func (c *Client) changeLocked() []func() {
	c.revision++
	listeners := make([]func(), 0, len(c.listeners))
	for _, listener := range c.listeners {
		listeners = append(listeners, listener)
	}
	return listeners
}
func notifyListeners(listeners []func()) {
	for _, listener := range listeners {
		safeCallback(listener)
	}
}

// AnonymousID returns the configured correlation identifier. Servers never rotate it.
func (c *Client) AnonymousID() string { return c.options.AnonymousID }

// Reset is a no-op on server clients, which hold no mutable per-user identity.
func (c *Client) Reset() {}

func (c *Client) versionValue() any {
	v, ok := c.SnapshotVersion()
	if !ok {
		return nil
	}
	return v
}
func (c *Client) reportError(err error, record bool) {
	if err == nil || c.ctx.Err() != nil {
		return
	}
	c.setError(err)
	if c.options.OnError != nil {
		safeCallback(func() { c.options.OnError(err) })
	}
	if record {
		message := err.Error()
		if len(message) > 4096 {
			message = message[:4096]
		}
		c.record(map[string]any{"type": "error", "message": message, "snapshotVersion": c.versionValue()})
	}
}
func (c *Client) recordTiming(stage string, duration time.Duration, version any) {
	if duration < 0 {
		return
	}
	if version == nil {
		version = c.versionValue()
	}
	event := map[string]any{"type": "timing", "stage": stage, "durationMs": duration.Milliseconds(), "snapshotVersion": version}
	c.mu.RLock()
	if c.snapshot != nil {
		event["environmentId"] = c.snapshot.EnvironmentID
	}
	c.mu.RUnlock()
	c.record(event)
}
func (c *Client) loadCache() {
	if c.options.Cache == nil {
		return
	}
	cached, err := c.options.Cache.Load(c.ctx)
	if err != nil {
		c.reportError(fmt.Errorf("shipd: cache load: %w", err), true)
		return
	}
	if cached == nil || c.ctx.Err() != nil {
		return
	}
	s, err := parseSnapshot(cached.Snapshot)
	if err != nil {
		c.reportError(fmt.Errorf("shipd: cache load: %w", err), true)
		return
	}
	c.applySnapshot(s, cached.Snapshot, cached.ETag, c.ctx, false)
}
func (c *Client) applySnapshot(next *snapshot, data []byte, etag string, ctx context.Context, persist bool) {
	c.mu.Lock()
	if c.closed || (c.snapshot != nil && next.Version < c.snapshot.Version) {
		c.mu.Unlock()
		return
	}
	var previous *int64
	if c.snapshot != nil {
		v := c.snapshot.Version
		previous = &v
	}
	c.snapshot = next
	c.etag = etag
	savedETag := c.etag
	var listeners []func()
	if previous == nil || *previous != next.Version {
		listeners = c.changeLocked()
	}
	c.readyOnce.Do(func() { close(c.ready) })
	c.mu.Unlock()
	notifyListeners(listeners)
	if previous != nil && *previous != next.Version {
		c.record(map[string]any{"type": "version_skew", "previousVersion": *previous, "currentVersion": next.Version})
	}
	if persist && c.options.Cache != nil {
		if err := c.options.Cache.Save(ctx, CachedSnapshot{Snapshot: append([]byte(nil), data...), ETag: savedETag}); err != nil {
			c.reportError(fmt.Errorf("shipd: cache save: %w", err), true)
		}
	}
}

func telemetryTraits(attrs map[string]any) map[string]any {
	if attrs == nil {
		return nil
	}
	keys := make([]string, 0, len(attrs))
	for k := range attrs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	result := map[string]any{}
	for _, k := range keys {
		v, valid := telemetryTrait(attrs[k])
		if !boundedString(k, 128, true) || !valid {
			continue
		}
		if s, ok := v.(string); ok && !boundedString(s, 1024, false) {
			continue
		}
		result[k] = v
		if len(result) == 100 {
			break
		}
	}
	return result
}
func telemetryTrait(value any) (any, bool) {
	if value == nil {
		return nil, true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return v.String(), true
	case reflect.Bool:
		return v.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		n := v.Float()
		return n, !math.IsNaN(n) && !math.IsInf(n, 0)
	default:
		return nil, false
	}
}

func automaticTraits(seed map[string]string) map[string]any {
	result := map[string]any{}
	for _, k := range []string{"country", "region", "city", "timezone"} {
		v := strings.TrimSpace(seed[k])
		if v == "" {
			continue
		}
		switch k {
		case "country":
			if len(v) != 2 || !asciiLetter(v[0]) || !asciiLetter(v[1]) {
				continue
			}
			v = strings.ToUpper(v)
		case "region", "city":
			max := 64
			if k == "city" {
				max = 128
			}
			if !boundedString(v, max, false) || strings.ContainsFunc(v, func(r rune) bool { return r < 32 || r == 127 }) {
				continue
			}
		case "timezone":
			if !boundedString(v, 64, false) {
				continue
			}
			if _, err := time.LoadLocation(v); err != nil {
				continue
			}
		}
		result[k] = v
	}
	return result
}
func asciiLetter(b byte) bool { return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z' }
func (c *Client) applyAutomaticHeader(header string) {
	if !c.options.AutomaticTraits {
		return
	}
	seed := map[string]string{}
	if len(header) <= 1024 {
		if params, err := url.ParseQuery(header); err == nil {
			for k, values := range params {
				if len(values) > 0 {
					seed[k] = values[len(values)-1]
				}
			}
		}
	}
	incoming := automaticTraits(seed)
	for k, v := range automaticTraits(c.options.AutomaticTraitSeed) {
		incoming[k] = v
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	var listeners []func()
	if !reflect.DeepEqual(c.automatic, incoming) {
		c.automatic = incoming
		listeners = c.changeLocked()
	}
	c.mu.Unlock()
	notifyListeners(listeners)
}
