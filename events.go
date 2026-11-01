package shipd

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"time"
)

// Identity is sent to shipd for audience insights. It does not set a global
// evaluation context; server applications pass context separately per request.
type Identity struct {
	UserID string
	Name   string
	Traits map[string]any
}

// Identify queues an identify event. Flush delivers it. Traits must be scalar
// JSON values or nil, matching the edge identify contract.
func (c *Client) Identify(identity Identity) error {
	switch {
	case identity.UserID == "":
		return errors.New("shipd: identify requires UserID")
	case !boundedString(identity.UserID, 256, true):
		return errors.New("shipd: identify UserID exceeds 256 characters")
	case !boundedString(identity.Name, 256, false):
		return errors.New("shipd: identify Name exceeds 256 characters")
	case len(identity.Traits) > 100:
		return errors.New("shipd: identify accepts at most 100 traits")
	}
	traits := make(map[string]any, len(identity.Traits))
	for key, v := range identity.Traits {
		normalized := cloneJSON(v)
		if !boundedString(key, 128, true) || (v != nil && (normalized == nil || !trait(normalized))) {
			return fmt.Errorf("shipd: trait %q must be a scalar JSON value", key)
		}
		if value, ok := normalized.(string); ok && !boundedString(value, 1024, false) {
			return fmt.Errorf("shipd: trait %q exceeds 1024 characters", key)
		}
		traits[key] = normalized
	}
	c.mu.RLock()
	closed := c.closed || c.shuttingDown
	var version *int64
	if c.snapshot != nil {
		v := c.snapshot.Version
		version = &v
	}
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	event := map[string]any{"type": "identify", "userId": identity.UserID, "name": identity.Name, "traits": traits, "snapshotVersion": version}
	if data, _ := json.Marshal(event); len(data) > maxEventBytes {
		return fmt.Errorf("shipd: identify event exceeds payload limit")
	}
	c.record(event)
	return nil
}
func (c *Client) record(event map[string]any) {
	if c.options.DisableEvents {
		return
	}
	event["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	// Bound individual events so a large JSON flag cannot poison a batch. Scalar
	// evaluation events are already bounded by the key, user ID, and rule ID
	// checks, so only events with string, object, or array payloads are measured.
	if needsSizeCheck(event) {
		data, err := json.Marshal(event)
		if err != nil {
			return
		}
		if len(data) > maxEventBytes {
			if event["type"] != "evaluation" {
				return
			}
			delete(event, "value")
			if data, err = json.Marshal(event); err != nil || len(data) > maxEventBytes {
				return
			}
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || c.shuttingDown {
		return
	}
	c.events = append(c.events, event)
	c.trimEvents()
	// After a failed flush, retry on the interval only so a down edge is not hammered.
	if c.options.FlushAt > 0 && len(c.events) >= c.options.FlushAt && !c.flushFailing {
		select {
		case c.flushNow <- struct{}{}:
		default:
		}
	}
}

// trimEvents drops the oldest events beyond the buffer limit. Callers hold mu.
func (c *Client) trimEvents() {
	if over := len(c.events) - maxBufferedEvents; over > 0 {
		c.dropped += uint64(over)
		c.events = c.events[over:]
	}
}

// Flush sends pending events. Failed batches are retained for a later retry;
// the buffer retains at most 200 events, dropping the oldest on overflow
// (see DroppedEvents). The client also flushes once FlushAt events are buffered.
func (c *Client) Flush(ctx context.Context) (err error) {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	select {
	case c.flushGate <- struct{}{}:
		defer func() { <-c.flushGate }()
	case <-ctx.Done():
		return ctx.Err()
	case <-c.ctx.Done():
		return ErrClosed
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	if err = ctx.Err(); err != nil {
		c.mu.Unlock()
		return err
	}
	events := c.events
	c.events = nil
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		c.lastError = err
		c.flushFailing = err != nil
		if err != nil {
			c.events = append(events, c.events...)
			c.trimEvents()
		}
	}()
	if c.options.DisableEvents || len(events) == 0 {
		return nil
	}
	// Encode each event once; batches are assembled from the encoded bytes.
	encoded := make([]json.RawMessage, len(events))
	for i, event := range events {
		if encoded[i], err = json.Marshal(event); err != nil {
			return err
		}
	}
	// The edge accepts at most 100 events and 256 KiB per request. Only
	// requeue unsent batches if a later request fails.
	for len(events) > 0 {
		n, size := 0, 128 // reserve envelope/timestamp/comma bytes
		for n < len(encoded) && n < 100 && size+len(encoded[n])+1 <= 256*1024 {
			size += len(encoded[n]) + 1
			n++
		}
		if n == 0 {
			return errors.New("shipd: event exceeds batch payload limit")
		}
		if err = c.sendEvents(ctx, batchBody(encoded[:n])); err != nil {
			return err
		}
		events, encoded = events[n:], encoded[n:]
	}
	return nil
}

// batchBody builds {"sentAt": ..., "events": [...]} from pre-encoded events.
func batchBody(events []json.RawMessage) []byte {
	var b bytes.Buffer
	b.WriteString(`{"sentAt":"`)
	b.WriteString(time.Now().UTC().Format(time.RFC3339Nano))
	b.WriteString(`","events":[`)
	for i, event := range events {
		if i > 0 {
			b.WriteByte(',')
		}
		b.Write(event)
	}
	b.WriteString("]}")
	return b.Bytes()
}

func needsSizeCheck(event map[string]any) bool {
	if event["type"] != "evaluation" {
		return event["type"] != "unregistered_flag"
	}
	switch value := event["value"].(type) {
	case string:
		// JSON escaping grows a byte to at most 6 ("\u00XX"), and the rest of the
		// event is bounded by the 256-character key, user ID, and rule ID checks.
		return 6*len(value)+eventOverheadBytes > maxEventBytes
	case map[string]any, []any:
		return true
	}
	return false
}

const (
	// eventOverheadBytes bounds an evaluation event apart from its value.
	eventOverheadBytes = 16 * 1024
	maxEventBytes      = 256*1024 - 128 - 1 // Reserve the per-event separator too.
	maxBufferedEvents  = 200
)

func (c *Client) sendEvents(ctx context.Context, data []byte) error {
	sendCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(c.ctx, cancel)
	defer stop()
	response, err := c.request(sendCtx, http.MethodPost, "/sdk/v1/events", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("shipd: events flush: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("shipd: events flush: HTTP %d", response.StatusCode)
	}
	return nil
}

func boundedString(s string, max int, nonempty bool) bool {
	if nonempty && s == "" {
		return false
	}
	n := 0
	for _, r := range s {
		n++
		if r > 0xffff {
			n++
		}
		if n > max {
			return false
		}
	}
	return n <= max
}
