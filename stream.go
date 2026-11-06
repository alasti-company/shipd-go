package shipd

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func streamURL(endpoint, ticket string) (string, error) {
	if endpoint == "" {
		endpoint = DefaultStreamEndpoint
	}
	endpoint = strings.TrimRight(jsTrim(endpoint), "/")
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || strings.ContainsAny(endpoint, "?#") {
		return "", errors.New("shipd: invalid Overrides.StreamEndpoint")
	}
	switch u.Scheme {
	case "http":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
	default:
		return "", errors.New("shipd: StreamEndpoint requires HTTP(S) or WS(S)")
	}
	u.Path += "/sdk/v1/stream"
	q := u.Query()
	q.Set("ticket", ticket)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

type streamTicket struct {
	Ticket     string   `json:"ticket"`
	ExpiresAt  string   `json:"expiresAt"`
	TTLSeconds *float64 `json:"ttlSeconds"`
}

func (c *Client) mintTicket() (string, time.Time, error) {
	response, err := c.request(c.ctx, http.MethodPost, "/sdk/v1/stream-ticket", nil)
	if err != nil {
		return "", time.Time{}, errors.New("shipd: stream ticket request failed")
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		return "", time.Time{}, fmt.Errorf("shipd: stream ticket: HTTP %d", response.StatusCode)
	}
	var body streamTicket
	if err := json.NewDecoder(io.LimitReader(response.Body, 64*1024)).Decode(&body); err != nil || body.Ticket == "" {
		return "", time.Time{}, errors.New("shipd: invalid stream ticket response")
	}
	expiry, err := time.Parse(time.RFC3339Nano, body.ExpiresAt)
	if body.ExpiresAt == "" && body.TTLSeconds != nil {
		expiry = time.Now().Add(time.Duration(*body.TTLSeconds * float64(time.Second)))
		err = nil
	}
	if err != nil {
		return "", time.Time{}, errors.New("shipd: stream ticket missing expiry")
	}
	return body.Ticket, expiry, nil
}
func waitContext(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// websocket.Dial requires *http.Client. Preserve arbitrary configured Do
// implementations through a transport adapter instead of bypassing them.
type streamTransport struct{ client HTTPClient }

func (t streamTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	return t.client.Do(request)
}
func (c *Client) streamHTTPClient() *http.Client {
	if client, ok := c.http.(*http.Client); ok {
		return client
	}
	return &http.Client{Transport: streamTransport{client: c.http}, CheckRedirect: func(_ *http.Request, _ []*http.Request) error { return http.ErrUseLastResponse }}
}

func (c *Client) dialStream(ticket string) (*websocket.Conn, bool, error) {
	endpoint, err := streamURL(c.options.Overrides.StreamEndpoint, ticket)
	if err != nil {
		return nil, false, err
	}
	ctx, cancel := c.requestContext(c.ctx)
	defer cancel()
	conn, response, err := websocket.Dial(ctx, endpoint, &websocket.DialOptions{HTTPClient: c.streamHTTPClient()})
	if err != nil {
		rejected := response != nil && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden)
		// Dial errors can contain the ticket URL. Never expose that credential.
		return nil, rejected, errors.New("shipd: stream connection failed")
	}
	return conn, false, nil
}

func (c *Client) runStream() {
	defer close(c.streamDone)
	attempt := 0
	ticket := ""
	var expiry time.Time
	terminalAuth := false
	for c.ctx.Err() == nil {
		var err error
		if ticket == "" || terminalAuth || time.Until(expiry) <= 30*time.Second {
			ticket, expiry, err = c.mintTicket()
		}
		if err == nil && c.ctx.Err() == nil {
			conn, rejected, dialErr := c.dialStream(ticket)
			if dialErr != nil {
				err = dialErr
				if rejected {
					terminalAuth = true
					ticket = ""
				}
			} else {
				err = c.readStream(conn, func() { attempt = 0; terminalAuth = false })
				var closeErr websocket.CloseError
				if errors.As(err, &closeErr) {
					text := strings.ToLower(closeErr.Reason)
					terminalAuth = strings.Contains(text, "unauthenticated") || strings.Contains(text, "authentication failed")
				}
				conn.CloseNow()
			}
		}
		if c.ctx.Err() != nil {
			return
		}
		c.reportError(err, true)
		exponent := attempt
		if terminalAuth && exponent < 5 {
			exponent = 5
		}
		if exponent > 6 {
			exponent = 6
		}
		capDelay := time.Second * time.Duration(1<<exponent)
		if capDelay > time.Minute {
			capDelay = time.Minute
		}
		delay := time.Duration(float64(capDelay) * (0.5 + rand.Float64()*0.5))
		attempt++
		c.recordTiming("ws_reconnect", delay, nil)
		if !waitContext(c.ctx, delay) {
			return
		}
	}
}
func validVersion(v *int64) bool { return v != nil && *v >= 0 && *v <= maxSafeVersion }
func (c *Client) readStream(conn *websocket.Conn, connected func()) error {
	conn.SetReadLimit(64 * 1024)
	for {
		_, data, err := conn.Read(c.ctx)
		if err != nil {
			return err
		}
		var frame struct {
			ProtocolVersion int    `json:"protocolVersion"`
			Type            string `json:"type"`
			ConnectionID    string `json:"connectionId"`
			EnvironmentID   string `json:"environmentId"`
			SnapshotVersion *int64 `json:"snapshotVersion"`
			Version         *int64 `json:"version"`
			Reason          string `json:"reason"`
			Code            string `json:"code"`
			Message         string `json:"message"`
			CommittedAt     *int64 `json:"committedAtMs"`
			VisibleAt       *int64 `json:"visibleAtMs"`
			NotifiedAt      *int64 `json:"notifiedAtMs"`
		}
		if err := json.Unmarshal(data, &frame); err != nil || frame.ProtocolVersion != StreamProtocolVersion {
			return errors.New("shipd: malformed or unsupported stream frame")
		}
		var minimum *int64
		switch frame.Type {
		case "connected":
			if frame.ConnectionID == "" || frame.EnvironmentID == "" || !validVersion(frame.SnapshotVersion) {
				return errors.New("shipd: malformed connected frame")
			}
			connected()
			subscribe := map[string]any{"protocolVersion": StreamProtocolVersion, "type": "subscribe", "environmentId": frame.EnvironmentID}
			if v, ok := c.SnapshotVersion(); ok {
				subscribe["lastKnownVersion"] = v
			}
			ctx, cancel := c.requestContext(c.ctx)
			err := wsjson.Write(ctx, conn, subscribe)
			cancel()
			if err != nil {
				return errors.New("shipd: stream subscribe failed")
			}
			minimum = frame.SnapshotVersion
		case "snapshot.updated":
			if frame.EnvironmentID == "" || !validVersion(frame.Version) {
				return errors.New("shipd: malformed update frame")
			}
			minimum = frame.Version
		case "snapshot.resync":
			if frame.Reason != "version_gap" && frame.Reason != "server_request" {
				return errors.New("shipd: malformed resync frame")
			}
		case "pong":
			continue
		case "error":
			if frame.Code == "" || frame.Message == "" {
				return errors.New("shipd: malformed error frame")
			}
			if frame.Code == "unauthenticated" {
				return websocket.CloseError{Code: websocket.StatusPolicyViolation, Reason: "unauthenticated"}
			}
			if frame.Code == "unsupported_protocol" {
				return errors.New("shipd: unsupported stream protocol")
			}
			c.reportError(fmt.Errorf("shipd stream: %s: %s", frame.Code, frame.Message), true)
			continue
		default:
			return errors.New("shipd: unknown stream frame")
		}
		c.mu.RLock()
		environment := ""
		if c.snapshot != nil {
			environment = c.snapshot.EnvironmentID
		}
		c.mu.RUnlock()
		if frame.EnvironmentID != "" && environment != "" && frame.EnvironmentID != environment {
			continue
		}
		if v, ok := c.SnapshotVersion(); minimum != nil && ok && *minimum <= v {
			continue
		}
		received := time.Now()
		if validVersion(frame.NotifiedAt) {
			c.recordTiming("notified_to_delivered", received.Sub(time.UnixMilli(*frame.NotifiedAt)), minimumValue(minimum))
		}
		if validVersion(frame.VisibleAt) {
			end := received
			if validVersion(frame.NotifiedAt) {
				end = time.UnixMilli(*frame.NotifiedAt)
			}
			c.recordTiming("visible_to_notified", end.Sub(time.UnixMilli(*frame.VisibleAt)), minimumValue(minimum))
		}
		if validVersion(frame.CommittedAt) && validVersion(frame.VisibleAt) {
			c.recordTiming("commit_to_visible", time.Duration(*frame.VisibleAt-*frame.CommittedAt)*time.Millisecond, minimumValue(minimum))
		}
		c.refreshMinimum(minimum)
		c.recordTiming("delivered_to_fetched", time.Since(received), nil)
		if validVersion(frame.CommittedAt) {
			c.recordTiming("commit_to_fetched", time.Since(time.UnixMilli(*frame.CommittedAt)), nil)
		}
	}
}
func minimumValue(v *int64) any {
	if v == nil {
		return nil
	}
	return *v
}
func (c *Client) refreshMinimum(minimum *int64) {
	for attempt := 0; attempt < 5 && c.ctx.Err() == nil; attempt++ {
		_ = c.Refresh(c.ctx)
		v, ok := c.SnapshotVersion()
		if minimum == nil || (ok && v >= *minimum) {
			return
		}
		if attempt < 4 {
			capDelay := 50 * time.Millisecond * time.Duration(1<<attempt)
			if capDelay > 200*time.Millisecond {
				capDelay = 200 * time.Millisecond
			}
			if !waitContext(c.ctx, time.Duration(float64(capDelay)*(0.5+rand.Float64()*0.5))) {
				return
			}
		}
	}
	if c.ctx.Err() == nil {
		c.reportError(fmt.Errorf("shipd: snapshot still below notified version %d after 5 attempts", *minimum), true)
	}
}
