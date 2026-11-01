// Package shipd evaluates shipd flags locally using periodically refreshed snapshots.
package shipd

import "time"

const (
	SnapshotSchemaVersion = 1
	// DefaultEndpoint is the edge origin used unless Overrides.UnsafeEndpoint is set.
	DefaultEndpoint = "https://edge.useshipd.com"
	SDKVersion      = "0.1.0"
	maxSafeVersion  = 9007199254740991
)

// EvaluationContext is request-scoped. Attributes should contain strings, numbers,
// booleans, or nil. The client never changes the supplied context.
type EvaluationContext struct {
	UserID string
	// UserIDPresent distinguishes an explicitly empty UserID from an omitted one.
	// Nonempty UserID values are always present. Empty IDs do not provide a bucket.
	UserIDPresent bool
	Attributes    map[string]any
	SegmentIDs    []string
}

// Options configures one client per process. Zero durations select the defaults.
// Set DisablePolling to fetch only the initial snapshot (or refresh manually).
type Options struct {
	SDKKey          string
	Defaults        map[string]any
	RefreshInterval time.Duration
	FetchTimeout    time.Duration
	DisablePolling  bool
	DisableEvents   bool
	// FlushAt flushes events once this many are buffered (default 50, at most 200).
	// A negative value flushes on FlushInterval only.
	FlushAt int
	// FlushInterval flushes buffered events periodically (default 5 seconds).
	// A negative value disables timed flushes; events then flush at FlushAt, on
	// Flush, or on Shutdown.
	FlushInterval time.Duration
	// Bootstrap is a snapshot (the JSON body of GET /sdk/v1/snapshot) served until
	// a newer one is fetched. It makes the client ready immediately; use it for
	// tests and offline starts. Invalid snapshots fail New.
	Bootstrap []byte
	// Overrides changes where the client connects. Production apps should not set it.
	Overrides Overrides
	// HTTPClient may supply a custom transport. Every request also has FetchTimeout.
	HTTPClient HTTPClient
}

// Overrides changes where the client connects.
type Overrides struct {
	// UnsafeEndpoint replaces the edge origin (https://edge.useshipd.com) that
	// receives your SDK key. Unsafe: for shipd development and tests only.
	UnsafeEndpoint string
}

// Result describes a local evaluation. Value is a detached JSON-compatible value.
type Result struct {
	Key             string
	Value           any
	Reason          string
	RuleID          string
	SnapshotVersion *int64
}

type snapshot struct {
	SchemaVersion  int                `json:"schemaVersion"`
	ProjectID      string             `json:"projectId"`
	EnvironmentID  string             `json:"environmentId"`
	EnvironmentKey string             `json:"environmentKey"`
	Version        int64              `json:"version"`
	GeneratedAt    string             `json:"generatedAt"`
	Flags          map[string]flag    `json:"flags"`
	Segments       map[string]segment `json:"segments"`
}
type flag struct {
	Key         string   `json:"key"`
	Type        string   `json:"type"`
	Enabled     bool     `json:"enabled"`
	Default     any      `json:"default"`
	Variants    []string `json:"variants"`
	Rules       []rule   `json:"rules"`
	Fallthrough serve    `json:"fallthrough"`
	Version     int64    `json:"version"`
}
type rule struct {
	ID         string      `json:"id"`
	Conditions []condition `json:"conditions"`
	Serve      serve       `json:"serve"`
}
type segment struct {
	ID         string      `json:"id"`
	Conditions []condition `json:"conditions"`
}
type condition struct {
	Kind       string      `json:"kind"`
	Attribute  string      `json:"attribute"`
	Operator   string      `json:"operator"`
	Value      any         `json:"value"`
	Rollout    float64     `json:"rollout"`
	Seed       string      `json:"seed"`
	SegmentID  string      `json:"segmentId"`
	Op         string      `json:"op"`
	Conditions []condition `json:"conditions"`
}
type serve struct {
	Kind       string      `json:"kind"`
	Value      any         `json:"value"`
	Seed       string      `json:"seed"`
	Attribute  string      `json:"attribute"`
	Variations []variation `json:"variations"`
}
type variation struct {
	Weight float64 `json:"weight"`
	Serve  serve   `json:"serve"`
}
