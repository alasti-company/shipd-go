// Package openfeature provides local Shipd flag evaluation for the OpenFeature Go SDK.
package openfeature

import (
	"context"
	"strings"
	"sync"
	"time"

	shipd "github.com/alasti-company/shipd-go"
	of "github.com/open-feature/go-sdk/openfeature"
)

const providerName = "shipd"

// Provider owns one Shipd client and is safe for concurrent evaluations.
// Create it with NewProvider and let OpenFeature manage its lifecycle.
type Provider struct {
	options shipd.Options
	events  chan of.Event

	initOnce     sync.Once
	initErr      error
	shutdownOnce sync.Once
	shutdownErr  error

	mu          sync.RWMutex
	client      *shipd.Client
	unsubscribe func()
	initialized bool
	ready       bool
	closed      bool
	version     int64
}

var (
	_ of.FeatureProvider = (*Provider)(nil)
	_ of.StateHandler    = (*Provider)(nil)
	_ of.EventHandler    = (*Provider)(nil)
)

// NewProvider constructs a provider that starts its Shipd client on registration.
// OpenFeature supplies evaluation context and caller defaults, so Options.Defaults,
// Context, AutomaticTraits, and AutomaticTraitSeed are ignored.
func NewProvider(options shipd.Options) *Provider {
	options.Defaults = nil
	options.Context = shipd.EvaluationContext{}
	options.AutomaticTraits = false
	options.AutomaticTraitSeed = nil
	return &Provider{options: options, events: make(chan of.Event, 2)}
}

func (*Provider) Metadata() of.Metadata { return of.Metadata{Name: providerName} }
func (*Provider) Hooks() []of.Hook      { return nil }

func (p *Provider) Init(evalCtx of.EvaluationContext) error {
	return p.InitWithContext(context.Background(), evalCtx)
}

// InitWithContext supports OpenFeature SDK versions with context-aware registration.
// A failed initial fetch leaves polling and streaming available for recovery.
func (p *Provider) InitWithContext(ctx context.Context, _ of.EvaluationContext) error {
	p.initOnce.Do(func() { p.initErr = p.start(ctx) })
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return closedError()
	}
	return p.initErr
}

func (p *Provider) start(ctx context.Context) error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return closedError()
	}
	if !strings.HasPrefix(p.options.SDKKey, "shipd_sdk_server_") {
		p.mu.Unlock()
		return &of.ProviderInitError{ErrorCode: of.ProviderFatalCode, Message: "Shipd provider requires a server SDK key"}
	}
	client, err := shipd.New(p.options)
	if err != nil {
		p.mu.Unlock()
		return &of.ProviderInitError{ErrorCode: of.ProviderFatalCode, Message: err.Error()}
	}
	p.client = client
	p.unsubscribe = client.Subscribe(p.snapshotChanged)
	p.mu.Unlock()

	err = client.Ready(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return closedError()
	}
	p.initialized = true
	if version, ok := client.SnapshotVersion(); ok {
		p.version = version
		p.ready = true
		return nil
	}
	message := "Shipd initialization finished without a usable snapshot"
	if err != nil {
		message += ": " + err.Error()
	}
	return &of.ProviderInitError{ErrorCode: of.ProviderNotReadyCode, Message: message}
}

func closedError() error {
	return &of.ProviderInitError{ErrorCode: of.ProviderNotReadyCode, Message: "Shipd provider has been closed"}
}

// EventChannel reports readiness after recovery and accepted snapshot version
// changes. OpenFeature emits the initial readiness or error event from Init.
func (p *Provider) EventChannel() <-chan of.Event { return p.events }

func (p *Provider) snapshotChanged() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.initialized || p.closed {
		return
	}
	version, ok := p.client.SnapshotVersion()
	if !ok || (p.ready && p.version == version) {
		return
	}
	eventType := of.ProviderConfigChange
	if !p.ready {
		eventType = of.ProviderReady
	}
	p.ready, p.version = true, version
	// Coalesce bursts when a consumer falls behind. Snapshot refresh must
	// complete even if OpenFeature has yet to subscribe to the channel.
	select {
	case p.events <- of.Event{ProviderName: providerName, EventType: eventType}:
	default:
	}
}

func (p *Provider) Shutdown() {
	_ = p.ShutdownWithContext(context.Background())
}

// ShutdownWithContext flushes diagnostics for up to five seconds and stops
// updates. A shorter caller deadline takes precedence. A closed provider
// cannot be initialized again.
func (p *Provider) ShutdownWithContext(ctx context.Context) error {
	p.shutdownOnce.Do(func() {
		p.mu.Lock()
		p.closed = true
		client, unsubscribe := p.client, p.unsubscribe
		p.client, p.unsubscribe = nil, nil
		p.mu.Unlock()
		if client != nil {
			unsubscribe()
			ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()
			p.shutdownErr = client.Shutdown(ctx)
		}
	})
	return p.shutdownErr
}
