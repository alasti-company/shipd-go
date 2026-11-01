# shipd Go SDK

A Go 1.23+ server SDK with no external dependencies. Evaluates flags locally from
an environment snapshot. One client is safe to share across goroutines; pass an
evaluation context for each request.

## Install

```sh
go get github.com/alasti-company/shipd-go
```

## Use

```go
package main

import (
    "context"
    "log"
    "os"
    "time"

    shipd "github.com/alasti-company/shipd-go"
)

func main() {
    client, err := shipd.New(shipd.Options{
        SDKKey: os.Getenv("SHIPD_SDK_KEY"),
        Defaults: map[string]any{
            "new-checkout":     false,
            "search-variant":   "control",
            "max-retries":      3,
            "checkout-payload": map[string]any{"mode": "strict"},
        },
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Shutdown(context.Background())

    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
    err = client.Ready(ctx)
    cancel()
    if err != nil {
        log.Printf("Using flag defaults while the SDK retries: %v", err)
    }

    user := shipd.EvaluationContext{
        UserID:     "usr_8f2k19",
        Attributes: map[string]any{"plan": "pro"},
    }
    log.Println("New checkout:", client.IsEnabled("new-checkout", user))
    log.Println("Search variant:", client.GetVariant("search-variant", user))
    log.Println("Max retries:", client.GetNumber("max-retries", user))
    log.Println("Checkout payload:", client.GetJSON("checkout-payload", user))
}
```

## Behavior

- Initial asynchronous `GET /sdk/v1/snapshot`; polls every 30 seconds using ETags.
- Evaluation makes no network calls. Failed requests keep the last valid snapshot.
- Before the first snapshot, or for an unknown flag, configured defaults apply.
  Built-in defaults are `false`, `"control"`, `0`, and an empty JSON object.
- Disabled flags use the snapshot's default. Weighted rules without a bucket
  identity are skipped. Weighted fallthrough without an identity uses the snapshot default.
- Supports all attribute operators, percentage rollouts, AND/OR groups,
  compiled audiences, explicit audience IDs, and weighted variations.
- FNV-1a bucketing uses UTF-16 code units, matching TypeScript rollout cohorts.
- Malformed snapshots and unsupported schema versions are rejected as a whole.
  Delayed responses never replace a snapshot with a lower version.
- JSON results are detached from client state; decoded numbers are `float64`.
- This release uses polling. It has no WebSocket stream, disk cache, remote identity overlay lookup. Supply traits per request.

`Evaluate` returns `Result` with `Value`, `Reason`, `RuleID`, and
`SnapshotVersion`. Evaluation continues to work after `Close` using the last
snapshot/defaults. `LastError` exposes the most recent request error; a successful
refresh or flush clears it. `Ready(ctx)` returns `nil` once a snapshot (or
`Bootstrap`) loads, or the initial fetch error as soon as that attempt fails;
polling keeps trying unless disabled.

## Options

| Option            | Default / behavior                                                 |
| ----------------- | ------------------------------------------------------------------ |
| `SDKKey`          | Required server SDK key                                            |
| `Defaults`        | JSON-compatible values by flag key; copied on construction         |
| `RefreshInterval` | `30 * time.Second`; zero selects default                           |
| `FetchTimeout`    | `10 * time.Second`; bounds the whole request, including body reads |
| `DisablePolling`  | `false`; initial fetch still runs; use `Refresh(ctx)` manually     |
| `DisableEvents`   | `false`; disables evaluation and identify telemetry                |
| `FlushAt`         | `50`; flush once this many events are buffered (max 200, <0 off)   |
| `FlushInterval`   | `5 * time.Second`; periodic event flush; negative disables it      |
| `Bootstrap`       | Snapshot JSON served until a newer fetch; client is ready at once  |
| `Overrides`       | `UnsafeEndpoint` replaces the edge; dev/test only (gets SDK key)   |
| `HTTPClient`      | Optional custom `Do(*http.Request)` implementation                 |

The default HTTP client rejects redirects. Custom HTTP clients control their own
redirect behavior. No automatic geographic traits are inferred for server keys.

## Events and shutdown

Events flush every `FlushInterval` (five seconds) and as soon as `FlushAt` (50)
events are buffered. The buffer retains at most 200 entries, dropping oldest
entries on overflow (`DroppedEvents` counts them); failed batches are retried on
the next interval. `SnapshotVersion` reports the loaded version without recording
an event.
Events carry flag values, user IDs, reasons, and snapshot versions. Evaluation
attributes are never included. Set `DisableEvents: true` to disable telemetry.

`Identify` queues identity traits for shipd audience insights; it does not change
the context used for evaluation:

```go
err := client.Identify(shipd.Identity{
    UserID: "usr_8f2k19",
    Traits: map[string]any{"plan": "pro"},
})
```

Traits must be strings, finite numbers, booleans, or nil. Identify supports at
most 100 traits and follows the edge's identifier and string length limits.

Stop accepting requests, then call `Shutdown` with a bounded context. It
delivers pending events and closes the client, and returns the flush error if
delivery fails:

```go
ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
err := client.Shutdown(ctx)
cancel()
```

Events recorded after `Shutdown` starts are not delivered. `Close` stops immediately without delivering events. Both are idempotent, cancel
in-flight requests, and wait for background work.

Snapshot errors can be matched with `errors.Is`: `ErrMalformedSnapshot` and
`ErrUnsupportedSchema`. `ErrClosed` reports use after close. A custom HTTP client must honor request
context cancellation for shutdown and request timeouts to work.

Evaluation values that exceed the event payload limit are omitted from telemetry. Identify events that exceed the payload limit return a validation error. Flush splits batches to respect the edge limit of 100 events and 256 KiB per request.

## Definitions and generated accessors

Author definitions with `config.ShipdConfig` from `github.com/alasti-company/shipd-go/config` in `shipd.config.go`. The shared CLI reads this Go source directly and automatically generates Go accessors after push and pull. Run `shipd types gen` to regenerate locally.

Install the CLI with `npm install -g @shipdit/cli` (Node.js 22.12+), then run
`shipd login`, `shipd diff`, and `shipd push`. Your Go application does not need
Node.js at runtime. Validate definitions in your own tests with `Config.Validate()`.

### Rules in code

Use `FlagDefinition.Targeting` to declare an environment's enabled state, ordered
rules, and fallthrough. A nil `Targeting` preserves dashboard rules and enabled
state; an explicit empty `Rules` slice clears rules. Optional
`FlagDefinition.Traits` declares this flag's accepted evaluation inputs.

Run `shipd diff` before `shipd push` to review changes. Production targeting
changes follow the dashboard draft and approval workflow; flag definitions and
trait contracts update project-wide immediately. Use `shipd pull` to download
published dashboard state. Pull regenerates the source file, replacing comments
and helper code, so review the diff before committing.

Generated accessors include per-flag trait structs. Their fields are pointers so
that zero values differ from missing inputs. `Context(userID)` returns an error
when required fields are nil; it preserves explicit `false`, `0`, and empty strings.

See the [Go SDK guide](https://docs.useshipd.com/sdks/go),
[configuration examples](https://docs.useshipd.com/reference/config#targeting-in-code),
and [sync conflict guide](https://docs.useshipd.com/config-as-code/conflicts).

## Development and releases

Run the checks from this repository's root:

```sh
go vet ./...
go test -race ./...
(cd examples/config && go test -race ./...)
go test -run='^$' -bench='^BenchmarkFlagEvaluation$' -benchtime=1x
```

The SDK is released independently with semantic version tags at the repository
root. After committing and pushing a release, publish its tag (for example):

```sh
git tag v0.1.0
git push origin v0.1.0
GOPROXY=https://proxy.golang.org go list -m github.com/alasti-company/shipd-go@v0.1.0
```

Published tags are immutable; release fixes under a new version.
