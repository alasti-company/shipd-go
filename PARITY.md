# TypeScript SDK parity

Baseline: npm `shipdit@0.6.0`, resolved from `shipdit@latest` on 2026-11-06.
The implementation was inspected alongside private repository
`alasti-company/useshipd.com` at main commit
`cd7bcad1f4bf032370f46d517c2188f7b204cc27`.

This Go module provides the server SDK corresponding to the npm root entry point
(`shipdit`). Browser and React entry points target different runtimes and are
outside the server feature parity claim. Passing tests establishes the cases
covered below, rather than proving equivalence for every possible input.

| TypeScript server capability | Go equivalent |
| --- | --- |
| Boolean, variant, number, JSON evaluation | `IsEnabled`, `GetVariant`, `GetNumber`, `GetJSON` |
| Local rules, disabled defaults, first match, fallthrough | `Evaluate` and typed getters |
| Attribute operators and coercion, dates, groups, audiences | Local evaluator and shared evaluation fixtures |
| Percentage and weighted rollouts, custom bucket attribute/seed | UTF-16 FNV-1a bucketing, including Unicode identities |
| Typed definitions, traits, targeting, validation | `config` package and CLI-generated Go accessors |
| `defaults`, `bootstrap` | `Defaults`, validated snapshot JSON in `Bootstrap` |
| Default context merged under per-call context | `Context`; `UserIDPresent` distinguishes empty from absent |
| ETag polling and monotonic snapshot versions | `RefreshInterval`, `DisablePolling`, `Refresh(ctx)` |
| Optional durable cache adapter | `SnapshotCache`, `CachedSnapshot` (JSON plus ETag) |
| Realtime stream with polling fallback | `Stream`, `Overrides.StreamEndpoint` |
| Ticket auth, expiry, reconnect jitter/auth backoff | Short-lived HTTP tickets; ticket renewal on auth failure |
| Stream version gaps, resync, delayed edge visibility | Full snapshot refresh; up to five bounded lag attempts |
| Request timeout and cancellation | `FetchTimeout`, `DisableFetchTimeout`, caller/lifecycle contexts |
| Readiness timeout | `ReadyTimeout`, plus caller context |
| Background error callback | `OnError`, plus `LastError` |
| Snapshot version, store revision, subscription | `SnapshotVersion`, `Revision`, `Subscribe` |
| Server identify without global identity changes | `Identify`; server `Reset` does nothing |
| Anonymous correlation | `AnonymousID`, `Identity.AnonymousID` |
| Opt-in automatic location traits | `AutomaticTraits`, `AutomaticTraitSeed`; edge < seed < default context < per-call context |
| Evaluation, identify, error, unregistered, version, timing events | Bounded event buffer, retry, payload splitting, `DisableEvents` |
| Compatibility and correlation headers | SDK/schema/stream/API versions and matching request/correlation IDs |
| Graceful close with event delivery | `Shutdown(ctx)`; immediate `Close()` also available |

## Go conventions and deliberate differences

- Go durations use zero for defaults. Use `DisablePolling`, `DisableFetchTimeout`,
  negative `FlushInterval`, and negative `ReadyTimeout` for TypeScript's respective
  zero/disabled options. The default readiness bound is five seconds.
- `Ready(ctx)` returns initial-fetch and timeout errors so Go callers can log
  them. TypeScript's `ready()` resolves without rejecting. Both retain defaults
  and continue polling after failure.
- `Shutdown(ctx)` returns delivery errors and uses the caller's deadline.
  TypeScript `close()` suppresses errors and defaults to a five-second deadline.
  Go's `Close()` stops immediately without delivering events.
- Go validates bootstrap/cache snapshots, identify payloads and configuration
  before using them. JSON results are detached copies. TypeScript's typed API can
  accept some invalid runtime input without immediate validation.
- JSON decoded numbers use `float64`; generated Go accessors supply compile-time
  flag/trait types in place of TypeScript's generic and declaration-merging APIs.
- Go listeners can evaluate and unsubscribe, but must not call blocking lifecycle
  methods. Callbacks run outside the state mutex; panics are contained.
- Cache `clear()` is optional in TypeScript and is never called by its runtime.
  Go adapters can expose their own clear method outside `SnapshotCache`.
- The browser-only options `remoteTraits`, `onAnonymousId`, browser anonymous ID
  persistence/rotation, localStorage adapters, remote value evaluation, and
  `shipdit/client/react` providers/hooks have no Go server counterpart. The server
  does not identify a mutable global user or download remote identity overlays.
- The Go package version tracks its own releases, independently of npm's version.
- Go keeps stricter telemetry payload handling: oversized evaluation values can
  be omitted while their attribution is retained; invalid identify payloads return
  errors. With events enabled, evaluation attributes now appear in telemetry,
  matching TypeScript's current behavior.

## Verification

`go test -race ./...` runs the shared evaluation/config validation fixtures and
13,804 typed-getter comparisons generated by the **published npm package**.
The compressed `testdata/published-sdk.json.gz` contains the source snapshot,
contexts, and expected values. It covers shared contexts and 256 deterministic
Unicode identities across every flag in the shared snapshot, plus missing-key
fallbacks. Go tests consume this fixture without Node or network access.

Runtime tests exercise cache loading/validation/persistence, bootstrap precedence,
context merge and telemetry, callbacks and subscriptions, readiness deadlines,
stream subscription/version filtering/lag retries, reconnect ticket reuse,
credential redaction, and shutdown. Existing tests cover snapshot rejection,
request cancellation, ETags, event buffering, failed delivery, and bounded drains.

To regenerate the published-package oracle from the repository root:

```sh
parity_dir=$(mktemp -d)
npm install --prefix "$parity_dir" --ignore-scripts --no-audit --no-fund shipdit@0.6.0
SHIPDIT_MODULE="$parity_dir/node_modules/shipdit/dist/index.mjs" node scripts/generate-parity.mjs
go test -race ./...
```

The generator checks `SDK_PACKAGE_VERSION` before writing the fixture. npm's
published tarball integrity for this baseline is
`sha512-yYqMnUN6kfTQecUR6pJW6UJSuaK0ajNcktJVEsCivItZNP4BT2g8UYjrDrU3EZsPyiVc3KmUGAq0S1x4cTWF/w==`.
When npm latest changes, review its public options, implementation, protocol and
config changes, then update the pinned baseline and regenerate the oracle.
