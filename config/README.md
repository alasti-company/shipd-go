# shipd Go config

Declare flags directly with a Go composite literal. This package is part of the
Go 1.23+ SDK module and does not depend on the runtime client.

```go
// shipd.config.go (use your application's package name)
package main

import "github.com/alasti-company/shipd-go/config"

var Config = config.ShipdConfig{
    "new-checkout": {
        Type:        "boolean",
        Default:     false,
        Description: "Redesigned checkout flow",
    },
    "search-variant": {
        Type:     "variant",
        Default:  "control",
        Variants: []string{"control", "semantic", "hybrid"},
    },
}
```

`ShipdConfig` is a named map of flag keys to `FlagDefinition` structs. Go requires
capitalized exported field names; their JSON names remain lowercase.
Definitions can include `Tags`, `Deprecated`, and `Disabled` as well.

## Install

The package ships with the Go SDK; one module install covers both:

```bash
go get github.com/alasti-company/shipd-go
```

## Sync and validate

Install the CLI with `npm install -g @shipdit/cli` (Node.js 22.12+), then run
`shipd login` to select your project and environment.

The CLI reads `shipd.config.go` directly, evaluates the exported `Config`
with Go, and selects Go accessor generation automatically. Run `shipd diff`,
`shipd push`, or `shipd types gen`; no JSON file or export command is needed.
`shipd pull` rewrites the Go definition file and regenerates accessors.

The definition file must be self-contained: imports and helper declarations in
that file work, but sibling Go files are not loaded. Do not define `main` there.

`Config.Validate()` checks keys, value types, variant defaults, JSON compatibility,
and the 500-flag limit. `Config.Defaults()` returns `(map[string]any, error)` for
runtime SDK defaults; returned values are copied and numbers become `float64`.
Do not mutate a config concurrently with these methods.

## Rules in code

Add `Targeting` to manage enabled state, ordered rules, and fallthrough for the
selected environment. `Traits` declares the accepted evaluation inputs. For
example, replace the earlier definition with:

```go
package main

import "github.com/alasti-company/shipd-go/config"

var Config = config.ShipdConfig{
    "new-checkout": {
        Type:    "boolean",
        Default: false,
        Traits: map[string]config.TraitDefinition{
            "plan": {Type: "string", Values: []string{"free", "pro"}, Required: true},
        },
        Targeting: &config.Targeting{
            Enabled: true,
            Rules: []config.Rule{{
                ID: "pro-customers",
                Conditions: []config.Condition{{
                    Kind: "attribute", Attribute: "plan", Operator: "eq", Value: "pro",
                }},
                Serve: config.Serve{Kind: "boolean", Value: true},
            }},
            Fallthrough: config.Serve{Kind: "boolean", Value: false},
        },
    },
}
```

Keep rule IDs stable when editing or reordering. The first matching rule wins.
A nil `Targeting` preserves dashboard rules and enabled state; an explicit empty
`Rules` slice clears rules. A nil `Traits` preserves the existing trait contract;
an empty trait map allows only built-in inputs such as `userId` and `country`.

Production targeting changes follow the dashboard draft and approval workflow.
Flag definitions and trait contracts update project-wide immediately. Pull reads
published targeting and replaces comments and helper code in the config file;
review the generated diff before committing.

See the [Go configuration reference](https://docs.useshipd.com/reference/config#go-shipdconfig),
[complete targeting examples](https://docs.useshipd.com/reference/config#targeting-in-code),
and [sync conflict guide](https://docs.useshipd.com/config-as-code/conflicts).
