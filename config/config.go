// Package config defines shipd flags in Go. It is part of the Go SDK module and
// does not depend on the runtime client.
package config

import (
	"encoding/json"
	"fmt"
	"strings"
)

// FlagDefinition describes a project-wide flag and its default in the target
// environment. Optional Targeting controls the complete selected environment state.
type FlagDefinition struct {
	Traits      map[string]TraitDefinition `json:"traits,omitempty"`
	Targeting   *Targeting                 `json:"targeting,omitempty"`
	Type        string                     `json:"type"`
	Default     any                        `json:"default"`
	Variants    []string                   `json:"variants,omitempty"`
	Description string                     `json:"description,omitempty"`
	Tags        []string                   `json:"tags"`
	Deprecated  bool                       `json:"-"`
	// Disabled preserves an intentionally disabled environment fallthrough on pull.
	Disabled bool `json:"disabled,omitempty"`
}

// ShipdConfig maps flag keys to their definitions. Declare it with a Go composite
// literal; validation occurs when loading definitions or extracting defaults.
//
// Config values are mutable. Do not mutate a config concurrently with its methods.
type ShipdConfig map[string]FlagDefinition

// normalized validates and copies the definitions into the CLI document format.
func (c ShipdConfig) normalized() (map[string]map[string]any, error) {
	definitions := c
	if len(definitions) > 500 {
		return nil, fmt.Errorf("shipd: at most 500 flag definitions are supported")
	}
	flags := make(map[string]map[string]any, len(definitions))
	for key, definition := range definitions {
		if strings.TrimSpace(key) == "" || key == "__proto__" {
			return nil, fmt.Errorf("shipd: invalid or reserved flag key %q", key)
		}
		encoded, err := json.Marshal(definition)
		if err != nil {
			return nil, fmt.Errorf("shipd: flag %q must contain JSON-compatible values: %w", key, err)
		}
		var f map[string]any
		if err := json.Unmarshal(encoded, &f); err != nil {
			return nil, err
		}
		if !validValue(definition.Type, f["default"]) {
			return nil, fmt.Errorf("shipd: flag %q has an invalid %s default", key, definition.Type)
		}
		if definition.Type == "variant" {
			found := false
			for _, v := range definition.Variants {
				if v == f["default"] {
					found = true
				}
			}
			if len(definition.Variants) == 0 || !found {
				return nil, fmt.Errorf("shipd: flag %q default must be one of its variants", key)
			}
		}
		for name, trait := range definition.Traits {
			switch name {
			case "userId", "country", "region", "city", "timezone":
				if trait.Type != "string" {
					return nil, fmt.Errorf("shipd: built-in trait %q must be a string", name)
				}
			}

			if strings.TrimSpace(name) == "" || name == "__proto__" || (trait.Type != "string" && trait.Type != "number" && trait.Type != "boolean") || (trait.Values != nil && (trait.Type != "string" || len(trait.Values) == 0)) {
				return nil, fmt.Errorf("shipd: invalid trait %q", name)
			}
		}
		if definition.Traits != nil {
			f["traits"] = definition.Traits
		}
		if definition.Targeting != nil {
			var normalized FlagDefinition
			if err := json.Unmarshal(encoded, &normalized); err != nil {
				return nil, err
			}
			normalized.Traits = definition.Traits
			if err := normalized.Targeting.validate(normalized); err != nil {
				return nil, fmt.Errorf("shipd: flag %q targeting: %w", key, err)
			}
			// Nil slices must encode as arrays for the language-neutral contract.
			if normalized.Targeting.Rules == nil {
				normalized.Targeting.Rules = []Rule{}
			}
			for i := range normalized.Targeting.Rules {
				if normalized.Targeting.Rules[i].Conditions == nil {
					normalized.Targeting.Rules[i].Conditions = []Condition{}
				}
			}
			f["targeting"] = normalized.Targeting
		}
		if definition.Tags == nil {
			f["tags"] = []string{}
		}
		f["lifecycle"] = "active"
		if definition.Deprecated {
			f["lifecycle"] = "deprecated"
		}
		flags[key] = f
	}
	return flags, nil
}

// Validate checks flag keys, types, defaults, variants, and JSON compatibility.
func (c ShipdConfig) Validate() error {
	_, err := c.normalized()
	return err
}

// MarshalJSON validates and encodes the version-1 CLI document.
func (c ShipdConfig) MarshalJSON() ([]byte, error) {
	flags, err := c.normalized()
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]any{"version": 1, "flags": flags})
}

// Defaults validates and returns a detached map suitable for the runtime SDK.
// JSON numbers are decoded as float64. Later mutations do not change the config.
func (c ShipdConfig) Defaults() (map[string]any, error) {
	flags, err := c.normalized()
	if err != nil {
		return nil, err
	}
	defaults := make(map[string]any, len(flags))
	for key, flag := range flags {
		defaults[key] = flag["default"]
	}
	return defaults, nil
}

func validValue(kind string, value any) bool {
	switch kind {
	case "boolean":
		_, ok := value.(bool)
		return ok
	case "variant":
		_, ok := value.(string)
		return ok
	case "number":
		_, ok := value.(float64)
		return ok
	case "json":
		return true
	default:
		return false
	}
}
