package config

import (
	"fmt"
	"math"
)

// TraitDefinition declares an evaluation input used by this flag. Required is an
// application contract; absent traits retain the evaluator's missing-value semantics.
type TraitDefinition struct {
	Type     string   `json:"type"`
	Values   []string `json:"values,omitempty"`
	Required bool     `json:"required,omitempty"`
}

// Targeting is the complete state for the CLI's selected environment.
// A nil pointer leaves dashboard targeting untouched. An empty Rules slice clears rules.
type Targeting struct {
	Enabled     bool   `json:"enabled"`
	Rules       []Rule `json:"rules"`
	Fallthrough Serve  `json:"fallthrough"`
}

// Rule IDs are stable within a flag. Rules are evaluated in slice order.
type Rule struct {
	ID          string      `json:"id"`
	Description string      `json:"description,omitempty"`
	Conditions  []Condition `json:"conditions"`
	Serve       Serve       `json:"serve"`
}

// Condition is a serializable targeting expression. Groups support and/or.
// Value is required for comparisons; use a slice for in and not_in.
type Condition struct {
	Kind       string      `json:"kind"`
	Attribute  string      `json:"attribute,omitempty"`
	Operator   string      `json:"operator,omitempty"`
	Value      any         `json:"value,omitempty"`
	Rollout    float64     `json:"rollout"`
	Seed       string      `json:"seed,omitempty"`
	SegmentID  string      `json:"segmentId,omitempty"`
	Op         string      `json:"op,omitempty"`
	Conditions []Condition `json:"conditions,omitempty"`
}

// Serve is a typed value, disabled state, or sticky weighted split.
// Value is deliberately not omitted: false, zero, and JSON null are valid values.
type Serve struct {
	Kind       string              `json:"kind"`
	Value      any                 `json:"value"`
	Variations []WeightedVariation `json:"variations,omitempty"`
	Seed       string              `json:"seed,omitempty"`
	Attribute  string              `json:"attribute,omitempty"`
}
type WeightedVariation struct {
	Weight float64 `json:"weight"`
	Serve  Serve   `json:"serve"`
}

func validateServe(s Serve, flag FlagDefinition, weighted bool) error {
	if s.Kind == "disabled" {
		return nil
	}
	if s.Kind == "weighted" && weighted {
		if len(s.Variations) == 0 {
			return fmt.Errorf("weighted serve requires variations")
		}
		total := 0.0
		for _, v := range s.Variations {
			if v.Weight <= 0 || math.IsNaN(v.Weight) || math.IsInf(v.Weight, 0) {
				return fmt.Errorf("weight must be positive and finite")
			}
			total += v.Weight
			if err := validateServe(v.Serve, flag, false); err != nil {
				return err
			}
		}
		if math.IsInf(total, 0) {
			return fmt.Errorf("total weight must be finite")
		}
		return nil
	}
	if s.Kind != flag.Type || !validValue(s.Kind, s.Value) {
		return fmt.Errorf("serve must match flag type")
	}
	if s.Kind == "variant" {
		for _, v := range flag.Variants {
			if v == s.Value {
				return nil
			}
		}
		return fmt.Errorf("serve must use a declared variant")
	}
	return nil
}

func validateCondition(c Condition, depth int) error {
	switch c.Kind {
	case "always":
		return nil
	case "percentage":
		if c.Rollout < 0 || c.Rollout > 100 || math.IsNaN(c.Rollout) || math.IsInf(c.Rollout, 0) {
			return fmt.Errorf("rollout must be between 0 and 100")
		}
		return nil
	case "segment":
		if c.SegmentID != "" && (c.Operator == "" || c.Operator == "in" || c.Operator == "not_in") {
			return nil
		}
	case "group":
		if depth >= 8 || len(c.Conditions) == 0 || (c.Op != "and" && c.Op != "or") {
			return fmt.Errorf("invalid condition group")
		}
		for _, child := range c.Conditions {
			if err := validateCondition(child, depth+1); err != nil {
				return err
			}
		}
		return nil
	case "attribute":
		if c.Attribute == "" {
			return fmt.Errorf("attribute is required")
		}
		scalar := func(v any) bool {
			switch v.(type) {
			case string, bool, float64:
				return true
			}
			return false
		}
		switch c.Operator {
		case "exists", "not_exists":
			return nil
		case "eq", "neq", "contains", "startsWith", "endsWith", "gt", "gte", "lt", "lte":
			if scalar(c.Value) {
				return nil
			}
		case "in", "not_in":
			if values, ok := c.Value.([]any); ok && len(values) > 0 {
				for _, v := range values {
					if !scalar(v) {
						return fmt.Errorf("invalid trait value")
					}
				}
				return nil
			}
		}
	}
	return fmt.Errorf("invalid condition %q", c.Kind)
}

func validateTraits(c Condition, traits map[string]TraitDefinition) error {
	if c.Kind == "group" {
		for _, child := range c.Conditions {
			if err := validateTraits(child, traits); err != nil {
				return err
			}
		}
		return nil
	}
	if c.Kind != "attribute" && c.Kind != "percentage" {
		return nil
	}
	if c.Attribute == "" {
		return nil
	}
	trait, ok := traits[c.Attribute]
	if !ok {
		return fmt.Errorf("undeclared trait %q", c.Attribute)
	}
	if c.Kind == "percentage" || c.Operator == "exists" || c.Operator == "not_exists" {
		return nil
	}
	switch c.Operator {
	case "contains", "startsWith", "endsWith":
		if trait.Type != "string" {
			return fmt.Errorf("string operator requires string trait")
		}
	case "gt", "gte", "lt", "lte":
		if trait.Type != "number" && trait.Type != "string" {
			return fmt.Errorf("ordered operator requires number or string trait")
		}
	}
	values, ok := c.Value.([]any)
	if !ok {
		values = []any{c.Value}
	}
	for _, v := range values {
		matches := false
		switch trait.Type {
		case "string":
			_, matches = v.(string)
		case "boolean":
			_, matches = v.(bool)
		case "number":
			_, matches = v.(float64)
		}
		if !matches {
			return fmt.Errorf("invalid value for trait %q", c.Attribute)
		}
		if trait.Type == "string" && len(trait.Values) > 0 && (c.Operator == "eq" || c.Operator == "neq" || c.Operator == "in" || c.Operator == "not_in") {
			found := false
			for _, allowed := range trait.Values {
				if v == allowed {
					found = true
				}
			}
			if !found {
				return fmt.Errorf("invalid enum value for trait %q", c.Attribute)
			}
		}
	}
	return nil
}
func (t *Targeting) validate(flag FlagDefinition) error {
	var traits map[string]TraitDefinition
	if flag.Traits != nil {
		traits = map[string]TraitDefinition{}
		for _, key := range []string{"userId", "country", "region", "city", "timezone"} {
			traits[key] = TraitDefinition{Type: "string"}
		}
		for key, trait := range flag.Traits {
			traits[key] = trait
		}
	}
	checkBucket := func(s Serve) error {
		if traits != nil && s.Kind == "weighted" && s.Attribute != "" {
			if _, ok := traits[s.Attribute]; !ok {
				return fmt.Errorf("undeclared bucketing trait %q", s.Attribute)
			}
		}
		return nil
	}
	if err := checkBucket(t.Fallthrough); err != nil {
		return err
	}

	if len(t.Rules) > 500 {
		return fmt.Errorf("at most 500 rules are supported")
	}
	if err := validateServe(t.Fallthrough, flag, true); err != nil {
		return err
	}
	ids := map[string]bool{}
	for _, r := range t.Rules {
		if r.ID == "" || ids[r.ID] {
			return fmt.Errorf("rule IDs must be nonempty and unique")
		}
		ids[r.ID] = true
		if err := validateServe(r.Serve, flag, true); err != nil {
			return err
		}
		if err := checkBucket(r.Serve); err != nil {
			return err
		}
		for _, c := range r.Conditions {
			if traits != nil {
				if err := validateTraits(c, traits); err != nil {
					return err
				}
			}
			if err := validateCondition(c, 0); err != nil {
				return err
			}
		}
	}
	return nil
}
