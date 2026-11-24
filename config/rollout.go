package config

import (
	"fmt"
	"math"
	"strings"
)

// Rollout defines the control-plane scheduler. SDK evaluation remains local.
// Connection names refer to credentials stored on the server, never in this file.
type Rollout struct {
	Provider   string         `json:"provider"`
	Connection string         `json:"connection"`
	Treatment  Serve          `json:"treatment"`
	Steps      []RolloutStep  `json:"steps"`
	Guards     []RolloutGuard `json:"guards"`
}
type RolloutStep struct {
	Percentage      float64 `json:"percentage"`
	MinDwellSeconds int     `json:"minDwellSeconds"`
}
type RolloutGuard struct {
	Metric     string  `json:"metric"`
	Field      string  `json:"field,omitempty"`
	Threshold  float64 `json:"threshold"`
	Operator   string  `json:"operator,omitempty"`
	MinSamples int     `json:"minSamples,omitempty"`
}

func (r Rollout) validate(flag FlagDefinition) error {
	if r.Provider != "sentry" {
		return fmt.Errorf("provider must be sentry")
	}
	if strings.TrimSpace(r.Connection) == "" {
		return fmt.Errorf("connection must contain a non-whitespace name")
	}
	treatment := Targeting{Fallthrough: r.Treatment}
	if err := treatment.validate(flag); err != nil {
		return fmt.Errorf("treatment: %w", err)
	}
	if len(r.Steps) < 1 || len(r.Steps) > 20 {
		return fmt.Errorf("steps must contain 1-20 entries")
	}
	last := 0.0
	for i, step := range r.Steps {
		if math.IsNaN(step.Percentage) || math.IsInf(step.Percentage, 0) || step.Percentage < .01 || step.Percentage > 100 {
			return fmt.Errorf("steps[%d].percentage must be finite and between 0.01 and 100", i)
		}
		if step.Percentage <= last {
			return fmt.Errorf("steps[%d].percentage must strictly increase", i)
		}
		if step.MinDwellSeconds < 1 || step.MinDwellSeconds > 604800 {
			return fmt.Errorf("steps[%d].minDwellSeconds must be between 1 and 604800", i)
		}
		last = step.Percentage
	}
	if last != 100 {
		return fmt.Errorf("last rollout step must be 100%%")
	}
	if len(r.Guards) < 1 || len(r.Guards) > 20 {
		return fmt.Errorf("guards must contain 1-20 entries")
	}
	for i, g := range r.Guards {
		if g.Metric != "error_rate" && g.Metric != "p95_latency" && g.Metric != "custom" {
			return fmt.Errorf("guards[%d].metric must be error_rate, p95_latency or custom", i)
		}
		if g.Metric == "custom" && strings.TrimSpace(g.Field) == "" {
			return fmt.Errorf("guards[%d].field is required for custom metrics", i)
		}
		if math.IsNaN(g.Threshold) || math.IsInf(g.Threshold, 0) || g.Threshold < 0 {
			return fmt.Errorf("guards[%d].threshold must be finite and nonnegative", i)
		}
		if g.Metric == "error_rate" && g.Threshold > 1 {
			return fmt.Errorf("guards[%d].threshold must be between 0 and 1 for error_rate", i)
		}
		if g.Operator != "" && g.Operator != "lte" && g.Operator != "gte" {
			return fmt.Errorf("guards[%d].operator must be lte or gte", i)
		}
		if g.MinSamples < 0 {
			return fmt.Errorf("guards[%d].minSamples must be positive (or omitted for the default)", i)
		}
	}
	return nil
}
