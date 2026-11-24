package config

import (
	"fmt"
	"math"
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
	if r.Provider != "sentry" || r.Connection == "" {
		return fmt.Errorf("provider must be sentry with a connection name")
	}
	if err := validateServe(r.Treatment, flag, false); err != nil {
		return err
	}
	if len(r.Steps) < 1 || len(r.Steps) > 20 || r.Steps[len(r.Steps)-1].Percentage != 100 {
		return fmt.Errorf("steps must end at 100%% (1-20 steps)")
	}
	last := 0.0
	for i, step := range r.Steps {
		if math.IsNaN(step.Percentage) || math.IsInf(step.Percentage, 0) || step.Percentage < .01 || step.Percentage > 100 || step.Percentage <= last || step.MinDwellSeconds < 1 || step.MinDwellSeconds > 604800 {
			return fmt.Errorf("invalid steps[%d]: increasing percentage and positive dwell required", i)
		}
		last = step.Percentage
	}
	if len(r.Guards) < 1 || len(r.Guards) > 20 {
		return fmt.Errorf("1-20 guards required")
	}
	for i, g := range r.Guards {
		if (g.Metric != "error_rate" && g.Metric != "p95_latency" && g.Metric != "custom") || (g.Metric == "custom" && g.Field == "") || math.IsNaN(g.Threshold) || math.IsInf(g.Threshold, 0) || g.Threshold < 0 || (g.Metric == "error_rate" && g.Threshold > 1) || (g.Operator != "" && g.Operator != "lte" && g.Operator != "gte") || g.MinSamples < 0 {
			return fmt.Errorf("invalid guards[%d]", i)
		}
	}
	return nil
}
