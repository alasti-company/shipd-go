package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestRolloutAndEnvironmentOverrides(t *testing.T) {
	c := ShipdConfig{"checkout": {Type: "boolean", Default: false, Targeting: &Targeting{Enabled: true, Fallthrough: Serve{Kind: "boolean", Value: false}, Environments: map[string]*Targeting{"staging": {Enabled: true, Fallthrough: Serve{Kind: "boolean", Value: true}}}}, Rollout: &Rollout{Provider: "sentry", Connection: "prod", Treatment: Serve{Kind: "boolean", Value: true}, Steps: []RolloutStep{{Percentage: 1, MinDwellSeconds: 60}, {Percentage: 100, MinDwellSeconds: 60}}, Guards: []RolloutGuard{{Metric: "error_rate", Threshold: .01}}}}}
	encoded, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"environments":{"staging"`) || !strings.Contains(string(encoded), `"rollout"`) {
		t.Fatal(string(encoded))
	}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	f := c["checkout"]
	f.Rollout.Steps[1].Percentage = 1
	if err := c.Validate(); err == nil {
		t.Fatal("expected rollout validation failure")
	}
	f.Rollout.Steps[1].Percentage = 100
	f.Targeting.Environments["staging"].Fallthrough = Serve{Kind: "number", Value: float64(3)}
	if err := c.Validate(); err == nil {
		t.Fatal("expected override serve type failure")
	}
}

type rolloutNumber int

func TestNumericRolloutTreatmentNormalization(t *testing.T) {
	for name, value := range map[string]any{"int": 2, "float32": float32(2.5), "named": rolloutNumber(3)} {
		t.Run(name, func(t *testing.T) {
			c := ShipdConfig{"limit": {Type: "number", Default: 1, Targeting: &Targeting{Enabled: true, Fallthrough: Serve{Kind: "number", Value: 1}}, Rollout: &Rollout{Provider: "sentry", Connection: "prod", Treatment: Serve{Kind: "number", Value: value}, Steps: []RolloutStep{{Percentage: 100, MinDwellSeconds: 60}}, Guards: []RolloutGuard{{Metric: "error_rate", Threshold: .01}}}}}
			if err := c.Validate(); err != nil {
				t.Fatal(err)
			}
			if _, err := json.Marshal(c); err != nil {
				t.Fatal(err)
			}
			if _, err := c.Defaults(); err != nil {
				t.Fatal(err)
			}
			c["limit"].Rollout.Treatment.Value = "wrong type"
			if err := c.Validate(); err == nil {
				t.Fatal("accepted a string number treatment")
			}
			if _, err := json.Marshal(c); err == nil {
				t.Fatal("marshaled a string number treatment")
			}
			if _, err := c.Defaults(); err == nil {
				t.Fatal("accepted invalid treatment in Defaults")
			}
		})
	}
}

func TestRolloutNamesAndReadableErrors(t *testing.T) {
	flag := FlagDefinition{Type: "boolean", Default: false, Targeting: &Targeting{Fallthrough: Serve{Kind: "boolean", Value: false}}, Rollout: &Rollout{Provider: "sentry", Connection: "prod", Treatment: Serve{Kind: "boolean", Value: true}, Steps: []RolloutStep{{Percentage: 100, MinDwellSeconds: 60}}, Guards: []RolloutGuard{{Metric: "error_rate", Threshold: .01}}}}
	c := ShipdConfig{"checkout": flag}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	flag.Targeting.Environments = map[string]*Targeting{"__proto__": {Fallthrough: Serve{Kind: "boolean", Value: false}}}
	if err := c.Validate(); err == nil {
		t.Fatal("accepted reserved environment")
	}
	flag.Targeting.Environments = nil
	flag.Rollout.Connection = "  "
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "connection") {
		t.Fatal(err)
	}
	flag.Rollout.Connection = "prod"
	flag.Rollout.Guards[0] = RolloutGuard{Metric: "custom", Field: "  "}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "guards[0].field") {
		t.Fatal(err)
	}
	flag.Rollout.Guards[0] = RolloutGuard{Metric: "error_rate", Threshold: 2}
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "threshold") {
		t.Fatal(err)
	}
}

func TestWeightedRolloutTreatmentTraits(t *testing.T) {
	flag := FlagDefinition{Type: "boolean", Default: false, Traits: map[string]TraitDefinition{"plan": {Type: "string"}}, Targeting: &Targeting{Enabled: true, Fallthrough: Serve{Kind: "boolean", Value: false}}, Rollout: &Rollout{Provider: "sentry", Connection: "prod", Treatment: Serve{Kind: "weighted", Attribute: "plan", Variations: []WeightedVariation{{Weight: 1, Serve: Serve{Kind: "boolean", Value: true}}, {Weight: 1, Serve: Serve{Kind: "boolean", Value: false}}}}, Steps: []RolloutStep{{Percentage: 100, MinDwellSeconds: 60}}, Guards: []RolloutGuard{{Metric: "error_rate", Threshold: .01}}}}
	c := ShipdConfig{"checkout": flag}
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	if _, err := json.Marshal(c); err != nil {
		t.Fatal(err)
	}
	flag.Rollout.Treatment.Attribute = "undeclared"
	if err := c.Validate(); err == nil {
		t.Fatal("accepted undeclared treatment bucketing trait")
	}
}
