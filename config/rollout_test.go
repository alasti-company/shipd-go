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
