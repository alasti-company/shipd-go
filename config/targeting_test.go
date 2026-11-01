package config

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestTargetingValidation(t *testing.T) {
	good := func() ShipdConfig {
		return ShipdConfig{"checkout": {Type: "boolean", Default: false, Targeting: &Targeting{Enabled: true, Rules: []Rule{{ID: "staff", Conditions: []Condition{{Kind: "attribute", Attribute: "employee", Operator: "eq", Value: false}}, Serve: Serve{Kind: "boolean", Value: true}}}, Fallthrough: Serve{Kind: "boolean", Value: false}}}}
	}
	if err := good().Validate(); err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*Targeting){
		"duplicate":  func(v *Targeting) { v.Rules = append(v.Rules, v.Rules[0]) },
		"wrong type": func(v *Targeting) { v.Fallthrough = Serve{Kind: "number", Value: 4} },
		"rollout":    func(v *Targeting) { v.Rules[0].Conditions = []Condition{{Kind: "percentage", Rollout: 101}} },
	} {
		t.Run(name, func(t *testing.T) {
			c := good()
			mutate(c["checkout"].Targeting)
			if c.Validate() == nil {
				t.Fatal("expected validation error")
			}
		})
	}
	c := good()
	c["checkout"].Targeting.Rules = nil
	data, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"rules":[]`) {
		t.Fatalf("missing explicit empty rules: %s", data)
	}
}
