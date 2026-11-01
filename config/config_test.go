package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"testing"
)

func TestShipdConfig(t *testing.T) {
	Config := ShipdConfig{
		"new-checkout":     {Type: "boolean", Default: false, Description: "Checkout", Tags: []string{"checkout"}},
		"search-variant":   {Type: "variant", Default: "control", Variants: []string{"control", "semantic"}},
		"max-retries":      {Type: "number", Default: 3, Deprecated: true, Disabled: true},
		"checkout-payload": {Type: "json", Default: map[string]any{"mode": "strict"}},
	}
	encoded, err := json.Marshal(Config)
	if err != nil {
		t.Fatal(err)
	}
	output := bytes.NewBuffer(encoded)
	var document struct {
		Version int                       `json:"version"`
		Flags   map[string]map[string]any `json:"flags"`
	}
	if err := json.Unmarshal(output.Bytes(), &document); err != nil {
		t.Fatal(err)
	}
	if document.Version != 1 || len(document.Flags) != 4 {
		t.Fatal(output.String())
	}
	number := document.Flags["max-retries"]
	if number["lifecycle"] != "deprecated" || number["disabled"] != true {
		t.Fatal(number)
	}
	if document.Flags["new-checkout"]["description"] != "Checkout" {
		t.Fatal(document.Flags)
	}
	if tags := document.Flags["search-variant"]["tags"].([]any); len(tags) != 0 {
		t.Fatal(tags)
	}
	defaults, err := Config.Defaults()
	if err != nil {
		t.Fatal(err)
	}
	if defaults["max-retries"] != float64(3) {
		t.Fatal(defaults)
	}
	defaults["checkout-payload"].(map[string]any)["mode"] = "mutated"
	if Config["checkout-payload"].Default.(map[string]any)["mode"] != "strict" {
		t.Fatal("defaults alias input")
	}
	marshaled, err := json.Marshal(Config)
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, output.Bytes()); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(compact.Bytes(), marshaled) {
		t.Fatalf("MarshalJSON differs: %s", marshaled)
	}
	repeated, err := json.Marshal(Config)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(output.Bytes(), repeated) {
		t.Fatal("encoding is not deterministic")
	}
}

func TestValidation(t *testing.T) {
	cases := []ShipdConfig{
		{"bad": {Type: "boolean", Default: "false"}},
		{"bad": {Type: "number", Default: math.NaN()}},
		{"bad": {Type: "number", Default: math.Inf(1)}},
		{"bad": {Type: "variant", Default: "other", Variants: []string{"control"}}},
		{"bad": {Type: "variant", Default: "control"}},
		{"bad": {Type: "unknown", Default: nil}},
		{"bad": {Type: "json", Default: make(chan int)}},
	}
	for _, key := range []string{"", " ", "__proto__"} {
		cases = append(cases, ShipdConfig{key: {Type: "boolean", Default: false}})
	}
	excessive := ShipdConfig{}
	for i := 0; i < 501; i++ {
		excessive[fmt.Sprint(i)] = FlagDefinition{Type: "boolean", Default: false}
	}
	cases = append(cases, excessive)
	for _, Config := range cases {
		if err := Config.Validate(); err == nil {
			t.Fatalf("accepted %+v", Config)
		}
		if _, err := Config.Defaults(); err == nil {
			t.Fatal("invalid config returned defaults")
		}
		if _, err := json.Marshal(Config); err == nil {
			t.Fatal("invalid config marshaled")
		}
	}
}

func TestEmptyConfig(t *testing.T) {
	for _, Config := range []ShipdConfig{nil, {}} {
		data, err := json.Marshal(Config)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != `{"flags":{},"version":1}` {
			t.Fatal(string(data))
		}
	}
}
