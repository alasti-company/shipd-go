package config

import (
	"encoding/json"
	"os"
	"testing"
)

func TestSharedConfigValidation(t *testing.T) {
	data, err := os.ReadFile("../testdata/config-validation.json")
	if err != nil {
		t.Fatal(err)
	}
	var cases []struct {
		Name       string         `json:"name"`
		Valid      bool           `json:"valid"`
		Definition FlagDefinition `json:"definition"`
	}
	if err := json.Unmarshal(data, &cases); err != nil {
		t.Fatal(err)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			config := ShipdConfig{"checkout": c.Definition}
			err := config.Validate()
			if (err == nil) != c.Valid {
				t.Fatalf("Validate() = %v, want valid=%v", err, c.Valid)
			}
			_, err = json.Marshal(config)
			if (err == nil) != c.Valid {
				t.Fatalf("MarshalJSON() = %v, want valid=%v", err, c.Valid)
			}
		})
	}
}
