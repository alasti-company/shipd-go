package main

import "testing"

// Validates shipd.config.go so a bad default or variant fails before `shipd push`.
func TestShipdConfigIsValid(t *testing.T) {
	if err := Config.Validate(); err != nil {
		t.Fatal(err)
	}
}
