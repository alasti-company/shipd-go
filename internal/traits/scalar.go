// Package traits normalizes scalar Shipd targeting attributes.
package traits

import (
	"math"
	"reflect"
)

// NormalizeScalar converts Go scalar types to the SDK's JSON context values.
func NormalizeScalar(value any) (any, bool) {
	if value == nil {
		return nil, true
	}
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.String:
		return v.String(), true
	case reflect.Bool:
		return v.Bool(), true
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(v.Int()), true
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return float64(v.Uint()), true
	case reflect.Float32, reflect.Float64:
		n := v.Float()
		return n, !math.IsNaN(n) && !math.IsInf(n, 0)
	default:
		return nil, false
	}
}
