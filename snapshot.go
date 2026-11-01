package shipd

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
)

var (
	// ErrMalformedSnapshot reports a snapshot that failed validation. The client
	// keeps its last valid snapshot.
	ErrMalformedSnapshot = errors.New("shipd: malformed snapshot ignored")
	// ErrUnsupportedSchema reports a snapshot schema newer than this SDK. Upgrade
	// the SDK; the client keeps its last valid snapshot.
	ErrUnsupportedSchema = errors.New("shipd: unsupported snapshot schema version")
)

func object(v any) (map[string]any, bool) { m, ok := v.(map[string]any); return m, ok }
func str(v any) string                    { s, _ := v.(string); return s }
func safeVersion(v any) bool {
	n, ok := v.(float64)
	return ok && n >= 0 && n <= maxSafeVersion && math.Trunc(n) == n
}
func trait(v any) bool {
	switch v.(type) {
	case string, bool, float64:
		return true
	}
	return false
}
func validValue(kind string, v any) bool {
	switch kind {
	case "boolean":
		_, ok := v.(bool)
		return ok
	case "variant":
		_, ok := v.(string)
		return ok
	case "number":
		_, ok := v.(float64)
		return ok
	case "json":
		return true
	}
	return false
}
func normalizeServe(v any, weighted bool) bool {
	m, ok := object(v)
	if !ok {
		return false
	}
	kind := str(m["kind"])
	switch kind {
	case "disabled":
		return true
	case "weighted":
		if !weighted {
			return false
		}
		vs, ok := m["variations"].([]any)
		if !ok || len(vs) == 0 {
			return false
		}
		total := 0.0
		for _, v := range vs {
			item, ok := object(v)
			if !ok {
				return false
			}
			w, ok := item["weight"].(float64)
			if !ok || w <= 0 || !normalizeServe(item["serve"], false) {
				return false
			}
			total += w
		}
		if math.IsInf(total, 0) {
			return false
		}
		normalizeOptionalString(m, "seed")
		normalizeOptionalString(m, "attribute")
		return true
	case "variant":
		if _, ok := m["value"].(string); !ok {
			m["value"] = m["variant"]
		}
		return validValue(kind, m["value"])
	default:
		_, present := m["value"]
		return present && validValue(kind, m["value"])
	}
}
func normalizeOptionalString(m map[string]any, key string) {
	if str(m[key]) == "" {
		delete(m, key)
	}
}
func normalizeConditions(v any, depth int, nonempty bool) bool {
	cs, ok := v.([]any)
	if !ok || (nonempty && len(cs) == 0) {
		return false
	}
	for _, c := range cs {
		if !normalizeCondition(c, depth) {
			return false
		}
	}
	return true
}
func normalizeCondition(v any, depth int) bool {
	m, ok := object(v)
	if !ok || depth > 8 {
		return false
	}
	switch str(m["kind"]) {
	case "always":
		return true
	case "percentage":
		rollout, _ := m["rollout"].(float64)
		m["rollout"] = math.Max(0, math.Min(100, rollout))
		normalizeOptionalString(m, "seed")
		normalizeOptionalString(m, "attribute")
		return true
	case "segment":
		if str(m["segmentId"]) == "" {
			return false
		}
		op, present := m["operator"]
		if present && op != "in" && op != "not_in" {
			return false
		}
		return true
	case "group":
		return (m["op"] == "and" || m["op"] == "or") && normalizeConditions(m["conditions"], depth+1, true)
	case "attribute":
		if str(m["attribute"]) == "" {
			return false
		}
		switch str(m["operator"]) {
		case "exists", "not_exists":
			return true
		case "in", "not_in":
			vs, ok := m["value"].([]any)
			if !ok {
				return false
			}
			for _, v := range vs {
				if !trait(v) {
					return false
				}
			}
			return true
		case "eq", "neq", "contains", "startsWith", "endsWith", "gt", "gte", "lt", "lte":
			return trait(m["value"])
		}
	}
	return false
}
func parseSnapshot(data []byte) (*snapshot, error) {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil || raw == nil {
		return nil, ErrMalformedSnapshot
	}
	if v, exists := raw["schemaVersion"]; exists {
		if !safeVersion(v) {
			return nil, ErrMalformedSnapshot
		}
		if v != float64(SnapshotSchemaVersion) {
			return nil, fmt.Errorf("%w %.0f; SDK supports version %d", ErrUnsupportedSchema, v, SnapshotSchemaVersion)
		}
	} else {
		raw["schemaVersion"] = float64(SnapshotSchemaVersion)
	}
	if str(raw["projectId"]) == "" || str(raw["environmentId"]) == "" || str(raw["environmentKey"]) == "" || !safeVersion(raw["version"]) {
		return nil, ErrMalformedSnapshot
	}
	if _, ok := raw["generatedAt"].(string); !ok {
		return nil, ErrMalformedSnapshot
	}
	flags, ok := object(raw["flags"])
	if !ok {
		return nil, ErrMalformedSnapshot
	}
	for key, v := range flags {
		f, ok := object(v)
		if !ok || key == "" {
			return nil, ErrMalformedSnapshot
		}
		if k := str(f["key"]); k != "" && k != key {
			return nil, ErrMalformedSnapshot
		}
		f["key"] = key
		if _, ok := f["enabled"].(bool); !ok {
			return nil, ErrMalformedSnapshot
		}
		if version, exists := f["version"]; exists {
			if !safeVersion(version) {
				return nil, ErrMalformedSnapshot
			}
		} else {
			f["version"] = float64(0)
		}
		if !normalizeServe(f["fallthrough"], true) {
			return nil, ErrMalformedSnapshot
		}
		kind := str(f["type"])
		if kind != "boolean" && kind != "variant" && kind != "number" && kind != "json" {
			return nil, ErrMalformedSnapshot
		}
		rs, ok := f["rules"].([]any)
		if !ok {
			return nil, ErrMalformedSnapshot
		}
		for i, v := range rs {
			r, ok := object(v)
			if !ok || !normalizeServe(r["serve"], true) {
				return nil, ErrMalformedSnapshot
			}
			if str(r["id"]) == "" {
				r["id"] = fmt.Sprintf("rule_%d", i)
			}
			if cs, ok := r["conditions"].([]any); ok {
				if !normalizeConditions(cs, 0, false) {
					return nil, ErrMalformedSnapshot
				}
				if len(cs) == 0 {
					r["conditions"] = []any{map[string]any{"kind": "always"}}
				}
			} else if (r["kind"] == "percentage" || r["kind"] == "attribute") && normalizeCondition(r, 0) {
				r["conditions"] = []any{rCopyCondition(r)}
			} else {
				return nil, ErrMalformedSnapshot
			}
		}
		variants := []any{}
		if vs, ok := f["variants"].([]any); ok {
			for _, v := range vs {
				if _, ok := v.(string); ok {
					variants = append(variants, v)
				}
			}
		}
		f["variants"] = variants
		if _, exists := f["default"]; !exists {
			f["default"] = defaultFromServe(kind, f["fallthrough"].(map[string]any), variants)
		}
		if !validValue(kind, f["default"]) {
			f["default"] = syntheticDefault(kind, variants)
		}
	}
	if v, exists := raw["segments"]; exists {
		segments, ok := object(v)
		if !ok {
			return nil, ErrMalformedSnapshot
		}
		normalizedSegments := make(map[string]any, len(segments))
		for key, v := range segments {
			s, ok := object(v)
			if !ok || key == "" || !normalizeConditions(s["conditions"], 0, true) {
				return nil, ErrMalformedSnapshot
			}
			if str(s["id"]) == "" {
				s["id"] = key
			}
			id := str(s["id"])
			if _, exists := normalizedSegments[id]; exists {
				return nil, ErrMalformedSnapshot
			}
			normalizedSegments[id] = s
		}
		raw["segments"] = normalizedSegments
	}
	// Decode the normalized representation, rejecting incompatible field types.
	normalized, err := json.Marshal(raw)
	if err != nil {
		return nil, ErrMalformedSnapshot
	}
	var result snapshot
	if err := json.Unmarshal(normalized, &result); err != nil {
		return nil, ErrMalformedSnapshot
	}
	return &result, nil
}
func rCopyCondition(r map[string]any) map[string]any {
	c := make(map[string]any)
	for k, v := range r {
		if k != "serve" && k != "conditions" {
			c[k] = v
		}
	}
	return c
}
func syntheticDefault(kind string, variants []any) any {
	switch kind {
	case "boolean":
		return false
	case "variant":
		if len(variants) > 0 {
			return variants[0]
		}
		return "control"
	case "number":
		return float64(0)
	default:
		return map[string]any{}
	}
}
func defaultFromServe(kind string, s map[string]any, variants []any) any {
	if s["kind"] == "weighted" {
		v := s["variations"].([]any)[0].(map[string]any)
		return defaultFromServe(kind, v["serve"].(map[string]any), variants)
	}
	if s["kind"] == kind {
		return s["value"]
	}
	return syntheticDefault(kind, variants)
}
