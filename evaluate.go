package shipd

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"unicode/utf16"
)

// bucket deliberately hashes UTF-16 code units to match the TypeScript SDK,
// including non-ASCII identities. Hashing UTF-8 bytes would change cohorts.
func bucket(key, unit string) uint32 {
	h := uint32(0x811c9dc5)
	for _, c := range utf16.Encode([]rune(key + ":" + unit)) {
		h = (h ^ uint32(c)) * 0x01000193
	}
	return h % 10000
}
func attribute(ctx EvaluationContext, name string) (any, bool) {
	if v, ok := ctx.Attributes[name]; ok {
		return v, true
	}
	if name == "userId" && (ctx.UserID != "" || ctx.UserIDPresent) {
		return ctx.UserID, true
	}
	return nil, false
}
func unit(ctx EvaluationContext, name string) string {
	if name == "" {
		return ctx.UserID
	}
	v, ok := attribute(ctx, name)
	if !ok || v == nil {
		return ""
	}
	return textValue(v)
}
func textValue(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return x
	case float64:
		return jsNumberString(x)
	default:
		r := reflect.ValueOf(v)
		switch r.Kind() {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			return jsNumberString(float64(r.Int()))
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			return jsNumberString(float64(r.Uint()))
		case reflect.Float32, reflect.Float64:
			return jsNumberString(r.Float())
		}
		return fmt.Sprint(v)
	}
}
func number(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, !math.IsNaN(x) && !math.IsInf(x, 0)
	case bool:
		if x {
			return 1, true
		}
		return 0, true
	case string:
		s := jsTrim(x)
		if s == "" {
			return 0, false
		}
		return jsNumber(s)
	default:
		if v != nil {
			r := reflect.ValueOf(v)
			switch r.Kind() {
			case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
				return float64(r.Int()), true
			case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
				return float64(r.Uint()), true
			case reflect.Float32, reflect.Float64:
				n := r.Float()
				return n, !math.IsNaN(n) && !math.IsInf(n, 0)
			}
		}
		return 0, false
	}
}
func boolean(v any) (bool, bool) {
	switch x := v.(type) {
	case bool:
		return x, true
	case float64:
		if x == 1 {
			return true, true
		}
		if x == 0 {
			return false, true
		}
	case string:
		switch strings.ToLower(jsTrim(x)) {
		case "true", "1":
			return true, true
		case "false", "0":
			return false, true
		}
	}
	if _, isString := v.(string); isString {
		return false, false
	}
	n, ok := number(v)
	if ok && (n == 0 || n == 1) {
		return n == 1, true
	}
	return false, false
}
func equal(a, b any) bool {
	if nonfinite(a) || nonfinite(b) {
		return false
	}
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	if _, ok := a.(bool); ok {
		x, xok := boolean(a)
		y, yok := boolean(b)
		return xok && yok && x == y
	}
	if _, ok := b.(bool); ok {
		x, xok := boolean(a)
		y, yok := boolean(b)
		return xok && yok && x == y
	}
	x, xok := number(a)
	y, yok := number(b)
	if xok && yok {
		return x == y
	}
	return textValue(a) == textValue(b)
}
func compare(a, b any) (int, bool) {
	if a == nil || b == nil || nonfinite(a) || nonfinite(b) {
		return 0, false
	}
	x, xok := number(a)
	y, yok := number(b)
	if xok && yok {
		if x < y {
			return -1, true
		}
		if x > y {
			return 1, true
		}
		return 0, true
	}
	as, bs := textValue(a), textValue(b)
	_, aString := a.(string)
	_, bString := b.(string)
	if aString && bString {
		if at, aok := timestampMillis(as); aok {
			if bt, bok := timestampMillis(bs); bok {
				return cmpInt64(at, bt), true
			}
		}
	}
	// JavaScript string ordering uses UTF-16 code units.
	au, bu := utf16.Encode([]rune(as)), utf16.Encode([]rune(bs))
	for i := 0; i < len(au) && i < len(bu); i++ {
		if au[i] < bu[i] {
			return -1, true
		}
		if au[i] > bu[i] {
			return 1, true
		}
	}
	if len(au) < len(bu) {
		return -1, true
	}
	if len(au) > len(bu) {
		return 1, true
	}
	return 0, true
}
func matches(c condition, key string, ctx EvaluationContext, segments map[string]segment, depth int) bool {
	if depth > 8 {
		return false
	}
	switch c.Kind {
	case "always":
		return true
	case "percentage":
		u := unit(ctx, c.Attribute)
		seed := c.Seed
		if seed == "" {
			seed = key
		}
		return u != "" && float64(bucket(seed, u)) < c.Rollout*100
	case "segment":
		member := false
		for _, id := range ctx.SegmentIDs {
			if id == c.SegmentID {
				member = true
				break
			}
		}
		if s, ok := segments[c.SegmentID]; ok && !member {
			member = all(s.Conditions, key, ctx, segments, depth+1)
		}
		if c.Operator == "not_in" {
			return !member
		}
		return member
	case "group":
		if c.Op == "or" {
			for _, child := range c.Conditions {
				if matches(child, key, ctx, segments, depth+1) {
					return true
				}
			}
			return false
		}
		return all(c.Conditions, key, ctx, segments, depth+1)
	case "attribute":
		actual, exists := attribute(ctx, c.Attribute)
		if c.Operator == "exists" || c.Operator == "not_exists" {
			// TS exists uses truthiness for the top-level userId, while an
			// explicit attributes.userId (including an empty string) exists.
			if c.Attribute == "userId" {
				_, attributeExists := ctx.Attributes["userId"]
				exists = attributeExists || ctx.UserID != ""
			}
			if c.Operator == "not_exists" {
				return !exists
			}
			return exists
		}
		if !exists {
			return false
		}
		switch c.Operator {
		case "eq":
			return equal(actual, c.Value)
		case "neq":
			return !equal(actual, c.Value)
		case "in", "not_in":
			candidates, ok := c.Value.([]any)
			if !ok {
				return false
			}
			found := false
			for _, candidate := range candidates {
				if equal(actual, candidate) {
					found = true
					break
				}
			}
			if c.Operator == "not_in" {
				return !found
			}
			return found
		case "contains", "startsWith", "endsWith":
			if actual == nil {
				return false
			}
			a, b := textValue(actual), textValue(c.Value)
			switch c.Operator {
			case "contains":
				return strings.Contains(a, b)
			case "startsWith":
				return strings.HasPrefix(a, b)
			default:
				return strings.HasSuffix(a, b)
			}
		default:
			n, ok := compare(actual, c.Value)
			if !ok {
				return false
			}
			switch c.Operator {
			case "gt":
				return n > 0
			case "gte":
				return n >= 0
			case "lt":
				return n < 0
			case "lte":
				return n <= 0
			}
		}
	}
	return false
}
func all(cs []condition, key string, ctx EvaluationContext, segments map[string]segment, depth int) bool {
	if len(cs) == 0 {
		return false
	}
	for _, c := range cs {
		if !matches(c, key, ctx, segments, depth) {
			return false
		}
	}
	return true
}
func resolve(s serve, key string, ctx EvaluationContext) serve {
	if s.Kind != "weighted" {
		return s
	}
	u := unit(ctx, s.Attribute)
	if u == "" {
		return serve{Kind: "disabled"}
	}
	seed := s.Seed
	if seed == "" {
		seed = key + ":weighted"
	}
	total := 0.0
	for _, v := range s.Variations {
		total += v.Weight
	}
	cumulative := 0.0
	b := float64(bucket(seed, u))
	for _, v := range s.Variations {
		cumulative += v.Weight / total * 10000
		if b < cumulative {
			return v.Serve
		}
	}
	return s.Variations[len(s.Variations)-1].Serve
}
func evaluate(s *snapshot, key string, ctx EvaluationContext, fallback any) Result {
	r := Result{Key: key, Value: fallback, Reason: "no_snapshot"}
	if s == nil {
		return r
	}
	version := s.Version
	r.SnapshotVersion = &version
	f, ok := s.Flags[key]
	if !ok {
		r.Reason = "not_found"
		return r
	}
	r.Value = f.Default
	if !f.Enabled {
		r.Reason = "flag_disabled"
		return r
	}
	chosen := f.Fallthrough
	r.Reason = "fallthrough"
	for _, rule := range f.Rules {
		if all(rule.Conditions, key, ctx, s.Segments, 0) && (rule.Serve.Kind != "weighted" || unit(ctx, rule.Serve.Attribute) != "") {
			chosen = rule.Serve
			r.Reason = "rule_match"
			r.RuleID = rule.ID
			break
		}
	}
	chosen = resolve(chosen, key, ctx)
	if chosen.Kind != "disabled" {
		r.Value = chosen.Value
	}
	return r
}

func nonfinite(v any) bool {
	switch n := v.(type) {
	case float64:
		return math.IsNaN(n) || math.IsInf(n, 0)
	case float32:
		return math.IsNaN(float64(n)) || math.IsInf(float64(n), 0)
	}
	return false
}

func cmpInt64(a, b int64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

func jsNumberString(n float64) string {
	if n == 0 {
		return "0"
	}
	if math.IsNaN(n) {
		return "NaN"
	}
	if math.IsInf(n, 1) {
		return "Infinity"
	}
	if math.IsInf(n, -1) {
		return "-Infinity"
	}
	if math.Abs(n) >= 1e21 || math.Abs(n) < 1e-6 {
		s := strconv.FormatFloat(n, 'e', -1, 64)
		parts := strings.Split(s, "e")
		exponent, _ := strconv.Atoi(parts[1])
		sign := ""
		if exponent >= 0 {
			sign = "+"
		}
		return parts[0] + "e" + sign + strconv.Itoa(exponent)
	}
	return strconv.FormatFloat(n, 'f', -1, 64)
}
