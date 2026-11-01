package shipd

import (
	"encoding/json"
	"errors"
	"math"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestSharedEvaluationParity(t *testing.T) {
	data, err := os.ReadFile("testdata/evaluation.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Snapshot json.RawMessage `json:"snapshot"`
		Cases    []struct {
			Name    string `json:"name"`
			Key     string `json:"key"`
			Context struct {
				UserID     *string        `json:"userId"`
				Attributes map[string]any `json:"attributes"`
				SegmentIDs []string       `json:"segmentIds"`
			} `json:"context"`
			Expected struct {
				Serve  serve  `json:"serve"`
				Reason string `json:"reason"`
				RuleID string `json:"ruleId"`
			} `json:"expected"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	s, err := parseSnapshot(fixture.Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range fixture.Cases {
		t.Run(c.Name, func(t *testing.T) {
			ctx := EvaluationContext{Attributes: c.Context.Attributes, SegmentIDs: c.Context.SegmentIDs}
			if c.Context.UserID != nil {
				ctx.UserID = *c.Context.UserID
				ctx.UserIDPresent = true
			}
			r := evaluate(s, c.Key, ctx, nil)
			var want any
			if c.Expected.Serve.Kind == "disabled" {
				if f, ok := s.Flags[c.Key]; ok {
					want = f.Default
				}
			} else {
				want = c.Expected.Serve.Value
			}
			if !reflect.DeepEqual(r.Value, want) || r.Reason != c.Expected.Reason || r.RuleID != c.Expected.RuleID {
				t.Fatalf("got %+v; want value %v reason %s rule %s", r, want, c.Expected.Reason, c.Expected.RuleID)
			}
		})
	}
}
func TestSnapshotRejectsMalformedData(t *testing.T) {
	original := wire(1, true)
	for name, data := range map[string]string{
		"unsafe version":     strings.Replace(original, `"version":1`, `"version":9007199254740992`, 1),
		"negative version":   strings.Replace(original, `"version":1`, `"version":-1`, 1),
		"fractional version": strings.Replace(original, `"version":1`, `"version":1.5`, 1),
		"key mismatch":       strings.Replace(original, `"key":"new-checkout"`, `"key":"other"`, 1),
		"missing enabled":    strings.Replace(original, `"enabled":true,`, "", 1),
		"null flags":         strings.Replace(original, `"flags":{`, `"flags":null,"unused":{`, 1),
		"malformed serve":    strings.Replace(original, `"value":true`, `"value":"true"`, 1),
		"missing rules":      strings.Replace(original, `"rules":[],`, "", 1),
		"invalid weighted":   strings.Replace(original, `"fallthrough":{"kind":"boolean","value":true}`, `"fallthrough":{"kind":"weighted","variations":[{"weight":0,"serve":{"kind":"boolean","value":true}}]}`, 1),
		"trailing payload":   original + `{}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseSnapshot([]byte(data)); !errors.Is(err, ErrMalformedSnapshot) {
				t.Fatalf("want ErrMalformedSnapshot, got %v", err)
			}
		})
	}
}
func TestSnapshotRejectsUnsupportedSchema(t *testing.T) {
	data := strings.Replace(wire(1, true), `"schemaVersion":1`, `"schemaVersion":2`, 1)
	_, err := parseSnapshot([]byte(data))
	if !errors.Is(err, ErrUnsupportedSchema) || !strings.HasPrefix(err.Error(), "shipd: ") || !strings.Contains(err.Error(), "version 2") {
		t.Fatalf("want wrapped ErrUnsupportedSchema with version, got %v", err)
	}
}
func TestLegacyAndDefaultNormalization(t *testing.T) {
	data := strings.Replace(wire(1, true), `"default":false,`, "", 1)
	s, err := parseSnapshot([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if s.Flags["new-checkout"].Default != true {
		t.Fatal("default should derive from fallthrough")
	}
	data = strings.Replace(wire(1, true), `"rules":[]`, `"rules":[{"conditions":[],"serve":{"kind":"boolean","value":false}}]`, 1)
	s, err = parseSnapshot([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if r := evaluate(s, "new-checkout", EvaluationContext{}, nil); r.Value != false || r.Reason != "rule_match" {
		t.Fatal(r)
	}
}
func TestGoNumericTraits(t *testing.T) {
	for _, v := range []any{int(20), int64(20), uint(20), float32(20), float64(20)} {
		if !matches(condition{Kind: "attribute", Attribute: "age", Operator: "gte", Value: "18"}, "flag", EvaluationContext{Attributes: map[string]any{"age": v}}, nil, 0) {
			t.Fatalf("failed numeric trait %T", v)
		}
	}
	if equal(math.NaN(), math.NaN()) || equal(math.Inf(1), "+Inf") {
		t.Fatal("nonfinite values must not compare equal")
	}
}

func TestMalformedMembershipDoesNotPanic(t *testing.T) {
	for _, operator := range []string{"in", "not_in"} {
		for _, value := range []any{nil, "pro", 42, map[string]any{"plan": "pro"}} {
			if matches(condition{Kind: "attribute", Attribute: "plan", Operator: operator, Value: value}, "flag", EvaluationContext{Attributes: map[string]any{"plan": "pro"}}, nil, 0) {
				t.Fatalf("malformed %s condition matched: %v", operator, value)
			}
		}
	}
}

func TestCanonicalTimestampWhitespace(t *testing.T) {
	for _, timestamp := range []string{"2026-01-02", "2026-01-02T10:00:00Z"} {
		if _, ok := timestampMillis(timestamp); !ok {
			t.Fatalf("canonical timestamp rejected: %q", timestamp)
		}
		for _, padded := range []string{" " + timestamp, timestamp + " ", timestamp + "\n", "\t" + timestamp} {
			if _, ok := timestampMillis(padded); ok {
				t.Fatalf("whitespace accepted outside canonical grammar: %q", padded)
			}
		}
	}
}

func TestNumericTraitsUseJavaScriptStrings(t *testing.T) {
	type namedFloat float64
	type namedInt int64
	for _, test := range []struct {
		value any
		want  string
	}{
		{float32(0.1), "0.10000000149011612"},
		{int64(9007199254740993), "9007199254740992"},
		{uint64(9007199254740993), "9007199254740992"},
		{namedFloat(1e21), "1e+21"},
		{namedInt(9007199254740993), "9007199254740992"},
	} {
		v, want := test.value, test.want
		ctx := EvaluationContext{Attributes: map[string]any{"cohort": v}}
		if got := unit(ctx, "cohort"); got != want {
			t.Fatalf("%T: got %q, want %q", v, got, want)
		}
		if bucket("flag", unit(ctx, "cohort")) != bucket("flag", want) {
			t.Fatalf("%T changed numeric cohort", v)
		}
		if !matches(condition{Kind: "attribute", Attribute: "cohort", Operator: "contains", Value: want}, "flag", ctx, nil, 0) {
			t.Fatalf("%T does not match its JavaScript string", v)
		}
	}
	if textValue(true) != "true" || textValue("01") != "01" {
		t.Fatal("numeric string conversion must preserve booleans and strings")
	}
}

func TestNullJSONDefault(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		data := strings.Replace(wire(1, enabled), `"type":"boolean"`, `"type":"json"`, 1)
		data = strings.Replace(data, `"default":false`, `"default":null`, 1)
		data = strings.Replace(data, `"fallthrough":{"kind":"boolean","value":true}`, `"fallthrough":{"kind":"disabled"}`, 1)
		s, err := parseSnapshot([]byte(data))
		if err != nil {
			t.Fatal(err)
		}
		if r := evaluate(s, "new-checkout", EvaluationContext{}, map[string]any{"fallback": true}); r.Value != nil {
			t.Fatalf("enabled=%t: null default replaced with fallback: %+v", enabled, r)
		}
	}
}

func TestSnapshotSegmentsUseDeclaredID(t *testing.T) {
	data := strings.Replace(wire(1, true), `"rules":[]`, `"rules":[{"id":"segment-rule","conditions":[{"kind":"segment","segmentId":"audience"}],"serve":{"kind":"boolean","value":false}}]`, 1)
	data = strings.TrimSuffix(data, "}") + `,"segments":{"wire-key":{"id":"audience","conditions":[{"kind":"attribute","attribute":"plan","operator":"eq","value":"pro"}]}}}`
	s, err := parseSnapshot([]byte(data))
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := s.Segments["wire-key"]; ok {
		t.Fatal("segment retained its wire key instead of its declared ID")
	}
	r := evaluate(s, "new-checkout", EvaluationContext{Attributes: map[string]any{"plan": "pro"}}, nil)
	if r.Value != false || r.Reason != "rule_match" {
		t.Fatalf("declared segment ID did not resolve: %+v", r)
	}
	duplicate := strings.Replace(data, `"segments":{`, `"segments":{"duplicate":{"id":"audience","conditions":[{"kind":"always"}]},`, 1)
	if _, err := parseSnapshot([]byte(duplicate)); !errors.Is(err, ErrMalformedSnapshot) {
		t.Fatalf("duplicate declared IDs should not depend on map order: %v", err)
	}
}

func TestNamedFloat64NumericConditions(t *testing.T) {
	type Price float64
	ctx := EvaluationContext{Attributes: map[string]any{"price": Price(100)}}
	if !matches(condition{Kind: "attribute", Attribute: "price", Operator: "gt", Value: float64(9)}, "flag", ctx, nil, 0) {
		t.Fatal("named float64 was compared as text")
	}
	if !equal(Price(100), float64(100)) {
		t.Fatal("named float64 must equal its numeric value")
	}
	for _, v := range []Price{Price(math.NaN()), Price(math.Inf(1)), Price(math.Inf(-1))} {
		if _, ok := number(v); ok {
			t.Fatalf("accepted nonfinite named float: %v", v)
		}
	}
}
