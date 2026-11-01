package shipd

import (
	"math"
	"math/big"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// String coercion follows the TypeScript evaluator exactly; see
// packages/core/src/coerce.ts. Shared cases in sdks/test/fixtures cover both.
var (
	// JavaScript StringNumericLiteral without Infinity: no underscores, no hex
	// floats, and radix literals are unsigned.
	decimalLiteral = regexp.MustCompile(`^[+-]?(?:[0-9]+\.?[0-9]*|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)
	radixLiteral   = regexp.MustCompile(`^0(?:[xX][0-9a-fA-F]+|[oO][0-7]+|[bB][01]+)$`)
	// Canonical timestamp grammar: a date (midnight UTC), or a date-time with a
	// required zone. Anything else compares as a string.
	timestampLiteral = regexp.MustCompile(`^([0-9]{4})-([0-9]{2})-([0-9]{2})(?:T([0-9]{2}):([0-9]{2})(?::([0-9]{2})(?:\.([0-9]{1,9}))?)?(Z|[+-][0-9]{2}:[0-9]{2}))?$`)
)

// jsTrim trims the characters String.prototype.trim removes. It differs from
// strings.TrimSpace on U+FEFF (trimmed) and U+0085 (kept).
func jsTrim(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		switch r {
		case '\t', '\n', '\v', '\f', '\r', ' ', 0x00a0, 0x1680, 0x2028, 0x2029, 0x202f, 0x205f, 0x3000, 0xfeff:
			return true
		}
		return r >= 0x2000 && r <= 0x200a
	})
}

// jsNumber mirrors Number(s) for a trimmed, non-empty string, accepting only
// finite results.
func jsNumber(s string) (float64, bool) {
	if radixLiteral.MatchString(s) {
		base := map[byte]int{'x': 16, 'X': 16, 'o': 8, 'O': 8, 'b': 2, 'B': 2}[s[1]]
		n, ok := new(big.Int).SetString(s[2:], base)
		if !ok {
			return 0, false
		}
		// Round to nearest, ties to even, as JavaScript does.
		f, _ := new(big.Float).SetInt(n).Float64()
		return f, !math.IsInf(f, 0)
	}
	if !decimalLiteral.MatchString(s) {
		return 0, false
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil && !(isRangeErr(err) && !math.IsInf(f, 0)) {
		return 0, false
	}
	return f, !math.IsInf(f, 0)
}

func isRangeErr(err error) bool {
	numErr, ok := err.(*strconv.NumError)
	return ok && numErr.Err == strconv.ErrRange
}

// timestampMillis parses the canonical timestamp grammar into Unix
// milliseconds. Fractions beyond milliseconds are truncated so Go and
// JavaScript compare at the same precision.
func timestampMillis(s string) (int64, bool) {
	m := timestampLiteral.FindStringSubmatch(s)
	if m == nil {
		return 0, false
	}
	field := func(i int) int { n, _ := strconv.Atoi(m[i]); return n }
	year, month, day := field(1), field(2), field(3)
	if month < 1 || month > 12 || day < 1 || day > daysIn(year, month) {
		return 0, false
	}
	var hour, minute, second, millis, offset int
	if m[4] != "" {
		hour, minute = field(4), field(5)
		if m[6] != "" {
			second = field(6)
		}
		if m[7] != "" {
			millis, _ = strconv.Atoi((m[7] + "00")[:3])
		}
		if hour > 23 || minute > 59 || second > 59 {
			return 0, false
		}
		if zone := m[8]; zone != "Z" {
			zh, _ := strconv.Atoi(zone[1:3])
			zm, _ := strconv.Atoi(zone[4:6])
			if zh > 23 || zm > 59 {
				return 0, false
			}
			offset = (zh*60 + zm) * 60
			if zone[0] == '-' {
				offset = -offset
			}
		}
	}
	t := time.Date(year, time.Month(month), day, hour, minute, second, 0, time.UTC)
	return t.Unix()*1000 - int64(offset)*1000 + int64(millis), true
}

func daysIn(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}
