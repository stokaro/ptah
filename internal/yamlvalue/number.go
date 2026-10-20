package yamlvalue

import (
	"fmt"
	"math/big"
	"regexp"
	"strings"
)

var leadingDotNumber = regexp.MustCompile(`^\.[0-9](?:_?[0-9])*(?:[eE][+-]?[0-9](?:_?[0-9])*)?$`)

var decimalNumber = regexp.MustCompile(`^[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+)(?:[eE][+-]?[0-9]+)?$`)

// ExactNumber converts a finite YAML !!int or !!float scalar to JSON number
// syntax without floating-point rounding or expanding a decimal exponent.
// Integer bases and separators follow the shared YAML frontend. Invalid tags,
// invalid numbers, infinities, and NaN return an error. Input is not mutated.
func ExactNumber(tag, value string) (string, error) {
	if tag != "!!int" && tag != "!!float" {
		return "", fmt.Errorf("expected YAML numeric tag, got %q", tag)
	}
	if value == "" || !strings.ContainsRune("+-0123456789.", rune(value[0])) ||
		(value[0] == '.' && !leadingDotNumber.MatchString(value)) {
		return "", fmt.Errorf("invalid finite YAML %s value %q", tag, value)
	}
	plain := strings.ReplaceAll(value, "_", "")
	if integer, ok := new(big.Int).SetString(plain, 0); ok {
		return integer.String(), nil
	}
	if tag == "!!int" || !decimalNumber.MatchString(plain) {
		return "", fmt.Errorf("invalid finite YAML %s value %q", tag, value)
	}
	mantissa, exponent, hasExponent := strings.Cut(strings.ToLower(plain), "e")
	negative := strings.HasPrefix(mantissa, "-")
	mantissa = strings.TrimPrefix(strings.TrimPrefix(mantissa, "+"), "-")
	whole, fraction, hasFraction := strings.Cut(mantissa, ".")
	whole = strings.TrimLeft(whole, "0")
	if whole == "" {
		whole = "0"
	}
	number := whole
	if negative {
		number = "-" + number
	}
	if hasFraction && fraction != "" {
		number += "." + fraction
	}
	if hasExponent {
		number += "e" + exponent
	}
	return number, nil
}
