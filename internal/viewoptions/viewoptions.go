// Package viewoptions reads, compares and changes the options PostgreSQL keeps
// for a view or a materialized view in pg_class.reloptions: a view's
// security_invoker and security_barrier, and a materialized view's storage
// parameters such as fillfactor.
//
// Every frontend that declares options, the comparison that holds them against
// the ones a database reports, and the planner that changes them in place read
// them through this package, so the three agree on what a spelling means. An
// option is held as `name=value`. The name is lower-cased, because PostgreSQL
// folds it; the value is kept as written, because PostgreSQL stores it as
// written: `security_invoker = on` is held as security_invoker=on, and
// `fillfactor = '070'` as fillfactor=070. Two spellings of one value are
// therefore equal to [Equal], and never to a string comparison.
package viewoptions

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// ErrInvalidOption is the error every refusal of [Normalize] wraps.
var ErrInvalidOption = errors.New("invalid view option")

// checkOption is the option PostgreSQL keeps a view's check option in. A
// declaration states it as WITH CHECK OPTION, which the model holds apart.
const checkOption = "check_option"

// toastPrefix starts an option PostgreSQL keeps on the TOAST table rather than
// on the relation itself.
const toastPrefix = "toast."

// Normalize returns options in the form a declaration holds them: each
// `name=value`, the name trimmed and lower-cased, the value trimmed and taken
// out of single quotes when it is written as a string literal, and a bare name
// written `name=true`, which is what PostgreSQL stores for it. The order is
// kept. Nil and an empty list both come back nil.
//
// It refuses, wrapping [ErrInvalidOption]:
//
//   - a name that is not lower-case letters, digits and underscores;
//   - a name given twice, which PostgreSQL refuses too;
//   - an empty value after the equals sign;
//   - check_option, which a declaration states as WITH CHECK OPTION: the model
//     holds that as a flag of the view, and an option beside it would say the
//     same thing twice;
//   - a toast.* option, which PostgreSQL keeps on the TOAST table rather than
//     on the relation, and drops without a word where the relation has none,
//     so a declaration of one could never be read back.
//
// It does not check that PostgreSQL knows the name: the server answers
// `unrecognized parameter` for one it does not, and the list of storage
// parameters grows with the server.
func Normalize(options []string) ([]string, error) {
	if len(options) == 0 {
		return nil, nil
	}
	normalized := make([]string, 0, len(options))
	seen := make(map[string]bool, len(options))
	for _, option := range options {
		name, value, err := split(option)
		if err != nil {
			return nil, err
		}
		if seen[name] {
			return nil, fmt.Errorf("%w: %q is given more than once", ErrInvalidOption, name)
		}
		seen[name] = true
		normalized = append(normalized, name+"="+value)
	}
	return normalized, nil
}

// split reads one option into its name and value, or refuses it; see
// [Normalize].
func split(option string) (name, value string, err error) {
	name, value, hasValue := strings.Cut(option, "=")
	name = strings.ToLower(strings.TrimSpace(name))
	switch {
	case strings.HasPrefix(name, toastPrefix):
		return "", "", fmt.Errorf("%w: %s is kept on the TOAST table, not on the view, and cannot be read back from it",
			ErrInvalidOption, name)
	case !validName(name):
		return "", "", fmt.Errorf("%w: %q is not an option name: use lower-case letters, digits and underscores", ErrInvalidOption, option)
	case name == checkOption:
		return "", "", fmt.Errorf("%w: %s is a view's check option; declare it as WITH CHECK OPTION", ErrInvalidOption, checkOption)
	}
	if !hasValue {
		return name, "true", nil
	}
	value = unquote(strings.TrimSpace(value))
	if value == "" {
		return "", "", fmt.Errorf("%w: %s has no value after =", ErrInvalidOption, name)
	}
	return name, value, nil
}

// validName reports whether name is spelled like a relation option: lower-case
// letters, digits and underscores, starting with a letter or an underscore.
func validName(name string) bool {
	if name == "" {
		return false
	}
	for i, c := range name {
		letter := (c >= 'a' && c <= 'z') || c == '_'
		digit := c >= '0' && c <= '9'
		if !letter && (i == 0 || !digit) {
			return false
		}
	}
	return true
}

// unquote takes a value out of single quotes when it is written as a SQL
// string literal, undoubling the quotes inside, and returns any other value
// unchanged.
func unquote(value string) string {
	if len(value) < 2 || value[0] != '\'' || value[len(value)-1] != '\'' {
		return value
	}
	return strings.ReplaceAll(value[1:len(value)-1], "''", "'")
}

// Equal reports whether two option lists set the same options to the same
// values, in any order. Each list is read as [Normalize] reads it, so a
// database's `security_invoker=on` equals a declaration's
// `security_invoker=true`, and `fillfactor=070` equals `fillfactor=70`. A list
// [Normalize] refuses equals only itself.
func Equal(a, b []string) bool {
	left, leftErr := byName(a)
	right, rightErr := byName(b)
	if leftErr != nil || rightErr != nil {
		return slices.Equal(a, b)
	}
	if len(left) != len(right) {
		return false
	}
	for name, value := range left {
		other, found := right[name]
		if !found || !sameValue(value, other) {
			return false
		}
	}
	return true
}

// Changes is what turns the options held into the ones declared in place:
// set is each declared option that is not held or is held with another value,
// in the order declared, and reset is the name of each held option that is
// not declared, in the order held. Both are nil when the two lists are
// [Equal]. A list [Normalize] refuses yields nothing: the comparison refuses
// such a declaration before a plan asks.
func Changes(held, declared []string) (set, reset []string) {
	heldByName, heldErr := byName(held)
	declaredByName, declaredErr := byName(declared)
	if heldErr != nil || declaredErr != nil {
		return nil, nil
	}
	for _, option := range declared {
		name, value, _ := split(option)
		if current, found := heldByName[name]; !found || !sameValue(current, value) {
			set = append(set, name+"="+value)
		}
	}
	for _, option := range held {
		name, _, _ := split(option)
		if _, found := declaredByName[name]; !found {
			reset = append(reset, name)
		}
	}
	return set, reset
}

// Describe spells options for a change message: sorted, joined by commas, and
// `(none)` for an empty list, so a message reads as a sentence rather than as
// an arrow with nothing on one side.
func Describe(options []string) string {
	if len(options) == 0 {
		return "(none)"
	}
	sorted := slices.Clone(options)
	slices.Sort(sorted)
	return strings.Join(sorted, ", ")
}

// byName reads options into a map from name to value.
func byName(options []string) (map[string]string, error) {
	normalized, err := Normalize(options)
	if err != nil {
		return nil, err
	}
	values := make(map[string]string, len(normalized))
	for _, option := range normalized {
		name, value, _ := strings.Cut(option, "=")
		values[name] = value
	}
	return values, nil
}

// sameValue reports whether two option values mean the same thing to
// PostgreSQL: equal as numbers when both are numbers, equal as booleans when
// both are spellings PostgreSQL reads as booleans, and otherwise equal when
// they differ only in case, as an enumerated value such as auto does.
func sameValue(a, b string) bool {
	left, leftNumber := number(a)
	right, rightNumber := number(b)
	if leftNumber && rightNumber {
		return left == right
	}
	leftBool, leftIsBool := Bool(a)
	rightBool, rightIsBool := Bool(b)
	if leftIsBool && rightIsBool {
		return leftBool == rightBool
	}
	return strings.EqualFold(strings.TrimSpace(a), strings.TrimSpace(b))
}

// number reads value as a number.
func number(value string) (float64, bool) {
	parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
	return parsed, err == nil
}

// booleans are the spellings PostgreSQL's parse_bool reads as a boolean: true,
// yes, on and 1, false, no, off and 0, and each unambiguous prefix of the
// words.
var booleans = map[string]bool{
	"t": true, "tr": true, "tru": true, "true": true, "y": true, "ye": true, "yes": true, "on": true, "1": true,
	"f": false, "fa": false, "fal": false, "fals": false, "false": false, "n": false, "no": false,
	"of": false, "off": false, "0": false,
}

// Bool reads value as PostgreSQL reads a boolean option's value: true, yes, on
// and 1, false, no, off and 0, each unambiguous prefix of the words, in any
// case. ok is false for any other value.
func Bool(value string) (truth, ok bool) {
	truth, ok = booleans[strings.ToLower(strings.TrimSpace(value))]
	return truth, ok
}
