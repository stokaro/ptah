// Package mysqlviewsecurity handles the SQL SECURITY mode of MySQL and
// MariaDB views.
package mysqlviewsecurity

import (
	"fmt"
	"strings"
)

// Parse accepts the two modes a view can declare. Empty leaves the server
// default in place, which is DEFINER.
func Parse(value string) (string, error) {
	mode := strings.ToUpper(strings.TrimSpace(value))
	switch mode {
	case "", "DEFINER", "INVOKER":
		return mode, nil
	default:
		return "", fmt.Errorf("invalid view SQL SECURITY mode %q: want DEFINER or INVOKER", value)
	}
}

// Effective names the mode a view has when its declaration omits the clause.
func Effective(value string) string {
	mode := strings.ToUpper(strings.TrimSpace(value))
	if mode == "" {
		return "DEFINER"
	}
	return mode
}

// Name is the name a plan's records give a view's SQL SECURITY mode: the
// statement effect property, and the option a refused plan names. The effect
// and the refusal report the same mode, so a consumer that reads one matches
// it against the other by this one name.
const Name = "sql_security"

// Lost reports whether a declaration that may name no mode would take INVOKER
// from a view that holds it. A statement that writes the view without the
// clause selects DEFINER, so the view would run with its definer's privileges
// and not its caller's. A declared mode, and a view held as DEFINER, lose
// nothing the declaration did not ask for.
func Lost(held, declared string) bool {
	return strings.TrimSpace(declared) == "" && Effective(held) == "INVOKER"
}

// Clause returns the view header clause, or empty for the server default.
func Clause(value string) (string, error) {
	mode, err := Parse(value)
	if err != nil || mode == "" {
		return "", err
	}
	return "SQL SECURITY " + mode, nil
}
