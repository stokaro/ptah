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

// Clause returns the view header clause, or empty for the server default.
func Clause(value string) (string, error) {
	mode, err := Parse(value)
	if err != nil || mode == "" {
		return "", err
	}
	return "SQL SECURITY " + mode, nil
}
