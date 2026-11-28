// Package logingrants writes the note a command prints when a description it
// makes from a read leaves out grants to logins. A MySQL or MariaDB read
// reads the grants to logins on host % ([ptah.run/catalog.Grant.Login]) for
// the revoke cover check and for convergence, and a description declares no
// login, so `ptah db read`, `ptah migrations checkpoint` and the compatibility
// inspect leave those grants out. Without the note the output reads as if
// no login held anything.
package logingrants

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"ptah.run/catalog"
)

// Report writes one note naming how many grants to logins schema holds and
// the logins, sorted, and nothing where it holds none.
//
// w may be nil, which is how the read surfaces spell "no diagnostics
// stream". Write errors are dropped: a diagnostic that fails to print must
// not fail a read that succeeded.
func Report(w io.Writer, schema *catalog.Database) {
	if w == nil || schema == nil {
		return
	}
	var logins []string
	count := 0
	for _, grant := range schema.Grants {
		if grant.Login {
			count++
			logins = append(logins, grant.Role)
		}
	}
	if count == 0 {
		return
	}
	slices.Sort(logins)
	logins = slices.Compact(logins)
	noun := "grants"
	if count == 1 {
		noun = "grant"
	}
	_, _ = fmt.Fprintf(w, "note: grants to logins are not captured, since no statement here creates a login: %d %s to %s\n",
		count, noun, strings.Join(logins, ", "))
}
