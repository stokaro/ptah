// Package usertypescope words the refusal of a declared user type a target
// cannot create, which the type owners give before any SQL is rendered.
//
// A named skip is not enough here, and that is measured rather than assumed.
// Gating the type alone on CockroachDB v26.2.5 left the declaration's own
// table behind -- `CREATE TABLE people ("addr" email)` -- which the server
// then refused with `type "email" does not exist`. The skip moved the failure
// from one server error to another instead of moving it before SQL, so an
// owner refuses: it names the object, names the reason, and stops
// (stokaro/ptah#1717).
package usertypescope

import (
	"fmt"

	"ptah.run/core/platform/capability"
)

// statements name the statement that creates each kind of user type, by the
// capability key that gates it.
var statements = map[capability.Capability]string{
	capability.DomainTypes:    "CREATE DOMAIN",
	capability.CompositeTypes: "CREATE TYPE ... AS (...)",
	capability.RangeTypes:     "CREATE TYPE ... AS RANGE",
}

// Statement names the statement that creates the kind of user type key
// gates, such as "CREATE DOMAIN", or is empty for another key.
func Statement(key capability.Capability) string { return statements[key] }

// Refusal is the reason a target refuses to create the user type name of the
// kind key gates. Every owner that plans the types refuses with it, so the
// refusal reads the same whichever owner reached it.
func Refusal(dialect string, key capability.Capability, name string) string {
	return fmt.Sprintf("%s: %s %s: this target does not create it, and a column declared with the type "+
		"would be left naming something the server has no definition of; "+
		"scope the declaration to the dialects that host it", dialect, Statement(key), name)
}
