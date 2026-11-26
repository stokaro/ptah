// Package devcollation refuses a dev database that compares names other than
// the target it stands in for.
//
// A command that rehearses or verifies a change on a dev database plans the
// change on the target and runs it, or the migrations behind it, on the dev
// database. On a target whose names only its catalog can compare (see
// [offlinenames.Unanswered]), the two databases can disagree about which
// names are one: under a case-insensitive collation `MixedCase` and
// `MIXEDCASE` name one procedure, and under a case-sensitive one they name
// two. A dev database that disagrees with its target builds another state
// than the target holds and proves nothing about it. The commands call
// [Refuse] once both connections are open and before the dev database is
// written to.
package devcollation

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/offlinenames"
)

// Session is a connected server that says how it compares names. The
// DatabaseConnection type of ptah.run/dbschema implements it.
type Session interface {
	Info() catalog.ServerInfo
	ResolveIdentifierSemantics(ctx context.Context, names []string) (identifier.Semantics, error)
}

// pair is two names a collation keeps apart or reads as one, and the
// sensitivity that decides it.
type pair struct {
	first, second string
	sensitivity   string
}

// probe lists the pairs whose answer tells two collations apart: the case
// and the accent of one letter. Each server is asked about the same names, so
// two servers that put every pair alike compare these names alike.
var probe = []pair{
	{first: "a", second: "A", sensitivity: "case"},
	{first: "a", second: "á", sensitivity: "accent"},
}

// Refuse returns an error when dev reads a probe pair as one name and target
// reads it as two, or the other way round, on a dialect whose offline rules
// leave names to the catalog. devName names the dev database in the message,
// such as `--dev-url`. Any other dialect, and two servers that agree, return
// nil. An error from either server is returned, wrapped with the database it
// came from.
func Refuse(ctx context.Context, target, dev Session, devName string) error {
	if !offlinenames.Unanswered(identifier.ForDialect(target.Info().Dialect)) {
		return nil
	}
	names := make([]string, 0, 2*len(probe))
	for _, probed := range probe {
		names = append(names, probed.first, probed.second)
	}
	targetRules, err := target.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return fmt.Errorf("compare the collations of the target and %s: read the target: %w", devName, err)
	}
	devRules, err := dev.ResolveIdentifierSemantics(ctx, names)
	if err != nil {
		return fmt.Errorf("compare the collations of the target and %s: read %s: %w", devName, devName, err)
	}
	var differ []string
	for _, probed := range probe {
		if oneName(targetRules, probed) != oneName(devRules, probed) {
			differ = append(differ, probed.sensitivity)
		}
	}
	if len(differ) == 0 {
		return nil
	}
	return fmt.Errorf(
		"%s compares names under the collation %s and the target under %s, which differ in %s sensitivity, "+
			"so a name the two read differently would build another state on %s than the target holds; "+
			"%s must use the target's collation",
		devName, collationOf(devRules), collationOf(targetRules), strings.Join(differ, " and "), devName, devName,
	)
}

// oneName reports whether rules read the two names of probed as one name.
func oneName(rules identifier.Semantics, probed pair) bool {
	return rules.TableIdentityKey(probed.first) == rules.TableIdentityKey(probed.second)
}

// collationOf is the collation rules name, or a placeholder where the server
// named none.
func collationOf(rules identifier.Semantics) string {
	if rules.CatalogCollation == "" {
		return "(unnamed)"
	}
	return rules.CatalogCollation
}
