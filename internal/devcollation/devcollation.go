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
// and the accent of a letter, its full width, and the kana of a syllable.
// Each server is asked about the same names, so two servers that put every
// pair alike agree on these four sensitivities. They can still disagree
// elsewhere: a collation of an older version gives no weight to some
// characters a newer one weighs, so SQL_Latin1_General_CP1_CI_AS reads `p`,
// `pȡ` and `p😀` as one name where Latin1_General_100_CI_AS reads three.
var probe = []pair{
	{first: "a", second: "A", sensitivity: "case"},
	{first: "a", second: "á", sensitivity: "accent"},
	{first: "M", second: "Ｍ", sensitivity: "width"},
	{first: "あ", second: "ア", sensitivity: "kana"},
}

// Refuse returns an error when dev reads a probe pair as one name and target
// reads it as two, or the other way round, on a dialect whose offline rules
// leave names to the catalog. The error names both collations and says to
// create the dev database with the target's. devName names the dev database
// in the message, such as `the --dev-url database`. Any other dialect, and
// two servers that agree, return nil. An error from either server is
// returned, wrapped with the database it came from.
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
			"create %s with COLLATE %s",
		devName, collationOf(devRules), collationOf(targetRules), joinSensitivities(differ), devName, devName,
		collationOf(targetRules),
	)
}

// joinSensitivities lists names as prose: `case`, `case and width`, or
// `case, width and kana`.
func joinSensitivities(names []string) string {
	if len(names) < 2 {
		return strings.Join(names, "")
	}
	return strings.Join(names[:len(names)-1], ", ") + " and " + names[len(names)-1]
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
