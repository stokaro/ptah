package pglint

import (
	"fmt"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/pgtype"
)

// postgresInPlaceRule (PG301P) is the other half of the PostgreSQL cost
// rules: they fire where a rewrite is established and stay quiet where the
// server applies the change in place, and quiet reads the same as "the rule
// did not look". PG301P says the clause was judged and applied as a catalog
// edit, and how, for a statement whose every clause the comparison could
// judge (stokaro/ptah#2956). Migration lint reports the same for the MySQL
// family as MY130P.
//
// The type half is the measurement in pgcost.go. The attribute half was
// measured on PostgreSQL 18.6 by relfilenode and the heap-scan counter, beside
// a varchar(10) -> varchar(20) that is itself a catalog edit:
//
//	ALTER COLUMN clause                       table      indexes    scans
//	SET DEFAULT, DROP DEFAULT, DROP NOT NULL  kept       kept       0
//	SET NOT NULL                              kept       kept       1
//
// A statement with a clause outside the measured set is not judged and gets
// no finding. The rule reads the schema state the version starts from and
// stays silent without it, named as unmet.
func postgresInPlaceRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "PG301P",
			Title:    "column type change applied as a catalog edit",
			Severity: lintext.Info,
			Targets:  []string{"postgres"},
			Subsumes: []string{"DS103"},
			Input:    lintext.InputBaselineSchema,
		},
		BaselineSubjects: pgTypeChangeStatements,
		File: func(file *lintkit.File) []lintext.Violation {
			if !file.IsUp {
				return nil
			}
			var findings []lintext.Violation
			for _, index := range pgTypeChangeStatements(file) {
				message, ok := pgInPlaceMessage(file, index)
				if !ok {
					continue
				}
				findings = append(findings, lintext.Violation{Statement: index, Message: message})
			}
			return findings
		},
	}
}

// pgInPlaceMessage judges one ALTER TABLE statement as a whole: every ALTER
// COLUMN ... TYPE must be a catalog edit, and every other clause must be a
// SET DEFAULT, DROP DEFAULT or DROP NOT NULL, which are catalog edits too.
func pgInPlaceMessage(file *lintkit.File, index int) (string, bool) {
	stmt := &file.Statements[index]
	w := stmt.Words
	changes := 0
	var sentences []string
	for _, change := range resolvePGTypeChanges(file) {
		if change.site.Statement != index {
			continue
		}
		if change.transition.Outcome != pgtype.InPlace && change.transition.Outcome != pgtype.Unchanged {
			return "", false
		}
		changes++
		if change.transition.Outcome == pgtype.InPlace {
			sentences = append(sentences, fmt.Sprintf("ALTER COLUMN %s.%s TYPE %s %s", change.site.Table.Name, change.site.Column, change.updated.Spell(), change.transition.Why))
		}
	}
	others := 0
	for _, i := range lintkit.ClauseStarts(w) {
		switch {
		case lintkit.AlterColumnTypeClause(w, i):
		case pgCatalogEditClause(w, i):
			others++
		default:
			return "", false
		}
	}
	if changes == 0 || changes+others != len(lintkit.ClauseStarts(w)) {
		return "", false
	}
	message := "PostgreSQL applies the statement as a catalog edit: no table rewrite, no index rebuild, and no scan of the rows"
	if len(sentences) > 0 {
		message = strings.Join(sentences, "; ") + "; " + message
	} else {
		message = fmt.Sprintf("ALTER TABLE %s restates its column types; ", lintkit.AlterTableReference(w, stmt.SourceWords).Name) + message
	}
	if others > 0 {
		message += fmt.Sprintf(", the %d SET DEFAULT, DROP DEFAULT or DROP NOT NULL clause%s in the same statement included", others, lintkit.Plural(others))
	}
	return message, true
}

// pgCatalogEditClause recognizes ALTER [COLUMN] name SET DEFAULT ..., DROP
// DEFAULT and DROP NOT NULL.
func pgCatalogEditClause(w []string, i int) bool {
	if i >= len(w) || w[i] != "ALTER" {
		return false
	}
	j := i + 1
	if j < len(w) && w[j] == "COLUMN" {
		j++
	}
	j = lintkit.SkipIfExists(w, j)
	if j >= len(w) || !lintkit.IdentLike(w[j]) {
		return false
	}
	rest := w[j+1 : lintkit.ClauseEnd(w, i)]
	return lintkit.HasWordPrefix(rest, "SET", "DEFAULT") || lintkit.HasWordPrefix(rest, "DROP", "DEFAULT") || lintkit.HasWordPrefix(rest, "DROP", "NOT", "NULL")
}
