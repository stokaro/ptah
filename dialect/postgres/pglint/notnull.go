package pglint

import (
	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
)

// pgWireFamily is the set of engines the SET NOT NULL spelling was measured
// on: PostgreSQL and the three that speak its wire protocol. PG303 names
// each of them deliberately, one measurement per engine, where every other
// PG rule names postgres alone.
var pgWireFamily = []string{"postgres", "cockroachdb", "yugabytedb", "spanner"}

// setNotNullConsequence says what each engine does with SET NOT NULL, as
// measured; the PostgreSQL sentence also answers a run with no dialect. The
// measurements are recorded beside DD103 in migration lint, the rule that
// reports the same hazard where the spelling restates the whole column.
func setNotNullConsequence(dialect string) string {
	switch dialect {
	case "cockroachdb":
		return "SET NOT NULL validates every existing row in a schema-change job, and a row holding NULL fails it " +
			"(validation of column NOT NULL failed on row, SQLSTATE 23502); backfill first so the validation meets no NULL"
	case "yugabytedb":
		return "SET NOT NULL checks every existing row, and a row holding NULL aborts it (column contains null values); " +
			"backfill first so the check meets no NULL"
	case "spanner":
		return "SET NOT NULL runs as a schema change that validates every existing row, and a row holding NULL fails it " +
			"(FAILED_PRECONDITION: Cannot specify a null value for column) and leaves the column nullable; backfill first"
	default:
		return "SET NOT NULL scans the whole table under an ACCESS EXCLUSIVE lock to check existing rows, and a row holding NULL aborts it " +
			"(column contains null values); backfill first, then add CHECK (col IS NOT NULL) NOT VALID, validate it under a weaker lock, " +
			"and SET NOT NULL afterwards, which then scans nothing"
	}
}

// setNotNullRule (PG303) reads the statement text: SET NOT NULL says what
// it does, and the row that decides the outcome is not in the schema.
func setNotNullRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "PG303",
			Title:    "not-null validation scans existing rows",
			Severity: lintext.Warning,
			Targets:  pgWireFamily,
		},
		File: func(file *lintkit.File) []lintext.Violation {
			var findings []lintext.Violation
			for index := range file.Statements {
				stmt := &file.Statements[index]
				if !lintkit.IsAlterTable(stmt.Words) || !lintkit.ScanSetNotNull(stmt.Words) {
					continue
				}
				findings = append(findings, lintext.Violation{Statement: index, Message: setNotNullConsequence(file.Target.Name)})
			}
			return findings
		},
	}
}
