package lint

import (
	"ptah.run/internal/lintkit"
)

// The rules of this comparison close the gaps a refresh of the Atlas analyzer
// comparison found (stokaro/ptah#2972). Each one reports a consequence no
// existing rule named: a broad warning that fires on the same statement is not
// coverage, so where an existing rule already fired -- MY101 on an ALTER TABLE
// form, DS101 on a DROP TABLE, PG101 on a CREATE INDEX -- these say the thing
// that rule does not.
//
// Two consequences are kept apart throughout, because collapsing them is how a
// finding stops being actionable:
//
//   - what the statement costs: metadata edit, in-place rebuild, or table copy;
//   - whether writes continue while it runs.
//
// They are independent. MY141 rebuilds in place AND blocks writes; MY147
// rebuilds in place and lets them through; the PostgreSQL owner's PG320 takes
// a light lock and is still a hazard, for a reason that is not cost at all.
//
// The comparison these rules close was made against Ptah at
// 48c3b61b5bd183218ea6527f87113d0a4fae65df. Which existing rule already
// reported a hazard decided whether a check here became a rule, an alias or a
// partial, so the answer is only reproducible against a named tree -- the same
// reason the server versions below are named rather than left as "current".
//
// The engine-specific rules of the same comparison are their owners': the
// PostgreSQL owner's PG108, PG109, PG312, PG314 and PG320, and the MySQL
// owner's MY137 to MY147, each with its measurements. This file keeps the
// two that read for every engine.

// atlasGapRules returns the rules that close the comparison gaps.
func atlasGapRules() []Rule {
	return atlasGapCompatibilityRules()
}

// atlasGapCompatibilityRules holds BC103 and BC104.
//
// Both are file-level so they inherit the exemption DS101 and BC101 already
// apply: an object this migration itself created was never visible to a
// deployed application version, so retiring it breaks no client. The
// destructive rules report the same statements for the other consequence, and
// the two stay separate because the unit of suppression is the rule and its
// family -- accepting the data loss in a drop is not accepting a rollout
// break, and a config that silences one must not silence the other.
//
// Both are scoped to the native surface. This is a deliberate divergence and
// the reason is the surface, not the hazard: on the compatibility surface a
// drop is already reported as destructive, and adding a second diagnostic
// there would change the exit code of a migration directory that passes today.
// Ptah's compatibility rows for a dropped column are measured behavior a
// ported pipeline reads, so the fuller answer lives where a reader asked for
// Ptah's own -- `ptah migrations lint`. [tableRenamedFindings] scopes the
// mirror-image case for the same reason.
func atlasGapCompatibilityRules() []Rule {
	return []Rule{atlasGapDroppedTableRule(), atlasGapDroppedColumnRule()}
}

// atlasGapReportsOnThisSurface reports whether the rollout-break rules run for
// this file.
//
// One predicate for both rules: two checks would agree until one of them was
// changed, and a rule that leaked onto the compatibility surface would show up
// as an exit code, not as a wrong message.
func atlasGapReportsOnThisSurface(file *File) bool {
	return file.IsUp && file.compatibility != CompatibilityProfileAtlas
}

// atlasGapDroppedTableRule reports BC103: a dropped table retires a name
// deployed clients still query.
func atlasGapDroppedTableRule() Rule {
	return Rule{
		Code:     "BC103",
		Title:    "dropped table breaks deployed code",
		Severity: SeverityWarning,
		CheckFile: func(file *File) []Finding {
			if !atlasGapReportsOnThisSurface(file) {
				return nil
			}
			rebuilds := fileRebuilds(file)
			var findings []Finding
			created := make(map[string]bool)
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if ref := lintkit.CreatedTableRef(stmt.Words); ref != "" {
					created[ref] = true
					continue
				}
				if rebuilds.Drops[i] {
					// The rebuild's copy holds the name again by the end of
					// the file; see [fileRebuilds].
					continue
				}
				findings = append(findings, atlasGapDroppedTableFindings(file, stmt, i, created)...)
			}
			return findings
		},
	}
}

// atlasGapDroppedTableFindings reports one finding per table a DROP TABLE
// retires.
//
// Every decision here is DS101's, taken from DS101's own implementation rather
// than reproduced: the target extraction, the create-then-drop exemption, the
// logical-name ordering, and the subject-less fail-closed finding for a target
// list that could not be read to the end. The two rules report the same
// statements for different consequences, so a statement one of them recognizes
// and the other does not would be a silent hole in whichever ran second.
func atlasGapDroppedTableFindings(file *File, stmt *Statement, index int, created map[string]bool) []Finding {
	if !lintkit.HasWordPrefix(stmt.Words, "DROP", "TABLE") {
		return nil
	}
	tables, complete := lintkit.DroppedTablesNotCreated(stmt.Words, stmt.sourceWords, created)
	if complete && len(tables) == 0 {
		return nil
	}
	finding := func(message string, subjects ...Subject) Finding {
		return Finding{
			Rule:     "BC103",
			Title:    "dropped table breaks deployed code",
			Severity: SeverityWarning,
			File:     file.Path,
			Line:     stmt.Line,
			Message:  message,
			Context:  statementFindingContext(index, subjects...),
		}
	}
	if len(tables) == 0 {
		return []Finding{finding("dropping the table retires a name " + atlasGapDroppedTableAdvice)}
	}
	ordered := orderedDropTargets(tables)
	findings := make([]Finding, 0, len(ordered))
	for _, table := range ordered {
		findings = append(findings, finding(
			"dropping table "+table.Name+" retires a name "+atlasGapDroppedTableAdvice,
			Subject{Kind: SubjectTable, Name: table.Name},
		))
	}
	return findings
}

// atlasGapDroppedTableAdvice is the rest of every BC103 message: what the
// retired name costs and what to do instead.
//
// It names the rollout break rather than the rows, because the rows are DS101's
// subject and a reader who has already accepted that loss still has to be told
// this. "Even on an empty table" is the part that separates the two: a backup
// answers DS101 and answers nothing here.
const atlasGapDroppedTableAdvice = "application versions already deployed against the old schema still " +
	"query, so each of them starts failing the moment this migration commits -- a rollout break that " +
	"lands even on an empty table, which no backup mitigates; deploy code that no longer reads it " +
	"first, then drop it in a later release"

// atlasGapDroppedColumnRule reports BC104: a dropped column retires a name
// deployed clients still select and insert.
//
// It differs from DS102 in one place, and the difference is the question rather
// than an oversight: a column on a table this migration itself created is
// exempt here and reported there. No deployed application version ever saw that
// table, so nothing can break on the name -- while the rows DS102 speaks for
// are its own question, which that rule answers for itself.
func atlasGapDroppedColumnRule() Rule {
	return Rule{
		Code:     "BC104",
		Title:    "dropped column breaks deployed code",
		Severity: SeverityWarning,
		CheckFile: func(file *File) []Finding {
			if !atlasGapReportsOnThisSurface(file) {
				return nil
			}
			var findings []Finding
			created := make(map[string]bool)
			for i := range file.Statements {
				stmt := &file.Statements[i]
				if ref := lintkit.CreatedTableRef(stmt.Words); ref != "" {
					created[ref] = true
					continue
				}
				if !lintkit.IsAlterTable(stmt.Words) {
					continue
				}
				// The recognizer is DS102's, so every spelling that rule
				// reports is reported here: MySQL's `DROP col` without the
				// keyword, the IF EXISTS and ONLY forms, and a comma list,
				// which stays one finding carrying every retired name.
				subjects := droppedColumnSubjects(stmt.Words, stmt.sourceWords)
				if len(subjects) == 0 {
					continue
				}
				owner := lintkit.AlterTableReference(stmt.Words, stmt.sourceWords)
				if owner.Normalized != "" && lintkit.RefersToCreated(created, owner.Normalized) {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "BC104",
					Title:    "dropped column breaks deployed code",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message: "dropping a column retires a name application versions already deployed against " +
						"the old schema still select and insert, so each of them starts failing the moment " +
						"this migration commits, whether or not the column held any rows; " + columnDroppedAdvice,
					Context: statementFindingContext(i, subjects...),
				})
			}
			return findings
		},
	}
}
