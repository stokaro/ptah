package lint

import (
	"fmt"
	"slices"

	"ptah.run/internal/yqlddl"
)

// ydbUndeclaredColumnFamilyRule reports an ALTER TABLE action that names a
// column family the table does not have. YDB does not refuse it: it creates
// the family with its own settings -- no compression, the database's default
// pool -- and the statement succeeds. Measured on 25.1.4.7 and 26.2.1.14,
// each of these created family `nope` on a table that had none:
//
//	ALTER TABLE t ALTER COLUMN c SET FAMILY nope
//	ALTER TABLE t ALTER FAMILY nope SET COMPRESSION "lz4"
//	ALTER TABLE t ADD COLUMN x Int32 FAMILY nope
//
// so a misspelled family moves a column's data into a new family nobody
// declared, and YQL has no DROP FAMILY to take it back. A CREATE TABLE that
// names an undeclared family is refused (`Unknown family nope`), and ADD
// FAMILY in the same statement declares one, so neither is reported. The
// table's families are read from the directory's own migrations, as YD104
// reads its indexes, and a table the directory did not create is not judged.
func ydbUndeclaredColumnFamilyRule() Rule {
	return Rule{
		Code:     "YD119",
		Title:    "column family the table does not have",
		Severity: SeverityWarning,
		Dialects: ydbOnly,
		CheckFile: func(file *File) []Finding {
			if !ydbRun(file.Target) {
				return nil
			}
			state := file.ydbBefore.clone()
			var findings []Finding
			for i := range file.Statements {
				stmt := &file.Statements[i]
				read := yqlddl.Read(stmt.SQL)
				if read.Kind != yqlddl.AlterTable {
					state.apply(read)
					continue
				}
				table := state.table(read.Name)
				for _, action := range read.Actions {
					if family, undeclared := table.undeclaredFamily(action); undeclared {
						findings = append(findings, Finding{
							Rule:     "YD119",
							Title:    "column family the table does not have",
							Severity: SeverityWarning,
							File:     file.Path,
							Line:     stmt.Line,
							Message: fmt.Sprintf(
								"%s has no column family %s, and YDB does not refuse the statement: it creates %s "+
									"with its own settings, no compression and the database's default pool, and YQL "+
									"has no DROP FAMILY; add the family with ADD FAMILY first, or name one the table has",
								read.Name, family, family),
							Context: statementFindingContext(i, Subject{Kind: SubjectTable, Name: read.Name}),
						})
					}
					table = table.applyAction(action)
				}
				state.store(read, table)
			}
			return findings
		},
	}
}

// undeclaredFamily names the column family an ALTER TABLE action uses that
// the table does not have, and reports whether there is one. A table whose
// families the directory did not declare has none to judge against.
func (t ydbTable) undeclaredFamily(action yqlddl.Action) (string, bool) {
	var family string
	switch action.Kind {
	case yqlddl.AlterFamily, yqlddl.SetColumnFamily:
		family = action.Family
	case yqlddl.AddColumn:
		family = action.Column.Family
	default:
		return "", false
	}
	if !t.familiesKnown || family == "" || slices.Contains(t.families, family) {
		return "", false
	}
	return family, true
}

// withFamily returns the table's families with family among them, as a
// statement that names it leaves them: YDB creates a family an action names
// and the table does not have.
func (t ydbTable) withFamily(family string) []string {
	if family == "" || slices.Contains(t.families, family) {
		return t.families
	}
	return append(slices.Clone(t.families), family)
}
