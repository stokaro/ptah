package lint

import (
	"fmt"
	"strings"
)

// Making a nullable column NOT NULL is a change whose outcome the rows
// decide: a row holding NULL either aborts the statement or is rewritten to
// something the author never wrote. The SET NOT NULL spelling is reported by
// PG303 from the statement text, on PostgreSQL and on the three engines that
// speak its wire protocol, and SQLite's staged rebuild by LT101. DD103
// covers the engines whose spelling restates the whole column -- MySQL and
// MariaDB's MODIFY and CHANGE, SQL Server's ALTER COLUMN, and ClickHouse's
// MODIFY COLUMN, where the nullability is in the type itself -- where the
// text cannot tell a column made NOT NULL from one that was NOT NULL
// already, so the rule reads the column's current nullability from the
// schema state the version starts from. Together the three rules are what
// Atlas MF104 reports, on every dialect the linter analyzes.
//
// Measured on MySQL 8.4.11, MariaDB 11.8.9 and SQL Server 2022 (16.0.4265)
// (stokaro/ptah#2942), and on CockroachDB v25.2.23, YugabyteDB 2025.1
// (PostgreSQL 15.12), the Cloud Spanner emulator's PostgreSQL interface
// through PGAdapter 0.55.2, and ClickHouse 25.8.33 (stokaro/ptah#2958):
//
//	MODIFY a INT NOT NULL, a NULL present
//	  MySQL, strict SQL mode (the default)    ERROR 1138: Invalid use of NULL value
//	  MariaDB, strict SQL mode (the default)  ERROR 1265: Data truncated for column 'a'
//	  either engine, strict mode off          succeeds; the NULL reads back as 0
//	  with DEFAULT 0 in the same clause       the same two errors: the default is
//	                                          for new rows, not existing ones
//	MODIFY a INT NOT NULL, no NULL present    INSTANT refused, INPLACE with LOCK=NONE
//	ALTER COLUMN a int NOT NULL, SQL Server   one scan of the table (STATISTICS IO:
//	  a NULL present                          Scan count 1); Msg 515, Cannot insert
//	                                          the value NULL into column 'a'
//	  with a DEFAULT constraint               the same error
//	ALTER COLUMN a SET NOT NULL, a NULL present
//	  CockroachDB                             ERROR: validation of column "a" NOT NULL
//	                                          failed on row: ..., SQLSTATE 23502
//	  YugabyteDB                              ERROR: column "a" of relation "t" contains
//	                                          null values (PostgreSQL's own message)
//	  Spanner, PostgreSQL interface           the schema change fails with
//	                                          FAILED_PRECONDITION: Cannot specify a null
//	                                          value for column: a; the column stays nullable
//	  any of the three, no NULL present       succeeds
//	MODIFY COLUMN a Int32, from Nullable(Int32), ClickHouse
//	  a NULL present                          the mutation fails (CANNOT_INSERT_NULL_IN_
//	                                          ORDINARY_COLUMN) and stays unfinished; the
//	                                          column is already declared Int32, and every
//	                                          read of the table fails with the same error
//	                                          until KILL MUTATION and a MODIFY back to
//	                                          Nullable(Int32); a DEFAULT in the clause
//	                                          does not rescue the NULL
//	  no NULL present                         succeeds
//
// The implicit default a non-strict MySQL writes is the type's: 0 for a
// number, '' for a string, the zero date for a temporal. That is the case
// worth a rule of its own beside the abort: nothing fails, and the data is
// different afterwards.

// notNullSite is one clause that restates a column as NOT NULL.
type notNullSite struct {
	statement int
	table     tableReference
	column    string
	clause    string
}

// notNullSites finds every MODIFY, CHANGE, or SQL Server ALTER COLUMN clause
// that spells NOT NULL after the type, and on ClickHouse every MODIFY COLUMN
// whose new type is not Nullable, since that engine spells nullability in
// the type and nowhere else.
func notNullSites(file *File) []notNullSite {
	var sites []notNullSite
	for _, site := range columnChangeSites(file) {
		stmt := &file.Statements[site.statement]
		w := stmt.Words
		_, after, ok := parseMySQLTypeAt(w, site.typeStart, "")
		if !ok {
			continue
		}
		clause := "MODIFY COLUMN"
		if site.oldName != site.newName {
			clause = "CHANGE COLUMN"
		}
		if file.dialect == "clickhouse" {
			if w[site.typeStart] != "NULLABLE" {
				sites = append(sites, notNullSite{statement: site.statement, table: site.table, column: site.oldName, clause: clause})
			}
			continue
		}
		if hasWordSeq(w[after:clauseEndFrom(w, site.typeStart)], "NOT", "NULL") {
			sites = append(sites, notNullSite{statement: site.statement, table: site.table, column: site.oldName, clause: clause})
		}
	}
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !isAlterTable(w) {
			continue
		}
		table := alterTableReference(w, stmt.sourceWords)
		for _, i := range clauseStarts(w) {
			column, ok := sqlServerNotNullClause(w, stmt.sourceWords, i)
			if !ok {
				continue
			}
			sites = append(sites, notNullSite{statement: index, table: table, column: column, clause: "ALTER COLUMN"})
		}
	}
	return sites
}

// clauseEndFrom is the end of the clause a word index sits in.
func clauseEndFrom(w []string, index int) int {
	end := len(w)
	for _, start := range clauseStarts(w) {
		if start > index {
			end = min(end, start-1)
		}
	}
	return end
}

// sqlServerNotNullClause reads ALTER COLUMN name type ... NOT NULL, the SQL
// Server spelling, which names the type again and never says SET.
func sqlServerNotNullClause(w, sourceWords []string, i int) (string, bool) {
	end := clauseEnd(w, i)
	if i+2 >= end || w[i] != "ALTER" || w[i+1] != "COLUMN" || !identLike(w[i+2]) {
		return "", false
	}
	rest := w[i+3 : end]
	if len(rest) == 0 || rest[0] == "SET" || rest[0] == "DROP" || rest[0] == "TYPE" || !hasWordSeq(rest, "NOT", "NULL") {
		return "", false
	}
	return sourceWordAt(w, sourceWords, i+2), true
}

func notNullStatements(file *File) []int {
	var indexes []int
	seen := -1
	for _, site := range notNullSites(file) {
		if site.statement == seen {
			continue
		}
		seen = site.statement
		indexes = append(indexes, site.statement)
	}
	return indexes
}

// notNullConsequence says what the engine does with a NULL row, measured
// per engine and neutral where the run named none.
func notNullConsequence(dialect string) string {
	switch dialect {
	case "mysql":
		return "a row holding NULL fails the rebuild under the strict SQL mode MySQL defaults to (Invalid use of NULL value), " +
			"and is silently rewritten to the type's implicit default (0, '', or the zero date) when strict mode is off; " +
			"a DEFAULT in the same clause applies to new rows only. The rebuild itself runs in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE)"
	case "mariadb":
		return "a row holding NULL fails the rebuild under the strict SQL mode MariaDB defaults to (Data truncated for column), " +
			"and is silently rewritten to the type's implicit default (0, '', or the zero date) when strict mode is off; " +
			"a DEFAULT in the same clause applies to new rows only. The rebuild itself runs in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE)"
	case "sqlserver":
		return "SQL Server scans the table once to check every row and a row holding NULL fails the statement " +
			"(Msg 515, Cannot insert the value NULL into column); a DEFAULT constraint applies to new rows only"
	case "clickhouse":
		return "ClickHouse runs the change as a mutation that fails on the first NULL (CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN) and " +
			"stays unfinished while the column is already declared non-Nullable, so every read of the table fails with the same " +
			"error until the mutation is killed and the column modified back to Nullable; a DEFAULT in the clause does not rescue the NULL"
	default:
		return "a row holding NULL either fails the statement or is rewritten to the type's implicit default, depending on the engine and its SQL mode"
	}
}

// pgWireFamily is the set of engines the SET NOT NULL spelling was measured
// on: PostgreSQL and the three that speak its wire protocol. PG303 names
// each of them deliberately, one measurement per engine, where every other
// PG rule names postgres alone.
var pgWireFamily = []string{"postgres", "cockroachdb", "yugabytedb", "spanner"}

// setNotNullConsequence says what each engine does with SET NOT NULL, as
// measured; the PostgreSQL sentence also answers a run with no dialect.
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

// notNullRules is the family: the SET NOT NULL rule for the PostgreSQL
// wire family, and the restating-spelling rule for the rest.
func notNullRules() []Rule {
	return []Rule{setNotNullRule(), nullableMadeNotNullRule()}
}

// setNotNullRule (PG303) reads the statement text: SET NOT NULL says what
// it does, and the row that decides the outcome is not in the schema.
func setNotNullRule() Rule {
	return Rule{
		Code:     "PG303",
		Title:    "not-null validation scans existing rows",
		Severity: SeverityWarning,
		Dialects: pgWireFamily,
		CheckFile: func(file *File) []Finding {
			var findings []Finding
			for index := range file.Statements {
				stmt := &file.Statements[index]
				if !isAlterTable(stmt.Words) || !scanSetNotNull(stmt.Words) {
					continue
				}
				findings = append(findings, Finding{
					Rule:     "PG303",
					Title:    "not-null validation scans existing rows",
					Severity: SeverityWarning,
					File:     file.Path,
					Line:     stmt.Line,
					Message:  setNotNullConsequence(file.dialect),
					Context:  statementFindingContext(index),
				})
			}
			return findings
		},
	}
}

func nullableMadeNotNullRule() Rule {
	return Rule{
		Code:             "DD103",
		Title:            "nullable column made NOT NULL",
		Severity:         SeverityWarning,
		Dialects:         []string{"mysql", "mariadb", "sqlserver", "clickhouse"},
		Input:            InputBaselineSchema,
		BaselineSubjects: notNullStatements,
		CheckFile: func(file *File) []Finding {
			if !file.IsUp {
				return nil
			}
			var findings []Finding
			for _, site := range notNullSites(file) {
				column, ok := file.baseline.column(site.table.normalized, normalizeIdent(site.column))
				if !ok || column.NotNull {
					continue
				}
				message := fmt.Sprintf("%s %s.%s makes a nullable column NOT NULL: %s. Backfill first (UPDATE %s SET %s = ... WHERE %s IS NULL) "+
					"so the change meets no NULL, and only then restate the column",
					site.clause, site.table.name, site.column, notNullConsequence(file.dialect),
					site.table.name, site.column, site.column)
				findings = append(findings, costFinding(file, site.statement, "DD103", "nullable column made NOT NULL", message,
					Subject{Kind: SubjectColumn, Name: site.column, Parent: site.table.name, DataType: strings.ToLower(column.ColumnType)}))
			}
			return findings
		},
	}
}
