package migrator

// White-box testing required: the revision table is created and read before
// any command reports anything, so a statement Oracle refuses surfaces only as
// a driver error from inside Initialize. The builders and the SQL fragments
// below are where the Oracle spelling is decided, and they are package-local.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
)

// TestPtahRevisionsTableDDL_SpellsTheTableOracleTakes pins the native revision
// table on Oracle.
//
// Without an Oracle branch the target receives the generic statement, and every
// migrator verb fails on it before a single migration runs. Measured on Oracle
// Free 23.26.3.0.0, one column at a time (stokaro/ptah#3298):
//
//	version BIGINT                                ORA-00902: invalid datatype
//	description TEXT                              ORA-00902: invalid datatype
//	state VARCHAR(32) NOT NULL DEFAULT 'applied'  ORA-03076: unexpected item DEFAULT
//
// Read as the MySQL family, the statement also carries ` ENGINE=InnoDB`. The
// absent strings are those defects; the present ones are the spelling the
// server accepts.
func TestPtahRevisionsTableDDL_SpellsTheTableOracleTakes(t *testing.T) {
	ddl := ptahRevisionsTableDDL(platform.Oracle, `"schema_migrations"`, "N'schema_migrations'", "")

	for _, want := range []string{
		`CREATE TABLE "schema_migrations" (`,
		"version NUMBER(19) PRIMARY KEY",
		"description CLOB NULL",
		"state VARCHAR2(32) DEFAULT ''applied'' NOT NULL",
		"execution_time_ms NUMBER(19) DEFAULT 0 NOT NULL",
		"PRAGMA EXCEPTION_INIT(already_exists, -955);",
	} {
		t.Run("carries "+want, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddl, qt.Contains, want)
		})
	}
	for _, absent := range []string{"BIGINT", " TEXT", "NOT NULL DEFAULT", "IF NOT EXISTS", "ENGINE"} {
		t.Run("omits "+absent, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddl, qt.Not(qt.Contains), absent)
		})
	}
}

// TestAtlasRevisionsTableDDL_SpellsTheTableOracleTakes is the same for the
// Atlas layout, whose generic statement Oracle refuses with ORA-03062, missing
// comma or right parenthesis. Read as the MySQL family, that statement also
// carries a MySQL character set on the version column.
func TestAtlasRevisionsTableDDL_SpellsTheTableOracleTakes(t *testing.T) {
	ddl := atlasRevisionsTableDDL(platform.Oracle, `"atlas_schema_revisions"`, "N'atlas_schema_revisions'", "")

	for _, want := range []string{
		`CREATE TABLE "atlas_schema_revisions" (`,
		"version VARCHAR2(255) PRIMARY KEY",
		"type NUMBER(19) DEFAULT 2 NOT NULL",
		"partial_hashes CLOB NULL",
		"PRAGMA EXCEPTION_INIT(already_exists, -955);",
	} {
		t.Run("carries "+want, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddl, qt.Contains, want)
		})
	}
	for _, absent := range []string{"BIGINT", " TEXT", " JSON", "NOT NULL DEFAULT", "IF NOT EXISTS", "ENGINE", "utf8mb4"} {
		t.Run("omits "+absent, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ddl, qt.Not(qt.Contains), absent)
		})
	}
}

// TestOracleCreateTableIfAbsent_CarriesTheStatementAsALiteral pins the quoting.
// The CREATE travels inside EXECUTE IMMEDIATE as a string, so a quote left
// single would end the literal early, and a quoted table name has to survive
// with its case.
func TestOracleCreateTableIfAbsent_CarriesTheStatementAsALiteral(t *testing.T) {
	c := qt.New(t)

	block := oracleCreateTableIfAbsent(`CREATE TABLE "it's" (x VARCHAR2(1) DEFAULT 'a')`)

	c.Assert(block, qt.Equals, `DECLARE
    already_exists EXCEPTION;
    PRAGMA EXCEPTION_INIT(already_exists, -955);
BEGIN
    EXECUTE IMMEDIATE 'CREATE TABLE "it''s" (x VARCHAR2(1) DEFAULT ''a'')';
EXCEPTION
    WHEN already_exists THEN NULL;
END;`)
}

// TestRevisionFirstRowClause_OracleHasNoLimit pins the row limit on the dirty
// revision read. Measured on Oracle Free 23.26.3.0.0, LIMIT 1 answers
// ORA-03049; the other rows are the control, whose SQL must not move.
func TestRevisionFirstRowClause_OracleHasNoLimit(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "oracle", dialect: platform.Oracle, want: "FETCH FIRST 1 ROWS ONLY"},
		{name: "postgres", dialect: platform.Postgres, want: "LIMIT 1"},
		{name: "mysql", dialect: platform.MySQL, want: "LIMIT 1"},
		{name: "sqlite", dialect: platform.SQLite, want: "LIMIT 1"},
		{name: "no dialect", dialect: "", want: "LIMIT 1"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisionFirstRowClause(test.dialect), qt.Equals, test.want)
		})
	}
}

// TestRevisionTextColumn_OracleReadsTheColumnAsItIs pins the projection of a
// nullable text column. On Oracle the usual COALESCE fold answers ORA-00932,
// CHAR incompatible with CLOB, so the column is read bare and its NULL becomes
// an empty string on the Go side.
func TestRevisionTextColumn_OracleReadsTheColumnAsItIs(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{name: "oracle", dialect: platform.Oracle, want: "error"},
		{name: "postgres", dialect: platform.Postgres, want: "COALESCE(error, '')"},
		{name: "sqlserver", dialect: platform.SQLServer, want: "COALESCE(error, '')"},
		{name: "no dialect", dialect: "", want: "COALESCE(error, '')"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(revisionTextColumn(test.dialect, "error"), qt.Equals, test.want)
		})
	}
}

// TestAtlasRevisionPredicates_OracleComparesTheErrorByLength pins the two
// complementary predicates that decide whether an Atlas row is applied.
//
// Oracle stores an empty string as NULL, so comparing the folded error column
// with an empty string is never true there: no row would ever read as applied,
// and a second run would apply every migration again. The PostgreSQL rows are
// the control.
func TestAtlasRevisionPredicates_OracleComparesTheErrorByLength(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		wantDirty   string
		wantApplied string
		absent      string
	}{
		{
			name:        "oracle",
			dialect:     platform.Oracle,
			wantDirty:   "OR COALESCE(LENGTH(error), 0) > 0 OR",
			wantApplied: "AND COALESCE(LENGTH(error), 0) = 0 AND",
			absent:      "COALESCE(error, '')",
		},
		{
			name:        "postgres",
			dialect:     platform.Postgres,
			wantDirty:   "OR COALESCE(error, '') <> '' OR",
			wantApplied: "AND COALESCE(error, '') = '' AND",
			absent:      "LENGTH(error)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			dirty := atlasDirtyRevisionPredicateFor(test.dialect)
			applied := atlasAppliedRevisionPredicateFor(test.dialect)

			c.Assert(dirty, qt.Contains, test.wantDirty)
			c.Assert(applied, qt.Contains, test.wantApplied)
			c.Assert(dirty, qt.Not(qt.Contains), test.absent)
			c.Assert(applied, qt.Not(qt.Contains), test.absent)
		})
	}
}
