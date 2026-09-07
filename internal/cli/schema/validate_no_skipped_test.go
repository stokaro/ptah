package schema_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/internal/exitcode"
)

// mySQLOptionsSchema declares the four table options the MySQL family owns.
//
// It is the ordinary shape of a schema written for MySQL first: stokaro/ptah#2969
// measured a PostgreSQL server rejecting the DDL rendered from it, and #2973
// stopped emitting the clause. What neither changed is that the render still
// exits 0 having dropped every one of them.
const mySQLOptionsSchema = `CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=100;
`

// clickHouseOptionsSchema is mySQLOptionsSchema with an engine ClickHouse
// accepts. ClickHouse refuses a MySQL-family engine outright
// (stokaro/ptah#3002), so measuring what it skips needs a source it renders.
const clickHouseOptionsSchema = `CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=MergeTree DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=100;
`

// TestSchemaValidateNoSkippedNamesEveryDroppedTableOption is the verb-level
// check stokaro/ptah#2976 asks for.
//
// The findings go to standard output and the summary to standard error, which
// is the convention this verb already had; a caller piping the findings must
// not also receive the count.
func TestSchemaValidateNoSkippedNamesEveryDroppedTableOption(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql", mySQLOptionsSchema)

	stdout, stderr, err := runSchemaStreams(
		"validate", "--schema-file", path, "--dialect", "postgres", "--no-skipped")

	c.Assert(exitcode.Code(err, 2), qt.Equals, 1)
	c.Assert(strings.Split(strings.TrimSpace(stdout), "\n"), qt.DeepEquals, []string{
		// The column line is the sharper half of this fixture, and it arrived
		// after the table options (stokaro/ptah#2983). PostgreSQL reads AutoInc
		// nowhere: it renders `"id" INT PRIMARY KEY NOT NULL`, so the key
		// generates nothing and every insert has to supply one. The remedy on
		// the line below tells the author to move the start onto
		// identity_start, which is advice about a key this target was not
		// generating at all.
		`postgres: column "users.id": auto-increment would be skipped; ` +
			`declare the column type as SERIAL or BIGSERIAL, or give it an ` +
			`identity clause with identity_generation`,
		`postgres: table "users": table option AUTO_INCREMENT=100 would be skipped; ` +
			`declare the start on the key column with identity_start`,
		`postgres: table "users": table option CHARSET=utf8mb4 would be skipped`,
		`postgres: table "users": table option COLLATE=utf8mb4_bin would be skipped`,
		`postgres: table "users": table option ENGINE=InnoDB would be skipped`,
	})
	c.Assert(strings.TrimSpace(stderr), qt.Equals, "5 problems")
}

// TestSchemaValidateNoSkippedReportsATargetThatSaidNothing is the half a
// comment-based check cannot have.
//
// SQLite, SQL Server and Oracle drop the same four options without writing a
// line, so a check built on the `skipped` comment would have called them
// stricter than PostgreSQL for saying less. The count is the subject here: the
// exact wording is pinned once, above.
func TestSchemaValidateNoSkippedReportsATargetThatSaidNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		// schema is the source each target reads. Only ClickHouse needs its
		// own, because it is the only target here that refuses the engine the
		// shared fixture declares.
		schema string
		want   int
	}{
		// The three that spell a generated key report the four table options
		// and nothing about the column.
		{name: "sqlite", dialect: "sqlite", schema: mySQLOptionsSchema, want: 4},
		{name: "sql server", dialect: "sqlserver", schema: mySQLOptionsSchema, want: 4},
		{name: "oracle", dialect: "oracle", schema: mySQLOptionsSchema, want: 4},
		// ClickHouse renders an engine clause, so that option survives, and it
		// generates no key, so the column adds a line of its own.
		{name: "clickhouse", dialect: "clickhouse", schema: clickHouseOptionsSchema, want: 4},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql", test.schema)

			stdout, _, err := runSchemaStreams(
				"validate", "--schema-file", path, "--dialect", test.dialect, "--no-skipped")

			c.Assert(exitcode.Code(err, 2), qt.Equals, 1)
			c.Assert(strings.Split(strings.TrimSpace(stdout), "\n"), qt.HasLen, test.want)
		})
	}
}

// TestSchemaValidateNoSkippedPassesOnTheTargetThatKeepsThem is the control.
//
// Every assertion above is satisfied by a check that fails any schema carrying
// table options. MySQL and MariaDB render all four, so the same source has to
// exit 0 there and print nothing.
func TestSchemaValidateNoSkippedPassesOnTheTargetThatKeepsThem(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: "mysql"},
		{name: "mariadb", dialect: "mariadb"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql", mySQLOptionsSchema)

			out, code := validateExitCode(c,
				"--schema-file", path, "--dialect", test.dialect, "--no-skipped")

			c.Assert(code, qt.Equals, 0)
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestSchemaValidateWithoutNoSkippedIsUnchanged keeps the flag opt-in.
//
// The same source that produces four findings above has to stay silent without
// the flag, or every existing pipeline validating a multi-engine schema starts
// failing on an upgrade.
func TestSchemaValidateWithoutNoSkippedIsUnchanged(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql", mySQLOptionsSchema)

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres")

	c.Assert(code, qt.Equals, 0)
	c.Assert(out, qt.Equals, "")
}

// TestSchemaValidateNoSkippedFailsARenderRefusal covers the limitation the
// documentation used to name.
//
// A SERIAL column validates against ClickHouse and exits 0 while
// `schema render` over the same source exits 2. Under this flag the verb owes
// the reader that answer, as a finding rather than a usage error.
func TestSchemaValidateNoSkippedFailsARenderRefusal(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "serial.sql",
		"CREATE TABLE things (id SERIAL PRIMARY KEY);\n")

	out, code := validateExitCode(c,
		"--schema-file", path, "--dialect", "clickhouse", "--no-skipped")

	c.Assert(code, qt.Equals, 1)
	c.Assert(out, qt.Contains, "SERIAL has no auto-increment equivalent")
}

// TestSchemaValidateNoSkippedReadsDeclarationsRatherThanText is the check that
// this is not a search for a word.
//
// A schema whose own data carries the word the diagnostic uses must not fail,
// and it would under any implementation that scanned rendered SQL for
// `skipped`.
func TestSchemaValidateNoSkippedReadsDeclarationsRatherThanText(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "notes.sql",
		"CREATE TABLE notes (id INT PRIMARY KEY, body TEXT DEFAULT 'skipped');\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres", "--no-skipped")

	c.Assert(code, qt.Equals, 0)
	c.Assert(out, qt.Equals, "")
}

// TestSchemaValidateNoSkippedAgreesAcrossSourceFormats covers the criterion
// about source formats.
//
// The three sources declare the one table option all three can express. A
// format that cannot spell CHARSET must not therefore look like a format that
// preserved it, so the comparison is over the property they share.
func TestSchemaValidateNoSkippedAgreesAcrossSourceFormats(t *testing.T) {
	tests := []struct {
		name     string
		selector string
		// file is where the source is written, relative to a temporary
		// directory. selected is what the selector is given: --schema-file
		// names the file and --root-dir names the directory holding it, so the
		// two are separate data rather than one derived from the other with a
		// separator this test would have to spell.
		file     string
		selected string
		content  string
	}{
		{
			name:     "sql",
			selector: "--schema-file",
			file:     "schema.sql",
			selected: "schema.sql",
			content:  "CREATE TABLE users (id INT PRIMARY KEY) ENGINE=InnoDB;\n",
		},
		{
			name:     "yaml",
			selector: "--schema-file",
			file:     "schema.yaml",
			selected: "schema.yaml",
			content: "tables:\n  users:\n    name: users\n    engine: InnoDB\n" +
				"    columns:\n      id:\n        name: id\n        type: INT\n        primary: true\n",
		},
		{
			name:     "go annotations",
			selector: "--root-dir",
			file:     "models/user.go",
			selected: "models",
			content: "package models\n\n" +
				"//ptah:schema:table name=\"users\" engine=\"InnoDB\"\ntype User struct {\n" +
				"\t//ptah:schema:field name=\"id\" type=\"INT\" primary=\"true\"\n\tID int64\n}\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			path := filepath.Join(dir, filepath.FromSlash(test.file))
			c.Assert(os.MkdirAll(filepath.Dir(path), 0o750), qt.IsNil)
			c.Assert(os.WriteFile(path, []byte(test.content), 0o600), qt.IsNil)

			out, code := validateExitCode(c,
				test.selector, filepath.Join(dir, test.selected),
				"--dialect", "postgres", "--no-skipped")

			c.Assert(code, qt.Equals, 1)
			c.Assert(strings.TrimSpace(out), qt.Contains,
				`postgres: table "users": table option ENGINE=InnoDB would be skipped`)
		})
	}
}

// TestSchemaValidateNoSkippedKeepsUsageErrorsApart holds the exit-code contract.
//
// The flag adds findings, which are the expected negative result and exit 1. A
// bad invocation is still exit 2, because a caller that cannot tell the two
// apart cannot use the status alone.
func TestSchemaValidateNoSkippedKeepsUsageErrorsApart(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql", mySQLOptionsSchema)

	_, findings := validateExitCode(c, "--schema-file", path, "--dialect", "postgres", "--no-skipped")
	_, noDialect := validateExitCode(c, "--schema-file", path, "--no-skipped")

	c.Assert(findings, qt.Equals, 1)
	c.Assert(noDialect, qt.Equals, 2)
}

// TestSchemaValidateNoSkippedRefusesAMySQLFamilyEngineOnClickHouse is what
// stokaro/ptah#3002 changes for this verb.
//
// The issue recorded that `--no-skipped` exited 0 on this source, and that it
// was right to: the ENGINE was not skipped, it reached the output inside a
// clause that could not hold it. Nothing on the skipped path could have caught
// that, so the refusal is what makes the verb able to.
//
// Measured: plain `validate` still exits 0 here, because it does not render.
// That is a separate gap and is not what this test claims.
func TestSchemaValidateNoSkippedRefusesAMySQLFamilyEngineOnClickHouse(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql", mySQLOptionsSchema)

	stdout, stderr, err := runSchemaStreams(
		"validate", "--schema-file", path, "--dialect", "clickhouse", "--no-skipped")

	c.Assert(exitcode.Code(err, 2), qt.Equals, 1)
	c.Assert(stdout, qt.Contains, "MySQL-family storage engine")
	c.Assert(stdout, qt.Contains, "platform.clickhouse.engine")
	c.Assert(strings.TrimSpace(stderr), qt.Equals, "1 problem")
}
