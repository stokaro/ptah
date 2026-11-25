//go:build integration

package integration_test

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// A schema name that begins with a space is a schema of its own on SQL Server,
// so [ b] and [b] are two. The tests below hold an object of one kind in each,
// under one name, and ask whether `ptah schema apply` reaches the one it names:
// the existence guard of a sequence, an extended property and a synonym. The
// catalog is read back to say which schema the server changed.

// sqlServerSpacedRows reads query, whose rows are one text value each, in the
// order the query gives.
func sqlServerSpacedRows(c *qt.C, db *sql.DB, query string) []string {
	c.Helper()
	rows, err := db.QueryContext(c.Context(), query)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", query))
	defer func() { c.Assert(rows.Close(), qt.IsNil) }()
	values := make([]string, 0)
	for rows.Next() {
		var value string
		c.Assert(rows.Scan(&value), qt.IsNil)
		values = append(values, value)
	}
	c.Assert(rows.Err(), qt.IsNil)
	return values
}

// sqlServerSpacedApply runs `ptah schema apply` over b and " b" with the
// schema file and asserts it exits 0.
func sqlServerSpacedApply(c *qt.C, database, schema string, extra ...string) clirun.Result {
	c.Helper()
	args := append([]string{"schema", "apply", "--db-url", database, "--schemas", `b," b"`, "--schema-file", schema}, extra...)
	result := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
	c.Assert(result.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", result.Stdout, result.Stderr))
	return result
}

// sqlServerSpacedSequences lists the sequences as [schema].name.
const sqlServerSpacedSequences = `SELECT '[' + sc.name + '].' + sq.name FROM sys.sequences sq
	JOIN sys.schemas sc ON sc.schema_id = sq.schema_id ORDER BY 1`

// sqlServerSpacedSequenceSQL declares a sequence s in each schema. The one in
// [ b] is guarded, which renders an existence test in place of the IF NOT
// EXISTS T-SQL does not have.
const sqlServerSpacedSequenceSQL = `CREATE SCHEMA [b];
CREATE SEQUENCE [b].[s] AS bigint;
CREATE SCHEMA [ b];
CREATE SEQUENCE IF NOT EXISTS [ b].[s] AS bigint;
`

// TestSQLServerSpacedSchemaSequenceGuardE2E applies the declaration to a
// database holding [b].[s] and an empty [ b]. The guard looks in [ b], finds no
// sequence and creates one there. A guard that looked in b would find [b].[s]
// and create nothing, and the apply would still report success.
func TestSQLServerSpacedSchemaSequenceGuardE2E(t *testing.T) {
	c := qt.New(t)
	database, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db, "CREATE SCHEMA [b]", "CREATE SEQUENCE [b].[s] AS bigint", "CREATE SCHEMA [ b]")
	schema := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(schema, []byte(sqlServerSpacedSequenceSQL), 0o600), qt.IsNil)

	applied := sqlServerSpacedApply(c, database, schema, "--auto-approve")

	c.Assert(applied.Stdout, qt.Contains, "WHERE sc.name = ' b' AND sq.name = 's'")
	c.Assert(sqlServerSpacedRows(c, db, sqlServerSpacedSequences), qt.DeepEquals, []string{"[ b].s", "[b].s"})

	again := sqlServerSpacedApply(c, database, schema, "--dry-run")

	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}

// sqlServerSpacedPropertiesAndSynonyms lists the extended property flag of each
// table t and the synonyms, as [schema] value.
const sqlServerSpacedPropertiesAndSynonyms = `SELECT '[' + s.name + '] flag=' + CAST(p.value AS nvarchar(40))
	FROM sys.extended_properties p
	JOIN sys.tables t ON t.object_id = p.major_id AND p.minor_id = 0
	JOIN sys.schemas s ON s.schema_id = t.schema_id
	WHERE p.name = 'flag'
	UNION ALL
	SELECT '[' + s.name + '] syn=' + y.base_object_name
	FROM sys.synonyms y JOIN sys.schemas s ON s.schema_id = y.schema_id
	ORDER BY 1`

// TestSQLServerSpacedSchemaPropertiesAndSynonymsE2E inspects a database
// holding an extended property and a synonym of one name in each schema, and
// applies the description back: nothing is planned, because each object pairs
// with its own. A changed description of the [ b] objects then changes those
// alone. Keyed without the space, the two of each kind would be one identity,
// and the read would refuse the database.
func TestSQLServerSpacedSchemaPropertiesAndSynonymsE2E(t *testing.T) {
	c := qt.New(t)
	database, db := declaredSQLServerDatabase(c)
	declaredSQLServerExec(c, db,
		"CREATE SCHEMA [b]", "CREATE SCHEMA [ b]",
		"CREATE TABLE [b].[t] (id INT)", "CREATE TABLE [ b].[t] (id INT)",
		"EXEC sp_addextendedproperty 'flag', 'plain', 'SCHEMA', 'b', 'TABLE', 't'",
		"EXEC sp_addextendedproperty 'flag', 'spaced', 'SCHEMA', ' b', 'TABLE', 't'",
		"CREATE SYNONYM [b].[syn] FOR [dbo].[x]", "CREATE SYNONYM [ b].[syn] FOR [dbo].[y]")
	inspected := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
		"schema", "inspect", "--db-url", database, "--schemas", `b," b"`, "--format", "hcl")
	c.Assert(inspected.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", inspected.Stdout, inspected.Stderr))
	dir := c.TempDir()
	same := filepath.Join(dir, "same.hcl")
	c.Assert(os.WriteFile(same, []byte(inspected.Stdout), 0o600), qt.IsNil)
	changed := filepath.Join(dir, "changed.hcl")
	c.Assert(os.WriteFile(changed, []byte(strings.NewReplacer(`value = "spaced"`, `value = "changed"`,
		`target = "dbo.y"`, `target = "dbo.z"`).Replace(inspected.Stdout)), 0o600), qt.IsNil)

	synced := sqlServerSpacedApply(c, database, same, "--dry-run")

	c.Assert(synced.Stdout, qt.Contains, "Schema is synced, no changes to be made.")

	sqlServerSpacedApply(c, database, changed, "--auto-approve")

	c.Assert(sqlServerSpacedRows(c, db, sqlServerSpacedPropertiesAndSynonyms), qt.DeepEquals, []string{
		"[ b] flag=changed", "[ b] syn=[dbo].[z]", "[b] flag=plain", "[b] syn=[dbo].[x]",
	})

	again := sqlServerSpacedApply(c, database, changed, "--dry-run")

	c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
}
