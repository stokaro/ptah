package inference_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/inference"
)

// TestOpen_AnotherEngineIsRefusedByName is stokaro/ptah#2386.
//
// Every verb here opens its database on the PostgreSQL driver directly, so
// another engine's URL was always refused -- by pgx failing to parse it, with a
// message about an invalid keyword/value that reads as a malformed connection
// string. An operator following it goes and checks a URL that is correct.
//
// The refusal names the engine and says what this namespace speaks to. No
// connection is attempted, which is why these rows need no database.
func TestOpen_AnotherEngineIsRefusedByName(t *testing.T) {
	tests := []struct {
		name  string
		dbURL string
		// scheme is what the operator typed, echoed back so they recognize it.
		scheme string
		// dialect is what Ptah calls that engine, which is not always the same
		// word. The rows where the two differ are the ones that make this
		// measurable at all: with `mysql://` alone, a message naming the scheme
		// twice and a message naming the dialect twice are the same bytes.
		dialect string
	}{
		{name: "mysql", dbURL: "mysql://localhost:3306/db",
			scheme: "mysql", dialect: "mysql"},
		{name: "mariadb", dbURL: "mariadb://localhost:3306/db",
			scheme: "mariadb", dialect: "mariadb"},
		{name: "sqlite", dbURL: "sqlite://./local.db",
			scheme: "sqlite", dialect: "sqlite"},
		{name: "clickhouse", dbURL: "clickhouse://localhost:9000/db",
			scheme: "clickhouse", dialect: "clickhouse"},
		// The PostgreSQL FAMILY, which is a different question: these speak the
		// wire protocol pgx would connect with and have no pgvector, so a run
		// against one would fail further in, on a missing type.
		{name: "cockroachdb", dbURL: "cockroachdb://localhost:26257/db",
			scheme: "cockroachdb", dialect: "cockroachdb"},
		{name: "yugabytedb", dbURL: "yugabytedb://localhost:5433/db",
			scheme: "yugabytedb", dialect: "yugabytedb"},
		{name: "spanner", dbURL: "spanner://localhost:5432/db",
			scheme: "spanner", dialect: "spanner"},
		// The spellings where the two differ. An operator who wrote `mssql://`
		// is told `sqlserver`, which is the name every other Ptah message uses.
		{name: "mssql spelled as itself", dbURL: "mssql://sa@localhost:1433?database=db",
			scheme: "mssql", dialect: "sqlserver"},
		{name: "sqlite3", dbURL: "sqlite3://./local.db",
			scheme: "sqlite3", dialect: "sqlite"},
		{name: "crdb", dbURL: "crdb://localhost:26257/db",
			scheme: "crdb", dialect: "cockroachdb"},
		{name: "libsql is a transport onto sqlite", dbURL: "libsql://localhost/db",
			scheme: "libsql", dialect: "sqlite"},
		{name: "oracledb", dbURL: "oracledb://scott@localhost:1521/free",
			scheme: "oracledb", dialect: "oracle"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			output, err := runInference(c, "plan", "--spec", writeSpec(c), "--db-url", test.dbURL)

			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, "ptah inference works against PostgreSQL with pgvector")
			// Both halves, at the position each occupies. The scheme is echoed
			// as typed so the operator recognizes their own URL; the dialect is
			// the name the rest of Ptah uses for that engine. Asserting on one
			// of them alone passes for a message that interpolates the other
			// twice -- measured, on the rows above where the two words differ.
			c.Assert(err.Error(), qt.Contains, `"`+test.scheme+`://"`)
			c.Assert(err.Error(), qt.Contains, "run against "+test.dialect)
			// The wording pgx produced is what an operator was reading before,
			// and it is the thing that must be gone rather than merely joined.
			c.Assert(output, qt.Not(qt.Contains), "invalid keyword/value")
		})
	}
}

// TestOpen_AFormWithNoSchemeStillReachesTheDriver is the control for the shape
// of the check.
//
// The refusal reads a URL scheme, and pgx also accepts a keyword/value DSN that
// has none. Refusing everything unrecognized would have rejected a form that
// works, so an absent scheme falls through -- and this fails with the driver's
// own error rather than with the refusal above.
func TestOpen_AFormWithNoSchemeStillReachesTheDriver(t *testing.T) {
	c := qt.New(t)

	_, err := runInference(c, "plan", "--spec", writeSpec(c),
		"--db-url", "host=127.0.0.1 port=1 user=ptah dbname=ptah connect_timeout=1")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "ptah inference works against PostgreSQL")
}

// TestOpen_AnUnrecognizedSchemeStillReachesTheDriver is the other half of that
// control.
//
// Ptah refuses a scheme it recognizes as another dialect, not any scheme it has
// not heard of. The two are different rules and only one of them is safe: a
// scheme nobody has taught Ptah about may still be something pgx handles.
func TestOpen_AnUnrecognizedSchemeStillReachesTheDriver(t *testing.T) {
	c := qt.New(t)

	_, err := runInference(c, "plan", "--spec", writeSpec(c), "--db-url", "nonesuch://localhost/db")

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Not(qt.Contains), "ptah inference works against PostgreSQL")
}

// runInference drives the namespace's own command tree.
func runInference(c *qt.C, args ...string) (string, error) {
	c.Helper()
	cmd := inference.NewCommand()
	var output bytes.Buffer
	cmd.SetOut(&output)
	cmd.SetErr(&output)
	cmd.SetArgs(args)
	err := cmd.ExecuteContext(context.Background())
	return output.String(), err
}

// writeSpec writes a valid specification, so a refusal is about the URL.
//
// The specification is resolved before the database is opened, so a run given a
// broken one would refuse for that reason instead and every row above would
// pass without measuring anything.
func writeSpec(c *qt.C) string {
	c.Helper()
	const document = `
version: 1
name: engine check
source:
  schema: public
  table: articles
  key_fields: [id]
  input_fields: [title]
  version_strategy: updated_at
  version_field: updated_at
  mutable: true
preprocessing:
  separator: "\n"
  null_policy: empty
  empty_policy: skip
  unicode_normalization: none
  truncate: refuse
model:
  provider: openai-compatible
  endpoint_class: local
  endpoint: http://127.0.0.1:9/v1
  identifier: test-embed
  revision: "1"
  reported_dimension: 4
  normalization: none
target:
  schema: public
  table: articles
  column: embedding
  representation: vector
  metric: cosine
consistency:
  mode: outbox
policy:
  require_exact_approval: true
  require_consistency_mode: true
`
	path := filepath.Join(c.TempDir(), "spec.yaml")
	c.Assert(os.WriteFile(path, []byte(document), 0o600), qt.IsNil)
	return path
}
