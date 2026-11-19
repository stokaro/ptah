//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/clirun"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
)

// handBuiltVirtualTable is table t with m a VIRTUAL generated column, as a
// user writes it without Ptah. PostgreSQL 18 and CockroachDB both take it.
const handBuiltVirtualTable = "CREATE TABLE t (id bigint PRIMARY KEY, a bigint, m bigint GENERATED ALWAYS AS (a * 2) VIRTUAL)"

// TestPostgresVirtualGeneratedColumnKindLive compares a VIRTUAL generated
// column with a declaration of each kind, on each engine of the PostgreSQL
// family that builds one. The column is written by hand, so the read reports
// what the server built rather than what a render wrote. A declaration that
// names no kind is STORED to Ptah on these engines, so it and one that names
// STORED are a column of another kind and plan a change; one that names
// VIRTUAL is the column the server holds.
//
// A comparator test builds its catalog by hand and says nothing about whether
// the reader reports the kind. With the reader's "v" arm removed, the read
// names no kind and every row fails here.
func TestPostgresVirtualGeneratedColumnKindLive(t *testing.T) {
	tests := []struct {
		name     string
		engine   dbtarget.Engine
		declared string
		changes  bool
	}{
		{name: "postgresql declared with no kind", engine: dbtarget.PostgreSQL, declared: "", changes: true},
		{name: "postgresql declared stored", engine: dbtarget.PostgreSQL, declared: "STORED", changes: true},
		{name: "postgresql declared virtual", engine: dbtarget.PostgreSQL, declared: "VIRTUAL", changes: false},
		{name: "cockroachdb declared with no kind", engine: dbtarget.CockroachDB, declared: "", changes: true},
		{name: "cockroachdb declared stored", engine: dbtarget.CockroachDB, declared: "STORED", changes: true},
		{name: "cockroachdb declared virtual", engine: dbtarget.CockroachDB, declared: "VIRTUAL", changes: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			c.Cleanup(cancel)
			dbURL := generatedKindDatabase(c, ctx, test.engine, handBuiltVirtualTable)
			conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
			c.Assert(err, qt.IsNil)
			c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
			read, err := conn.Reader().ReadSchemaContext(ctx)
			c.Assert(err, qt.IsNil)

			diff, err := schemadiff.CompareWithDatabaseInfo(ctx, virtualKindDesired(test.declared), read, conn.Info(), nil, must.Must(builtin.New()))

			c.Assert(err, qt.IsNil)
			c.Assert(generatedKindOf(c, read, "m"), qt.Equals, "VIRTUAL")
			c.Assert(diff.HasChanges(), qt.Equals, test.changes, qt.Commentf("%+v", diff.TablesModified))
		})
	}
}

// TestPostgresVirtualGeneratedColumnPrintedE2E prints a database holding a
// hand-built VIRTUAL generated column through the read-only commands that
// render it as SQL. Each prints the column VIRTUAL, the kind the server holds.
// A renderer that refuses VIRTUAL fails both commands on a database the user
// did not build with Ptah, and one that writes STORED prints another table.
func TestPostgresVirtualGeneratedColumnPrintedE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		target clirun.Target
		// args precede the database address, which the command takes last.
		args []string
		want string
	}{
		{
			name: "postgresql db read", engine: dbtarget.PostgreSQL, target: clirun.Ptah,
			args: []string{"db", "read", "--db-url"},
			want: `"m" bigint GENERATED ALWAYS AS ((a * 2)) VIRTUAL`,
		},
		{
			name: "postgresql schema inspect", engine: dbtarget.PostgreSQL, target: clirun.Compat,
			args: []string{"schema", "inspect", "--format", "{{ sql . }}", "--url"},
			want: `"m" bigint NULL GENERATED ALWAYS AS ((a * 2)) VIRTUAL`,
		},
		{
			name: "cockroachdb db read", engine: dbtarget.CockroachDB, target: clirun.Ptah,
			args: []string{"db", "read", "--db-url"},
			want: `"m" bigint GENERATED ALWAYS AS (a * 2) VIRTUAL`,
		},
		{
			name: "cockroachdb schema inspect", engine: dbtarget.CockroachDB, target: clirun.Compat,
			args: []string{"schema", "inspect", "--format", "{{ sql . }}", "--url"},
			want: `"m" bigint GENERATED ALWAYS AS (a * 2) VIRTUAL`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 2*time.Minute)
			c.Cleanup(cancel)
			dbURL := generatedKindDatabase(c, ctx, test.engine, handBuiltVirtualTable)

			got := clirun.Run(c, test.target, clirun.Options{Dir: c.TempDir()}, append(test.args, dbURL)...)

			c.Assert(got.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stdout, qt.Contains, test.want)
		})
	}
}

// TestPostgresVirtualGeneratedColumnRefusedWithoutTheKeyE2E applies a schema
// file declaring a VIRTUAL generated column to a server line without
// capability.VirtualGeneratedColumns. The apply is refused by name, and the
// database holds no table afterward: the refusal comes from rendering the
// plan, before a statement is sent.
func TestPostgresVirtualGeneratedColumnRefusedWithoutTheKeyE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
		want   string
	}{
		{
			name: "postgresql 16", engine: dbtarget.PostgreSQL16,
			want: `postgres on this server line does not support VIRTUAL generated columns (capability virtual_generated_columns); column "m" declares one`,
		},
		{
			name: "yugabytedb", engine: dbtarget.YugabyteDB,
			want: `yugabytedb on this server line does not support VIRTUAL generated columns (capability virtual_generated_columns); column "m" declares one`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
			c.Cleanup(cancel)
			dbURL := generatedKindDatabase(c, ctx, test.engine, "")
			dir := c.TempDir()
			schemaFile := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(schemaFile, []byte(handBuiltVirtualTable+";\n"), 0o600), qt.IsNil)

			got := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaFile, "--auto-approve")

			c.Assert(got.ExitCode, qt.Equals, 2, qt.Commentf("stdout:\n%s\nstderr:\n%s", got.Stdout, got.Stderr))
			c.Assert(got.Stderr, qt.Contains, test.want)
			c.Assert(postgresFamilyObjectCount(c, ctx, dbURL), qt.Equals, 0)
		})
	}
}

// TestGeneratedColumnWithNoKindConvergesE2E applies a schema file declaring a
// generated column with no kind, then plans the same file again. Ptah builds
// the column STORED on each engine, and the second plan finds it synced: the
// declaration with no kind compares as the kind Ptah built. Without that,
// CockroachDB and YugabyteDB compare it as changed on every run, and neither
// can change the expression in place, so every plan holds a warning and never
// converges.
func TestGeneratedColumnWithNoKindConvergesE2E(t *testing.T) {
	tests := []struct {
		name   string
		engine dbtarget.Engine
	}{
		{name: "postgresql", engine: dbtarget.PostgreSQL},
		{name: "cockroachdb", engine: dbtarget.CockroachDB},
		{name: "yugabytedb", engine: dbtarget.YugabyteDB},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(c.Context(), 3*time.Minute)
			c.Cleanup(cancel)
			dbURL := generatedKindDatabase(c, ctx, test.engine, "")
			dir := c.TempDir()
			schemaFile := filepath.Join(dir, "schema.sql")
			c.Assert(os.WriteFile(schemaFile,
				[]byte("CREATE TABLE t (id bigint PRIMARY KEY, a bigint, m bigint GENERATED ALWAYS AS (a * 2));\n"), 0o600), qt.IsNil)
			applied := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaFile, "--auto-approve")
			c.Assert(applied.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", applied.Stdout, applied.Stderr))

			again := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: dir},
				"schema", "apply", "--db-url", dbURL, "--schema-file", schemaFile, "--dry-run")

			c.Assert(again.ExitCode, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", again.Stdout, again.Stderr))
			c.Assert(again.Stdout, qt.Contains, "Schema is synced, no changes to be made.")
			c.Assert(generatedKindRead(c, ctx, dbURL), qt.Equals, "STORED")
		})
	}
}

// generatedKindDatabase creates an empty database on engine, runs ddl in it
// when ddl is not empty, drops it when the test ends, and answers its address.
func generatedKindDatabase(c *qt.C, ctx context.Context, engine dbtarget.Engine, ddl string) string {
	c.Helper()
	adminURL := dbtarget.URL(c, engine)
	admin, err := sql.Open("pgx", postgresFamilyDriverURL(c, adminURL))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(admin.Close(), qt.IsNil) })
	name := fmt.Sprintf("ptah_virtual_kind_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, admin, name)
	c.Cleanup(func() { dropPostgresFamilyE2EDatabase(c, admin, name) })
	dbURL := replaceDatabaseName(c, adminURL, name)
	if ddl != "" {
		execPostgresFamilySQL(c, ctx, dbURL, ddl)
	}
	return dbURL
}

// generatedKindRead answers the kind the server reports for column m of table
// t, read back through Ptah's reader.
func generatedKindRead(c *qt.C, ctx context.Context, dbURL string) string {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)
	return generatedKindOf(c, read, "m")
}

// virtualKindDesired declares table t with m generated from a, of kind.
func virtualKindDesired(kind string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", FieldName: "A", Name: "a", Type: "BIGINT", Nullable: true},
			{StructName: "T", FieldName: "M", Name: "m", Type: "BIGINT", Nullable: true, GeneratedExpression: "a * 2", GeneratedKind: kind},
		},
	}
}

// generatedKindOf answers the generated kind the read reports for column of
// table t.
func generatedKindOf(c *qt.C, read *catalog.Database, column string) string {
	c.Helper()
	for _, table := range read.Tables {
		for _, held := range table.Columns {
			if table.Name == "t" && held.Name == column {
				return held.GeneratedKind
			}
		}
	}
	c.Fatalf("table t holds no column %s", column)
	return ""
}
