//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/migrator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// Spanner had capability coverage on every pull request and no end-to-end
// suite, so nothing exercised render, plan, apply and read together the way
// every other engine has. Two defects lived in that gap, and both are asserted
// here (stokaro/ptah#1719).

// spannerLiveSchema declares a table and a secondary index.
//
// The varchar column is the point of the second assertion below: Spanner's
// catalog reports data_type "character varying" with udt_name EMPTY, where
// PostgreSQL answers "varchar" and never reaches that arm of the comparison.
func spannerLiveSchema(table string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Author", Name: table}},
		Fields: []schemamodel.Field{
			{StructName: "Author", Name: "id", Type: "bigint", Primary: true},
			{StructName: "Author", Name: "name", Type: "varchar(200)", Nullable: false},
		},
		Indexes: []schemamodel.Index{{
			StructName: "Author",
			Name:       "idx_" + table + "_name",
			Fields:     []string{"name"},
		}},
	}
}

// TestSpannerLiveSchemaRoundTrip is the convergence assertion the engine had no
// suite to make: apply, read the catalog back, compare, and require nothing
// left to do.
func TestSpannerLiveSchemaRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_sp_%d", time.Now().UnixNano())
	description := spannerLiveSchema(table)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP INDEX IF EXISTS "idx_`+table+`_name"`)
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}()

	// 1. The renderer's statements are what the server is given. Spanner
	// refuses DDL inside an explicit transaction, so each runs on its own.
	statements, err := builtin.GetOrderedCreateStatements(description, platform.Spanner)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The read returns the table, its primary key and the secondary index,
	// through the SQL-standard catalog rather than pg_index.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	c.Assert(spannerLiveTableNames(live.Tables), qt.Contains, table)

	// 3. Convergence. Without the `character varying` fold this reports a type
	// change on every run, and applying it fails: Spanner refuses to alter a
	// column an index refers to.
	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), description, live, platform.Spanner, must.Must(builtin.New())))
	c.Assert(settled.TablesModified, qt.HasLen, 0,
		qt.Commentf("a second run must have nothing to do"))
	c.Assert(settled.TablesAdded, qt.HasLen, 0)
	c.Assert(settled.IndexAdditions(), qt.HasLen, 0)
}

// TestSpannerLiveRefusesDDLInsideATransaction is the fact the apply path now
// reads off a capability instead of discovering at runtime.
//
// capability.DDLInsideTransaction was already false for this target; nothing
// consulted it, so `schema apply` opened a transaction and failed on its first
// statement. This asserts the server really does refuse, so the capability is
// a measurement rather than a belief.
func TestSpannerLiveRefusesDDLInsideATransaction(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_sp_tx_%d", time.Now().UnixNano())
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}()

	tx, err := conn.SchemaWriter().BeginTransaction(ctx)
	c.Assert(err, qt.IsNil)
	execErr := tx.ExecuteSQL(ctx, `CREATE TABLE "`+table+`" ("id" bigint PRIMARY KEY)`)
	_ = tx.Rollback()

	c.Assert(execErr, qt.IsNotNil,
		qt.Commentf("the capability says this target refuses DDL inside a transaction"))
	c.Assert(execErr.Error(), qt.Contains, "outside explicit transactions")
}

// spannerLiveTableNames lists the table names a catalog read reported.
func spannerLiveTableNames(tables []catalog.Table) []string {
	names := make([]string, 0, len(tables))
	for _, table := range tables {
		names = append(names, table.Name)
	}
	return names
}

// TestSpannerLiveApplyPathRunsOutsideATransaction is the assertion the
// round trip above could not make: it renders and execs statements itself, so
// it never reaches the apply path that opened the transaction.
//
// This goes through atlasschema.ApplyStatements with the DEFAULT per-file
// transaction mode -- the mode an operator gets without asking -- which is
// exactly the call that failed on Spanner before the capability was consulted.
func TestSpannerLiveApplyPathRunsOutsideATransaction(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	table := fmt.Sprintf("ptah_sp_apply_%d", time.Now().UnixNano())
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`)
	}()

	err = atlasschema.ApplyStatements(ctx, conn, migrator.MigrationTxModeFile, []string{
		`CREATE TABLE "` + table + `" ("id" bigint PRIMARY KEY)`,
	})

	c.Assert(err, qt.IsNil, qt.Commentf("the default transaction mode must not reach this target"))
	live, readErr := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"public"})
	c.Assert(readErr, qt.IsNil)
	c.Assert(spannerLiveTableNames(live.Tables), qt.Contains, table)
}

// spannerLiveSequenceSchema declares one standalone sequence, stating only what
// this target's CREATE SEQUENCE grammar takes.
func spannerLiveSequenceSchema(name string) *schemamodel.Database {
	start := int64(1000)
	return &schemamodel.Database{
		Sequences: []schemamodel.Sequence{{Name: name, AsType: "bigint", Start: &start}},
	}
}

// TestSpannerLiveSequenceRoundTrip is the convergence assertion behind the
// Sequences capability on this target.
//
// The key was false because no catalog appeared to report a sequence, which
// would have made Ptah emit a CREATE SEQUENCE and plan the same one again on
// every run. The catalog does report them -- through the quoted spelling of
// information_schema.sequences, where the unquoted one is a PGAdapter stub
// answering zero rows (stokaro/ptah#1856). This asserts the whole loop, so the
// day that stub changes shape, something goes red instead of silent.
func TestSpannerLiveSequenceRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	name := fmt.Sprintf("ptah_sq_%d", time.Now().UnixNano())
	description := spannerLiveSequenceSchema(name)
	defer func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SEQUENCE IF EXISTS "`+name+`"`)
	}()

	statements, err := builtin.GetOrderedCreateStatements(description, platform.Spanner)
	c.Assert(err, qt.IsNil)
	c.Assert(spannerLiveJoined(statements), qt.Contains, "CREATE SEQUENCE",
		qt.Commentf("a claimed capability has to reach an executable statement"))
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	c.Assert(spannerLiveSequenceNames(live.Sequences), qt.Contains, name,
		qt.Commentf("the reader must find the sequence it just applied"))

	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), description, live, platform.Spanner, must.Must(builtin.New())))
	c.Assert(settled.SequencesAdded, qt.HasLen, 0,
		qt.Commentf("a second run must have nothing to do"))
	c.Assert(settled.SequencesModified, qt.HasLen, 0)
}

// TestSpannerLiveRefusesTheSequenceOptionClauses pins that a declaration this
// grammar cannot carry is named rather than emitted without it.
//
// Emitting the sequence with the clause dropped would create an object that
// behaves differently from what was written, and say nothing.
func TestSpannerLiveRefusesTheSequenceOptionClauses(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	increment := int64(2)
	name := fmt.Sprintf("ptah_sqi_%d", time.Now().UnixNano())
	description := &schemamodel.Database{
		Sequences: []schemamodel.Sequence{{Name: name, Increment: &increment}},
	}

	statements, err := builtin.GetOrderedCreateStatements(description, platform.Spanner)
	c.Assert(err, qt.IsNil)
	rendered := spannerLiveJoined(statements)
	c.Assert(rendered, qt.Contains, "INCREMENT BY",
		qt.Commentf("the clause that could not be carried has to be named"))
	c.Assert(rendered, qt.Contains, "skipped")
	// Neither "contains no CREATE SEQUENCE" nor "contains no semicolon" is the
	// discriminator: the skip line names the statement it is about and ends in
	// a sentence. What has to be true is that nothing executable was produced.
	c.Assert(spannerLiveExecutableLines(rendered), qt.HasLen, 0,
		qt.Commentf("a refused declaration must render no statement at all, only the reason:\n%s", rendered))

	// And the server agrees the refusal is the right answer.
	_, execErr := conn.ExecContext(ctx, `CREATE SEQUENCE "`+name+`" INCREMENT BY 2`)
	c.Assert(execErr, qt.IsNotNil)
}

// spannerLiveSequenceNames lists the sequence names a schema read returned.
func spannerLiveSequenceNames(sequences []catalog.Sequence) []string {
	names := make([]string, 0, len(sequences))
	for _, sequence := range sequences {
		names = append(names, sequence.Name)
	}
	return names
}

// spannerLiveExecutableLines lists the rendered lines a server would run, which
// is every line that is not a comment.
func spannerLiveExecutableLines(rendered string) []string {
	var executable []string
	for line := range strings.SplitSeq(rendered, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "--") {
			continue
		}
		executable = append(executable, trimmed)
	}
	return executable
}

// spannerLiveJoined renders a statement list as one string for assertions.
func spannerLiveJoined(statements []string) string {
	return strings.Join(statements, "\n")
}

// TestSpannerLiveComparesADatabaseOfSeveralTables pins stokaro/ptah#4287.
// Spanner names the index of every table's primary key PRIMARY_KEY, so a read
// of a database holding two tables reports two indexes of that one name, and
// the comparison refused the read as two owners of one identity before it
// compared anything. The read is of the whole schema, as a command's is, and
// both tables have nothing left to plan.
func TestSpannerLiveComparesADatabaseOfSeveralTables(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	suffix := time.Now().UnixNano()
	tables := []string{fmt.Sprintf("ptah_sp_many_a_%d", suffix), fmt.Sprintf("ptah_sp_many_b_%d", suffix)}
	description := &schemamodel.Database{}
	for _, table := range tables {
		description.Tables = append(description.Tables, schemamodel.Table{StructName: table, Name: table})
		description.Fields = append(description.Fields, schemamodel.Field{StructName: table, Name: "id", Type: "bigint", Primary: true})
		c.Cleanup(func() { _, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+table+`"`) })
	}
	statements, err := builtin.GetOrderedCreateStatements(description, platform.Spanner)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	c.Assert(spannerLivePrimaryKeyIndexes(live, tables), qt.DeepEquals, []string{"PRIMARY_KEY", "PRIMARY_KEY"},
		qt.Commentf("the shape this test exists for: one index name for every table's key"))

	engine := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(ctx, description, live, conn.Info(), nil, engine)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	planned, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, engine, diff, platform.Spanner,
		planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)
	for _, table := range tables {
		c.Assert(spannerLiveJoined(planned), qt.Not(qt.Contains), table)
	}
}

// spannerLivePrimaryKeyIndexes lists the names of the primary key indexes a
// read reported for tables.
func spannerLivePrimaryKeyIndexes(live *catalog.Database, tables []string) []string {
	var names []string
	for _, index := range live.Indexes {
		if index.IsPrimary && slices.Contains(tables, index.TableName) {
			names = append(names, index.Name)
		}
	}
	return names
}

// TestSpannerLiveReadsCompositeKeysAsDeclared pins stokaro/ptah#4292. The
// constraint read joined a key's columns to every column the constraint uses,
// so a key of two columns arrived as four rows: a composite primary key read
// back with each column twice, a composite foreign key paired every column
// with every referenced one, and each plan dropped and added both. The keys
// now read back as declared, and the plan has nothing to do for them.
//
// The foreign key lists its referenced columns in the referenced key's order.
// Listed in another order, the emulator reports positions that pair them
// differently from what it enforces; see informationSchemaConstraintQuery.
func TestSpannerLiveReadsCompositeKeysAsDeclared(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.Spanner)
	c := qt.New(t)
	ctx := t.Context()
	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	suffix := time.Now().UnixNano()
	parent, child := fmt.Sprintf("ptah_sp_ck_parent_%d", suffix), fmt.Sprintf("ptah_sp_ck_child_%d", suffix)
	ddl := fmt.Sprintf(`CREATE TABLE %[1]s (a bigint NOT NULL, b bigint NOT NULL, PRIMARY KEY (a, b));
CREATE TABLE %[2]s (id bigint PRIMARY KEY, x bigint, y bigint, CONSTRAINT fk_%[2]s FOREIGN KEY (x, y) REFERENCES %[1]s (a, b));
`, parent, child)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+child+`"`)
		_, _ = conn.ExecContext(context.Background(), `DROP TABLE IF EXISTS "`+parent+`"`)
	})
	for statement := range strings.SplitSeq(strings.TrimSpace(ddl), "\n") {
		_, execErr := conn.ExecContext(ctx, strings.TrimSuffix(statement, ";"))
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"public"})
	c.Assert(err, qt.IsNil)
	c.Assert(spannerLiveConstraint(c, live, parent, "PRIMARY KEY").ColumnNames, qt.DeepEquals, []string{"a", "b"})
	foreignKey := spannerLiveConstraint(c, live, child, "FOREIGN KEY")
	c.Assert(foreignKey.ColumnNames, qt.DeepEquals, []string{"x", "y"})
	c.Assert(foreignKey.ForeignColumns, qt.DeepEquals, []string{"a", "b"})

	desired, _, err := sqlschema.Read([]byte(ddl), platform.Spanner)
	c.Assert(err, qt.IsNil)
	engine := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(ctx, &desired, live, conn.Info(), nil, engine)
	c.Assert(err, qt.IsNil)
	planned, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, engine, diff, platform.Spanner,
		planner.Options{Capabilities: conn.Info().Capabilities})
	c.Assert(err, qt.IsNil)
	for _, table := range []string{parent, child} {
		c.Assert(spannerLiveJoined(planned), qt.Not(qt.Contains), table)
	}
}

// spannerLiveConstraint is the one constraint of kind on table a read
// reported.
func spannerLiveConstraint(c *qt.C, live *catalog.Database, table, kind string) catalog.Constraint {
	c.Helper()
	var found []catalog.Constraint
	for _, constraint := range live.Constraints {
		if constraint.TableName == table && constraint.Type == kind {
			found = append(found, constraint)
		}
	}
	c.Assert(found, qt.HasLen, 1)
	return found[0]
}
