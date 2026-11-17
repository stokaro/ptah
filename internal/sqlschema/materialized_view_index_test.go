package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

// indexTablesOf maps the name of every index to the table name it records.
func indexTablesOf(database schemamodel.Database) map[string]string {
	tables := make(map[string]string, len(database.Indexes))
	for _, index := range database.Indexes {
		tables[index.Name] = index.TableName
	}
	return tables
}

const (
	// viewIndexFixture declares the materialized view mv with an index mv_a.
	viewIndexFixture = "CREATE TABLE o (a int);\nCREATE MATERIALIZED VIEW mv AS SELECT a FROM o;\nCREATE INDEX mv_a ON mv (a);\n"
	// otherSchemaViewIndexFixture declares the materialized view s2.smv with an
	// index smv_a.
	otherSchemaViewIndexFixture = "CREATE SCHEMA s2;\nCREATE TABLE o (a int);\n" +
		"CREATE MATERIALIZED VIEW s2.smv AS SELECT a FROM o;\nCREATE INDEX smv_a ON s2.smv (a);\n"
	// sameNamedTableFixture declares a table s2.mv beside the relations a
	// statement after it declares in the default schema.
	sameNamedTableFixture = "CREATE SCHEMA s2;\nCREATE TABLE s2.mv (a int);\nCREATE TABLE o (a int);\n"
)

// PostgreSQL indexes a materialized view as it indexes a table, and every
// statement that names an index reaches the view's index, in the view's
// schema. Each statement was measured on PostgreSQL 18.6. The IF EXISTS rename
// is the row a read that finds only tables' indexes gets wrong in silence: it
// skips the statement, and the index keeps its old name.
func TestRead_MaterializedViewIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want map[string]string
	}{
		{name: "DROP INDEX", sql: viewIndexFixture + "DROP INDEX mv_a;", want: make(map[string]string)},
		{name: "DROP INDEX IF EXISTS", sql: viewIndexFixture + "DROP INDEX IF EXISTS mv_a;", want: make(map[string]string)},
		{name: "ALTER INDEX RENAME TO", sql: viewIndexFixture + "ALTER INDEX mv_a RENAME TO mv_b;", want: map[string]string{"mv_b": "mv"}},
		{
			name: "ALTER INDEX IF EXISTS RENAME TO",
			sql:  viewIndexFixture + "ALTER INDEX IF EXISTS mv_a RENAME TO mv_b;",
			want: map[string]string{"mv_b": "mv"},
		},
		{
			name: "ALTER INDEX RENAME TO, by the default schema's name",
			sql:  viewIndexFixture + "ALTER INDEX public.mv_a RENAME TO mv_b;",
			want: map[string]string{"mv_b": "mv"},
		},
		{name: "DROP INDEX of another schema's view", sql: otherSchemaViewIndexFixture + "DROP INDEX s2.smv_a;", want: make(map[string]string)},
		{
			name: "ALTER INDEX RENAME TO, of another schema's view",
			sql:  otherSchemaViewIndexFixture + "ALTER INDEX s2.smv_a RENAME TO smv_b;",
			want: map[string]string{"smv_b": "s2.smv"},
		},
		{
			name: "DROP MATERIALIZED VIEW takes its indexes",
			sql:  viewIndexFixture + "CREATE INDEX o_a ON o (a);\nDROP MATERIALIZED VIEW mv;",
			want: map[string]string{"o_a": "o"},
		},
		{
			name: "CREATE INDEX on a bare name, beside another schema's table of that name",
			sql:  sameNamedTableFixture + "CREATE MATERIALIZED VIEW mv AS SELECT a FROM o;\nCREATE INDEX mv_a ON mv (a);",
			want: map[string]string{"mv_a": "mv"},
		},
		{
			name: "CREATE INDEX on a bare name of a view declared with the default schema",
			sql:  sameNamedTableFixture + "CREATE MATERIALIZED VIEW public.mv AS SELECT a FROM o;\nCREATE INDEX mv_a ON mv (a);",
			want: map[string]string{"mv_a": "public.mv"},
		},
		{
			name: "DROP INDEX of the view's index, beside another schema's table of the view's name",
			sql:  sameNamedTableFixture + "CREATE MATERIALIZED VIEW mv AS SELECT a FROM o;\nCREATE INDEX mv_a ON mv (a);\nDROP INDEX mv_a;",
			want: make(map[string]string),
		},
		{
			name: "CREATE INDEX on a bare name of a table declared with the default schema",
			sql:  "CREATE SCHEMA s2;\nCREATE TABLE s2.t (a int);\nCREATE TABLE public.t (a int);\nCREATE INDEX t_a ON t (a);",
			want: map[string]string{"t_a": "public.t"},
		},
		{
			name: "CREATE INDEX on a bare name of a table declared without a schema",
			sql:  "CREATE SCHEMA s2;\nCREATE TABLE s2.t (a int);\nCREATE TABLE t (a int);\nCREATE INDEX t_a ON t (a);",
			want: map[string]string{"t_a": "t"},
		},
		{
			name: "CREATE INDEX on a table no source of this document declares",
			sql:  "CREATE INDEX x_a ON x (a);",
			want: map[string]string{"x_a": "x"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(indexTablesOf(database), qt.DeepEquals, test.want)
		})
	}
}

// What the server refuses is refused: a name of another schema, a new name a
// relation holds, and a relation the statement's schema does not hold. Each
// was measured on PostgreSQL 18.6, whose answer each row quotes.
func TestRead_MaterializedViewIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			// index "smv_a" does not exist.
			name:    "DROP INDEX, a bare name of another schema's view",
			sql:     otherSchemaViewIndexFixture + "DROP INDEX smv_a;",
			wantErr: `line 5, column 1: DROP INDEX smv_a names an index this schema does not declare`,
		},
		{
			// relation "smv_a" does not exist.
			name:    "ALTER INDEX RENAME TO, a bare name of another schema's view",
			sql:     otherSchemaViewIndexFixture + "ALTER INDEX smv_a RENAME TO smv_b;",
			wantErr: `line 5, column 1: ALTER INDEX smv_a names an index this schema does not declare`,
		},
		{
			// index "mv_a" does not exist.
			name:    "DROP INDEX, the view's index by another schema's name",
			sql:     "CREATE SCHEMA s2;\n" + viewIndexFixture + "DROP INDEX s2.mv_a;",
			wantErr: `line 5, column 1: DROP INDEX s2.mv_a names an index this schema does not declare`,
		},
		{
			// relation "o" already exists.
			name:    "ALTER INDEX RENAME TO, onto a table",
			sql:     viewIndexFixture + "ALTER INDEX mv_a RENAME TO o;",
			wantErr: `line 4, column 1: ALTER INDEX mv_a RENAME TO o: the schema already holds a relation named o`,
		},
		{
			// relation "mv" already exists.
			name:    "ALTER INDEX RENAME TO, onto the view",
			sql:     viewIndexFixture + "ALTER INDEX mv_a RENAME TO mv;",
			wantErr: `line 4, column 1: ALTER INDEX mv_a RENAME TO mv: the schema already holds a relation named mv`,
		},
		{
			// relation "t" does not exist.
			name:    "CREATE INDEX on a bare name only another schema's table carries",
			sql:     "CREATE SCHEMA s2;\nCREATE TABLE s2.t (a int);\nCREATE INDEX t_a ON t (a);",
			wantErr: `line 3, column 1: CREATE INDEX t_a ON t names a relation its schema does not declare; s2.t is in another schema`,
		},
		{
			// relation "smv" does not exist.
			name:    "CREATE INDEX on a bare name only another schema's view carries",
			sql:     "CREATE SCHEMA s2;\nCREATE TABLE o (a int);\nCREATE MATERIALIZED VIEW s2.smv AS SELECT a FROM o;\nCREATE INDEX smv_a ON smv (a);",
			wantErr: `line 4, column 1: CREATE INDEX smv_a ON smv names a relation its schema does not declare; s2.smv is in another schema`,
		},
		{
			// relation "s3.t" does not exist.
			name:    "CREATE INDEX on a schema that does not hold the table",
			sql:     "CREATE SCHEMA s2;\nCREATE SCHEMA s3;\nCREATE TABLE s2.t (a int);\nCREATE INDEX t_a ON s3.t (a);",
			wantErr: `line 4, column 1: CREATE INDEX t_a ON s3.t names a relation its schema does not declare; s2.t is in another schema`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Indexes, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A refusal of CREATE INDEX on a relation its schema does not hold is a
// refusal of an undeclared object, which a load that read other sources beside
// the document says may be theirs.
func TestRead_CreateIndexOnAnotherSchemasRelationIsUndeclared(t *testing.T) {
	c := qt.New(t)

	_, _, err := sqlschema.Read([]byte("CREATE SCHEMA s2;\nCREATE TABLE s2.t (a int);\nCREATE INDEX t_a ON t (a);"), "postgres")

	c.Assert(err, qt.ErrorIs, sqlext.ErrUndeclared)
}

// readViewIndexOnto reads first, then second onto what first declared, as two
// files of one schema directory, and returns the indexes the two files leave.
func readViewIndexOnto(c *qt.C, first, second string) map[string]string {
	c.Helper()
	var base schemamodel.Database
	document := sqlschema.NewDocument(&base)
	earlier, _, err := sqlschema.ReadOnto([]byte(first), "postgres", document)
	c.Assert(err, qt.IsNil)
	base = earlier

	later, _, err := sqlschema.ReadOnto([]byte(second), "postgres", document)

	c.Assert(err, qt.IsNil)
	return indexTablesOf(schemamodel.Database{Indexes: append(base.Indexes, later.Indexes...)})
}

// A later file of a schema directory reaches the index an earlier file
// declared on a materialized view, beside a table of the view's name in
// another schema. The earlier file is finalized before the later one reads it,
// so an index recorded by its bare name would by then be the table's: the
// later file's statements would not find it, and the view's drop would leave
// it behind on the table.
func TestReadOnto_MaterializedViewIndex_HappyPath(t *testing.T) {
	const first = sameNamedTableFixture + "CREATE MATERIALIZED VIEW mv AS SELECT a FROM o;\nCREATE INDEX mv_a ON mv (a);\n"
	tests := []struct {
		name   string
		second string
		want   map[string]string
	}{
		{name: "DROP INDEX", second: "DROP INDEX mv_a;", want: make(map[string]string)},
		{name: "ALTER INDEX RENAME TO", second: "ALTER INDEX mv_a RENAME TO mv_b;", want: map[string]string{"mv_b": "mv"}},
		{name: "COMMENT ON INDEX", second: "COMMENT ON INDEX mv_a IS 'x';", want: map[string]string{"mv_a": "mv"}},
		{name: "DROP MATERIALIZED VIEW", second: "DROP MATERIALIZED VIEW mv;", want: make(map[string]string)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(readViewIndexOnto(c, first, test.second), qt.DeepEquals, test.want)
		})
	}
}
