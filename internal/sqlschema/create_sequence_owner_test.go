package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/internal/sqlschema"
)

// CREATE SEQUENCE ... OWNED BY records the owner a read of the database
// reports: `table.column` of a table in the sequence's own schema, each name
// as the server stores it, however many parts the clause writes.
func TestRead_CreateSequenceOwnedBy_HappyPath(t *testing.T) {
	const table = "CREATE TABLE t (id bigint NOT NULL);\n"
	for _, test := range []struct {
		name, sql, want string
	}{
		{
			name: "a table and its column",
			sql:  table + "CREATE SEQUENCE s OWNED BY t.id;",
			want: "t.id",
		},
		{
			name: "the default schema written on the owner",
			sql:  table + "CREATE SEQUENCE s OWNED BY public.t.id;",
			want: "t.id",
		},
		{
			name: "the default schema written on both",
			sql:  "CREATE TABLE public.t (id bigint NOT NULL);\nCREATE SEQUENCE public.s OWNED BY public.t.id;",
			want: "t.id",
		},
		{
			name: "another schema written on both",
			sql:  "CREATE SCHEMA app;\nCREATE TABLE app.u (id bigint NOT NULL);\nCREATE SEQUENCE app.s OWNED BY app.u.id;",
			want: "u.id",
		},
		{
			name: "quoted parts",
			sql:  "CREATE TABLE \"T\" (\"Id\" bigint NOT NULL);\nCREATE SEQUENCE \"S\" OWNED BY \"public\".\"T\".\"Id\";",
			want: "T.Id",
		},
		{
			name: "unquoted parts, folded",
			sql:  table + "CREATE SEQUENCE s OWNED BY PUBLIC.T.Id;",
			want: "t.id",
		},
		{
			name: "a table qualified with the default schema, the owner without",
			sql:  "CREATE TABLE public.t (id bigint NOT NULL);\nCREATE SEQUENCE s OWNED BY t.id;",
			want: "t.id",
		},
		{
			name: "a table whose name holds a dot",
			sql:  "CREATE TABLE \"a.b\" (id bigint NOT NULL);\nCREATE SEQUENCE s OWNED BY \"a.b\".id;",
			want: `"a.b".id`,
		},
		{
			name: "a column whose name holds a dot",
			sql:  "CREATE TABLE a (id bigint NOT NULL, \"x.y\" int);\nCREATE SEQUENCE s OWNED BY a.\"x.y\";",
			want: `a."x.y"`,
		},
		{
			name: "no owner",
			sql:  table + "CREATE SEQUENCE s OWNED BY NONE;",
			want: "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := readModel(c, platform.Postgres, test.sql)

			c.Assert(database.Sequences, qt.HasLen, 1)
			c.Assert(database.Sequences[0].OwnedBy, qt.Equals, test.want)
		})
	}
}

// A CREATE SEQUENCE IF NOT EXISTS of a sequence the file declares already is
// skipped, and the server does not read its OWNED BY, so a clause it would
// refuse is not refused. Measured on PostgreSQL 18.6 and CockroachDB v25.4.20.
// The two reads skip the statement at different steps: PostgreSQL's where the
// stored-name pass decides what the server skips, CockroachDB's where the
// sequence would be recorded.
func TestRead_SkippedCreateSequenceOwnedBy_HappyPath(t *testing.T) {
	for _, dialect := range []string{platform.Postgres, platform.CockroachDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			database := readModel(c, dialect, "CREATE SEQUENCE s;\nCREATE SEQUENCE IF NOT EXISTS s OWNED BY id;")

			c.Assert(database.Sequences, qt.HasLen, 1)
			c.Assert(database.Sequences[0].OwnedBy, qt.Equals, "")
		})
	}
}

// A sequence CREATE SEQUENCE gives its serial column with a three-part owner
// folds into the column, as the one ALTER SEQUENCE ... OWNED BY gives does.
func TestRead_CreateSequenceOwnedByFoldsIntoItsColumn_HappyPath(t *testing.T) {
	c := qt.New(t)

	database := readModel(c, platform.Postgres, `CREATE TABLE public.t (id integer NOT NULL);
CREATE SEQUENCE public.t_id_seq AS integer OWNED BY public.t.id;
ALTER TABLE ONLY public.t ALTER COLUMN id SET DEFAULT nextval('public.t_id_seq'::regclass);`)

	c.Assert(database.Sequences, qt.IsNil)
	c.Assert(database.Fields, qt.HasLen, 1)
	c.Assert(database.Fields[0].Type, qt.Equals, "serial")
	c.Assert(database.Fields[0].DefaultExpr, qt.Equals, "")
}

// An OWNED BY clause the server refuses is refused, with the reason.
func TestRead_CreateSequenceOwnedBy_FailurePath(t *testing.T) {
	const tables = "CREATE SCHEMA app;\nCREATE TABLE t (id bigint NOT NULL);\nCREATE TABLE app.u (id bigint NOT NULL);\n"
	for _, test := range []struct {
		name, sql, wantErr string
	}{
		{
			name:    "a column with no table",
			sql:     tables + "CREATE SEQUENCE s OWNED BY id;",
			wantErr: `.*CREATE SEQUENCE s OWNED BY id: OWNED BY names a column of a table, or NONE`,
		},
		{
			name:    "a table in another schema",
			sql:     tables + "CREATE SEQUENCE s OWNED BY app.u.id;",
			wantErr: `.*CREATE SEQUENCE s OWNED BY app.u.id: a sequence is owned by a column of a table in its own schema`,
		},
		{
			name:    "a table of the default schema for a sequence in another",
			sql:     tables + "CREATE SEQUENCE app.s OWNED BY t.id;",
			wantErr: `.*CREATE SEQUENCE app.s OWNED BY t.id: a sequence is owned by a column of a table in its own schema`,
		},
		{
			name:    "a column a database qualifies",
			sql:     tables + "CREATE SEQUENCE s OWNED BY db.public.t.id;",
			wantErr: `.*CREATE SEQUENCE s OWNED BY db.public.t.id: OWNED BY names a column as table.column or schema.table.column`,
		},
		{
			name:    "a table the schema does not declare",
			sql:     tables + "CREATE SEQUENCE s OWNED BY nosuch.id;",
			wantErr: `.*CREATE SEQUENCE s OWNED BY nosuch.id names a table this schema does not declare`,
		},
		{
			name:    "a column the table does not declare",
			sql:     tables + "CREATE SEQUENCE s OWNED BY t.nope;",
			wantErr: `.*CREATE SEQUENCE s OWNED BY t.nope names a column the table does not declare`,
		},
		{
			name:    "a table the file creates after the sequence",
			sql:     tables + "CREATE SEQUENCE s OWNED BY later.id;\nCREATE TABLE later (id bigint NOT NULL);",
			wantErr: `.*CREATE SEQUENCE s OWNED BY later.id names a table this schema does not declare`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// An owner in a table the load does not declare is refused as a statement that
// changes an undeclared object, as ALTER SEQUENCE ... OWNED BY refuses it, so a
// load that reads other sources beside the file can say the table may be
// theirs.
func TestRead_CreateSequenceOwnedByAnUndeclaredTable_FailurePath(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte("CREATE SEQUENCE s OWNED BY nosuch.id;"), platform.Postgres)

	c.Assert(err, qt.ErrorIs, sqlext.ErrUndeclared)
	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(database, qt.DeepEquals, schemamodel.Database{})
}
