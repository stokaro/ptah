package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// indexShape is what an index rename can change on table c: the indexes of
// the database, its constraints, the primary key's name, and whether column b
// keeps a UNIQUE of its own.
type indexShape struct {
	Indexes     []string
	Constraints []string
	PrimaryKey  string
	BUnique     bool
}

// indexShapeOf reads indexShape off the database.
func indexShapeOf(c *qt.C, database schemamodel.Database) indexShape {
	c.Helper()
	shape := indexShape{
		Indexes:     indexNamesOf(database),
		Constraints: constraintNamesOf(database),
		BUnique:     columnOfC(c, database, "b").Unique,
	}
	for _, table := range database.Tables {
		if table.Name == "c" {
			shape.PrimaryKey = table.PrimaryKeyName
		}
	}
	return shape
}

// ALTER INDEX renames what the name reaches, as PostgreSQL does. Measured on
// PostgreSQL 18.6, each file applies, and Atlas CE v1.3.0 reports it synced
// with the database it builds (stokaro/ptah#3879). The index behind a
// constraint renames the constraint, and a column's own UNIQUE becomes a
// UNIQUE constraint of the new name, as RENAME CONSTRAINT makes it.
func TestRead_AlterIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want indexShape
	}{
		{
			name: "a declared index",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO other;",
			want: indexShape{Indexes: []string{"other"}},
		},
		{
			name: "IF EXISTS on an index that exists",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX IF EXISTS ix RENAME TO other;",
			want: indexShape{Indexes: []string{"other"}},
		},
		{
			name: "IF EXISTS on an index nothing declares",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX IF EXISTS nope RENAME TO other;",
			want: indexShape{Indexes: []string{"ix"}},
		},
		{
			name: "an index of another schema, named by its schema",
			sql: "CREATE SCHEMA app;\nCREATE TABLE app.c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON app.c (b);\n" +
				"ALTER INDEX app.ix RENAME TO other;",
			want: indexShape{Indexes: []string{"other"}},
		},
		{
			name: "an index of a bare table, named by public",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX public.ix RENAME TO other;",
			want: indexShape{Indexes: []string{"other"}},
		},
		{
			name: "a column's own UNIQUE",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER INDEX c_b_key RENAME TO other;",
			want: indexShape{Constraints: []string{"other"}},
		},
		{
			name: "a column's own UNIQUE under the next name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, y int);\nCREATE UNIQUE INDEX c_b_key ON c (y);\n" +
				"ALTER TABLE c ADD COLUMN b int UNIQUE;\nALTER INDEX c_b_key1 RENAME TO other;",
			want: indexShape{Indexes: []string{"c_b_key"}, Constraints: []string{"other"}},
		},
		{
			name: "a named UNIQUE",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, CONSTRAINT uq UNIQUE (b));\nALTER INDEX uq RENAME TO other;",
			want: indexShape{Constraints: []string{"other"}},
		},
		{
			name: "the primary key under the name PostgreSQL gives it",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int);\nALTER INDEX c_pkey RENAME TO c_pk;",
			want: indexShape{PrimaryKey: "c_pk"},
		},
		{
			name: "a named primary key",
			sql:  "CREATE TABLE c (id int, b int, CONSTRAINT pk PRIMARY KEY (id));\nALTER INDEX pk RENAME TO pk2;",
			want: indexShape{PrimaryKey: "pk2"},
		},
		{
			name: "an EXCLUDE",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int, CONSTRAINT ex EXCLUDE USING gist (b WITH =));\nALTER INDEX ex RENAME TO ex2;",
			want: indexShape{Constraints: []string{"ex2"}},
		},
		{
			name: "a plain index onto the name of a CHECK of its table",
			sql:  "CREATE TABLE c (id int PRIMARY KEY, b int CONSTRAINT ck CHECK (b > 0));\nCREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO ck;",
			want: indexShape{Indexes: []string{"ck"}},
		},
		{
			name: "a plain index onto the name of a NOT NULL of its table",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int, d int CONSTRAINT nn NOT NULL);\n" +
				"CREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO nn;",
			want: indexShape{Indexes: []string{"nn"}},
		},
		{
			name: "a key onto the name of a CHECK of another table",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\n" +
				"CREATE TABLE e (id int PRIMARY KEY, y int CONSTRAINT ck CHECK (y > 0));\nALTER INDEX c_b_key RENAME TO ck;",
			want: indexShape{Constraints: []string{"ck"}},
		},
		{
			name: "onto the name of an enum, which is not a relation",
			sql: "CREATE TYPE mood AS ENUM ('a');\nCREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\n" +
				"ALTER INDEX ix RENAME TO mood;",
			want: indexShape{Indexes: []string{"mood"}},
		},
		{
			name: "onto the name of a table of another schema",
			sql: "CREATE SCHEMA app;\nCREATE TABLE app.t (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, b int);\n" +
				"CREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO t;",
			want: indexShape{Indexes: []string{"t"}},
		},
		{
			name: "a renamed key dropped under its new name",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nALTER INDEX c_b_key RENAME TO other;\n" +
				"ALTER TABLE c DROP CONSTRAINT other;",
			want: indexShape{},
		},
		{
			name: "the old name taken again",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO other;\n" +
				"CREATE INDEX ix ON c (id, b);",
			want: indexShape{Indexes: []string{"other", "ix"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(indexShapeOf(c, database), qt.DeepEquals, test.want, qt.Commentf("%+v", indexShapeOf(c, database)))
		})
	}
}

// A rename the server refuses is refused. Measured on PostgreSQL 18.6: a name
// no index of the schema holds answers `relation "x" does not exist`; a new
// name another relation of the schema holds, `relation "x" already exists`;
// and the index behind a constraint renamed onto the name of another
// constraint of its table, `constraint "x" for relation "c" already exists`.
func TestRead_AlterIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "an index nothing declares",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int);\nALTER INDEX nope RENAME TO other;",
			wantErr: `ALTER INDEX nope names an index this schema does not declare`,
		},
		{
			name: "an index of another schema, named bare",
			sql: "CREATE SCHEMA app;\nCREATE TABLE app.c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON app.c (b);\n" +
				"ALTER INDEX ix RENAME TO other;",
			wantErr: `ALTER INDEX ix names an index this schema does not declare`,
		},
		{
			name:    "onto a table",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO c;",
			wantErr: `ALTER INDEX ix RENAME TO c: the schema already holds a relation named c`,
		},
		{
			name:    "onto its own name",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO ix;",
			wantErr: `ALTER INDEX ix RENAME TO ix: the schema already holds a relation named ix`,
		},
		{
			name: "onto another index",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int, d int);\nCREATE INDEX ix ON c (b);\nCREATE INDEX iy ON c (d);\n" +
				"ALTER INDEX ix RENAME TO iy;",
			wantErr: `ALTER INDEX ix RENAME TO iy: the schema already holds a relation named iy`,
		},
		{
			name:    "onto a column's own UNIQUE",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE, d int);\nCREATE INDEX ix ON c (d);\nALTER INDEX ix RENAME TO c_b_key;",
			wantErr: `ALTER INDEX ix RENAME TO c_b_key: the schema already holds a relation named c_b_key`,
		},
		{
			name: "onto the own UNIQUE of a column of another table",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE TABLE e (id int PRIMARY KEY, y int UNIQUE);\n" +
				"CREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO e_y_key;",
			wantErr: `ALTER INDEX ix RENAME TO e_y_key: the schema already holds a relation named e_y_key`,
		},
		{
			name:    "onto the primary key",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO c_pkey;",
			wantErr: `ALTER INDEX ix RENAME TO c_pkey: the schema already holds a relation named c_pkey`,
		},
		{
			name:    "onto the sequence of a serial column",
			sql:     "CREATE TABLE c (id serial PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\nALTER INDEX ix RENAME TO c_id_seq;",
			wantErr: `ALTER INDEX ix RENAME TO c_id_seq: the schema already holds a relation named c_id_seq`,
		},
		{
			name: "onto the sequence of an identity column",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int, d int GENERATED ALWAYS AS IDENTITY);\nCREATE INDEX ix ON c (b);\n" +
				"ALTER INDEX ix RENAME TO c_d_seq;",
			wantErr: `ALTER INDEX ix RENAME TO c_d_seq: the schema already holds a relation named c_d_seq`,
		},
		{
			name: "onto a sequence",
			sql: "CREATE SEQUENCE s;\nCREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\n" +
				"ALTER INDEX ix RENAME TO s;",
			wantErr: `ALTER INDEX ix RENAME TO s: the schema already holds a relation named s`,
		},
		{
			name: "onto a view",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE VIEW v AS SELECT id FROM c;\nCREATE INDEX ix ON c (b);\n" +
				"ALTER INDEX ix RENAME TO v;",
			wantErr: `ALTER INDEX ix RENAME TO v: the schema already holds a relation named v`,
		},
		{
			name: "onto a composite type",
			sql: "CREATE TYPE pair AS (a int, b int);\nCREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\n" +
				"ALTER INDEX ix RENAME TO pair;",
			wantErr: `ALTER INDEX ix RENAME TO pair: the schema already holds a relation named pair`,
		},
		{
			name: "onto a table declared as public's",
			sql: "CREATE TABLE public.t (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, b int);\nCREATE INDEX ix ON c (b);\n" +
				"ALTER INDEX ix RENAME TO t;",
			wantErr: `ALTER INDEX ix RENAME TO t: the schema already holds a relation named t`,
		},
		{
			name:    "a key onto a CHECK of its table",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int CONSTRAINT ck CHECK (b > 0) UNIQUE);\nALTER INDEX c_b_key RENAME TO ck;",
			wantErr: `ALTER INDEX c_b_key RENAME TO ck: table c already holds a constraint named ck`,
		},
		{
			name:    "a key onto a NOT NULL of its table",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE, d int CONSTRAINT nn NOT NULL);\nALTER INDEX c_b_key RENAME TO nn;",
			wantErr: `ALTER INDEX c_b_key RENAME TO nn: table c already holds a constraint named nn`,
		},
		{
			name:    "the primary key onto a foreign key of its table",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, b int, p_id int REFERENCES c (id));\nALTER INDEX c_pkey RENAME TO c_p_id_fkey;",
			wantErr: `ALTER INDEX c_pkey RENAME TO c_p_id_fkey: table c already holds a constraint named c_p_id_fkey`,
		},
		{
			name: "a named UNIQUE onto a CHECK the table declares",
			sql: "CREATE TABLE c (id int PRIMARY KEY, b int, CONSTRAINT uq UNIQUE (b), CONSTRAINT ck2 CHECK (b > 0));\n" +
				"ALTER INDEX uq RENAME TO ck2;",
			wantErr: `ALTER INDEX uq RENAME TO ck2: table c already holds a constraint named ck2`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
		})
	}
}

// A schema directory reads its files in order, so a later file renames an
// index an earlier one declared. The UNIQUE a column's key becomes is the
// later file's, as every object a file adds is.
func TestReadOnto_AlterIndexRenamesAnEarlierIndex(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nCREATE INDEX ix ON c (b);"), "postgres")
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte("ALTER INDEX ix RENAME TO other;\nALTER INDEX c_b_key RENAME TO b_uq;"), "postgres",
		sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.IsNil)
	c.Assert(indexShapeOf(c, earlier), qt.DeepEquals, indexShape{Indexes: []string{"other"}})
	c.Assert(constraintNamesOf(later), qt.DeepEquals, []string{"b_uq"})
}
