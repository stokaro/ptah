package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// notNullField is what the model keeps of one column's NOT NULL.
type notNullField struct {
	Nullable bool
	Name     string
}

// columnNotNull answers what database keeps of the column.
func columnNotNull(c *qt.C, database schemamodel.Database, column string) notNullField {
	c.Helper()
	for _, field := range database.Fields {
		if field.Name == column {
			return notNullField{Nullable: field.Nullable, Name: field.NotNullConstraintName}
		}
	}
	c.Fatalf("no column %s", column)
	return notNullField{}
}

// TestReadAddNotNull_HappyPath leaves the column as PostgreSQL 18.6 leaves it: NOT NULL,
// under the name the statement states. Naming the constraint it holds changes nothing.
func TestReadAddNotNull_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want notNullField
	}{
		{
			name: "a name",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, a text); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			want: notNullField{Name: "nn"},
		},
		{
			name: "no name",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, a text); ALTER TABLE t ADD NOT NULL a;",
			want: notNullField{},
		},
		{
			name: "a quoted name keeps its case",
			sql:  `CREATE TABLE t (id int PRIMARY KEY, a text); ALTER TABLE t ADD CONSTRAINT "Nn" NOT NULL a;`,
			want: notNullField{Name: "Nn"},
		},
		{
			name: "an unquoted name folds",
			sql:  `CREATE TABLE t (id int PRIMARY KEY, a text); ALTER TABLE t ADD CONSTRAINT NN NOT NULL a;`,
			want: notNullField{Name: "nn"},
		},
		{
			name: "the name the column holds",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			want: notNullField{Name: "nn"},
		},
		{
			name: "the name the server gave the column",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL); ALTER TABLE t ADD CONSTRAINT t_a_not_null NOT NULL a;",
			want: notNullField{Name: "t_a_not_null"},
		},
		{
			name: "SET NOT NULL keeps the name the column holds",
			sql:  "CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT nn NOT NULL); ALTER TABLE t ALTER COLUMN a SET NOT NULL;",
			want: notNullField{Name: "nn"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.IsNil)
			c.Assert(columnNotNull(c, database, "a"), qt.Equals, test.want)
		})
	}
}

// TestReadAddNotNull_FailurePath refuses what PostgreSQL 18.6 refuses: `cannot create
// not-null constraint` for a column NOT NULL under another name, `already exists` for a
// name another constraint holds, and `does not exist` for the column.
func TestReadAddNotNull_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "a column NOT NULL under the name the server gave it",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a text NOT NULL); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			wantErr: `.*the column is NOT NULL under another name`,
		},
		{
			name:    "a column NOT NULL under a name it declares",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a text CONSTRAINT m NOT NULL); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			wantErr: `.*the column is NOT NULL under another name`,
		},
		{
			name:    "a column of the primary key",
			sql:     "CREATE TABLE t (id int, a text, PRIMARY KEY (a)); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			wantErr: `.*the column is NOT NULL under another name`,
		},
		{
			name:    "a name another column holds",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a text, b text CONSTRAINT nn NOT NULL); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			wantErr: `.*another constraint of the table holds the name`,
		},
		{
			name:    "a name a check holds",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a text, CONSTRAINT nn CHECK (id > 0)); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a;",
			wantErr: `.*another constraint of the table holds the name`,
		},
		{
			name:    "a check that takes a name the clause took",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a text); ALTER TABLE t ADD CONSTRAINT nn NOT NULL a; ALTER TABLE t ADD CONSTRAINT nn CHECK (id > 0);",
			wantErr: `.*check "nn" in table "public.t" takes the name of not-null constraint "nn".*`,
		},
		{
			name:    "a column the table does not have",
			sql:     "CREATE TABLE t (id int PRIMARY KEY, a text); ALTER TABLE t ADD CONSTRAINT nn NOT NULL b;",
			wantErr: `.*names a column the table does not declare`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := sqlschema.Read([]byte(test.sql), platform.Postgres)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
