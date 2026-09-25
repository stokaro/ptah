package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// columnsOf lists the columns the model carries for the table whose struct
// name the SQL table name users produces, in order.
func columnsOf(database schemamodel.Database) []string {
	var columns []string
	for _, field := range database.Fields {
		if field.StructName == usersStructName {
			columns = append(columns, field.Name)
		}
	}
	return columns
}

// usersStructName is the struct name the reader gives the table users. It is
// read back from a declaration rather than spelled here, so a change in how
// struct names are derived does not quietly empty every assertion below.
var usersStructName = func() string {
	database, _, err := sqlschema.Read([]byte("CREATE TABLE users (id uuid PRIMARY KEY);"), "postgres")
	if err != nil || len(database.Tables) != 1 {
		panic("the users fixture does not read as one table")
	}
	return database.Tables[0].StructName
}()

// A column added by ALTER TABLE ... ADD COLUMN reaches the table. The statement
// parsed and was then dropped, so a schema file that declared a column this way
// rendered, planned and compared as though the column did not exist
// (stokaro/ptah#3562).
func TestRead_AlterTableAddColumnReachesTheTable_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		sql         string
		wantColumns []string
	}{
		{
			name:        "ADD COLUMN",
			sql:         "CREATE TABLE users (id uuid PRIMARY KEY);\nALTER TABLE users ADD COLUMN note text;",
			wantColumns: []string{"id", "note"},
		},
		{
			name:        "ADD COLUMN IF NOT EXISTS of a new column",
			sql:         "CREATE TABLE users (id uuid PRIMARY KEY);\nALTER TABLE users ADD COLUMN IF NOT EXISTS note text;",
			wantColumns: []string{"id", "note"},
		},
		{
			// The server skips the second declaration; it says the same thing,
			// so the schema is the same.
			name:        "ADD COLUMN IF NOT EXISTS of a column declared the same way",
			sql:         "CREATE TABLE users (id uuid PRIMARY KEY, note text);\nALTER TABLE users ADD COLUMN IF NOT EXISTS note text;",
			wantColumns: []string{"id", "note"},
		},
		{
			name: "columns keep the order the document adds them in",
			sql: "CREATE TABLE users (id uuid PRIMARY KEY);\n" +
				"ALTER TABLE users ADD COLUMN IF NOT EXISTS b text;\nALTER TABLE users ADD COLUMN a text;",
			wantColumns: []string{"id", "b", "a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert(columnsOf(database), qt.DeepEquals, test.wantColumns)
		})
	}
}

func TestRead_AlterTableAddColumnReachesTheTable_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "a column the table already declares, unguarded",
			sql:     "CREATE TABLE users (id uuid PRIMARY KEY, note text);\nALTER TABLE users ADD COLUMN note text;",
			wantErr: `ALTER TABLE users ADD COLUMN note names a column the table already declares`,
		},
		{
			// Keeping either definition would drop what the other says.
			name: "a guarded column declared differently",
			sql:  "CREATE TABLE users (id uuid PRIMARY KEY, note text);\nALTER TABLE users ADD COLUMN IF NOT EXISTS note varchar(10);",
			wantErr: `ALTER TABLE users ADD COLUMN IF NOT EXISTS note declares the column differently from the ` +
				`table's own declaration; .*`,
		},
		{
			name:    "a table the document does not declare",
			sql:     "ALTER TABLE ghosts ADD COLUMN note text;",
			wantErr: `the schema model has no place for this statement: ALTER TABLE ghosts ADD COLUMN note names a table this schema does not declare`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.sql), "postgres")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Fields, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// ReadOnto reads one file of a schema directory against what the earlier files
// declared: a later file may add a column to a table an earlier one created,
// and the result is only what this file contributes.
func TestReadOnto_AddsAColumnToAnEarlierTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE users (id uuid PRIMARY KEY, note text);"), "postgres")
	c.Assert(err, qt.IsNil)

	database, _, err := sqlschema.ReadOnto([]byte(
		"ALTER TABLE users ADD COLUMN IF NOT EXISTS password_changed_at timestamptz;\n"+
			"ALTER TABLE users ADD COLUMN IF NOT EXISTS note text;",
	), "postgres", &earlier)

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 0)
	c.Assert(columnsOf(database), qt.DeepEquals, []string{"password_changed_at"})
}

func TestReadOnto_AddsAColumnToAnEarlierTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE users (id uuid PRIMARY KEY, note text);"), "postgres")
	c.Assert(err, qt.IsNil)

	database, _, err := sqlschema.ReadOnto([]byte("ALTER TABLE users ADD COLUMN note text;"), "postgres", &earlier)

	c.Assert(err, qt.ErrorMatches, `ALTER TABLE users ADD COLUMN note names a column the table already declares`)
	c.Assert(database.Fields, qt.HasLen, 0)
}

// The routine attributes a SQL document states reach the model. Parsed and not
// carried, they would compare as the defaults and plan the routine's
// replacement on every run.
func TestRead_RoutineAttributesReachTheModel(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE FUNCTION add_arrays(a integer[], b integer[]) RETURNS integer[] "+
			"LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE LEAKPROOF AS $$ SELECT a || b $$;",
	), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Functions, qt.HasLen, 1)
	c.Assert(database.Functions[0].Strict, qt.IsTrue)
	c.Assert(database.Functions[0].Parallel, qt.Equals, "SAFE")
	c.Assert(database.Functions[0].Leakproof, qt.IsTrue)
}
