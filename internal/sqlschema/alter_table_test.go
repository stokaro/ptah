package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

const alterFixture = "CREATE TABLE p (id int PRIMARY KEY);\n" +
	"CREATE TABLE t (id int NOT NULL, a int, b text, c int DEFAULT 3);\n"

// fieldOf returns the column name of the table t, as the model carries it.
func fieldOf(c *qt.C, database schemamodel.Database, name string) schemamodel.Field {
	c.Helper()
	var found []schemamodel.Field
	for _, field := range database.Fields {
		if field.StructName == tStructName && field.Name == name {
			found = append(found, field)
		}
	}
	c.Assert(found, qt.HasLen, 1, qt.Commentf("column %s", name))
	return found[0]
}

// columnNamesOf lists the columns of table t, in order.
func columnNamesOf(database schemamodel.Database) []string {
	var names []string
	for _, field := range database.Fields {
		if field.StructName == tStructName {
			names = append(names, field.Name)
		}
	}
	return names
}

var tStructName = func() string {
	database, _, err := sqlschema.Read([]byte("CREATE TABLE t (id int);"), "postgres")
	if err != nil || len(database.Tables) != 1 {
		panic("the t fixture does not read as one table")
	}
	return database.Tables[0].StructName
}()

func readAlter(c *qt.C, alter string) schemamodel.Database {
	c.Helper()
	database, _, err := sqlschema.Read([]byte(alterFixture+alter), "postgres")
	c.Assert(err, qt.IsNil)
	return database
}

// Each ALTER COLUMN action changes the one property it names.
func TestRead_AlterColumnChangesTheColumn(t *testing.T) {
	c := qt.New(t)

	database := readAlter(c, "ALTER TABLE t ALTER COLUMN a SET DEFAULT 5, ALTER COLUMN a SET NOT NULL,"+
		" ALTER COLUMN b TYPE varchar(20) USING b::varchar(20), ALTER COLUMN c DROP DEFAULT,"+
		" ALTER COLUMN id DROP NOT NULL, ALTER b SET DEFAULT now();")

	a, b, cc, id := fieldOf(c, database, "a"), fieldOf(c, database, "b"), fieldOf(c, database, "c"), fieldOf(c, database, "id")
	c.Assert([]any{a.Default, a.DefaultSet, a.Nullable}, qt.DeepEquals, []any{"5", true, false})
	c.Assert([]any{b.Type, b.DefaultExpr, b.Default}, qt.DeepEquals, []any{"varchar(20)", "now()", ""})
	c.Assert([]any{cc.Default, cc.DefaultSet, cc.DefaultExpr}, qt.DeepEquals, []any{"", false, ""})
	c.Assert(id.Nullable, qt.IsTrue)
}

// DROP COLUMN and RENAME COLUMN reshape the table; the primary key follows a
// rename.
func TestRead_DropAndRenameColumn(t *testing.T) {
	tests := []struct {
		name        string
		alter       string
		wantColumns []string
		wantPK      []string
	}{
		{name: "DROP COLUMN", alter: "ALTER TABLE t DROP COLUMN b;", wantColumns: []string{"id", "a", "c"}},
		{name: "DROP without COLUMN", alter: "ALTER TABLE t DROP b;", wantColumns: []string{"id", "a", "c"}},
		{name: "DROP COLUMN IF EXISTS of an absent column", alter: "ALTER TABLE t DROP COLUMN IF EXISTS zz;", wantColumns: []string{"id", "a", "b", "c"}},
		{name: "RENAME COLUMN", alter: "ALTER TABLE t RENAME COLUMN b TO note;", wantColumns: []string{"id", "a", "note", "c"}},
		{
			name:        "RENAME a primary key column",
			alter:       "ALTER TABLE t ADD PRIMARY KEY (id, a);\nALTER TABLE t RENAME id TO t_id;",
			wantColumns: []string{"t_id", "a", "b", "c"},
			wantPK:      []string{"t_id", "a"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := readAlter(c, test.alter)

			c.Assert(columnNamesOf(database), qt.DeepEquals, test.wantColumns)
			c.Assert(database.Tables[1].PrimaryKey, qt.DeepEquals, test.wantPK)
		})
	}
}

// Constraints are dropped and renamed by the name the declaration gave them,
// wherever the model keeps that name.
func TestRead_DropAndRenameConstraint(t *testing.T) {
	tests := []struct {
		name            string
		alter           string
		wantConstraints []string
		wantCheckName   string
	}{
		{
			name:            "DROP CONSTRAINT of a table constraint",
			alter:           "ALTER TABLE t ADD CONSTRAINT t_a_ck CHECK (a > 0), ADD CONSTRAINT t_uq UNIQUE (b);\nALTER TABLE t DROP CONSTRAINT t_a_ck;",
			wantConstraints: []string{"t_uq"},
		},
		{
			name:            "RENAME CONSTRAINT",
			alter:           "ALTER TABLE t ADD CONSTRAINT t_uq UNIQUE (b);\nALTER TABLE t RENAME CONSTRAINT t_uq TO t_b_uq;",
			wantConstraints: []string{"t_b_uq"},
		},
		{
			name:  "DROP CONSTRAINT of a column's foreign key",
			alter: "CREATE TABLE q (id int PRIMARY KEY, p_id int CONSTRAINT q_p_fk REFERENCES p (id));\nALTER TABLE q DROP CONSTRAINT q_p_fk;",
		},
		{
			name:          "RENAME CONSTRAINT of a column's CHECK",
			alter:         "CREATE TABLE q (id int PRIMARY KEY, n int CONSTRAINT q_n_ck CHECK (n > 0));\nALTER TABLE q RENAME CONSTRAINT q_n_ck TO q_n_positive;",
			wantCheckName: "q_n_positive",
		},
		{
			name:  "DROP CONSTRAINT IF EXISTS of a name nothing carries",
			alter: "ALTER TABLE t DROP CONSTRAINT IF EXISTS nope;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database := readAlter(c, test.alter)

			var names []string
			for _, constraint := range database.Constraints {
				names = append(names, constraint.Name)
			}
			var checkNames, foreignKeys []string
			for _, field := range database.Fields {
				checkNames = append(checkNames, field.CheckName)
				foreignKeys = append(foreignKeys, field.Foreign)
			}
			c.Assert(names, qt.DeepEquals, test.wantConstraints)
			c.Assert(checkNames, qt.Contains, test.wantCheckName)
			c.Assert(foreignKeys, qt.Not(qt.Contains), "p(id)")
		})
	}
}

// A primary key added later lands on the table, and DROP PRIMARY KEY takes it
// away.
func TestRead_AddAndDropPrimaryKey(t *testing.T) {
	tests := []struct {
		name   string
		sql    string
		wantPK []string
	}{
		{name: "ADD PRIMARY KEY", sql: "ALTER TABLE t ADD PRIMARY KEY (id, a);", wantPK: []string{"id", "a"}},
		{name: "DROP PRIMARY KEY", sql: "ALTER TABLE t ADD PRIMARY KEY (id, a);\nALTER TABLE t DROP PRIMARY KEY;"},
		{name: "DROP CONSTRAINT of the named key", sql: "ALTER TABLE t ADD CONSTRAINT t_pk PRIMARY KEY (id, a);\nALTER TABLE t DROP CONSTRAINT t_pk;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(alterFixture+test.sql), "mysql")

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables[1].PrimaryKey, qt.DeepEquals, test.wantPK)
		})
	}
}

// MySQL's MODIFY replaces the definition, so a default it does not restate is
// gone; SQL Server's ALTER COLUMN changes the type and nullability and keeps
// the default, which is a constraint of its own there.
func TestRead_ModifyColumn(t *testing.T) {
	tests := []struct {
		name         string
		dialect      string
		sql          string
		wantType     string
		wantNullable bool
		wantDefault  string
	}{
		{name: "MySQL MODIFY", dialect: "mysql", sql: "ALTER TABLE t MODIFY c bigint NOT NULL;", wantType: "bigint"},
		{name: "SQL Server ALTER COLUMN", dialect: "sqlserver", sql: "ALTER TABLE t ALTER COLUMN c bigint NULL;", wantType: "bigint", wantNullable: true, wantDefault: "3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(alterFixture+test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			field := fieldOf(c, database, "c")
			c.Assert([]any{field.Type, field.Nullable, field.Default}, qt.DeepEquals,
				[]any{test.wantType, test.wantNullable, test.wantDefault})
		})
	}
}

// What the model cannot hold, or what the server would refuse, is refused by
// name.
func TestRead_AlterTable_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{
			name:    "a table no file declares",
			sql:     "ALTER TABLE ghost ADD CONSTRAINT g_uq UNIQUE (a);",
			wantErr: `the schema model has no place for this statement: ALTER TABLE ghost ADD CONSTRAINT names a table this schema does not declare`,
		},
		{
			name:    "a column the table does not declare",
			sql:     "ALTER TABLE t ALTER COLUMN zz SET NOT NULL;",
			wantErr: `ALTER TABLE t ALTER COLUMN zz names a column the table does not declare`,
		},
		{
			// PostgreSQL 18.6: column "id" is in a primary key.
			name:    "DROP NOT NULL of a primary key column",
			sql:     "ALTER TABLE p ALTER COLUMN id DROP NOT NULL;",
			wantErr: `ALTER TABLE p ALTER COLUMN id DROP NOT NULL: the column is in the primary key`,
		},
		{
			name:    "a second primary key",
			sql:     "ALTER TABLE p ADD PRIMARY KEY (id);",
			wantErr: `ALTER TABLE p ADD PRIMARY KEY: the table already declares a primary key`,
		},
		{
			name:    "DROP COLUMN of an absent column",
			sql:     "ALTER TABLE t DROP COLUMN zz;",
			wantErr: `ALTER TABLE t DROP COLUMN zz names a column the table does not declare`,
		},
		{
			name:    "DROP COLUMN CASCADE",
			sql:     "ALTER TABLE t DROP COLUMN b CASCADE;",
			wantErr: `ALTER TABLE t DROP COLUMN b CASCADE also drops the objects that depend on the column, which a schema file does not list; drop them by name`,
		},
		{
			name:    "DROP COLUMN of a primary key column",
			sql:     "ALTER TABLE p DROP COLUMN id;",
			wantErr: `ALTER TABLE p DROP COLUMN id: the column is in the primary key`,
		},
		{
			name:    "DROP COLUMN a constraint names",
			sql:     "ALTER TABLE t ADD CONSTRAINT t_ck CHECK (a > 0);\nALTER TABLE t DROP COLUMN a;",
			wantErr: `ALTER TABLE t DROP COLUMN a: check constraint t_ck still refers to the column`,
		},
		{
			name:    "DROP COLUMN an index names",
			sql:     "CREATE INDEX t_b_idx ON t (lower(b));\nALTER TABLE t DROP COLUMN b;",
			wantErr: `ALTER TABLE t DROP COLUMN b: index t_b_idx still refers to the column`,
		},
		{
			name:    "DROP COLUMN another table's foreign key references",
			sql:     "CREATE TABLE r (id int PRIMARY KEY, t_a int REFERENCES t (a));\nALTER TABLE t DROP COLUMN a;",
			wantErr: `ALTER TABLE t DROP COLUMN a: the foreign key on column t_a still refers to the column`,
		},
		{
			name:    "DROP COLUMN a generated column reads",
			sql:     "CREATE TABLE g (id int PRIMARY KEY, a int, twice int GENERATED ALWAYS AS (a * 2) STORED);\nALTER TABLE g DROP COLUMN a;",
			wantErr: `ALTER TABLE g DROP COLUMN a: column twice still refers to the column`,
		},
		{
			name:    "RENAME COLUMN a constraint names",
			sql:     "ALTER TABLE t ADD CONSTRAINT t_uq UNIQUE (b);\nALTER TABLE t RENAME COLUMN b TO note;",
			wantErr: `ALTER TABLE t RENAME COLUMN b: unique constraint t_uq names the column, and a schema file keeps its text; declare the column under its new name`,
		},
		{
			name:    "RENAME COLUMN onto a declared name",
			sql:     "ALTER TABLE t RENAME COLUMN a TO b;",
			wantErr: `ALTER TABLE t RENAME COLUMN a TO b: the table already declares b`,
		},
		{
			name:    "RENAME TO",
			sql:     "ALTER TABLE t RENAME TO t2;",
			wantErr: `the schema model has no place for this statement: ALTER TABLE t RENAME TO t2: every other declaration names the table by its old name; declare the table under its new name`,
		},
		{
			// The name a server generates for an unnamed constraint is not in
			// the model.
			name:    "DROP CONSTRAINT of a name nothing carries",
			sql:     "ALTER TABLE t DROP CONSTRAINT t_a_check;",
			wantErr: `ALTER TABLE t DROP CONSTRAINT t_a_check names a constraint this schema does not declare by that name`,
		},
		{
			name:    "RENAME CONSTRAINT of a name nothing carries",
			sql:     "ALTER TABLE t RENAME CONSTRAINT x TO y;",
			wantErr: `ALTER TABLE t RENAME CONSTRAINT x names a constraint this schema does not declare by that name`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(alterFixture+test.sql), "postgres")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// ReadOnto applies a later file's ALTER to what an earlier file declared, in
// place, and returns only what the later file adds.
func TestReadOnto_AltersAnEarlierTable(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte(alterFixture), "postgres")
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte(
		"ALTER TABLE t ADD PRIMARY KEY (id), ALTER COLUMN a SET DEFAULT 5, DROP COLUMN b, ADD COLUMN d int;",
	), "postgres", &earlier)

	c.Assert(err, qt.IsNil)
	c.Assert(columnNamesOf(later), qt.DeepEquals, []string{"d"})
	c.Assert(columnNamesOf(earlier), qt.DeepEquals, []string{"id", "a", "c"})
	c.Assert(fieldOf(c, earlier, "a").Default, qt.Equals, "5")
	c.Assert(fieldOf(c, earlier, "id").Primary, qt.IsTrue)
}

// An operation the conversion has no rule for is refused rather than dropped.
// The parser produces none today; a statement list built another way can.
func TestToDatabase_RefusesAnAlterOperationItDoesNotModel(t *testing.T) {
	c := qt.New(t)
	table := ast.NewCreateTable("t")
	table.AddColumn(ast.NewColumn("id", "int"))
	statements := &ast.StatementList{Statements: []ast.Node{
		table,
		&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{
			&ast.ValidateConstraintOperation{ConstraintName: "t_ck"},
		}},
	}}

	database, err := sqlschema.ToDatabase(statements, "postgres")

	c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
	c.Assert(err, qt.ErrorMatches, `the schema model has no place for this statement: ALTER TABLE t ValidateConstraintOperation`)
	c.Assert(database.Tables, qt.HasLen, 0)
}
