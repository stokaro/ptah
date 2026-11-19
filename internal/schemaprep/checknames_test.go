package schemaprep_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// checkTable is a table of database whose CHECK constraints carry names, in
// order. A name starting with "column:" is the CHECK of a column of the table.
type checkTable struct {
	schema string
	name   string
	checks []string
}

// databaseOf builds the model of tables, each CHECK over the column a.
func databaseOf(tables ...checkTable) *schemamodel.Database {
	database := &schemamodel.Database{}
	for _, table := range tables {
		model := schemamodel.Table{StructName: table.schema + table.name, Schema: table.schema, Name: table.name}
		database.Tables = append(database.Tables, model)
		for _, check := range table.checks {
			if column, ok := strings.CutPrefix(check, "column:"); ok {
				database.Fields = append(database.Fields, schemamodel.Field{
					StructName: model.StructName, Name: "a", Type: "int", Check: "a > 0", CheckName: column,
				})
				continue
			}
			database.Constraints = append(database.Constraints, schemamodel.Constraint{
				StructName: model.StructName, Name: check, Type: "CHECK", Table: model.QualifiedName(), CheckExpression: "a > 0",
			})
		}
	}
	return database
}

func TestValidateCheckNames_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
	}{
		{name: "MariaDB keeps a name per table", dialect: "mariadb",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{"ck"}})},
		{name: "PostgreSQL keeps a name per table", dialect: "postgres",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{"ck"}})},
		{name: "PostgreSQL compares the names the model records exactly", dialect: "postgres",
			database: databaseOf(checkTable{name: "t", checks: []string{"dd", "DD"}})},
		{name: "MySQL keeps a letter's accent", dialect: "mysql",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ek"}}, checkTable{name: "t2", checks: []string{"ék"}})},
		{name: "MySQL keeps a name per schema", dialect: "mysql",
			database: databaseOf(checkTable{schema: "a", name: "t", checks: []string{"ck"}}, checkTable{schema: "b", name: "t", checks: []string{"ck"}})},
		{name: "Oracle keeps a quoted name its unquoted spelling does not reach", dialect: "oracle",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{`"ck"`}},
				checkTable{name: "t3", checks: []string{`"Ck"`}})},
		{name: "SQL Server keeps a name per schema", dialect: "sqlserver",
			database: databaseOf(checkTable{schema: "dbo", name: "t", checks: []string{"ck"}}, checkTable{schema: "other", name: "t", checks: []string{"ck"}})},
		{name: "a dialect that states no scope keeps a name per table", dialect: "sqlite",
			database: databaseOf(checkTable{name: "t1", checks: []string{"dd"}}, checkTable{name: "t2", checks: []string{"dd"}})},
		{name: "no database", dialect: "mysql", database: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateCheckNames(test.database, test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateCheckNames_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
		wantErr  string
	}{
		{
			name: "MySQL across tables, in another case", dialect: "mysql",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{"CK"}}),
			wantErr: `table "t1" declares CHECK "ck" and table "t2" CHECK "CK", and the server keeps one CHECK of a name per schema: ` +
				`Error 3822 (HY000): Duplicate check constraint name 'CK'.`,
		},
		{
			name: "MySQL, a column's CHECK and a table's", dialect: "mysql",
			database: databaseOf(checkTable{schema: "app", name: "t1", checks: []string{"column:ck"}}, checkTable{schema: "app", name: "t2", checks: []string{"ck"}}),
			wantErr: `table "app.t1" declares CHECK "ck" and table "app.t2" CHECK "ck", and the server keeps one CHECK of a name per schema: ` +
				`Error 3822 (HY000): Duplicate check constraint name 'ck'.`,
		},
		{
			name: "MySQL folds a letter beyond ASCII", dialect: "mysql",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ék"}}, checkTable{name: "t2", checks: []string{"ÉK"}}),
			wantErr: `table "t1" declares CHECK "ék" and table "t2" CHECK "ÉK", and the server keeps one CHECK of a name per schema: ` +
				`Error 3822 (HY000): Duplicate check constraint name 'ÉK'.`,
		},
		{
			name: "MariaDB in one table", dialect: "mariadb",
			database: databaseOf(checkTable{name: "t", checks: []string{"dd", "DD"}}),
			wantErr: `table "t" declares CHECK "dd" and CHECK "DD", and the server keeps one CHECK of a name per table: ` +
				`Error 1826 (HY000): Duplicate CHECK constraint name 'DD'`,
		},
		{
			name: "PostgreSQL in one table", dialect: "postgres",
			database: databaseOf(checkTable{name: "t", checks: []string{"column:dd", "dd"}}),
			wantErr: `table "t" declares CHECK "dd" and CHECK "dd", and the server keeps one CHECK of a name per table: ` +
				`ERROR: check constraint "dd" already exists (SQLSTATE 42710)`,
		},
		{
			name: "Oracle, a quoted name in the fold's case", dialect: "oracle",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{`"CK"`}}),
			wantErr: `table "t1" declares CHECK "ck" and table "t2" CHECK "CK", and the server keeps one CHECK of a name per schema: ` +
				`ORA-02264: name already used by an existing constraint`,
		},
		{
			name: "SQL Server across tables", dialect: "sqlserver",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{"CK"}}),
			wantErr: `table "t1" declares CHECK "ck" and table "t2" CHECK "CK", and the server keeps one CHECK of a name per schema: ` +
				`mssql: There is already an object named 'CK' in the database. (2714)`,
		},
		{
			name: "SQL Server in one table", dialect: "sqlserver",
			database: databaseOf(checkTable{name: "t", checks: []string{"dd", "dd"}}),
			wantErr: `table "t" declares CHECK "dd" and CHECK "dd", and the server keeps one CHECK of a name per schema: ` +
				`mssql: Cannot create, drop, enable, or disable more than one constraint, column, index, or trigger named 'dd' ` +
				`in this context. Duplicate names are not allowed. (8168)`,
		},
		{
			name: "SQLite in one table, which the server accepts", dialect: "sqlite",
			database: databaseOf(checkTable{name: "t", checks: []string{"dd", "DD"}}),
			wantErr:  `table "t" declares CHECK "dd" and CHECK "DD", and Ptah keeps one CHECK of a name per table`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateCheckNames(test.database, test.dialect)

			c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName)
			c.Assert(err.Error(), qt.Equals, "two CHECK constraints claim the same name: "+test.wantErr)
		})
	}
}

func TestValidateCheckNamesWithinTables_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
	}{
		{name: "MySQL across tables", dialect: "mysql",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{"CK"}})},
		{name: "SQL Server across tables", dialect: "sqlserver",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{"ck"}})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateCheckNamesWithinTables(test.database, test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestValidateCheckNamesWithinTables_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *schemamodel.Database
		wantErr  string
	}{
		{
			name: "MySQL in one table, compared as the server compares", dialect: "mysql",
			database: databaseOf(checkTable{name: "t1", checks: []string{"ck"}}, checkTable{name: "t2", checks: []string{"ék", "ÉK"}}),
			wantErr: `table "t2" declares CHECK "ék" and CHECK "ÉK", and the server keeps one CHECK of a name per schema: ` +
				`Error 3822 (HY000): Duplicate check constraint name 'ÉK'.`,
		},
		{
			name: "SQLite in one table", dialect: "sqlite",
			database: databaseOf(checkTable{name: "t", checks: []string{"column:dd", "dd"}}),
			wantErr:  `table "t" declares CHECK "dd" and CHECK "dd", and Ptah keeps one CHECK of a name per table`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateCheckNamesWithinTables(test.database, test.dialect)

			c.Assert(err, qt.ErrorIs, schemaprep.ErrDuplicateCheckName)
			c.Assert(err.Error(), qt.Equals, "two CHECK constraints claim the same name: "+test.wantErr)
		})
	}
}
