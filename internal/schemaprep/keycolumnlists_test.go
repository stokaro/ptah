package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// keyTable is the model of one table "t" over the columns a and b, with the
// primary key primary, the columns in unique declared unique by themselves,
// and the constraints given.
func keyTable(primary, unique []string, constraints ...schemamodel.Constraint) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: primary}},
	}
	for _, name := range []string{"a", "b"} {
		field := schemamodel.Field{StructName: "T", Name: name, Type: "NUMBER(10)"}
		for _, column := range unique {
			field.Unique = field.Unique || column == name
		}
		database.Fields = append(database.Fields, field)
	}
	for _, constraint := range constraints {
		constraint.StructName = "T"
		constraint.Table = "t"
		database.Constraints = append(database.Constraints, constraint)
	}
	return database
}

// TestValidateKeyColumnLists_FailurePath refuses, on Oracle, every pair of
// keys of one table over one ordered column list, which the server answers
// with ORA-02261 at the second key's statement.
func TestValidateKeyColumnLists_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
		wantErr  string
	}{
		{
			name:     "a primary key column declared unique",
			database: keyTable([]string{"a"}, []string{"a"}),
			wantErr:  `two keys claim the same columns: table "t" declares the primary key and column "a" as unique, both over \(a\): ORA-02261: .*`,
		},
		{
			name:     "a UNIQUE over the primary key's columns",
			database: keyTable([]string{"a", "b"}, nil, schemamodel.Constraint{Name: "t_u", Type: "UNIQUE", Columns: []string{"a", "b"}}),
			wantErr:  `two keys claim the same columns: table "t" declares the primary key and UNIQUE "t_u", both over \(a, b\): ORA-02261: .*`,
		},
		{
			name: "two UNIQUE constraints over one list",
			database: keyTable(nil, nil,
				schemamodel.Constraint{Name: "t_u1", Type: "UNIQUE", Columns: []string{"b", "a"}},
				schemamodel.Constraint{Type: "UNIQUE", Columns: []string{"B", "A"}},
			),
			wantErr: `two keys claim the same columns: table "t" declares UNIQUE "t_u1" and UNIQUE, both over \(B, A\): ORA-02261: .*`,
		},
		{
			name:     "a column declared unique and a UNIQUE over it",
			database: keyTable(nil, []string{"b"}, schemamodel.Constraint{Name: "t_u", Type: "UNIQUE", Columns: []string{"b"}}),
			wantErr:  `two keys claim the same columns: table "t" declares column "b" as unique and UNIQUE "t_u", both over \(b\): ORA-02261: .*`,
		},
		{
			name:     "a PRIMARY KEY constraint over another order of the table's key",
			database: keyTable([]string{"a", "b"}, nil, schemamodel.Constraint{Name: "t_pk", Type: "PRIMARY KEY", Columns: []string{"b", "a"}}, schemamodel.Constraint{Name: "t_u", Type: "UNIQUE", Columns: []string{"b", "a"}}),
			wantErr:  `two keys claim the same columns: table "t" declares PRIMARY KEY "t_pk" and UNIQUE "t_u", both over \(b, a\): ORA-02261: .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateKeyColumnLists(test.database, "oracle")

			c.Assert(err, qt.ErrorIs, schemaprep.ErrSecondKeyOverColumns)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestValidateKeyColumnLists_HappyPath passes what the server takes: keys over
// one set of columns in two orders, a primary key the model writes twice over
// its own columns, and any pair on a server that takes a second key.
func TestValidateKeyColumnLists_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		database *schemamodel.Database
		dialect  string
	}{
		{
			name:     "another order",
			database: keyTable([]string{"a", "b"}, nil, schemamodel.Constraint{Name: "t_u", Type: "UNIQUE", Columns: []string{"b", "a"}}),
			dialect:  "oracle",
		},
		{
			name:     "the table's key written again as a constraint",
			database: keyTable([]string{"a", "b"}, nil, schemamodel.Constraint{Name: "t_pk", Type: "PRIMARY KEY", Columns: []string{"a", "b"}}),
			dialect:  "oracle",
		},
		{
			name:     "a single-column key per column",
			database: keyTable([]string{"a"}, []string{"b"}),
			dialect:  "oracle",
		},
		{
			name:     "a server that takes a second key",
			database: keyTable([]string{"a"}, []string{"a"}),
			dialect:  "mysql",
		},
		{
			name:    "no database",
			dialect: "oracle",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateKeyColumnLists(test.database, test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}
