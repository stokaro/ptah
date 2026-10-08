package postgres_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/postgres"
	"ptah.run/migration/schemadiff/difftypes"
)

// columnKeyTable declares c with a primary key over id, and id's own UNIQUE
// when unique is set.
func columnKeyTable(unique bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "C", Name: "c", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{{StructName: "C", Name: "id", Type: "int", Primary: true, Unique: unique}},
	}
}

// TestPlanner_ColumnKeyOverThePrimaryKeyFollowsTheTable plans a new table
// whose column declares its own UNIQUE over the primary key's column. Measured
// on PostgreSQL 18.6, `CREATE TABLE c (id int UNIQUE, PRIMARY KEY (id))` builds
// c_pkey alone, and adding the UNIQUE after the table builds c_id_key too, as
// the SQL `CREATE TABLE c (id int UNIQUE); ALTER TABLE c ADD PRIMARY KEY (id)`
// does.
func TestPlanner_ColumnKeyOverThePrimaryKeyFollowsTheTable(t *testing.T) {
	tests := []struct {
		name    string
		unique  bool
		want    string
		wantNot string
	}{
		{
			name:    "a column's own UNIQUE over the key",
			unique:  true,
			want:    `ALTER TABLE "c" ADD UNIQUE ("id");`,
			wantNot: "int UNIQUE",
		},
		{
			name:    "no UNIQUE of the column's",
			unique:  false,
			want:    `CREATE TABLE "c"`,
			wantNot: "ADD UNIQUE",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := columnKeyTable(test.unique)
			diff := &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{difftypes.TableCreationFor(desired, desired.Tables[0], "c", identifier.ForDialect("postgres"))},
			}

			nodes, err := postgres.New().GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				withDeclaredObjects(diff, desired),
			)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQL("postgres", nodes...)
			c.Assert(err, qt.IsNil)

			c.Assert(sql, qt.Contains, test.want)
			c.Assert(sql, qt.Not(qt.Contains), test.wantNot)
		})
	}
}
