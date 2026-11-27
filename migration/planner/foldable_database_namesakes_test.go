package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// A MySQL or MariaDB server started with lower_case_table_names 1 or 2 reads
// Sales and sales as one database, so a plan that creates the first and drops
// the second creates nothing and drops the database with its tables
// (stokaro/ptah#6785). Only a diff that says the server keeps the two apart
// is planned.

// databaseCaseNamesakes is a diff that creates the database Sales and drops
// the database sales, as a server whose folding is folding answered.
func databaseCaseNamesakes(folding catalog.NameFolding) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		NameFolding:    folding,
		SchemasAdded:   []schemamodel.Schema{{Name: "Sales"}},
		SchemasRemoved: []string{"sales"},
	}
}

func TestGenerateSchemaDiffSQLStatements_RefusesDatabasesThatDifferOnlyInCase_FailurePath(t *testing.T) {
	const unknown = `the plan creates database "Sales" and drops database "sales", which differ only in case, ` +
		`and the diff does not say how the server compares database names: on a server started with ` +
		`lower_case_table_names 1 or 2 the two are one database, the create makes nothing, and the drop would ` +
		`remove the database with every table in it; plan the diff of a comparison with a connection to the ` +
		`server, which records how it compares names`
	const folds = `the plan creates database "Sales" and drops database "sales", and the server the comparison ` +
		`read compares database names without regard to case, so the two are one database: the create makes ` +
		`nothing, and the drop would remove the database with every table in it; declare the database as the ` +
		`server spells it`
	tests := []struct {
		name    string
		dialect string
		folding catalog.NameFolding
		want    string
	}{
		{name: "a MySQL diff built by hand", dialect: platform.MySQL, folding: catalog.NameFoldingUnknown, want: unknown},
		{name: "a MariaDB diff built by hand", dialect: platform.MariaDB, folding: catalog.NameFoldingUnknown, want: unknown},
		{name: "a MySQL server that folds names", dialect: platform.MySQL, folding: catalog.NameFoldingFolds, want: folds},
		{name: "a MariaDB server that folds names", dialect: platform.MariaDB, folding: catalog.NameFoldingFolds, want: folds},
		{
			// Only the answer a connection records says the names are two.
			name: "a folding no connection records", dialect: platform.MySQL, folding: catalog.NameFolding("lower"), want: unknown,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())

			statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), runtime, databaseCaseNamesakes(test.folding), test.dialect)
			nodes, astErr := planner.GenerateSchemaDiffAST(t.Context(), runtime, databaseCaseNamesakes(test.folding), test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.want)
			c.Assert(statements, qt.IsNil)
			c.Assert(astErr, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(astErr, qt.ErrorMatches, `(?s).*`+test.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateSchemaDiffSQLStatements_RefusesDatabasesThatDifferOnlyInCase_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		want    []string
	}{
		{
			// The control for the refusals: lower_case_table_names 0 keeps
			// the two databases apart, and the plan creates one and drops
			// the other.
			name:    "a MySQL server that keeps names apart",
			dialect: platform.MySQL,
			diff:    databaseCaseNamesakes(catalog.NameFoldingExact),
			want:    []string{"CREATE SCHEMA IF NOT EXISTS `Sales`", "DROP DATABASE `sales`"},
		},
		{
			name:    "a MariaDB server that keeps names apart",
			dialect: platform.MariaDB,
			diff:    databaseCaseNamesakes(catalog.NameFoldingExact),
			want:    []string{"CREATE SCHEMA IF NOT EXISTS `Sales`", "DROP DATABASE `sales`"},
		},
		{
			// Names that differ beyond case are two databases on every
			// server, whatever the diff says of it.
			name:    "two databases of different names",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				SchemasAdded:   []schemamodel.Schema{{Name: "Sales"}},
				SchemasRemoved: []string{"archive"},
			},
			want: []string{"CREATE SCHEMA IF NOT EXISTS `Sales`", "DROP DATABASE `archive`"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := planner.GenerateSchemaDiffSQLStatements(t.Context(), must.Must(builtin.New()), test.diff, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
