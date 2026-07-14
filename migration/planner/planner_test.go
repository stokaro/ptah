package planner_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	dbtypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestGetPlanner(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantErr bool
	}{
		{
			name:    "postgres planner",
			dialect: platform.Postgres,
			wantErr: false,
		},
		{
			name:    "mysql planner",
			dialect: platform.MySQL,
			wantErr: false,
		},
		{
			name:    "mariadb planner not implemented",
			dialect: platform.MariaDB,
			wantErr: false,
		},
		{
			name:    "cockroachdb planner",
			dialect: platform.CockroachDB,
			wantErr: false,
		},
		{
			name:    "yugabytedb planner",
			dialect: platform.YugabyteDB,
			wantErr: false,
		},
		{
			name:    "spanner planner",
			dialect: platform.Spanner,
			wantErr: false,
		},
		{
			name:    "unknown dialect",
			dialect: "unknown",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			if tt.wantErr {
				defer func() {
					r := recover()
					c.Assert(r, qt.IsNotNil)
				}()
				planner.GetPlanner(tt.dialect)
				c.Assert(false, qt.IsTrue, qt.Commentf("Expected panic but none occurred"))
			} else {
				plannerInstance := planner.GetPlanner(tt.dialect)
				c.Assert(plannerInstance, qt.IsNotNil)
			}
		})
	}
}

func TestRequiresNoTransaction(t *testing.T) {
	c := qt.New(t)

	enumAdd := ast.NewAlterType("status").AddOperation(ast.NewAddEnumValueOperation("archived"))
	enumRename := ast.NewAlterType("status").AddOperation(ast.NewRenameEnumValueOperation("old", "new"))

	c.Assert(planner.RequiresNoTransaction(platform.Postgres, []ast.Node{enumAdd}), qt.IsTrue)
	c.Assert(planner.RequiresNoTransaction(platform.YugabyteDB, []ast.Node{enumAdd}), qt.IsTrue)
	c.Assert(planner.RequiresNoTransaction(platform.MySQL, []ast.Node{enumAdd}), qt.IsFalse)
	c.Assert(planner.RequiresNoTransaction(platform.Postgres, []ast.Node{enumRename}), qt.IsFalse)
	c.Assert(planner.RequiresNoTransaction(platform.Postgres, []ast.Node{ast.NewComment("noop")}), qt.IsFalse)
}

func TestGeneratedNarrowingTypeChangeIsDestructive(t *testing.T) {
	c := qt.New(t)

	generated := &goschema.Database{
		Tables: []goschema.Table{
			{Name: "users", StructName: "User"},
		},
		Fields: []goschema.Field{
			{Name: "name", Type: "VARCHAR(100)", StructName: "User"},
		},
	}
	database := &dbtypes.DBSchema{
		Tables: []dbtypes.DBTable{
			{
				Name: "users",
				Columns: []dbtypes.DBColumn{
					{Name: "name", DataType: "VARCHAR(255)", IsNullable: "NO"},
				},
			},
		},
	}

	diff := schemadiff.Compare(generated, database)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified[0].Changes["type"], qt.Equals, "VARCHAR(255) -> VARCHAR(100)")

	nodes := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)
	assessments, err := safety.AssessRendered(nodes, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsTrue)
}

func TestGeneratedRLSPolicyRemovalIsDestructive(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		RLSPoliciesRemoved: []types.RLSPolicyRef{
			{PolicyName: "tenant_isolation", TableName: "accounts"},
		},
	}

	nodes := planner.GenerateSchemaDiffAST(diff, &goschema.Database{}, platform.Postgres)
	assessments, err := safety.AssessRendered(nodes, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsTrue)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(assessments[0].Statement, qt.Contains, "DROP POLICY IF EXISTS tenant_isolation ON accounts")
}

func TestGenerateMigrationAST(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		diff      *types.SchemaDiff
		generated *goschema.Database
		wantErr   bool
	}{
		{
			name:    "postgres migration generation",
			dialect: platform.Postgres,
			diff: &types.SchemaDiff{
				TablesAdded: []string{"users"},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "SERIAL", StructName: "User", Primary: true},
				},
			},
			wantErr: false,
		},
		{
			name:    "mysql migration generation",
			dialect: platform.MySQL,
			diff: &types.SchemaDiff{
				TablesAdded: []string{"users"},
			},
			generated: &goschema.Database{
				Tables: []goschema.Table{
					{Name: "users", StructName: "User"},
				},
				Fields: []goschema.Field{
					{Name: "id", Type: "INT", StructName: "User", Primary: true, AutoInc: true},
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			if tt.wantErr {
				defer func() {
					r := recover()
					c.Assert(r, qt.IsNotNil)
				}()
				planner.GenerateSchemaDiffAST(tt.diff, tt.generated, tt.dialect)
				c.Assert(false, qt.IsTrue, qt.Commentf("Expected panic but none occurred"))
			} else {
				nodes := planner.GenerateSchemaDiffAST(tt.diff, tt.generated, tt.dialect)
				c.Assert(nodes, qt.IsNotNil)
				c.Assert(nodes, qt.HasLen, 1) // Should have one CREATE TABLE statement
			}
		})
	}
}
