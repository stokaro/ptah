package planner_test

import (
	"fmt"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/ptaherr"
	dbtypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

var externalPlannerDialectSeq atomic.Int64

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
			name:    "mariadb planner",
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
			name:    "sqlite planner",
			dialect: platform.SQLite,
			wantErr: false,
		},
		{
			name:    "clickhouse planner",
			dialect: platform.ClickHouse,
			wantErr: false,
		},
		{
			name:    "sqlite3 planner",
			dialect: "sqlite3",
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

			plannerInstance, err := planner.GetPlanner(tt.dialect)
			if tt.wantErr {
				c.Assert(plannerInstance, qt.IsNil)
				c.Assert(err, qt.ErrorMatches, "unsupported database dialect: "+tt.dialect)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(plannerInstance, qt.IsNotNil)
		})
	}
}

func TestGetPlanner_MariaDBUsesMySQLPlanner(t *testing.T) {
	c := qt.New(t)

	plannerInstance, err := planner.GetPlanner(platform.MariaDB)
	c.Assert(err, qt.IsNil)
	_, ok := plannerInstance.(*mysql.Planner)

	c.Assert(ok, qt.IsTrue)
}

func TestRegisterExternalPlanner(t *testing.T) {
	c := qt.New(t)

	dialect := nextExternalPlannerDialect("external_planner_test")
	err := planner.Register(dialect, func(opts planner.Options) planner.Planner {
		return externalPlanner{caps: opts.CapabilitiesFor(dialect)}
	})
	c.Assert(err, qt.IsNil)

	caps := capability.Postgres13()
	registered, err := planner.GetPlannerWithOptions(dialect, planner.Options{Capabilities: caps})
	c.Assert(err, qt.IsNil)

	external, ok := registered.(externalPlanner)
	c.Assert(ok, qt.IsTrue)
	c.Assert(external.caps.Has(capability.CreateIndexConcurrently), qt.IsTrue)
}

func TestRegisterRejectsDuplicateAndNilFactories(t *testing.T) {
	c := qt.New(t)

	dialect := nextExternalPlannerDialect("duplicate_planner_test")
	err := planner.Register(dialect, func(planner.Options) planner.Planner {
		return externalPlanner{}
	})
	c.Assert(err, qt.IsNil)

	err = planner.Register(dialect, func(planner.Options) planner.Planner {
		return externalPlanner{}
	})
	c.Assert(err, qt.ErrorMatches, fmt.Sprintf(`planner registry: dialect %q is already registered`, dialect))

	nilDialect := nextExternalPlannerDialect("nil_factory_planner_test")
	err = planner.Register(nilDialect, nil)
	c.Assert(err, qt.ErrorMatches, fmt.Sprintf(`planner registry: factory for dialect %q must not be nil`, nilDialect))
}

func TestGetPlannerRejectsFactoryReturningNil(t *testing.T) {
	c := qt.New(t)

	dialect := nextExternalPlannerDialect("nil_planner_test")
	err := planner.Register(dialect, func(planner.Options) planner.Planner {
		return nil
	})
	c.Assert(err, qt.IsNil)

	registered, err := planner.GetPlanner(dialect)
	c.Assert(registered, qt.IsNil)
	c.Assert(err, qt.ErrorMatches, fmt.Sprintf(`planner registry: factory for dialect %q returned nil`, dialect))
}

func TestGenerateSchemaDiffSQL_UnsupportedDialectReturnsError(t *testing.T) {
	c := qt.New(t)

	nodes, err := planner.GenerateSchemaDiffAST(&types.SchemaDiff{}, &goschema.Database{}, "sqlserver")
	c.Assert(nodes, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)

	var astPlanErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &astPlanErr)
	c.Assert(astPlanErr.Dialect, qt.Equals, "sqlserver")

	sql, err := planner.GenerateSchemaDiffSQL(&types.SchemaDiff{}, &goschema.Database{}, "sqlserver")
	c.Assert(sql, qt.Equals, "")
	c.Assert(err, qt.ErrorMatches, "unsupported database dialect: sqlserver")
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)

	var planErr *ptaherr.PlanError
	c.Assert(err, qt.ErrorAs, &planErr)
	c.Assert(planErr.Dialect, qt.Equals, "sqlserver")
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

type externalPlanner struct {
	caps capability.Capabilities
}

func nextExternalPlannerDialect(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, externalPlannerDialectSeq.Add(1))
}

func (p externalPlanner) GenerateMigrationAST(
	diff *types.SchemaDiff,
	generated *goschema.Database,
) []ast.Node {
	nodes, _ := p.GenerateMigrationASTChecked(diff, generated)
	return nodes
}

func (p externalPlanner) GenerateMigrationASTChecked(
	_ *types.SchemaDiff,
	_ *goschema.Database,
) ([]ast.Node, error) {
	return []ast.Node{ast.NewComment("external planner")}, nil
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

	nodes, err := planner.GenerateSchemaDiffAST(diff, generated, platform.Postgres)
	c.Assert(err, qt.IsNil)
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

	nodes, err := planner.GenerateSchemaDiffAST(diff, &goschema.Database{}, platform.Postgres)
	c.Assert(err, qt.IsNil)
	assessments, err := safety.AssessRendered(nodes, platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsTrue)
	c.Assert(assessments[0].Severity, qt.Equals, safety.Destructive)
	c.Assert(legacyRenderedSQL(assessments[0].Statement), qt.Contains, "DROP POLICY IF EXISTS tenant_isolation ON accounts")
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

			nodes, err := planner.GenerateSchemaDiffAST(tt.diff, tt.generated, tt.dialect)
			if tt.wantErr {
				c.Assert(nodes, qt.IsNil)
				c.Assert(err, qt.IsNotNil)
				return
			}
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.IsNotNil)
			c.Assert(nodes, qt.HasLen, 1) // Should have one CREATE TABLE statement
		})
	}
}
