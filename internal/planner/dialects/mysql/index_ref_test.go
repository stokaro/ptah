package mysql_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/planner/dialects/mysql"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

type mysqlFamilyPlannerCase struct {
	name    string
	planner *mysql.Planner
}

func mysqlFamilyPlannerCases() []mysqlFamilyPlannerCase {
	return []mysqlFamilyPlannerCase{
		{name: "mysql", planner: mysql.New()},
		{name: "mariadb", planner: mysql.NewWithCapabilities(capability.MariaDB1011())},
	}
}

func TestPlanner_IndexRefs_MySQLFamilyRoutesDuplicateAdditions(t *testing.T) {
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_shared", TableName: "orders"},
			{Name: "idx_shared", TableName: "users"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_shared", TableName: "users", Fields: []string{"email"}},
		{Name: "idx_shared", TableName: "orders", Fields: []string{"reference"}},
	}}

	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := test.planner.GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)

			c.Assert(nodes, qt.HasLen, 2)
			ordersIndex, ok := nodes[0].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(ordersIndex.Name, qt.Equals, "idx_shared")
			c.Assert(ordersIndex.Table, qt.Equals, "orders")
			c.Assert(ordersIndex.Columns, qt.DeepEquals, []string{"reference"})
			usersIndex, ok := nodes[1].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(usersIndex.Name, qt.Equals, "idx_shared")
			c.Assert(usersIndex.Table, qt.Equals, "users")
			c.Assert(usersIndex.Columns, qt.DeepEquals, []string{"email"})
		})
	}
}

func TestPlanner_IndexRefs_MySQLFamilyRoutesDuplicateRemovals(t *testing.T) {
	diff := &types.SchemaDiff{
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_shared", TableName: "orders"},
			{Name: "idx_shared", TableName: "users"},
		},
	}

	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := test.planner.GenerateMigrationASTChecked(diff, &goschema.Database{})
			c.Assert(err, qt.IsNil)

			c.Assert(nodes, qt.HasLen, 2)
			ordersDrop, ok := nodes[0].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(ordersDrop.Name, qt.Equals, "idx_shared")
			c.Assert(ordersDrop.Table, qt.Equals, "orders")
			usersDrop, ok := nodes[1].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(usersDrop.Name, qt.Equals, "idx_shared")
			c.Assert(usersDrop.Table, qt.Equals, "users")
		})
	}
}

func TestPlanner_IndexRefs_MySQLFamilyReplacesOnlyExactRef(t *testing.T) {
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{{Name: "idx_shared", TableName: "users"}},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_shared", TableName: "users"},
			{Name: "idx_shared", TableName: "orders"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_shared", TableName: "users", Fields: []string{"email"}},
	}}

	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := test.planner.GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)

			c.Assert(nodes, qt.HasLen, 3)
			replacementDrop, ok := nodes[0].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(replacementDrop.Table, qt.Equals, "users")
			replacementCreate, ok := nodes[1].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(replacementCreate.Table, qt.Equals, "users")
			otherDrop, ok := nodes[2].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(otherDrop.Table, qt.Equals, "orders")
		})
	}
}

func TestPlanner_IndexRefs_MySQLFamilyPreservesReplacementAddition(t *testing.T) {
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_email", TableName: "users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_email", TableName: "users"},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "idx_email", TableName: "users", Fields: []string{"email"}},
		},
	}

	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := test.planner.GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)

			c.Assert(nodes, qt.HasLen, 2)
			drop, ok := nodes[0].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(drop.Name, qt.Equals, "idx_email")
			c.Assert(drop.Table, qt.Equals, "users")
			create, ok := nodes[1].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(create.Name, qt.Equals, "idx_email")
			c.Assert(create.Table, qt.Equals, "users")
		})
	}
}

func TestPlanner_IndexRefs_MySQLFamilyCaseInsensitiveReplacementDropsFirst(t *testing.T) {
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "IDX_Email", TableName: "users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_email", TableName: "users"},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "IDX_Email", TableName: "users", Fields: []string{"email"}},
		},
	}

	for _, test := range mysqlFamilyPlannerCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := test.planner.GenerateMigrationASTChecked(diff, generated)
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 2)

			drop, ok := nodes[0].(*ast.DropIndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(drop.Name, qt.Equals, "idx_email")
			c.Assert(drop.Table, qt.Equals, "users")
			create, ok := nodes[1].(*ast.IndexNode)
			c.Assert(ok, qt.IsTrue)
			c.Assert(create.Name, qt.Equals, "IDX_Email")
			c.Assert(create.Table, qt.Equals, "users")
		})
	}
}

func TestPlanner_IndexRefs_SQLServerSharedPlannerRoutesDuplicateAdditions(t *testing.T) {
	c := qt.New(t)
	semantics := sqlServerIndexSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"audit"},
		[]string{"dbo"},
		[]string{"records"},
		[]string{"idx_shared"},
	)
	diff := &types.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded: []types.IndexRef{
			{Name: "idx_shared", TableName: "audit.records"},
			{Name: "idx_shared", TableName: "dbo.records"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_shared", TableName: "dbo.records", Fields: []string{"external_id"}},
		{Name: "idx_shared", TableName: "audit.records", Fields: []string{"recorded_at"}},
	}}

	nodes, err := mysql.NewForDialect(platform.SQLServer, capability.SQLServer2022()).
		GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)

	c.Assert(nodes, qt.HasLen, 2)
	auditIndex, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(auditIndex.Table, qt.Equals, "audit.records")
	c.Assert(auditIndex.Columns, qt.DeepEquals, []string{"recorded_at"})
	dboIndex, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(dboIndex.Table, qt.Equals, "dbo.records")
	c.Assert(dboIndex.Columns, qt.DeepEquals, []string{"external_id"})
}

func TestPlanner_IndexRefs_SQLServerPreservesIndexPartDirection(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_users_lookup", TableName: "dbo.users"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_users_lookup",
			TableName: "dbo.users",
			Fields:    []string{"email", "status"},
			Parts: []goschema.IndexPart{
				{Name: "email", Desc: true},
				{Name: "status"},
			},
		},
	}}

	nodes, err := mysql.NewForDialect(platform.SQLServer, capability.SQLServer2022()).
		GenerateMigrationASTChecked(diff, generated)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 1)
	index, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(index.EffectiveParts(), qt.DeepEquals, []ast.IndexPart{
		{Name: "email", Desc: true},
		{Name: "status"},
	})
}

func TestPlanner_IndexRefs_SQLServerPreservesFilteredIndexPredicate(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "idx_active_users", TableName: "dbo.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_active_users", TableName: "dbo.users"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_active_users",
			TableName: "dbo.users",
			Fields:    []string{"status"},
			Condition: "[status] = 2",
		},
	}}

	nodes, err := mysql.NewForDialect(platform.SQLServer, capability.SQLServer2022()).
		GenerateMigrationASTChecked(diff, generated)

	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_active_users")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Condition, qt.Equals, "[status] = 2")
}

func TestPlanner_IndexRefs_SQLServerUnknownCollationOrdersPotentialReplacementSafely(t *testing.T) {
	c := qt.New(t)
	diff := &types.SchemaDiff{
		IndexesAdded: []types.IndexRef{
			{Name: "IDX_Email", TableName: "dbo.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_email", TableName: "dbo.users"},
		},
	}
	generated := &goschema.Database{
		Indexes: []goschema.Index{
			{Name: "IDX_Email", TableName: "dbo.users", Fields: []string{"email"}},
		},
	}

	nodes, err := mysql.NewForDialect(platform.SQLServer, capability.SQLServer2022()).
		GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)

	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_email")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Name, qt.Equals, "IDX_Email")
}

func TestPlanner_IndexRefs_SQLServerCaseInsensitiveReplacementDropsFirst(t *testing.T) {
	c := qt.New(t)
	semantics := sqlServerIndexSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"idx_email", "IDX_Email"},
	)
	diff := &types.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded: []types.IndexRef{
			{Name: "IDX_Email", TableName: "dbo.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_email", TableName: "dbo.users"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "IDX_Email", TableName: "dbo.users", Fields: []string{"email"}},
	}}

	nodes, err := mysql.NewForDialect(platform.SQLServer, capability.SQLServer2022()).
		GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)

	drop, ok := nodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_email")
	create, ok := nodes[1].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Name, qt.Equals, "IDX_Email")
}

func TestPlanner_IndexRefs_SQLServerCaseSensitiveVariantsRemainIndependent(t *testing.T) {
	c := qt.New(t)
	semantics := sqlServerIndexSemantics(
		"SQL_Latin1_General_CP1_CS_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"idx_email"},
		[]string{"IDX_Email"},
	)
	diff := &types.SchemaDiff{
		IdentifierSemantics: &semantics,
		IndexesAdded: []types.IndexRef{
			{Name: "IDX_Email", TableName: "dbo.users"},
		},
		IndexesRemoved: []types.IndexRef{
			{Name: "idx_email", TableName: "dbo.users"},
		},
	}
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "IDX_Email", TableName: "dbo.users", Fields: []string{"email"}},
	}}

	nodes, err := mysql.NewForDialect(platform.SQLServer, capability.SQLServer2022()).
		GenerateMigrationASTChecked(diff, generated)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)

	create, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Name, qt.Equals, "IDX_Email")
	drop, ok := nodes[1].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_email")
}

func sqlServerIndexSemantics(
	collation string,
	groups ...[]string,
) identifier.Semantics {
	resolved := make([]identifier.ResolvedName, 0)
	for _, group := range groups {
		key := slices.Min(group)
		for _, name := range group {
			resolved = append(resolved, identifier.ResolvedName{
				Name: name,
				Key:  key,
			})
		}
	}
	return identifier.ForSQLServerCatalog(collation).WithResolvedNames(resolved)
}
