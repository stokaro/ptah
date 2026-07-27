package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/internal/planner/dialects/mysql"
	"github.com/stokaro/ptah/migration/schemadiff/types"
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
	diff := &types.SchemaDiff{
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

func TestPlanner_IndexRefs_SQLServerDoesNotAssumeCaseInsensitiveCollation(t *testing.T) {
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

	create, ok := nodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(create.Name, qt.Equals, "IDX_Email")
	drop, ok := nodes[1].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(drop.Name, qt.Equals, "idx_email")
}
