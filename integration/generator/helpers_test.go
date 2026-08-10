//go:build integration

package generator_test

import (
	"os"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/generator"
)

var simpleRenderedIdentifierQuoteRE = regexp.MustCompile("[`\"]([a-z_][a-z0-9_]*)[`\"]")

func requireGeneratorDatabaseURL(t *testing.T, envKey string) string {
	t.Helper()
	value := os.Getenv(envKey)
	if value == "" {
		t.Skipf("%s is not set", envKey)
	}
	return value
}

func requireGeneratorDatabaseConnection(
	t *testing.T,
	envKey string,
) *dbschema.DatabaseConnection {
	t.Helper()
	conn, err := dbschema.ConnectToDatabase(t.Context(), requireGeneratorDatabaseURL(t, envKey))
	qt.Assert(t, err, qt.IsNil)
	t.Cleanup(func() {
		qt.Check(t, conn.Close(), qt.IsNil)
	})
	return conn
}

func legacyRenderedSQL(sql string) string {
	return simpleRenderedIdentifierQuoteRE.ReplaceAllString(sql, "$1")
}

func generateLiveMigrationSQL(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	desired *goschema.Database,
) (upSQL, downSQL string) {
	c.Helper()
	outputDir := c.TempDir()
	files, err := generator.GenerateMigration(c.Context(), generator.GenerateMigrationOptions{
		Generated:     desired,
		DBConn:        conn,
		MigrationName: "integration",
		OutputDir:     outputDir,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.IsNotNil)
	c.Assert(files.Files, qt.HasLen, 1)
	up, err := os.ReadFile(files.Files[0].UpFile)
	c.Assert(err, qt.IsNil)
	down, err := os.ReadFile(files.Files[0].DownFile)
	c.Assert(err, qt.IsNil)
	return string(up), string(down)
}

func fkOrderSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "PtahFKOrderAccount", Name: "ptah_fk_order_accounts"},
			{StructName: "PtahFKOrderProject", Name: "ptah_fk_order_projects"},
			{StructName: "PtahFKOrderMembership", Name: "ptah_fk_order_memberships"},
			{StructName: "PtahFKOrderTask", Name: "ptah_fk_order_tasks"},
		},
		Fields: []goschema.Field{
			{StructName: "PtahFKOrderAccount", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKOrderProject", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKOrderProject", Name: "account_id", Type: "VARCHAR(36)", Foreign: "ptah_fk_order_accounts(id)", ForeignKeyName: "fk_ptah_fk_order_projects_account"},
			{StructName: "PtahFKOrderMembership", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKOrderMembership", Name: "account_id", Type: "VARCHAR(36)", Foreign: "ptah_fk_order_accounts(id)", ForeignKeyName: "fk_ptah_fk_order_memberships_account"},
			{StructName: "PtahFKOrderTask", Name: "id", Type: "VARCHAR(36)", Primary: true},
			{StructName: "PtahFKOrderTask", Name: "project_id", Type: "VARCHAR(36)", Foreign: "ptah_fk_order_projects(id)", ForeignKeyName: "fk_ptah_fk_order_tasks_project"},
			{StructName: "PtahFKOrderTask", Name: "membership_id", Type: "VARCHAR(36)", Foreign: "ptah_fk_order_memberships(id)", ForeignKeyName: "fk_ptah_fk_order_tasks_membership"},
		},
	}
}

func mutualFKCycleSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "LeftNode", Name: "left_nodes"},
			{StructName: "RightNode", Name: "right_nodes"},
		},
		Fields: []goschema.Field{
			{StructName: "LeftNode", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "LeftNode", Name: "right_id", Type: "INTEGER", Foreign: "right_nodes(id)", ForeignKeyName: "fk_left_nodes_right_id"},
			{StructName: "RightNode", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "RightNode", Name: "left_id", Type: "INTEGER", Foreign: "left_nodes(id)", ForeignKeyName: "fk_right_nodes_left_id"},
		},
	}
}
