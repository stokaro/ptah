//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	mysqlreader "github.com/stokaro/ptah/internal/dbschema/mysql"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestConstraintsActionsConformanceFixture_RoundTrip_MySQL(t *testing.T) {
	dsn := skipIfNoMySQL(t)
	c := qt.New(t)

	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, _ = db.Exec("DROP TABLE IF EXISTS projects")
	_, _ = db.Exec("DROP TABLE IF EXISTS organizations")
	defer func() {
		_, _ = db.Exec("DROP TABLE IF EXISTS projects")
		_, _ = db.Exec("DROP TABLE IF EXISTS organizations")
	}()

	target := constraintsActionsConformanceSchema()
	createSQL := renderConformanceSQL(c, target, platform.MySQL)
	execConformanceSQL(c, db, createSQL, "constraints/actions")

	reader := mysqlreader.NewMySQLReader(db, "")
	liveSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	liveSchema = filterConformanceSchema(liveSchema, constraintsActionsConformanceTables())

	diff := schemadiff.CompareWithDialect(target, liveSchema, platform.MySQL)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func constraintsActionsConformanceSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Organization", Name: "organizations"},
			{StructName: "Project", Name: "projects"},
		},
		Fields: []goschema.Field{
			{StructName: "Organization", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName: "Organization",
				Name:       "slug",
				Type:       "VARCHAR(64)",
				Nullable:   false,
				Unique:     true,
			},
			{StructName: "Project", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName:     "Project",
				Name:           "organization_id",
				Type:           "INTEGER",
				Nullable:       false,
				Foreign:        "organizations(id)",
				ForeignKeyName: "fk_projects_organization",
				OnDelete:       "CASCADE",
				OnUpdate:       "RESTRICT",
			},
			{
				StructName: "Project",
				Name:       "slug",
				Type:       "VARCHAR(64)",
				Nullable:   false,
			},
			{
				StructName: "Project",
				Name:       "status",
				Type:       "VARCHAR(16)",
				Nullable:   false,
				Default:    "active",
				DefaultSet: true,
			},
			{
				StructName:  "Project",
				Name:        "budget_cents",
				Type:        "INTEGER",
				Nullable:    false,
				DefaultExpr: "0",
				Check:       "budget_cents >= 0",
				CheckName:   "projects_budget_nonnegative",
			},
		},
		Constraints: []goschema.Constraint{
			{
				StructName: "Project",
				Name:       "projects_org_slug_unique",
				Type:       "UNIQUE",
				Table:      "projects",
				Columns:    []string{"organization_id", "slug"},
			},
			{
				StructName:      "Project",
				Name:            "projects_status_check",
				Type:            "CHECK",
				Table:           "projects",
				CheckExpression: "status IN ('active', 'archived')",
			},
		},
	}
}

func constraintsActionsConformanceTables() map[string]struct{} {
	return map[string]struct{}{
		"organizations": {},
		"projects":      {},
	}
}
