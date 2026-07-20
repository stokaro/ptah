//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	"github.com/stokaro/ptah/internal/dbschema/postgres"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestGeneratedColumnAndPartialIndex_RoundTrip_Postgres(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	const schemaName = "ptah_generated_columns_test"
	_, _ = db.Exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE")
	_, err = db.Exec("CREATE SCHEMA " + schemaName)
	c.Assert(err, qt.IsNil)
	defer func() { _, _ = db.Exec("DROP SCHEMA IF EXISTS " + schemaName + " CASCADE") }()

	target := generatedPartialIndexSchema(schemaName, "lower(email)")
	createAST := fromschema.FromDatabase(*target, platform.Postgres)
	createSQL, err := renderer.RenderSQL(platform.Postgres, createAST.Statements...)
	c.Assert(err, qt.IsNil)
	c.Assert(createSQL, qt.Contains, "GENERATED ALWAYS AS (lower(email)) STORED")
	c.Assert(createSQL, qt.Contains, "WHERE deleted_at IS NULL")

	_, err = db.Exec(createSQL)
	c.Assert(err, qt.IsNil, qt.Commentf("generated/partial schema must apply: %s", createSQL))

	reader := postgres.NewPostgreSQLReader(db, "public")
	reader.SetSchemas([]string{schemaName})
	liveSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	expressionIndex := findDBIndex(liveSchema.Indexes, "idx_ptah_generated_users_email_expr_active")
	c.Assert(expressionIndex, qt.IsNotNil)
	c.Assert(expressionIndex.Columns, qt.DeepEquals, []string{"\"left\"(email, 2)", "deleted_at"})
	c.Assert(expressionIndex.Condition, qt.Equals, "(deleted_at IS NULL)")

	roundTripDiff := schemadiff.CompareWithDialect(target, liveSchema, platform.Postgres)
	c.Assert(roundTripDiff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", roundTripDiff))

	changed := generatedPartialIndexSchema(schemaName, "upper(email)")
	changedDiff := schemadiff.CompareWithDialect(changed, liveSchema, platform.Postgres)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		changedDiff,
		changed,
		platform.Postgres,
		capability.Postgres17(),
	)
	c.Assert(err, qt.IsNil)
	plannedSQL := strings.Join(statements, "\n")
	c.Assert(plannedSQL, qt.Contains, `ALTER COLUMN "email_lc" SET EXPRESSION AS (upper(email))`)
	c.Assert(plannedSQL, qt.Not(qt.Contains), `DROP COLUMN "email_lc"`)
}

func generatedPartialIndexSchema(schemaName, expression string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: schemaName, Name: "ptah_generated_users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "User", Name: "email", Type: "TEXT", Nullable: false},
			{StructName: "User", Name: "deleted_at", Type: "TIMESTAMP", Nullable: true},
			{
				StructName:          "User",
				Name:                "email_lc",
				Type:                "TEXT",
				Nullable:            true,
				GeneratedExpression: expression,
				GeneratedKind:       "STORED",
			},
		},
		Indexes: []goschema.Index{
			{
				StructName: "User",
				Name:       "idx_ptah_generated_users_email_active",
				Fields:     []string{"email"},
				Condition:  "deleted_at IS NULL",
			},
			{
				StructName: "User",
				Name:       "idx_ptah_generated_users_email_expr_active",
				Fields:     []string{`"left"(email, 2)`, "deleted_at"},
				Parts: []goschema.IndexPart{
					{Expr: `"left"(email, 2)`},
					{Name: "deleted_at"},
				},
				Condition: "deleted_at IS NULL",
			},
		},
	}
}

func findDBIndex(indexes []dbschematypes.DBIndex, name string) *dbschematypes.DBIndex {
	for i := range indexes {
		if indexes[i].Name == name {
			return &indexes[i]
		}
	}
	return nil
}
