//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	mysqlreader "github.com/stokaro/ptah/internal/dbschema/mysql"
	postgresreader "github.com/stokaro/ptah/internal/dbschema/postgres"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestGeneratedColumnConformanceFixture_RoundTrip_Postgres(t *testing.T) {
	dsn := skipIfNoPostgreSQL(t)
	c := qt.New(t)

	db, err := sql.Open("pgx", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, _ = db.Exec(`DROP TABLE IF EXISTS contacts`)
	defer func() { _, _ = db.Exec(`DROP TABLE IF EXISTS contacts`) }()

	target := generatedColumnConformanceSchema()
	createSQL := renderGeneratedColumnConformanceSQL(c, target, platform.Postgres)
	execGeneratedColumnConformanceSQL(c, db, createSQL)

	reader := postgresreader.NewPostgreSQLReader(db, "public")
	liveSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	liveSchema = filterGeneratedColumnConformanceSchema(liveSchema)

	diff := schemadiff.CompareWithDialect(target, liveSchema, platform.Postgres)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func TestGeneratedColumnConformanceFixture_RoundTrip_MySQL(t *testing.T) {
	dsn := skipIfNoMySQL(t)
	c := qt.New(t)

	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, _ = db.Exec("DROP TABLE IF EXISTS contacts")
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS contacts") }()

	target := generatedColumnConformanceSchema()
	createSQL := renderGeneratedColumnConformanceSQL(c, target, platform.MySQL)
	execGeneratedColumnConformanceSQL(c, db, createSQL)

	reader := mysqlreader.NewMySQLReader(db, "")
	liveSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	liveSchema = filterGeneratedColumnConformanceSchema(liveSchema)

	diff := schemadiff.CompareWithDialect(target, liveSchema, platform.MySQL)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func generatedColumnConformanceSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Contact", Name: "contacts"},
		},
		Fields: []goschema.Field{
			{StructName: "Contact", Name: "id", Type: "SERIAL", Primary: true},
			{StructName: "Contact", Name: "email", Type: "VARCHAR(255)", Nullable: false},
			{
				StructName:          "Contact",
				Name:                "email_normalized",
				Type:                "VARCHAR(255)",
				GeneratedExpression: "lower(email)",
				GeneratedKind:       "stored",
			},
		},
		Indexes: []goschema.Index{
			{StructName: "Contact", Name: "idx_contacts_email_normalized", Fields: []string{"email_normalized"}},
		},
	}
}

func renderGeneratedColumnConformanceSQL(c *qt.C, target *goschema.Database, dialect string) string {
	createAST := fromschema.FromDatabase(*target, dialect)
	createSQL, err := renderer.RenderSQL(dialect, createAST.Statements...)
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(createSQL)
}

func execGeneratedColumnConformanceSQL(c *qt.C, db *sql.DB, sqlText string) {
	for _, stmt := range migrator.SplitSQLStatements(sqlText) {
		_, err := db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("generated-column schema statement must apply: %s", stmt))
	}
}

func filterGeneratedColumnConformanceSchema(in *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	keepTables := map[string]struct{}{
		"contacts": {},
	}
	out := *in
	out.Tables = filterTables(in.Tables, keepTables)
	out.Indexes = filterIndexes(in.Indexes, keepTables)
	out.Constraints = filterConstraints(in.Constraints, keepTables)
	return &out
}
