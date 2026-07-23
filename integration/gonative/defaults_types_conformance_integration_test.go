//go:build integration

package gonative_test

import (
	"database/sql"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	mysqlreader "github.com/stokaro/ptah/internal/dbschema/mysql"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestDefaultsTypesConformanceFixture_RoundTrip_MySQL(t *testing.T) {
	dsn := skipIfNoMySQL(t)
	c := qt.New(t)

	db, err := sql.Open("mysql", dsn)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	_, _ = db.Exec("DROP TABLE IF EXISTS invoices")
	defer func() { _, _ = db.Exec("DROP TABLE IF EXISTS invoices") }()

	target := defaultsTypesConformanceSchema()
	createSQL := renderDefaultsTypesConformanceSQL(c, target, platform.MySQL)
	execDefaultsTypesConformanceSQL(c, db, createSQL)

	reader := mysqlreader.NewMySQLReader(db, "")
	liveSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	liveSchema = filterDefaultsTypesConformanceSchema(liveSchema)

	diff := schemadiff.CompareWithDialect(target, liveSchema, platform.MySQL)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("round-trip diff: %+v", diff))
}

func defaultsTypesConformanceSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Invoice", Name: "invoices"},
		},
		Fields: []goschema.Field{
			{StructName: "Invoice", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName: "Invoice",
				Name:       "invoice_number",
				Type:       "VARCHAR(32)",
				Nullable:   false,
				Unique:     true,
			},
			{
				StructName: "Invoice",
				Name:       "subtotal",
				Type:       "DECIMAL(12,2)",
				Nullable:   false,
				Default:    "0.00",
				DefaultSet: true,
			},
			{
				StructName:  "Invoice",
				Name:        "tax_rate",
				Type:        "DECIMAL(5,4)",
				Nullable:    false,
				DefaultExpr: "0",
			},
			{
				StructName:  "Invoice",
				Name:        "issued_at",
				Type:        "TIMESTAMP",
				Nullable:    false,
				DefaultExpr: "CURRENT_TIMESTAMP",
			},
			{
				StructName: "Invoice",
				Name:       "paid",
				Type:       "BOOLEAN",
				Nullable:   false,
				Default:    "false",
				DefaultSet: true,
			},
		},
	}
}

func renderDefaultsTypesConformanceSQL(c *qt.C, target *goschema.Database, dialect string) string {
	createAST := fromschema.FromDatabase(*target, dialect)
	createSQL, err := renderer.RenderSQL(dialect, createAST.Statements...)
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(createSQL)
}

func execDefaultsTypesConformanceSQL(c *qt.C, db *sql.DB, sqlText string) {
	for _, stmt := range migrator.SplitSQLStatements(sqlText) {
		_, err := db.Exec(stmt)
		c.Assert(err, qt.IsNil, qt.Commentf("defaults/types schema statement must apply: %s", stmt))
	}
}

func filterDefaultsTypesConformanceSchema(in *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	keepTables := map[string]struct{}{
		"invoices": {},
	}
	out := *in
	out.Tables = filterTables(in.Tables, keepTables)
	out.Indexes = filterIndexes(in.Indexes, keepTables)
	out.Constraints = filterConstraints(in.Constraints, keepTables)
	return &out
}
