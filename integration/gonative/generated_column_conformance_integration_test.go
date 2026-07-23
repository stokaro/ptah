//go:build integration

package gonative_test

import (
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	mysqlreader "github.com/stokaro/ptah/internal/dbschema/mysql"
	postgresreader "github.com/stokaro/ptah/internal/dbschema/postgres"
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
	createSQL := renderConformanceSQL(c, target, platform.Postgres)
	execConformanceSQL(c, db, createSQL, "generated-column")

	reader := postgresreader.NewPostgreSQLReader(db, "public")
	liveSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	liveSchema = filterConformanceSchema(liveSchema, generatedColumnConformanceTables())

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
	createSQL := renderConformanceSQL(c, target, platform.MySQL)
	execConformanceSQL(c, db, createSQL, "generated-column")

	reader := mysqlreader.NewMySQLReader(db, "")
	liveSchema, err := reader.ReadSchema()
	c.Assert(err, qt.IsNil)
	liveSchema = filterConformanceSchema(liveSchema, generatedColumnConformanceTables())

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

func generatedColumnConformanceTables() map[string]struct{} {
	return map[string]struct{}{
		"contacts": {},
	}
}
