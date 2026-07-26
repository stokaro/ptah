package toschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/internal/convert/toschema"
	"github.com/stokaro/ptah/internal/parser"
)

// These tests exercise the SQL -> goschema.Database path used by the
// external_schema source and SQL --schema-file, asserting that table-level
// PRIMARY KEY and ALTER TABLE ADD CONSTRAINT FOREIGN KEY survive (see #708).

func parseToDatabase(c *qt.C, sql string) goschema.Database {
	statements, err := parser.NewParser(sql).Parse()
	c.Assert(err, qt.IsNil)
	return toschema.ToDatabase(statements)
}

func findTable(db goschema.Database, name string) (goschema.Table, bool) {
	for _, t := range db.Tables {
		if t.Name == name {
			return t, true
		}
	}
	return goschema.Table{}, false
}

func findConstraint(db goschema.Database, typ, table string) (goschema.Constraint, bool) {
	for _, con := range db.Constraints {
		if con.Type == typ && con.Table == table {
			return con, true
		}
	}
	return goschema.Constraint{}, false
}

func TestToDatabase_TableLevelPrimaryKeyCaptured(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE users (id bigserial, name varchar(255), PRIMARY KEY (id));`)

	users, ok := findTable(db, "users")
	c.Assert(ok, qt.IsTrue)
	c.Assert(users.PrimaryKey, qt.DeepEquals, []string{"id"})
}

func TestToDatabase_AlterTableForeignKeyCaptured(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE users (id bigserial, PRIMARY KEY (id));
CREATE TABLE pets (id bigserial, user_id bigint, PRIMARY KEY (id));
ALTER TABLE pets ADD CONSTRAINT fk_pets_user FOREIGN KEY (user_id) REFERENCES users(id);`)

	fk, ok := findConstraint(db, "FOREIGN KEY", "pets")
	c.Assert(ok, qt.IsTrue)
	c.Assert(fk.Name, qt.Equals, "fk_pets_user")
	c.Assert(fk.Columns, qt.DeepEquals, []string{"user_id"})
	c.Assert(fk.ForeignTable, qt.Equals, "users")
	c.Assert(fk.ForeignColumn, qt.Equals, "id")
}

func TestToDatabase_TableLevelForeignKeyInCreateCaptured(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE pets (id bigserial, user_id bigint, FOREIGN KEY (user_id) REFERENCES users(id));`)

	fk, ok := findConstraint(db, "FOREIGN KEY", "pets")
	c.Assert(ok, qt.IsTrue)
	c.Assert(fk.ForeignTable, qt.Equals, "users")
	c.Assert(fk.Columns, qt.DeepEquals, []string{"user_id"})
}

func TestToDatabase_SQLRoundTripRendersPrimaryKeyAndForeignKey(t *testing.T) {
	c := qt.New(t)

	// End-to-end: SQL (as an ORM exporter emits it) -> goschema -> render must
	// preserve a single-column table-level PK (inline) and an ALTER TABLE foreign
	// key (#708).
	db := parseToDatabase(c, `CREATE TABLE users (id bigserial, name varchar(255), PRIMARY KEY (id));
CREATE TABLE pets (id bigserial, user_id bigint, PRIMARY KEY (id));
ALTER TABLE pets ADD CONSTRAINT fk_pets_user FOREIGN KEY (user_id) REFERENCES users(id);`)
	goschema.Finalize(&db)

	statements, err := renderer.GetOrderedCreateStatements(&db, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")

	c.Assert(rendered, qt.Contains, `"id" bigserial PRIMARY KEY`)
	c.Assert(rendered, qt.Contains, `ALTER TABLE "pets" ADD CONSTRAINT "fk_pets_user" FOREIGN KEY ("user_id") REFERENCES "users"("id")`)
}
