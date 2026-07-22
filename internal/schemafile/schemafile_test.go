package schemafile_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/schemadiff"
)

func TestLoad_SQLFile(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`
CREATE TABLE users (
  id INTEGER PRIMARY KEY,
  name TEXT NOT NULL
);
CREATE INDEX idx_users_name ON users (name);
`), 0o600), qt.IsNil)

	db, err := schemafile.Load("file://"+path, schemafile.Options{Dialect: platform.SQLite})

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "idx_users_name")
}

func TestToDBSchema_PreservesTableAndColumnMetadata(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(t.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(`
table "users" {
  column "id" {
    type = int
  }
  column "email" {
    null = false
    type = varchar(255)
  }
  primary_key {
    columns = [column.id]
  }
  index "idx_users_email" {
    unique = true
    columns = [column.email]
  }
}
`), 0o600), qt.IsNil)

	db, err := schemafile.Load(path, schemafile.Options{})
	c.Assert(err, qt.IsNil)

	got := schemafile.ToDBSchema(db)

	c.Assert(got.Tables, qt.HasLen, 1)
	c.Assert(got.Tables[0].Name, qt.Equals, "users")
	c.Assert(got.Tables[0].Columns, qt.HasLen, 2)
	c.Assert(got.Tables[0].Columns[0].Name, qt.Equals, "id")
	c.Assert(got.Tables[0].Columns[0].IsPrimaryKey, qt.IsTrue)
	c.Assert(got.Tables[0].Columns[1].Name, qt.Equals, "email")
	c.Assert(got.Tables[0].Columns[1].IsNullable, qt.Equals, "NO")
	c.Assert(got.Indexes, qt.HasLen, 1)
	c.Assert(got.Indexes[0].Name, qt.Equals, "idx_users_email")
	c.Assert(got.Indexes[0].IsUnique, qt.IsTrue)
}

func TestToDBSchema_FieldLevelConstraintsStayIdempotent(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Post", Name: "posts"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "User",
				Name:       "status",
				Type:       "TEXT",
				Check:      "status IN ('active', 'disabled')",
				CheckName:  "users_status_check",
			},
			{StructName: "Post", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName:     "Post",
				Name:           "user_id",
				Type:           "INTEGER",
				Foreign:        "users(id)",
				ForeignKeyName: "posts_user_id_fkey",
				OnDelete:       "CASCADE",
			},
		},
	}

	current := schemafile.ToDBSchema(db)
	diff := schemadiff.CompareWithDialect(db, current, platform.Postgres)

	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("field-level CHECK/FK should not produce a file-to-file churn diff: %#v", diff))
}

func TestToDBSchema_ExplicitConstraintOverridesFieldLevelConstraintWithSameName(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "User", Name: "users"}},
		Fields: []goschema.Field{{
			StructName: "User",
			Name:       "status",
			Type:       "TEXT",
			Check:      "status <> ''",
			CheckName:  "users_status_check",
		}},
		Constraints: []goschema.Constraint{{
			StructName:      "User",
			Name:            "users_status_check",
			Type:            "CHECK",
			Table:           "users",
			CheckExpression: "status IN ('active', 'disabled')",
		}},
	}

	got := schemafile.ToDBSchema(db)

	c.Assert(got.Constraints, qt.HasLen, 1)
	c.Assert(*got.Constraints[0].CheckClause, qt.Equals, "status IN ('active', 'disabled')")
}

func TestLocalFilePath_RejectsRemoteURL(t *testing.T) {
	c := qt.New(t)

	_, err := schemafile.LocalFilePath("postgres://localhost/db")

	c.Assert(err, qt.ErrorMatches, `only local file:// schema files are supported`)
}
