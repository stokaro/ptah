package atlashcl_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/atlashcl"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
)

func TestParseTablesColumnsPrimaryKeyAndIndexes(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
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
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Tables[0].Schema, qt.Equals, "main")
	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Fields[0].Name, qt.Equals, "id")
	c.Assert(db.Fields[0].Primary, qt.IsTrue)
	c.Assert(db.Fields[0].Nullable, qt.IsFalse)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"email"})

	sql := strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, `CREATE TABLE main.users`)
	c.Assert(sql, qt.Contains, `id int PRIMARY KEY`)
	c.Assert(sql, qt.Contains, `CREATE UNIQUE INDEX`)
}

func TestParseCompositePrimaryKeyAndForeignKey(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}

table "posts" {
  schema = schema.main
  column "tenant_id" {
    type = int
  }
  column "id" {
    type = int
  }
  column "owner_id" {
    type = int
    null = true
  }
  primary_key {
    columns = [column.tenant_id, column.id]
  }
  foreign_key "owner_id" {
    columns = [table.posts.column.owner_id]
    ref_columns = [table.users.column.id]
    on_delete = SET_NULL
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 2)
	posts := tableByName(db.Tables, "posts")
	c.Assert(posts.PrimaryKey, qt.DeepEquals, []string{"tenant_id", "id"})
	ownerID := fieldByName(db.Fields, "posts", "owner_id")
	c.Assert(ownerID.Foreign, qt.Equals, "users(id)")
	c.Assert(ownerID.ForeignKeyName, qt.Equals, "owner_id")
	c.Assert(db.Dependencies["main.posts"], qt.DeepEquals, []string{"main.users"})
}

func TestParseDefaultsAndChecks(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "accounts" {
  column "id" {
    type = bigint
    auto_increment = true
  }
  column "created_at" {
    type = timestamp
    default = sql("CURRENT_TIMESTAMP")
  }
  column "status" {
    type = varchar(20)
    default = "active"
  }
  primary_key {
    columns = [column.id]
  }
  check "status_valid" {
    expr = "status IN ('active', 'disabled')"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields[0].AutoInc, qt.IsTrue)
	c.Assert(db.Fields[1].DefaultExpr, qt.Equals, "CURRENT_TIMESTAMP")
	c.Assert(db.Fields[2].Default, qt.Equals, "active")
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Constraints[0].CheckExpression, qt.Equals, "status IN ('active', 'disabled')")
}

func TestParseBracketQuotedColumnReferences(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "email,normalized" {
    type = text
  }
  index "idx_users_email_normalized" {
    columns = [column["email,normalized"]]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"email,normalized"})
}

func TestParseRejectsUnsupportedSemanticColumnAttributes(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "slug" {
    type = text
    as = "lower(name)"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported column attribute "as".*`)
}

func TestParseRejectsUnsupportedSchemaAttributes(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
schema "main" {
  charset = "utf8mb4"
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported schema attribute "charset".*`)
}

func TestParseRejectsUnsupportedIndexPartAttributes(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "email" {
    type = varchar(255)
  }
  index "idx_users_email" {
    on {
      column = column.email
      prefix = 32
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported index on attribute "prefix".*`)
}

func TestParseRejectsForeignKeyWithUnknownLocalColumn(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
  }
}

table "posts" {
  column "id" {
    type = int
  }
  foreign_key "owner_id" {
    columns = [column.owner_id]
    ref_columns = [table.users.column.id]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*foreign_key "owner_id" references unknown local column "owner_id".*`)
}

func tableByName(tables []goschema.Table, name string) goschema.Table {
	for _, table := range tables {
		if table.Name == name {
			return table
		}
	}
	return goschema.Table{}
}

func fieldByName(fields []goschema.Field, structName, name string) goschema.Field {
	for _, field := range fields {
		if field.StructName == structName && field.Name == name {
			return field
		}
	}
	return goschema.Field{}
}
