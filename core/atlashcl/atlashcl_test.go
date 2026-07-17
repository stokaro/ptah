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
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{Name: "main"}})
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

func TestParsePrimaryKeyParts(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "tokens" {
  schema = schema.main
  column "id" {
    type = tinytext
  }
  column "tenant_id" {
    type = tinytext
  }
  primary_key {
    on {
      column = column.id
      prefix = 7
    }
    on {
      desc   = true
      column = column.tenant_id
      prefix = 1
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id", "tenant_id"})
	c.Assert(db.Tables[0].PrimaryKeyParts, qt.DeepEquals, []goschema.PrimaryKeyPart{
		{Name: "id", Prefix: "7"},
		{Name: "tenant_id", Prefix: "1", Desc: true},
	})

	sql := strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n")
	c.Assert(sql, qt.Contains, "PRIMARY KEY (id (7), tenant_id (1) DESC)")
	c.Assert(sql, qt.Not(qt.Contains), "id tinytext PRIMARY KEY")
}

func TestParseSQLiteTableOptions(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "events" {
  schema = schema.main
  strict = true
  without_rowid = true
  column "id" {
    null = false
    type = integer
  }
  primary_key {
    columns = [column.id]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Strict, qt.IsTrue)
	c.Assert(db.Tables[0].WithoutRowID, qt.IsTrue)
}

func TestParseMySQLTableAutoIncrement(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  auto_increment = 1000
  column "id" {
    null = false
    type = bigint
    auto_increment = true
  }
  primary_key {
    columns = [column.id]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].AutoIncrement, qt.Equals, "1000")

	sql := strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n")
	c.Assert(sql, qt.Contains, "AUTO_INCREMENT=1000")
}

func TestParseMySQLTableCharsetCollate(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  charset = "utf8mb4"
  collate = "utf8mb4_bin"
  column "name" {
    null = false
    type = varchar(255)
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Charset, qt.Equals, "utf8mb4")
	c.Assert(db.Tables[0].Collate, qt.Equals, "utf8mb4_bin")

	sql := strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n")
	c.Assert(sql, qt.Contains, "CHARSET=utf8mb4")
	c.Assert(sql, qt.Contains, "COLLATE=utf8mb4_bin")
}

func TestParseSQLiteTableOptionsRejectNonBool(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "events" {
  strict = "true"
  column "id" {
    type = integer
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*table attribute "strict" must be a bool.*`)
}

func TestParseIndexParts(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "rank" {
    type = int
  }
  column "score" {
    type = int
  }
  index "rank_score_idx" {
    on {
      column = column.rank
      prefix = 7
    }
    on {
      column = column.score
      desc = true
    }
  }
  index "full_name" {
    on {
      expr = "first_name || ' ' || last_name"
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 2)
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"rank", "score"})
	c.Assert(db.Indexes[0].Parts, qt.DeepEquals, []goschema.IndexPart{
		{Name: "rank", Prefix: "7"},
		{Name: "score", Desc: true},
	})
	c.Assert(db.Indexes[1].Fields, qt.DeepEquals, []string{"first_name || ' ' || last_name"})
	c.Assert(db.Indexes[1].Parts, qt.DeepEquals, []goschema.IndexPart{
		{Expr: "first_name || ' ' || last_name"},
	})
}

func TestParseFulltextIndexParser(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "bio" {
    type = text
  }
  index "idx_users_bio" {
    type = FULLTEXT
    parser = ngram
    columns = [column.bio]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"bio"})
	c.Assert(db.Indexes[0].Type, qt.Equals, "FULLTEXT")
	c.Assert(db.Indexes[0].Parser, qt.Equals, "ngram")
}

func TestParseRejectsIndexParserWithoutFulltext(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "bio" {
    type = text
  }
  index "idx_users_bio" {
    parser = ngram
    columns = [column.bio]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*index parser requires FULLTEXT type.*`)
}

func TestParseRejectsIndexColumnsAndOnBlocks(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "email" {
    type = varchar(255)
  }
  index "idx_users_email" {
    columns = [column.email]
    on {
      column = column.email
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*index cannot mix columns attribute with on blocks.*`)
}

func TestParseSchemaComment(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "public" {
  comment = "This is a test schema"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{
		Name:    "public",
		Comment: "This is a test schema",
	}})

	sql := strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n")
	c.Assert(sql, qt.Contains, `CREATE SCHEMA IF NOT EXISTS public;`)
	c.Assert(sql, qt.Contains, `COMMENT ON SCHEMA public IS 'This is a test schema';`)
}

func TestParseSchemaCharsetAndCollate(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "app" {
  charset = "utf8mb4"
  collate = "utf8mb4_0900_ai_ci"
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Schemas, qt.DeepEquals, []goschema.Schema{{
		Name:    "app",
		Charset: "utf8mb4",
		Collate: "utf8mb4_0900_ai_ci",
	}})

	sql := strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n")
	c.Assert(sql, qt.Contains, `CREATE SCHEMA IF NOT EXISTS app DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;`)
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

func TestParseGeneratedColumns(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "slug" {
    type = text
    as = "lower(name)"
  }
  column "name_key" {
    type = text
    as {
      expr = "lower(name)"
      type = STORED
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Fields[0].GeneratedExpression, qt.Equals, "lower(name)")
	c.Assert(db.Fields[0].GeneratedKind, qt.Equals, "VIRTUAL")
	c.Assert(db.Fields[1].GeneratedExpression, qt.Equals, "lower(name)")
	c.Assert(db.Fields[1].GeneratedKind, qt.Equals, "STORED")
}

func TestParseRejectsInvalidGeneratedColumnForms(t *testing.T) {
	tests := []struct {
		name  string
		hcl   string
		match string
	}{
		{
			name: "as attribute and block",
			hcl: `
table "users" {
  column "slug" {
    type = text
    as = "lower(name)"
    as {
      expr = "lower(name)"
    }
  }
}
`,
			match: `.*column cannot mix as attribute with as block.*`,
		},
		{
			name: "as block without expr",
			hcl: `
table "users" {
  column "slug" {
    type = text
    as {
      type = STORED
    }
  }
}
`,
			match: `.*column as block requires expr.*`,
		},
		{
			name: "unsupported as block attribute",
			hcl: `
table "users" {
  column "slug" {
    type = text
    as {
      expr = "lower(name)"
      unknown = true
    }
  }
}
`,
			match: `.*unsupported column as attribute "unknown".*`,
		},
		{
			name: "unsupported column block",
			hcl: `
table "users" {
  column "slug" {
    type = text
    generated {
      expr = "lower(name)"
    }
  }
}
`,
			match: `.*unsupported column block "generated".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.Parse([]byte(test.hcl), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
}

func TestParseRejectsUnsupportedSchemaAttributes(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
schema "main" {
  owner = "app"
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported schema attribute "owner".*`)
}

func TestParseRejectsIndexExprPrefix(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "email" {
    type = varchar(255)
  }
  index "idx_users_email" {
    on {
      expr = "lower(email)"
      prefix = 32
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*index on prefix requires column.*`)
}

func TestParseRejectsInvalidIndexParts(t *testing.T) {
	tests := []struct {
		name  string
		part  string
		match string
	}{
		{
			name: "missing column and expr",
			part: `
    on {
      desc = true
    }`,
			match: `.*index on block requires column or expr.*`,
		},
		{
			name: "column and expr together",
			part: `
    on {
      column = column.email
      expr = "lower(email)"
    }`,
			match: `.*index on block cannot set both column and expr.*`,
		},
		{
			name: "non-bool desc",
			part: `
    on {
      column = column.email
      desc = "true"
    }`,
			match: `.*index on attribute "desc" must be a bool.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.Parse([]byte(`
table "users" {
  column "email" {
    type = varchar(255)
  }
  index "idx_users_email" {`+test.part+`
  }
}
`), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
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
