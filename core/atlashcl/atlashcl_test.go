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

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `CREATE TABLE main.users`)
	c.Assert(sql, qt.Contains, `id int PRIMARY KEY`)
	c.Assert(sql, qt.Contains, `CREATE UNIQUE INDEX`)
}

func TestParseTablePartition(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "metrics" {
  schema = schema.main
  column "x" {
    null = false
    type = integer
  }
  column "y" {
    null = false
    type = integer
  }
  partition {
    type = RANGE
    by {
      column = column.x
    }
    by {
      expr = "floor(y)"
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Partition, qt.DeepEquals, &goschema.PartitionSpec{
		Type: "RANGE",
		Parts: []goschema.PartitionPart{
			{Name: "x"},
			{Expr: "floor(y)"},
		},
	})

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, `PARTITION BY RANGE (x, (floor(y)))`)
}

func TestParseTablePartitionRequiresColumnOrBy(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "metrics" {
  column "x" {
    type = integer
  }
  partition {
    type    = RANGE
    columns = []
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*partition requires at least one column.*`)
}

func TestParsePostgreSQLEnumBlock(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

enum "status" {
  schema = schema.main
  values = ["active", "inactive"]
}

table "users" {
  schema = schema.main
  column "status" {
    type    = enum.status
    default = "active"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Enums, qt.DeepEquals, []goschema.Enum{
		{Name: "status", Values: []string{"active", "inactive"}},
	})
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Type, qt.Equals, "status")
	c.Assert(db.Fields[0].Default, qt.Equals, "active")
	c.Assert(db.Fields[0].DefaultSet, qt.IsTrue)

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, "CREATE TYPE status AS ENUM ('active', 'inactive');")
	c.Assert(sql, qt.Contains, "status status NOT NULL DEFAULT 'active'")
}

func TestParseEnumRequiresStringValues(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
enum "status" {
  values = ["active", 1]
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*values must be a list of strings.*`)
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

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n"))
	c.Assert(sql, qt.Contains, "PRIMARY KEY (id (7), tenant_id (1) DESC)")
	c.Assert(sql, qt.Not(qt.Contains), "id tinytext PRIMARY KEY")
}

func TestParsePrimaryKeyInclude(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "covering" {
    type = int
  }
  primary_key {
    columns = [column.id]
    include = [column.covering]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})
	c.Assert(db.Tables[0].PrimaryKeyParts, qt.DeepEquals, []goschema.PrimaryKeyPart{{Name: "id"}})
	c.Assert(db.Tables[0].PrimaryKeyInclude, qt.DeepEquals, []string{"covering"})

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, "PRIMARY KEY (id) INCLUDE (covering)")
	c.Assert(sql, qt.Not(qt.Contains), "id int PRIMARY KEY")
}

func TestParsePrimaryKeyType(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  column "id" {
    type = varchar(128)
  }
  primary_key {
    columns = [column.id]
    type    = HASH
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].PrimaryKey, qt.DeepEquals, []string{"id"})

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n"))
	c.Assert(sql, qt.Contains, "id varchar(128) PRIMARY KEY")
}

func TestParsePrimaryKeyTypeRejectsUnknownValue(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
schema "main" {}

table "users" {
  schema = schema.main
  column "id" {
    type = varchar(128)
  }
  primary_key {
    columns = [column.id]
    type    = GIANT
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported primary_key type "GIANT".*`)
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

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n"))
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

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n"))
	c.Assert(sql, qt.Contains, "CHARSET=utf8mb4")
	c.Assert(sql, qt.Contains, "COLLATE=utf8mb4_bin")
}

func TestParseMySQLColumnCharsetCollate(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "name" {
    null = false
    type = varchar(255)
    charset = "hebrew"
    collate = "hebrew_general_ci"
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Charset, qt.Equals, "hebrew")
	c.Assert(db.Fields[0].Collate, qt.Equals, "hebrew_general_ci")

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n"))
	c.Assert(sql, qt.Contains, "name varchar(255) CHARACTER SET hebrew COLLATE hebrew_general_ci NOT NULL")
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
      ops    = bpchar_ops
      prefix = 7
    }
    on {
      column = column.score
      ops    = sql("tsvector_ops(siglen=8)")
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
		{Name: "rank", Operator: "bpchar_ops", Prefix: "7"},
		{Name: "score", Operator: "tsvector_ops(siglen=8)", Desc: true},
	})
	c.Assert(db.Indexes[1].Fields, qt.DeepEquals, []string{"first_name || ' ' || last_name"})
	c.Assert(db.Indexes[1].Parts, qt.DeepEquals, []goschema.IndexPart{
		{Expr: "first_name || ' ' || last_name"},
	})
}

func TestParseIndexInclude(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "name" {
    type = text
  }
  column "active" {
    type = bool
  }
  index "idx_users_name" {
    columns = [column.name]
    include = [column.active]
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"name"})
	c.Assert(db.Indexes[0].IncludeColumns, qt.DeepEquals, []string{"active"})
}

func TestParsePostgreSQLIndexStorageParams(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "c" {
    type = int
  }
  index "idx_users_c" {
    type = BRIN
    columns = [column.c]
    page_per_range = 2
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Type, qt.Equals, "BRIN")
	c.Assert(db.Indexes[0].StorageParams, qt.DeepEquals, map[string]string{"pages_per_range": "2"})
}

func TestParsePostgreSQLIndexStorageParamsRejectsDuplicateAliases(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "c" {
    type = int
  }
  index "idx_users_c" {
    type = BRIN
    columns = [column.c]
    page_per_range = 2
    pages_per_range = 3
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*index cannot set both page_per_range and pages_per_range`)
}

func TestParsePostgreSQLNullsDistinctIndexAndUnique(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "c" {
    type = int
  }
  index "nulls_not_distinct" {
    unique = true
    columns = [column.c]
    nulls_distinct = false
  }
  unique "nulls_not_distinct2" {
    columns = [column.c]
    nulls_distinct = false
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Unique, qt.IsTrue)
	c.Assert(db.Indexes[0].NullsDistinct, qt.IsNotNil)
	c.Assert(*db.Indexes[0].NullsDistinct, qt.IsFalse)
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Constraints[0].Type, qt.Equals, "UNIQUE")
	c.Assert(db.Constraints[0].Columns, qt.DeepEquals, []string{"c"})
	c.Assert(db.Constraints[0].NullsDistinct, qt.IsNotNil)
	c.Assert(*db.Constraints[0].NullsDistinct, qt.IsFalse)

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n"))
	c.Assert(sql, qt.Contains, "CREATE UNIQUE INDEX IF NOT EXISTS nulls_not_distinct ON users (c) NULLS NOT DISTINCT")
	c.Assert(sql, qt.Contains, "CONSTRAINT nulls_not_distinct2 UNIQUE NULLS NOT DISTINCT (c)")
}

func TestParsePostgreSQLIndexNullsDistinctRequiresUnique(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "c" {
    type = int
  }
  index "idx_users_c" {
    columns = [column.c]
    nulls_distinct = false
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*index nulls_distinct requires unique = true`)
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

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "postgres"), "\n"))
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

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n"))
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

func TestParseCompositeForeignKey(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "accounts" {
  column "tenant_id" {
    type = int
  }
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.tenant_id, column.id]
  }
}

table "posts" {
  column "tenant_id" {
    type = int
  }
  column "owner_id" {
    type = int
  }
  foreign_key "owner_ref" {
    columns = [column.tenant_id, column.owner_id]
    ref_columns = [table.accounts.column.tenant_id, table.accounts.column.id]
    on_delete = CASCADE
    on_update = NO_ACTION
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)

	constraint := constraintByName(db.Constraints, "owner_ref")
	c.Assert(constraint.Type, qt.Equals, "FOREIGN KEY")
	c.Assert(constraint.Table, qt.Equals, "posts")
	c.Assert(constraint.Columns, qt.DeepEquals, []string{"tenant_id", "owner_id"})
	c.Assert(constraint.ForeignTable, qt.Equals, "accounts")
	c.Assert(constraint.ForeignColumn, qt.Equals, "tenant_id")
	c.Assert(constraint.ForeignColumns, qt.DeepEquals, []string{"tenant_id", "id"})
	c.Assert(constraint.OnDelete, qt.Equals, "CASCADE")
	c.Assert(constraint.OnUpdate, qt.Equals, "NO_ACTION")
	c.Assert(db.Dependencies["posts"], qt.DeepEquals, []string{"accounts"})

	sql := legacyRenderedSQL(strings.Join(renderer.GetOrderedCreateStatements(db, "mysql"), "\n"))
	c.Assert(sql, qt.Contains, `CONSTRAINT owner_ref FOREIGN KEY (tenant_id, owner_id) REFERENCES accounts(tenant_id, id) ON DELETE CASCADE ON UPDATE NO_ACTION`)
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
	c.Assert(db.Fields[2].DefaultSet, qt.IsTrue)
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Constraints[0].CheckExpression, qt.Equals, "status IN ('active', 'disabled')")
}

func TestParseEmptyLiteralDefault(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "name" {
    type = varchar(255)
    default = ""
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Default, qt.Equals, "")
	c.Assert(db.Fields[0].DefaultSet, qt.IsTrue)
	c.Assert(db.Fields[0].DefaultExpr, qt.Equals, "")
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
	c.Assert(db.Fields[0].GeneratedKind, qt.Equals, "")
	c.Assert(db.Fields[1].GeneratedExpression, qt.Equals, "lower(name)")
	c.Assert(db.Fields[1].GeneratedKind, qt.Equals, "STORED")
}

func TestParseIdentityColumn(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
    null = false
    identity {
      generated = ALWAYS
      start = 10
      increment = 5
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].AutoInc, qt.IsTrue)
	c.Assert(db.Fields[0].IdentityGeneration, qt.Equals, "ALWAYS")
	c.Assert(db.Fields[0].IdentityStart, qt.Equals, "10")
	c.Assert(db.Fields[0].IdentityIncrement, qt.Equals, "5")
}

func TestParseIdentityColumnDefaultsGeneration(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
    identity {
      start = 100
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].AutoInc, qt.IsTrue)
	c.Assert(db.Fields[0].IdentityGeneration, qt.Equals, "BY_DEFAULT")
	c.Assert(db.Fields[0].IdentityStart, qt.Equals, "100")
}

func TestParseIdentityColumnRejectsUnsupportedAttrs(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
    identity {
      generated = ALWAYS
      cache = 10
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported column identity attribute "cache".*`)
}

func TestParseIdentityColumnRejectsInvalidGeneration(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "users" {
  column "id" {
    type = int
    identity {
      generated = SOMETIMES
    }
  }
}
`), "schema.hcl")
	c.Assert(err, qt.ErrorMatches, `.*unsupported identity generated value "SOMETIMES".*`)
}

func TestParseColumnOnUpdateExpression(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
table "users" {
  column "updated_at" {
    null      = false
    type      = timestamp(6)
    default   = sql("CURRENT_TIMESTAMP(6)")
    on_update = sql("CURRENT_TIMESTAMP(6)")
  }
}
`), "schema.hcl")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].DefaultExpr, qt.Equals, "CURRENT_TIMESTAMP(6)")
	c.Assert(db.Fields[0].UpdateExpression, qt.Equals, "CURRENT_TIMESTAMP(6)")
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

func TestParsePostgreSQLSchemaObjects(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "public" {}

extension "pg_trgm" {
  version = "1.6"
  comment = "trigram search"
}

role "app_user" {
  login   = true
  inherit = true
  comment = "application role"
}

table "users" {
  schema = schema.public

  column "id" {
    type = int
  }

  row_security {
    enabled = true
  }
}

function "get_tenant" {
  schema     = schema.public
  lang       = SQL
  arg "tenant_id" {
    type = text
  }
  return     = text
  security   = DEFINER
  volatility = STABLE
  as         = "SELECT $1"
  comment    = "tenant helper"
}

view "active_users" {
  schema       = schema.public
  as           = "SELECT id FROM users"
  check_option = LOCAL
  comment      = "active users"
}

materialized "user_stats" {
  schema  = schema.public
  as      = "SELECT count(*) FROM users"
  comment = "user stats"
}

trigger "users_set_updated_at" {
  on = table.users
  before {
    update = true
  }
  for     = ROW
  as      = "NEW.updated_at = now(); RETURN NEW;"
  comment = "timestamp trigger"
}

policy "users_tenant_policy" {
  on      = table.users
  for     = SELECT
  to      = [role.app_user, PUBLIC, "adhoc_role"]
  using   = "true"
  check   = "true"
  comment = "tenant policy"
}

permission {
  to         = role.app_user
  for        = table.users
  privileges = [SELECT, INSERT]
  grantable  = true
  comment    = "table grant"
}

permission {
  to         = PUBLIC
  for        = schema.public
  privileges = [USAGE]
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Extensions, qt.HasLen, 1)
	c.Assert(db.Extensions[0].Version, qt.Equals, "1.6")
	c.Assert(db.Roles, qt.HasLen, 1)
	c.Assert(db.Roles[0].Login, qt.IsTrue)
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(db.Functions[0].Name, qt.Equals, "public.get_tenant")
	c.Assert(db.Functions[0].Parameters, qt.Equals, "tenant_id text")
	c.Assert(db.Functions[0].Security, qt.Equals, "DEFINER")
	c.Assert(db.Views, qt.HasLen, 1)
	c.Assert(db.Views[0].Name, qt.Equals, "public.active_users")
	c.Assert(db.Views[0].WithCheck, qt.IsTrue)
	c.Assert(db.MaterializedViews, qt.HasLen, 1)
	c.Assert(db.MaterializedViews[0].Name, qt.Equals, "public.user_stats")
	c.Assert(db.Triggers, qt.HasLen, 1)
	c.Assert(db.Triggers[0].Event, qt.Equals, "UPDATE")
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.RLSEnabledTables[0].Table, qt.Equals, "public.users")
	c.Assert(db.RLSPolicies, qt.HasLen, 1)
	c.Assert(db.RLSPolicies[0].ToRoles, qt.Equals, "app_user,PUBLIC,adhoc_role")
	c.Assert(db.Grants, qt.HasLen, 2)
	tableGrant := grantByTable(db.Grants, "users")
	c.Assert(tableGrant.Privileges, qt.DeepEquals, []string{"SELECT", "INSERT"})
	c.Assert(tableGrant.WithOption, qt.IsTrue)
	schemaGrant := grantBySchema(db.Grants, "public")
	c.Assert(schemaGrant.Role, qt.Equals, "PUBLIC")
}

func TestParseRejectsUnsupportedExtensionSchema(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
extension "pg_trgm" {
  schema = schema.public
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*unsupported extension attribute "schema".*`)
}

func TestParseRejectsIncompleteSchemaObjects(t *testing.T) {
	tests := []struct {
		name  string
		input string
		match string
	}{
		{
			name: "function missing body",
			input: `
function "missing_body" {
  lang = SQL
}`,
			match: `.*function "missing_body" requires as.*`,
		},
		{
			name: "permission unsupported target",
			input: `
permission {
  to         = role.app_user
  for        = function.get_tenant
  privileges = [EXECUTE]
}`,
			match: `.*permission requires table or schema target.*`,
		},
		{
			name: "permission missing privileges",
			input: `
permission {
  to  = role.app_user
  for = table.users
}`,
			match: `.*permission requires privileges.*`,
		},
		{
			name: "permission non-bool grantable",
			input: `
permission {
  to         = role.app_user
  for        = table.users
  privileges = [SELECT]
  grantable  = "true"
}`,
			match: `.*permission attribute "grantable" must be a bool.*`,
		},
		{
			name: "role non-bool superuser",
			input: `
role "app_user" {
  superuser = "true"
}`,
			match: `.*role attribute "superuser" must be a bool.*`,
		},
		{
			name: "row_security non-bool enabled",
			input: `
table "users" {
  column "id" {
    type = int
  }
  row_security {
    enabled = "true"
  }
}`,
			match: `.*row_security attribute "enabled" must be a bool.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.Parse([]byte(test.input), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.match)
		})
	}
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

func constraintByName(constraints []goschema.Constraint, name string) goschema.Constraint {
	for _, constraint := range constraints {
		if constraint.Name == name {
			return constraint
		}
	}
	return goschema.Constraint{}
}

func grantByTable(grants []goschema.Grant, table string) goschema.Grant {
	for _, grant := range grants {
		if grant.OnTable == table || strings.HasSuffix(grant.OnTable, "."+table) {
			return grant
		}
	}
	return goschema.Grant{}
}

func grantBySchema(grants []goschema.Grant, schema string) goschema.Grant {
	for _, grant := range grants {
		if grant.OnSchema == schema {
			return grant
		}
	}
	return goschema.Grant{}
}
