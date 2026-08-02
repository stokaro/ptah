package toschema_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/convert/toschema"
	"go.5x5.cz/ptah/internal/parser"
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

func TestToDatabase_ForeignKeysPreserveTableReferenceIdentity(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE "tenant.data" (id BIGINT PRIMARY KEY);
CREATE TABLE "tenant"."data" (id BIGINT PRIMARY KEY);
CREATE TABLE children (
	literal_id BIGINT REFERENCES "tenant.data"(id),
	qualified_id BIGINT REFERENCES "tenant"."data"(id)
);`)

	c.Assert(db.Fields, qt.HasLen, 4)
	c.Assert(db.Fields[2].Foreign, qt.Equals, `"tenant.data"(id)`)
	c.Assert(db.Fields[3].Foreign, qt.Equals, "tenant.data(id)")

	goschema.Finalize(&db)
	statements, err := renderer.GetOrderedCreateStatements(&db, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")

	c.Assert(rendered, qt.Contains, `REFERENCES "tenant.data"("id")`)
	c.Assert(rendered, qt.Contains, `REFERENCES "tenant"."data"("id")`)
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

func TestToDatabase_QuotedIdentifiersAreCanonicalized(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE "users" (
		"id" INTEGER PRIMARY KEY,
		"email" TEXT NOT NULL,
		CONSTRAINT "users_email_key" UNIQUE ("email")
	);
	CREATE TABLE "posts" (
		"id" INTEGER PRIMARY KEY,
		"user_id" INTEGER NOT NULL,
		CONSTRAINT "posts_user_fk" FOREIGN KEY ("user_id") REFERENCES "users" ("id")
	);
	CREATE INDEX "posts_user_idx" ON "posts" ("user_id");`)

	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.Tables[0].Name, qt.Equals, "users")
	c.Assert(db.Tables[1].Name, qt.Equals, "posts")
	c.Assert(db.Fields, qt.HasLen, 4)
	c.Assert(db.Fields[0].Name, qt.Equals, "id")
	c.Assert(db.Fields[1].Name, qt.Equals, "email")
	c.Assert(db.Fields[2].Name, qt.Equals, "id")
	c.Assert(db.Fields[3].Name, qt.Equals, "user_id")
	c.Assert(db.Constraints, qt.HasLen, 2)
	c.Assert(db.Constraints[0].Name, qt.Equals, "users_email_key")
	c.Assert(db.Constraints[0].Columns, qt.DeepEquals, []string{"email"})
	c.Assert(db.Constraints[1].Name, qt.Equals, "posts_user_fk")
	c.Assert(db.Constraints[1].Table, qt.Equals, "posts")
	c.Assert(db.Constraints[1].Columns, qt.DeepEquals, []string{"user_id"})
	c.Assert(db.Constraints[1].ForeignTable, qt.Equals, "users")
	c.Assert(db.Constraints[1].ForeignColumn, qt.Equals, "id")
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "posts_user_idx")
	c.Assert(db.Indexes[0].TableName, qt.Equals, "posts")
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"user_id"})
}

func TestToDatabase_QuotedDotsPreserveIdentifierBoundaries(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE "tenant.data" ("event.id" INTEGER);
		CREATE INDEX "literal.lookup" ON "tenant.data" ("event.id");
		CREATE TABLE "tenant"."data" ("id" INTEGER);
		CREATE INDEX "qualified.lookup" ON "tenant"."data" ("id");`)

	c.Assert(db.Tables, qt.HasLen, 2)
	c.Assert(db.Tables[0].Schema, qt.Equals, "")
	c.Assert(db.Tables[0].Name, qt.Equals, "tenant.data")
	c.Assert(db.Tables[1].Schema, qt.Equals, "tenant")
	c.Assert(db.Tables[1].Name, qt.Equals, "data")
	c.Assert(db.Fields, qt.HasLen, 2)
	c.Assert(db.Fields[0].Name, qt.Equals, "event.id")
	c.Assert(db.Fields[1].Name, qt.Equals, "id")
	c.Assert(db.Indexes, qt.HasLen, 2)
	c.Assert(db.Indexes[0].TableName, qt.Equals, `"tenant.data"`)
	c.Assert(db.Indexes[1].TableName, qt.Equals, "tenant.data")

	statements, err := renderer.GetOrderedCreateStatements(&db, "postgres")
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, `CREATE TABLE "tenant.data"`)
	c.Assert(rendered, qt.Contains, `"event.id" INTEGER`)
	c.Assert(rendered, qt.Contains, `CREATE TABLE "tenant"."data"`)
	c.Assert(rendered, qt.Contains, `CREATE INDEX IF NOT EXISTS "literal.lookup" ON "tenant.data"`)
	c.Assert(rendered, qt.Contains, `CREATE INDEX IF NOT EXISTS "qualified.lookup" ON "tenant"."data"`)
}

func TestToDatabase_SQLServerBracketIdentifiersStayAtomic(t *testing.T) {
	c := qt.New(t)

	sql := `CREATE TABLE [user's] (
		[event--id] INTEGER,
		[event/*id] INTEGER,
		[close]]id] INTEGER
	);`
	statements, err := parser.NewParser(sql, parser.WithDialect("sqlserver")).Parse()
	c.Assert(err, qt.IsNil)

	db := toschema.ToDatabase(statements)

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "user's")
	c.Assert(db.Fields, qt.HasLen, 3)
	c.Assert(db.Fields[0].Name, qt.Equals, "event--id")
	c.Assert(db.Fields[1].Name, qt.Equals, "event/*id")
	c.Assert(db.Fields[2].Name, qt.Equals, "close]id")
}

func TestToDatabase_ParserCanonicalizesEscapedDoubleQuotes(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE "user""events" ("event""id" INTEGER);`)

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, `user"events`)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Name, qt.Equals, `event"id`)
}

func TestToDatabase_ParserCanonicalizesEscapedBackticks(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, "CREATE TABLE `user``events` (`event``id` INTEGER);")

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "user`events")
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Name, qt.Equals, "event`id")
}

func TestToDatabase_ParserCanonicalizesEscapedBrackets(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE [user]]events] ([event]]id] INTEGER);`)

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Name, qt.Equals, "user]events")
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Name, qt.Equals, "event]id")
}

func TestToDatabase_ParserPreservesIndexExpression(t *testing.T) {
	c := qt.New(t)

	db := parseToDatabase(c, `CREATE TABLE events (
			first_name TEXT,
			last_name TEXT
		);
		CREATE INDEX "events_name_idx"
			ON "events" (concat(first_name, '. ', last_name));`)

	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "events_name_idx")
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{
		`concat(first_name, '. ', last_name)`,
	})
	c.Assert(db.Indexes[0].Parts, qt.DeepEquals, []goschema.IndexPart{{
		Expr: `concat(first_name, '. ', last_name)`,
	}})

	rendered, err := renderer.RenderSQL(
		"postgres",
		fromschema.FromIndex(db.Indexes[0]),
	)
	c.Assert(err, qt.IsNil)
	c.Assert(rendered, qt.Contains, `(concat(first_name, '. ', last_name))`)
}

func TestToDatabase_DialectQuotedIdentifiersAreCanonicalized(t *testing.T) {
	c := qt.New(t)

	table := ast.NewCreateTable(`"audit"."user""events"`).
		AddColumn(
			ast.NewColumn("`event``id`", "BIGINT").
				SetCheck("event_id > 0").
				SetCheckName(`"event_id_check"`),
		).
		AddConstraint(ast.NewUniqueConstraint(
			"[user]]events_key]",
			"`event``id`",
		))
	index := ast.NewIndex(
		"`event``lookup`",
		`"audit"."user""events"`,
		"[event]]id]",
	)
	extension := ast.NewExtension(`"uuid-ossp"`)
	enum := ast.NewEnum(`"audit"."event""kind"`, "created")

	db := toschema.ToDatabase(&ast.StatementList{Statements: []ast.Node{
		table,
		index,
		enum,
	}})

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Schema, qt.Equals, "audit")
	c.Assert(db.Tables[0].Name, qt.Equals, `user"events`)
	c.Assert(db.Fields, qt.HasLen, 1)
	c.Assert(db.Fields[0].Name, qt.Equals, "event`id")
	c.Assert(db.Fields[0].CheckName, qt.Equals, "event_id_check")
	c.Assert(db.Constraints, qt.HasLen, 1)
	c.Assert(db.Constraints[0].Name, qt.Equals, "user]events_key")
	c.Assert(db.Constraints[0].Columns, qt.DeepEquals, []string{"event`id"})
	c.Assert(db.Indexes, qt.HasLen, 1)
	c.Assert(db.Indexes[0].Name, qt.Equals, "event`lookup")
	c.Assert(db.Indexes[0].TableName, qt.Equals, `audit."user""events"`)
	c.Assert(db.Indexes[0].Fields, qt.DeepEquals, []string{"event]id"})
	c.Assert(toschema.ToExtension(extension), qt.DeepEquals, goschema.Extension{
		Name: "uuid-ossp",
	})
	c.Assert(db.Enums, qt.DeepEquals, []goschema.Enum{{
		Name:   `audit.event"kind`,
		Values: []string{"created"},
	}})
}

func TestToConstraint_DialectQuotedIdentifiersAreCanonicalized(t *testing.T) {
	c := qt.New(t)

	constraint, ok := toschema.ToConstraint(
		ast.NewForeignKeyConstraint(
			`"events_actor_fk"`,
			[]string{"`tenant``id`", "[actor]]id]"},
			&ast.ForeignKeyRef{
				Table:   `"identity"."user""accounts"`,
				Columns: []string{`"tenant_id"`, `"id"`},
			},
		),
		"Event",
		`"audit"."events"`,
	)

	c.Assert(ok, qt.IsTrue)
	c.Assert(constraint.Name, qt.Equals, "events_actor_fk")
	c.Assert(constraint.Table, qt.Equals, "audit.events")
	c.Assert(constraint.Columns, qt.DeepEquals, []string{"tenant`id", "actor]id"})
	c.Assert(constraint.ForeignTable, qt.Equals, `identity."user""accounts"`)
	c.Assert(constraint.ForeignColumns, qt.DeepEquals, []string{"tenant_id", "id"})
}

func TestToConstraint_LiteralDotForeignTableIsCanonicalized(t *testing.T) {
	c := qt.New(t)

	constraint, ok := toschema.ToConstraint(
		ast.NewForeignKeyConstraint(
			"events_owner_fk",
			[]string{"owner_id"},
			&ast.ForeignKeyRef{Table: `"tenant.data"`, Column: "id"},
		),
		"Event",
		"events",
	)

	c.Assert(ok, qt.IsTrue)
	c.Assert(constraint.ForeignTable, qt.Equals, `"tenant.data"`)
}

func TestToConstraint_LiteralDotOwnerIsCanonicalized(t *testing.T) {
	c := qt.New(t)

	constraint, ok := toschema.ToConstraint(
		ast.NewUniqueConstraint("tenant_data_key", "id"),
		"Literal",
		`"tenant.data"`,
	)

	c.Assert(ok, qt.IsTrue)
	c.Assert(constraint.Table, qt.Equals, `"tenant.data"`)
}

func TestToIndex_ExpressionIsPreserved(t *testing.T) {
	c := qt.New(t)

	index := ast.NewIndex(`"events_payload_idx"`, `"events"`).SetParts(
		[]ast.IndexPart{{
			Expr: `json_extract(payload, '$.user.id')`,
		}},
	)

	got := toschema.ToIndex(index)

	c.Assert(got.Name, qt.Equals, "events_payload_idx")
	c.Assert(got.TableName, qt.Equals, "events")
	c.Assert(got.Fields, qt.DeepEquals, []string{`json_extract(payload, '$.user.id')`})
	c.Assert(got.Parts, qt.DeepEquals, []goschema.IndexPart{{
		Expr: `json_extract(payload, '$.user.id')`,
	}})
}
