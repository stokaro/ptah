package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// namedNotNullSchema is one table whose non-key column carries a NOT NULL
// constraint name, as a PostgreSQL 18 reader reports it.
//
// primaryNotNullName is the name the key column carries, empty for none. A
// PostgreSQL 18 reader supplies one there too, because that server names every
// NOT NULL, the key's included.
func namedNotNullSchema(primaryNotNullName string) *schemamodel.Database {
	id := schemamodel.Field{
		StructName: "A", Name: "id", Type: "BIGINT", Primary: true,
		NotNullConstraintName: primaryNotNullName,
	}
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "A", Name: "accounts"}},
		Fields: []schemamodel.Field{id, {
			StructName: "A", Name: "email", Type: "TEXT",
			NotNullConstraintName: "accounts_email_nn",
		}},
	}
	schemamodel.Finalize(database)
	return database
}

// TestCollectDatabase_NotNullConstraintNameReachesTheDDL covers stokaro/ptah#2590.
//
// The renderer emits `CONSTRAINT <name> NOT NULL` from the AST node and refuses
// rather than drops when a target cannot keep the name. None of that could fire,
// because the model to AST conversion never copied the field: the name was lost
// one step earlier, and the guard protecting it never saw one.
func TestCollectDatabase_NotNullConstraintNameReachesTheDDL(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullSchema(""), platform.Postgres, capability.Postgres18())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, `CONSTRAINT "accounts_email_nn" NOT NULL`)
}

// TestCollectDatabase_NotNullConstraintNameIsDroppedOnAKeyColumn is the half that
// keeps the fix from turning a database read into a refusal.
//
// PostgreSQL 18 names the key column's NOT NULL as well, and the renderer
// refuses such a name: the NOT NULL there is synthesized for comparison, the key
// is the constraint the column has, and its name is the addressable one. So the
// conversion drops it deliberately, and this is the one column where dropping is
// right.
func TestCollectDatabase_NotNullConstraintNameIsDroppedOnAKeyColumn(t *testing.T) {
	c := qt.New(t)

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullSchema("accounts_id_nn"), platform.Postgres, capability.Postgres18())

	c.Assert(err, qt.IsNil)
	c.Assert(statements[0], qt.Contains, `"id" BIGINT PRIMARY KEY NOT NULL`)
	c.Assert(statements[0], qt.Not(qt.Contains), "accounts_id_nn")
	// The non-key column beside it still carries its name, so this is the key
	// column being treated differently rather than the copy being switched off.
	c.Assert(statements[0], qt.Contains, `CONSTRAINT "accounts_email_nn" NOT NULL`)
}

// TestCollectDatabase_NotNullConstraintNameIsRefusedWhereItCannotBeKept pins the
// consequence of carrying the name at all.
//
// A target that accepts the syntax and records nothing would leave the name lost
// on the way in and every later comparison reporting a difference no apply can
// settle. The renderer refuses instead, and that refusal is unreachable while
// the conversion drops the name — which is what made this a silent loss rather
// than an error (stokaro/ptah#2161).
func TestCollectDatabase_NotNullConstraintNameIsRefusedWhereItCannotBeKept(t *testing.T) {
	c := qt.New(t)

	_, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullSchema(""), platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "does not keep a NOT NULL constraint name")
}
