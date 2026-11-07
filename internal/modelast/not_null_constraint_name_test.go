package modelast_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
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

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullSchema(""), platform.Postgres, capability.Postgres18())

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, `CONSTRAINT "accounts_email_nn" NOT NULL`)
}

// TestCollectDatabase_NotNullConstraintNameOnAKeyColumn leaves out the name the server
// gives a key column, which a PostgreSQL 18 read reports on every table, and carries any
// other, which a later plan compares. The non-key column beside it keeps its name in
// both rows.
func TestCollectDatabase_NotNullConstraintNameOnAKeyColumn(t *testing.T) {
	tests := []struct {
		name string
		want string
	}{
		{name: "accounts_id_not_null", want: `"id" BIGINT PRIMARY KEY NOT NULL`},
		{name: "accounts_id_nn", want: `"id" BIGINT PRIMARY KEY CONSTRAINT "accounts_id_nn" NOT NULL`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(
				namedNotNullSchema(test.name), platform.Postgres, capability.Postgres18())

			c.Assert(err, qt.IsNil)
			c.Assert(statements[0], qt.Contains, test.want)
			c.Assert(statements[0], qt.Contains, `CONSTRAINT "accounts_email_nn" NOT NULL`)
		})
	}
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

	_, err := builtin.GetOrderedCreateStatementsWithCapabilities(
		namedNotNullSchema(""), platform.Postgres, capability.Postgres17())

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "does not keep a NOT NULL constraint name")
}
