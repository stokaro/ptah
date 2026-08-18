package mssql

// White-box testing required: applySynonymTargetParts decides whether a synonym
// counts as local, and it is unexported because it is a decoding detail rather
// than a reader operation. Reaching it from outside means standing up a SQL
// Server holding a linked-server synonym, which is the one target shape a test
// environment cannot create.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema/types"
)

func TestApplySynonymTargetParts(t *testing.T) {
	for _, tc := range []struct {
		name     string
		target   string
		server   string
		database string
		schema   string
		object   string
		external bool
		local    string
	}{
		{
			name: "an unqualified object", target: "[orders]",
			object: "orders", local: "orders",
		},
		{
			name: "schema and object", target: "[dbo].[orders]",
			schema: "dbo", object: "orders", local: "dbo.orders",
		},
		{
			name: "another database", target: "[sales].[dbo].[orders]",
			database: "sales", schema: "dbo", object: "orders", external: true,
		},
		{
			name: "a linked server", target: "[remote].[sales].[dbo].[orders]",
			server: "remote", database: "sales", schema: "dbo", object: "orders", external: true,
		},
		{
			name: "unquoted parts", target: "sales.dbo.orders",
			database: "sales", schema: "dbo", object: "orders", external: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			synonym := types.DBSynonym{Target: tc.target}

			applySynonymTargetParts(&synonym)

			c.Assert(synonym.TargetServer, qt.Equals, tc.server)
			c.Assert(synonym.TargetDatabase, qt.Equals, tc.database)
			c.Assert(synonym.TargetSchema, qt.Equals, tc.schema)
			c.Assert(synonym.TargetObject, qt.Equals, tc.object)
			c.Assert(synonym.IsExternal(), qt.Equals, tc.external)
			c.Assert(synonym.TargetQualifiedName(), qt.Equals, tc.local)
			c.Assert(synonym.Target, qt.Equals, tc.target,
				qt.Commentf("the raw catalog value is what the server resolves and must survive parsing"))
		})
	}
}

// TestApplySynonymTargetParts_ReadsFromTheRight is the case a left-to-right
// parser gets wrong. SQL Server writes an absent middle part as an empty pair
// of brackets, so this target names a server and no database -- and a parser
// that assigned parts from the left would record the server as the database and
// then treat a linked-server alias as a same-instance one.
func TestApplySynonymTargetParts_ReadsFromTheRight(t *testing.T) {
	c := qt.New(t)
	synonym := types.DBSynonym{Target: "[remote]..[dbo].[orders]"}

	applySynonymTargetParts(&synonym)

	c.Assert(synonym.TargetServer, qt.Equals, "remote")
	c.Assert(synonym.TargetDatabase, qt.Equals, "")
	c.Assert(synonym.TargetSchema, qt.Equals, "dbo")
	c.Assert(synonym.TargetObject, qt.Equals, "orders")
	c.Assert(synonym.IsExternal(), qt.IsTrue)
	c.Assert(synonym.TargetQualifiedName(), qt.Equals, "")
}
