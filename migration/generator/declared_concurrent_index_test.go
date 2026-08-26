package generator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/generator"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// A desired state may ask for PostgreSQL's non-locking index build in its own
// words -- `CREATE INDEX CONCURRENTLY` survives parsing into
// schemamodel.Index.Concurrently -- and nothing carried the answer to the planner,
// so the build was planned as a locking one, silently (stokaro/ptah#2019).
//
// Every row below declares the index on an EMPTY table, which is what separates
// the declaration from the automatic heuristic: that heuristic builds
// concurrently only where the table already holds rows, so it would choose the
// plain build for this table on its own.

func TestPlanBidirectionalSchemaDiff_DeclaredConcurrentIndex(t *testing.T) {
	tests := []struct {
		name              string
		declared          bool
		mode              generator.ConcurrentIndexMode
		capabilities      capability.Capabilities
		partitioned       bool
		wantUp            string
		wantNoTransaction bool
	}{
		{
			name:              "the description asks for it",
			declared:          true,
			mode:              generator.ConcurrentIndexAutomatic,
			capabilities:      capability.Postgres17(),
			wantUp:            "CREATE INDEX CONCURRENTLY IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n",
			wantNoTransaction: true,
		},
		{
			// The control. Same empty table, same mode, and the only thing that
			// changed is that the description did not ask.
			name:              "the description does not ask",
			declared:          false,
			mode:              generator.ConcurrentIndexAutomatic,
			capabilities:      capability.Postgres17(),
			wantUp:            "CREATE INDEX IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n",
			wantNoTransaction: false,
		},
		{
			// Turning concurrent builds off is an operator's instruction, and a
			// description does not overrule it.
			name:              "the operator turned it off",
			declared:          true,
			mode:              generator.ConcurrentIndexDisabled,
			capabilities:      capability.Postgres17(),
			wantUp:            "CREATE INDEX IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n",
			wantNoTransaction: false,
		},
		{
			// A target that cannot do it keeps the plain build rather than
			// failing, which is what the heuristic does for the same target.
			name:              "the target cannot do it",
			declared:          true,
			mode:              generator.ConcurrentIndexAutomatic,
			capabilities:      capability.Postgres17().With(capability.CreateIndexConcurrently, false),
			wantUp:            "CREATE INDEX IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n",
			wantNoTransaction: false,
		},
		{
			// PostgreSQL supports no concurrent form for a partitioned parent.
			// It is EXCLUDED rather than refused, for the reason the heuristic
			// records: refusing would leave a project with a partitioned table
			// unable to generate an index migration at all.
			name:              "the table is a partitioned parent",
			declared:          true,
			mode:              generator.ConcurrentIndexAutomatic,
			capabilities:      capability.Postgres17(),
			partitioned:       true,
			wantUp:            "CREATE INDEX IF NOT EXISTS \"idx_users_reference\" ON \"users\" (\"reference\");\n",
			wantNoTransaction: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			diff.SetIndexAdditions([]difftypes.IndexRef{{Name: "idx_users_reference", TableName: "users"}})

			plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
				Diff:         diff,
				Desired:      usersSchemaDeclaring(test.declared),
				Current:      emptyUsersTable(test.partitioned),
				Dialect:      platform.Postgres,
				Capabilities: test.capabilities,
				Policy: generator.BidirectionalPlanPolicy{
					Create: test.mode,
					Drop:   generator.ConcurrentIndexDisabled,
				},
			})

			c.Assert(err, qt.IsNil)
			up, renderErr := renderer.RenderSQLWithCapabilities(
				platform.Postgres, test.capabilities, plan.Forward.Nodes...)
			c.Assert(renderErr, qt.IsNil)
			c.Assert(up, qt.Equals, test.wantUp)
			// The statement and the file's transaction mode are one answer: a
			// concurrent build cannot run inside a transaction block, and the
			// mode is read off the node rather than off the policy.
			c.Assert(plan.Forward.RequiresNoTransaction, qt.Equals, test.wantNoTransaction)
		})
	}
}

// TestPlanBidirectionalSchemaDiff_DeclaredConcurrentIndexIsPostgresOnly is the
// dialect gate, and it is a separate question from the capability beside it.
//
// Every default preset that claims CreateIndexConcurrently is a PostgreSQL-family
// one, so on default capabilities the two conditions answer together. A caller
// can hand any dialect any capability set, though -- `.With(...)` is how the
// rows above build theirs -- and MySQL has no concurrent index build whatever a
// set claims, so a statement carrying CONCURRENTLY is one MySQL rejects.
func TestPlanBidirectionalSchemaDiff_DeclaredConcurrentIndexIsPostgresOnly(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions([]difftypes.IndexRef{{Name: "idx_users_reference", TableName: "users"}})

	plan, err := generator.PlanBidirectionalSchemaDiff(generator.BidirectionalSchemaPlanOptions{
		Diff:         diff,
		Desired:      usersSchemaDeclaring(true),
		Current:      emptyUsersTable(false),
		Dialect:      platform.MySQL,
		Capabilities: capability.MySQL84().With(capability.CreateIndexConcurrently, true),
		Policy: generator.BidirectionalPlanPolicy{
			Create: generator.ConcurrentIndexAutomatic,
			Drop:   generator.ConcurrentIndexDisabled,
		},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.Forward.ConcurrentIndexRefs, qt.HasLen, 0)
	c.Assert(plan.Forward.RequiresNoTransaction, qt.IsFalse)
}

// usersSchemaDeclaring is the target schema, with or without the request.
func usersSchemaDeclaring(concurrently bool) *schemamodel.Database {
	description := singleConcurrentIndexSchema()
	description.Indexes[0].Concurrently = concurrently
	return description
}

// emptyUsersTable is the read: a table holding no rows, so the automatic
// heuristic would choose the plain build for it on its own.
func emptyUsersTable(partitioned bool) *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{
		Name: "users", Type: "BASE TABLE", EstimatedRows: 0, Partitioned: partitioned,
	}}}
}
