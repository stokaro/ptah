package atlas

// White-box testing required: the dependency-injection choice is intentionally
// private to the compatibility adapter. The shared planner and the internal
// migrate-diff consumer have black-box coverage in their owning packages.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/migration/schemadiff/types"
)

func TestCompatBidirectionalPlannerForFormat_NativeAtlasStaysForwardOnly(t *testing.T) {
	c := qt.New(t)

	planner := compatBidirectionalPlannerForFormat(atlasmigrateimport.FormatAtlas)

	c.Assert(planner, qt.IsNil)
}

func TestCompatBidirectionalPlannerForFormat_ForeignLayoutKeepsYugabyteBlockingRollback(t *testing.T) {
	c := qt.New(t)
	planFn := compatBidirectionalPlannerForFormat(atlasmigrateimport.FormatGolangMigrate)
	c.Assert(planFn, qt.IsNotNil)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_users_email", TableName: "users"}})

	plan, err := planFn(atlasmigrate.BidirectionalPlanInput{
		Diff: diff,
		DesiredSchema: &goschema.Database{
			Tables: []goschema.Table{{StructName: "User", Name: "users"}},
			Indexes: []goschema.Index{{
				StructName: "User", Name: "idx_users_email", Fields: []string{"email"},
			}},
		},
		CurrentSchema:         &dbschematypes.DBSchema{},
		Dialect:               platform.YugabyteDB,
		Capabilities:          capability.YugabyteDB25(),
		ConcurrentIndexCreate: true,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(plan.ForwardNodes, qt.HasLen, 1)
	forward, ok := plan.ForwardNodes[0].(*ast.IndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(forward.Concurrently, qt.IsTrue)
	c.Assert(plan.ReverseNodes, qt.HasLen, 1)
	reverse, ok := plan.ReverseNodes[0].(*ast.DropIndexNode)
	c.Assert(ok, qt.IsTrue)
	c.Assert(reverse.Concurrently, qt.IsFalse)
	c.Assert(plan.ReverseRequiresNoTransaction, qt.IsFalse)
}

func TestCompatBidirectionalPlannerForFormat_ExplicitUnavailableForwardStillRefuses(t *testing.T) {
	c := qt.New(t)
	planFn := compatBidirectionalPlannerForFormat(atlasmigrateimport.FormatGolangMigrate)
	c.Assert(planFn, qt.IsNotNil)
	diff := &types.SchemaDiff{}
	diff.SetIndexAdditions([]types.IndexRef{{Name: "idx_users_email", TableName: "users"}})

	plan, err := planFn(atlasmigrate.BidirectionalPlanInput{
		Diff:                  diff,
		DesiredSchema:         &goschema.Database{},
		CurrentSchema:         &dbschematypes.DBSchema{},
		Dialect:               platform.CockroachDB,
		Capabilities:          capability.CockroachDB23(),
		ConcurrentIndexCreate: true,
	})

	c.Assert(plan, qt.DeepEquals, atlasmigrate.BidirectionalPlan{})
	c.Assert(err, qt.ErrorMatches,
		`CREATE INDEX CONCURRENTLY requested by diff\.concurrent_index\.create cannot be generated for dialect "cockroachdb": target capability create_index_concurrently is unavailable`)
}
