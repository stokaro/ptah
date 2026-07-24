package generator

// White-box testing required: planGeneratedMigrationSpecs is an unexported
// orchestration helper, so exercising how the diff policy threads into the
// planner (skipped drops, forced concurrent indexes) means calling it directly.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform/capability"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/diffpolicy"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestPlanGeneratedMigrationSpecs_SkipDropTable(t *testing.T) {
	c := qt.New(t)

	// A dropped table (to be skipped) alongside a kept index add, so the
	// migration is non-empty and we can assert the drop was omitted in place.
	diff := &types.SchemaDiff{
		TablesRemoved: []string{"legacy"},
		IndexesAdded:  []string{"idx_users_email"},
	}

	specs, assessments, err := planGeneratedMigrationSpecs(
		diff,
		indexOnlyGeneratedSchema(),
		&dbschematypes.DBSchema{},
		postgresInfo(capability.Postgres17()),
		100,
		"drop_legacy",
		DiffPolicy{SkipChangeKinds: []diffpolicy.ChangeKind{diffpolicy.DropTable}},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "DROP TABLE IF EXISTS")
	c.Assert(specs[0].UpSQL, qt.Contains, "skip: drop_table")
	c.Assert(specs[0].UpSQL, qt.Contains, "CREATE INDEX")
	// A skipped drop never reaches the plan, so the destructive gate sees nothing.
	c.Assert(safety.HasDestructiveAssessment(assessments), qt.IsFalse)
}

func TestPlanGeneratedMigrationSpecs_SkipDropTableAlsoFiltersDown(t *testing.T) {
	c := qt.New(t)

	// The database still has `legacy`; the Go schema removed it. The dbSchema
	// carries the table definition so the down path could reconstruct a
	// CREATE TABLE — which it must not, since the up migration keeps the table.
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{
				Name: "legacy",
				Type: "BASE TABLE",
				Columns: []dbschematypes.DBColumn{
					{Name: "id", DataType: "integer", IsNullable: "NO", OrdinalPosition: 1},
				},
			},
			// A kept index-bearing table so the migration is non-empty.
			{Name: "users", Type: "BASE TABLE"},
		},
	}
	diff := &types.SchemaDiff{
		TablesRemoved: []string{"legacy"},
		IndexesAdded:  []string{"idx_users_email"},
	}

	specs, _, err := planGeneratedMigrationSpecs(
		diff,
		indexOnlyGeneratedSchema(),
		dbSchema,
		postgresInfo(capability.Postgres17()),
		100,
		"drop_legacy",
		DiffPolicy{SkipChangeKinds: []diffpolicy.ChangeKind{diffpolicy.DropTable}},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	// Up keeps the table.
	c.Assert(specs[0].UpSQL, qt.Not(qt.Contains), "DROP TABLE IF EXISTS")
	// Down must not recreate a table the up migration never dropped.
	c.Assert(specs[0].DownSQL, qt.Not(qt.Contains), "CREATE TABLE", qt.Commentf("down:\n%s", specs[0].DownSQL))
}

// TestPlanGeneratedMigrationSpecs_SkipDropIndexKeepsRedefinition guards against
// the concurrent-index split silently discarding an index redefinition when
// skip: [drop_index] is set. An index redefinition emits the same name in both
// IndexesAdded and IndexesRemoved; the populated-table heuristic makes the
// recreate concurrent, which triggers the transactional/no-transaction split.
// The skip must not treat the split-orphaned removal as a genuine standalone
// drop — the DROP and the CREATE CONCURRENTLY must both survive.
func TestPlanGeneratedMigrationSpecs_SkipDropIndexKeepsRedefinition(t *testing.T) {
	c := qt.New(t)

	diff := &types.SchemaDiff{
		IndexesAdded:   []string{"idx_users_email"},
		IndexesRemoved: []string{"idx_users_email"},
	}
	// users is populated, so the recreate is built concurrently and the split fires.
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 100}},
	}

	specs, _, err := planGeneratedMigrationSpecs(
		diff,
		indexOnlyGeneratedSchema(),
		dbSchema,
		postgresInfo(capability.Postgres17()),
		100,
		"rebuild_index",
		// concurrent_index policy is OFF; only skip: [drop_index] is set, proving
		// the heuristic alone reaches the buggy path.
		DiffPolicy{SkipChangeKinds: []diffpolicy.ChangeKind{diffpolicy.DropIndex}},
	)

	c.Assert(err, qt.IsNil)
	var upBuilder strings.Builder
	for _, spec := range specs {
		upBuilder.WriteString(spec.UpSQL)
		upBuilder.WriteByte('\n')
	}
	allUp := upBuilder.String()
	// The redefinition must survive: drop the old index and recreate it concurrently.
	c.Assert(allUp, qt.Contains, "DROP INDEX", qt.Commentf("plan:\n%s", allUp))
	c.Assert(allUp, qt.Contains, "CREATE INDEX CONCURRENTLY", qt.Commentf("plan:\n%s", allUp))
	// It must not have been treated as a skipped standalone drop.
	c.Assert(allUp, qt.Not(qt.Contains), "skip: drop_index", qt.Commentf("plan:\n%s", allUp))
}

func TestPlanGeneratedMigrationSpecs_ConcurrentIndexPolicyForcesConcurrent(t *testing.T) {
	c := qt.New(t)

	// An unpopulated table: the populated-table heuristic would keep plain
	// CREATE INDEX, so any CONCURRENTLY must come from the policy.
	dbSchema := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{{Name: "users", Type: "BASE TABLE", EstimatedRows: 0}},
	}

	specs, _, err := planGeneratedMigrationSpecs(
		indexOnlyDiff(),
		indexOnlyGeneratedSchema(),
		dbSchema,
		postgresInfo(capability.Postgres17()),
		100,
		"add_index",
		DiffPolicy{ConcurrentIndex: true},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(specs, qt.HasLen, 1)
	c.Assert(specs[0].NoTransaction, qt.IsTrue)
	c.Assert(specs[0].UpSQL, qt.Contains, "CREATE INDEX CONCURRENTLY")
}
