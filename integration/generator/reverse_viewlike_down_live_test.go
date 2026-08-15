//go:build integration

package generator_test

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// TestReverseViewLikeObjects_DownRoundTrip_Integration is the live-database gate
// for issue #1287. Rendering a rollback proves the statement exists, not that
// PostgreSQL accepts it: the CREATE OR REPLACE VIEW an earlier attempt produced
// renders perfectly and is refused at execution time for every column-list
// change except a trailing append.
//
// Each case seeds the pre-up state through Ptah itself, applies the generated
// up migration, applies the generated down migration, and then asserts two
// things: that the down applied at all, and that the catalog it left behind is
// the pre-up catalog. The table the objects hang off exists before and after
// every case, so a DROP TABLE ... CASCADE can never be what removes them.
func TestReverseViewLikeObjects_DownRoundTrip_Integration(t *testing.T) {
	cases := []struct {
		name   string
		prior  func() *goschema.Database
		target func() *goschema.Database
		// downHas and downLacks pin the shape of the rollback where the shape is
		// the point. The legality test reads the body PostgreSQL hands back
		// through pg_get_viewdef, which spells the same projection differently
		// from the annotation ("SELECT id" comes back as " SELECT id\n FROM t;"),
		// so these assertions are what prove the decision survives the catalog's
		// own spelling rather than only the hand-written fixtures'.
		downHas   []string
		downLacks []string
		// upLacks pins the shape of the FORWARD plan. The same modify step
		// serves both directions, and a statement that is merely wasteful on a
		// rollback can be destructive going forward.
		upLacks []string
	}{
		{
			name:      "up creates the view, materialized view and trigger",
			prior:     func() *goschema.Database { return revIntSchema(revIntOptions{}) },
			target:    func() *goschema.Database { return revIntSchema(revIntOptions{objects: true}) },
			downHas:   []string{"DROP VIEW IF EXISTS", "DROP MATERIALIZED VIEW IF EXISTS", "DROP TRIGGER IF EXISTS"},
			downLacks: []string{"DROP TABLE"},
		},
		{
			name:      "up drops the view, materialized view and trigger",
			prior:     func() *goschema.Database { return revIntSchema(revIntOptions{objects: true}) },
			target:    func() *goschema.Database { return revIntSchema(revIntOptions{}) },
			downHas:   []string{"CREATE VIEW", "CREATE MATERIALIZED VIEW", "CREATE TRIGGER"},
			downLacks: []string{"DROP TABLE"},
		},
		{
			name:  "up appends a column to the view",
			prior: func() *goschema.Database { return revIntSchema(revIntOptions{objects: true}) },
			target: func() *goschema.Database {
				return revIntSchema(revIntOptions{
					objects:  true,
					viewBody: "SELECT id, email FROM " + revIntTable,
				})
			},
			// Taking the column away again is what PostgreSQL refuses with
			// "cannot drop columns from view", so the rollback must not replace.
			downHas:   []string{"DROP VIEW IF EXISTS " + revIntView + " CASCADE"},
			downLacks: []string{"CREATE OR REPLACE VIEW"},
		},
		{
			name:  "up changes only the view predicate",
			prior: func() *goschema.Database { return revIntSchema(revIntOptions{objects: true}) },
			target: func() *goschema.Database {
				return revIntSchema(revIntOptions{
					objects:  true,
					viewBody: "SELECT id FROM " + revIntTable + " WHERE id > 10",
				})
			},
			// The column list never moved, so the rollback keeps the replace and
			// its dependents.
			downHas:   []string{"CREATE OR REPLACE VIEW " + revIntView},
			downLacks: []string{"DROP VIEW"},
		},
		{
			name:  "up rewrites the materialized view and the trigger",
			prior: func() *goschema.Database { return revIntSchema(revIntOptions{objects: true}) },
			target: func() *goschema.Database {
				return revIntSchema(revIntOptions{
					objects:     true,
					matViewBody: "SELECT count(id) AS total FROM " + revIntTable,
					triggerBody: "NEW.email = lower(NEW.email); RETURN NEW;",
				})
			},
			downHas: []string{
				"DROP MATERIALIZED VIEW IF EXISTS " + revIntMatView + " CASCADE",
				"CREATE MATERIALIZED VIEW " + revIntMatView,
				"CREATE OR REPLACE TRIGGER " + revIntTrigger,
			},
		},
		{
			// The forward direction of a body the legality parser cannot read.
			// A WITH prefix hides the projection, so nothing can be proved about
			// the change -- but the edit here is predicate-only, PostgreSQL
			// accepts the replace, and the declared dependent view survives it.
			// Dropping the view instead applies cleanly and leaves the database
			// short of the schema it was generated from, which step 6 catches.
			name: "up changes only the predicate inside a WITH view that has a dependent",
			prior: func() *goschema.Database {
				return revIntSchema(revIntOptions{objects: true, dependent: true, viewBody: revIntWithBody})
			},
			target: func() *goschema.Database {
				return revIntSchema(revIntOptions{
					objects:   true,
					dependent: true,
					viewBody:  revIntWithBodyFiltered,
				})
			},
			upLacks: []string{"DROP VIEW"},
			// The rollback cannot afford the undecidable answer, so it drops --
			// and then rebuilds the dependent the CASCADE took with it.
			downHas:   []string{"DROP VIEW IF EXISTS " + revIntView + " CASCADE", "VIEW " + revIntDepView + " AS"},
			downLacks: []string{"DROP TABLE"},
		},
		{
			// The rollback the select-list comparison got wrong. The up swaps the
			// relation under a column whose name does not change, and the two
			// relations type that column differently; a rollback that puts the
			// prior body back with CREATE OR REPLACE VIEW is refused with
			// "cannot change data type of view column", which step 3 executes.
			name: "up swaps the relation under a shared column",
			prior: func() *goschema.Database {
				return revIntSchema(revIntOptions{
					objects:   true,
					altTable:  true,
					viewBody:  "SELECT id, email FROM " + revIntTable,
					matView:   false,
					noTrigger: true,
				})
			},
			target: func() *goschema.Database {
				return revIntSchema(revIntOptions{
					objects:   true,
					altTable:  true,
					viewBody:  "SELECT id FROM " + revIntAltTable,
					matView:   false,
					noTrigger: true,
				})
			},
			downHas:   []string{"DROP VIEW IF EXISTS " + revIntView + " CASCADE"},
			downLacks: []string{"CREATE OR REPLACE VIEW"},
		},
		{
			// The cascade rebuild list decides who else the plan touches, and it
			// reads the code of the declared bodies rather than their raw text.
			// A materialized view that only spells the dropped view's name inside
			// a string literal reads no view at all; naming it here means
			// DROP MATERIALIZED VIEW ... CASCADE against an object the migration
			// never touched, which on PostgreSQL 17.10 took a hand-made dependent
			// view, a unique index and a GRANT with it.
			name: "up appends a column while a materialized view only names the view in a literal",
			prior: func() *goschema.Database {
				return revIntSchema(revIntOptions{objects: true, matViewBody: revIntLabelMatViewBody})
			},
			target: func() *goschema.Database {
				return revIntSchema(revIntOptions{
					objects:     true,
					viewBody:    "SELECT id, email FROM " + revIntTable,
					matViewBody: revIntLabelMatViewBody,
				})
			},
			downHas:   []string{"DROP VIEW IF EXISTS " + revIntView + " CASCADE"},
			downLacks: []string{"MATERIALIZED VIEW " + revIntMatView},
		},
	}

	url := revIntRequireURL(t)

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			conn := revIntConnect(t, url)
			revIntDropAll(conn)
			t.Cleanup(func() { revIntDropAll(conn) })

			// 1. Install the pre-up state with Ptah's own planner, so the bodies
			//    in the catalog are the ones Ptah writes rather than a hand-typed
			//    approximation of them.
			prior := tc.prior()
			seedSQL, _ := generateLiveMigrationSQL(c, conn, prior)
			execScript(c, conn, seedSQL, "SEED")

			dbPrior, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			priorCatalog := revIntCatalog(dbPrior)

			// 2. The up migration under test.
			target := tc.target()
			upDiff := schemadiff.CompareWithDialect(target, dbPrior, "postgres")
			c.Assert(upDiff.HasChanges(), qt.IsTrue, qt.Commentf("the up migration must have something to do"))

			upSQL, downSQL := generateLiveMigrationSQL(c, conn, target)
			unquotedUp := legacyRenderedSQL(upSQL)
			for _, fragment := range tc.upLacks {
				c.Assert(unquotedUp, qt.Not(qt.Contains), fragment, qt.Commentf("up SQL:\n%s", upSQL))
			}
			execScript(c, conn, upSQL, "UP")

			dbAfterUp, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(revIntCatalog(dbAfterUp), qt.Not(qt.DeepEquals), priorCatalog,
				qt.Commentf("the up migration must actually change the catalog, or the down proves nothing"))

			// 2b. Applying cleanly is not the same as arriving. Every object the
			//     target declares has to be in the catalog afterwards -- a plan
			//     that drops a dependent it never rebuilds exits 0 and leaves the
			//     database short of the schema it was generated from.
			c.Assert(revIntMissingObjects(target, dbAfterUp), qt.HasLen, 0,
				qt.Commentf("up SQL:\n%s", upSQL))

			// 3. THE GATE. The down is generated exactly as the generator does it,
			//    and applying it must not error. A CREATE OR REPLACE VIEW that
			//    removes or renames a column fails right here.
			unquotedDown := legacyRenderedSQL(downSQL)
			for _, fragment := range tc.downHas {
				c.Assert(unquotedDown, qt.Contains, fragment, qt.Commentf("down SQL:\n%s", downSQL))
			}
			for _, fragment := range tc.downLacks {
				c.Assert(unquotedDown, qt.Not(qt.Contains), fragment, qt.Commentf("down SQL:\n%s", downSQL))
			}
			execScript(c, conn, downSQL, "DOWN")

			// 4. The rollback has to land on the pre-up catalog, not merely apply.
			dbAfterDown, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			c.Assert(revIntCatalog(dbAfterDown), qt.DeepEquals, priorCatalog,
				qt.Commentf("down SQL:\n%s", downSQL))

			// 5. And the table the objects hang off must have survived untouched.
			c.Assert(revIntHasTable(dbAfterDown), qt.IsTrue,
				qt.Commentf("the rollback must not have taken the pre-existing table with it"))
		})
	}
}

// revIntRequireURL and revIntConnect keep the two skip decisions out of the
// test body: an absent POSTGRES_URL and an unreachable server are environment
// facts, not branches of the behavior under test.
func revIntRequireURL(t *testing.T) string {
	t.Helper()
	return dbtarget.URL(t, dbtarget.PostgreSQL)
}

func revIntConnect(t *testing.T, url string) *dbschema.DatabaseConnection {
	t.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), url)
	if err != nil {
		t.Skipf("skipping reverse view-like down round trip: cannot connect: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

const (
	revIntTable    = "ptah_rev_view_users"
	revIntAltTable = "ptah_rev_alt_users"
	revIntView     = "ptah_rev_active_users"
	revIntDepView  = "ptah_rev_active_ids"
	revIntMatView  = "ptah_rev_user_stats"
	revIntTrigger  = "ptah_rev_touch"
	revIntViewBody = "SELECT id FROM " + revIntTable
	// revIntWithBody and revIntWithBodyFiltered differ only in the predicate
	// inside the CTE, so the column list never moves and PostgreSQL accepts
	// CREATE OR REPLACE VIEW between them -- while the WITH prefix keeps the
	// legality parser from being able to say so.
	revIntWithBody         = "WITH src AS (SELECT id, email FROM " + revIntTable + ") SELECT id, email FROM src"
	revIntWithBodyFiltered = "WITH src AS (SELECT id, email FROM " + revIntTable +
		" WHERE id > 1) SELECT id, email FROM src"
	revIntDepViewBody = "SELECT id FROM " + revIntView
	revIntMatViewBody = "SELECT count(*) AS total FROM " + revIntTable
	// revIntLabelMatViewBody spells the view's name and reads none of it. The
	// cascade rebuild list has to tell those apart, because every name on it is
	// answered with a DROP MATERIALIZED VIEW ... CASCADE. The cast is written
	// out because PostgreSQL adds it when it reads the body back, and a body
	// that does not round-trip would land in MaterializedViewsModified on its
	// own and prove nothing about the rebuild list.
	revIntLabelMatViewBody = "SELECT '" + revIntView + "'::text AS label, count(*) AS total FROM " + revIntTable
	revIntTriggerBody      = "NEW.email = NEW.email; RETURN NEW;"
)

type revIntOptions struct {
	objects     bool
	viewBody    string
	matViewBody string
	triggerBody string
	// dependent declares a second view reading the first, which is what
	// DROP VIEW ... CASCADE takes and what the plan then has to put back.
	dependent bool
	// altTable declares a second table typing "id" differently, so a view can
	// be moved from one to the other without its column names changing.
	altTable bool
	// matView and noTrigger trim the fixture where a case is about the view
	// alone and the other objects would only add noise to the catalog compare.
	matView   bool
	noTrigger bool
}

func revIntSchema(opts revIntOptions) *goschema.Database {
	schema := &goschema.Database{
		Tables: []goschema.Table{{StructName: "RevIntUser", Name: revIntTable}},
		Fields: []goschema.Field{
			{StructName: "RevIntUser", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "RevIntUser", Name: "email", Type: "TEXT", Nullable: true},
		},
	}
	if opts.altTable {
		schema.Tables = append(schema.Tables, goschema.Table{StructName: "RevIntAltUser", Name: revIntAltTable})
		schema.Fields = append(schema.Fields,
			goschema.Field{StructName: "RevIntAltUser", Name: "id", Type: "TEXT", Primary: true},
			goschema.Field{StructName: "RevIntAltUser", Name: "email", Type: "TEXT", Nullable: true},
		)
	}
	if opts.objects {
		schema.Views = []goschema.View{{
			StructName: "RevIntActiveUsers",
			Name:       revIntView,
			Body:       cmp.Or(opts.viewBody, revIntViewBody),
		}}
		if opts.dependent {
			schema.Views = append(schema.Views, goschema.View{
				StructName: "RevIntActiveIDs",
				Name:       revIntDepView,
				Body:       revIntDepViewBody,
			})
		}
		if !opts.noTrigger {
			schema.Triggers = []goschema.Trigger{{
				StructName: "RevIntUser",
				Name:       revIntTrigger,
				Table:      revIntTable,
				Timing:     "BEFORE",
				Event:      "UPDATE",
				ForEach:    "ROW",
				Body:       cmp.Or(opts.triggerBody, revIntTriggerBody),
			}}
		}
		if !opts.noTrigger || opts.matView {
			schema.MaterializedViews = []goschema.MaterializedView{{
				StructName:      "RevIntUserStats",
				Name:            revIntMatView,
				Body:            cmp.Or(opts.matViewBody, revIntMatViewBody),
				RefreshStrategy: "manual",
			}}
		}
	}
	goschema.Finalize(schema)
	return schema
}

// revIntMissingObjects names the view-like objects a schema declares that the
// catalog does not hold. An empty result is what "the migration arrived" means;
// a non-empty one is a plan that applied and still left work undone.
func revIntMissingObjects(schema *goschema.Database, db *dbschematypes.DBSchema) []string {
	var missing []string
	for _, view := range schema.Views {
		if !slices.ContainsFunc(db.Views, func(candidate dbschematypes.DBView) bool {
			return candidate.Name == view.Name
		}) {
			missing = append(missing, "view "+view.Name)
		}
	}
	for _, view := range schema.MaterializedViews {
		if !slices.ContainsFunc(db.MatViews, func(candidate dbschematypes.DBMatView) bool {
			return candidate.Name == view.Name
		}) {
			missing = append(missing, "matview "+view.Name)
		}
	}
	for _, trigger := range schema.Triggers {
		if !slices.ContainsFunc(db.Triggers, func(candidate dbschematypes.DBTrigger) bool {
			return candidate.Name == trigger.Name
		}) {
			missing = append(missing, "trigger "+trigger.Name)
		}
	}
	return missing
}

// revIntCatalog reduces the introspected schema to the view-like objects this
// test governs, so a mismatch names the object rather than dumping a whole
// schema.
func revIntCatalog(db *dbschematypes.DBSchema) []string {
	var lines []string
	for _, view := range db.Views {
		lines = append(lines, fmt.Sprintf("view %s = %s", view.Name, revIntNormalize(view.Body)))
	}
	for _, view := range db.MatViews {
		lines = append(lines, fmt.Sprintf("matview %s = %s", view.Name, revIntNormalize(view.Body)))
	}
	for _, trigger := range db.Triggers {
		lines = append(lines, fmt.Sprintf("trigger %s on %s = %s %s %s / %s",
			trigger.Name, trigger.Table, trigger.Timing, trigger.Event, trigger.ForEach,
			revIntNormalize(trigger.Body)))
	}
	for _, function := range db.Functions {
		lines = append(lines, fmt.Sprintf("function %s = %s", function.Name, revIntNormalize(function.Body)))
	}
	sort.Strings(lines)
	return lines
}

func revIntNormalize(body string) string {
	return strings.Join(strings.Fields(body), " ")
}

func revIntHasTable(db *dbschematypes.DBSchema) bool {
	return slices.ContainsFunc(db.Tables, func(table dbschematypes.DBTable) bool {
		return table.Name == revIntTable
	})
}

func revIntDropAll(conn *dbschema.DatabaseConnection) {
	statements := []string{
		"DROP VIEW IF EXISTS " + revIntDepView + " CASCADE",
		"DROP VIEW IF EXISTS " + revIntView + " CASCADE",
		"DROP MATERIALIZED VIEW IF EXISTS " + revIntMatView + " CASCADE",
		"DROP TRIGGER IF EXISTS " + revIntTrigger + " ON " + revIntTable + " CASCADE",
		"DROP TABLE IF EXISTS " + revIntTable + " CASCADE",
		"DROP TABLE IF EXISTS " + revIntAltTable + " CASCADE",
		"DROP FUNCTION IF EXISTS ptah_trigger_" + revIntTable + "_" + revIntTrigger + "() CASCADE",
	}
	for _, statement := range statements {
		_, _ = conn.Exec(statement)
	}
}
