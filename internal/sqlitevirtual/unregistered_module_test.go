package sqlitevirtual_test

import (
	"bytes"
	"errors"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/internal/sqlitevirtual"
	"go.5x5.cz/ptah/migration/diffpolicy"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// TestValidateComparisonRefusesAnUnregisteredModule is the guard on the second
// data-loss path of stokaro/ptah#1028, the one #1469 left open and whose remedy
// was the weapon.
//
// Measured end to end against master ebd2ba2e, on fts3 and fts4 databases built
// by a system SQLite that has those modules, with an fts5 control:
//
//	module  registered  guard says            --exclude docs then does   MATCH after
//	fts3    no          exclude "docs"        3 DROP TABLE, exit 0       SQL logic error
//	fts4    no          exclude "docs"        5 DROP TABLE, exit 0       SQL logic error
//	fts5    yes         exclude "docs"        Schema is synced, exit 0   1 row
//
// Ptah refused, printed a remedy, and following that remedy destroyed the
// index. The fts5 control is what proves the difference is module registration
// and not the flag.
//
// The cause is one signal. SQLite marks a table `shadow` only while the module
// that owns it is loaded, so on fts3 and fts4 the module's five storage tables
// arrive as ordinary user tables. `--exclude docs` removes the virtual table
// and leaves every one of them in the comparison, where a desired state that
// does not name them reads as a request to drop them.
//
// So the rows below are mostly about what survives exclusion. The refusal is
// keyed on a list the reader records BEFORE any selection runs, which is the
// only thing that still speaks on the run that does the damage.
func TestValidateComparisonRefusesAnUnregisteredModule(t *testing.T) {
	// fts4 is genuinely absent from the module set this build registers, which
	// PRAGMA module_list reports as exactly: dbstat, fts5, fts5vocab, geopoly,
	// rtree, rtree_i32, sqlite_dbpage. Nothing here hard-codes that name as
	// special; it is unregistered the same way an invented module is.
	unclassified := []types.DBVirtualTable{{Name: "docs", Module: "fts4"}}

	tests := []struct {
		name            string
		dialect         string
		env             func(testing.TB)
		desired         *goschema.Database
		database        *types.DBSchema
		policy          sqlitevirtual.Policy
		wantErr         bool
		wantUnsupported bool
		wantContains    []string
		wantAbsent      []string
	}{
		{
			name:    "a database holding a module this build cannot load is refused",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "docs", VirtualModule: "fts4"}, {Name: "users"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`virtual table "docs" (module fts4)`,
				"does not register",
				"this build registers dbstat, fts5, fts5vocab, geopoly, rtree, rtree_i32, sqlite_dbpage",
				sqlitevirtual.AllowUnregisteredModuleEnvVar,
			},
		},
		{
			// THE ROW THIS CHANGE EXISTS FOR. The virtual table is gone from
			// Tables, exactly as `--exclude docs` leaves it, and the module's
			// storage is still here under names nothing marked. On master this
			// comparison returned nil and the planner dropped all five.
			name:    "the refusal survives excluding the virtual table",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables: []types.DBTable{
					{Name: "docs_content"},
					{Name: "docs_docsize"},
					{Name: "docs_segdir"},
					{Name: "docs_segments"},
					{Name: "docs_stat"},
					{Name: "users"},
				},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`virtual table "docs" (module fts4)`, "does not register"},
		},
		{
			// The diagnostic must not repeat the advice that destroyed the
			// index. Naming the exclusion as a remedy here is the exact defect.
			name:    "the refusal does not advise excluding the table",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "docs", VirtualModule: "fts4"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`excluding "docs" does not protect the index`,
				"Ptah cannot list them without the module",
			},
			wantAbsent: []string{
				"exclude \"docs\" from the comparison to leave it in place",
				sqlitevirtual.AllowDropEnvVar,
			},
		},
		{
			// A DBSchema built by something other than the SQLite reader
			// carries no list. The virtual table in front of the validator is
			// still checked directly, so a zero value cannot read as "every
			// module is present".
			//
			// The desired state DECLARES the virtual table, so the #1469 removal
			// refusal cannot fire and answer for this row. Asserting only that
			// some error came back would have been vacuous: with the list
			// empty and the table undeclared, the removal refusal produces a
			// message containing the same table and module.
			//
			// `docs_content` is what makes the run dangerous and therefore
			// refusable -- a live table nothing names, which on a real fts4
			// database is the module's own storage.
			name:    "a virtual table is checked even with no list recorded",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaringVirtual("docs", "fts4", "title, body"),
			database: &types.DBSchema{
				Tables: []types.DBTable{
					{Name: "docs", VirtualModule: "fts4", VirtualArguments: "title, body"},
					{Name: "docs_content"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`virtual table "docs" (module fts4)`, "does not register"},
		},
		{
			// Two tables, one missing module. The noun agrees with the module
			// count rather than the table count, because "whose modules ...
			// registers fts4" names a set of one and reads as a second module
			// the operator should go looking for.
			name:    "two tables of one module name one module",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables: []types.DBTable{{Name: "users"}, {Name: "docs_content"}},
				UnregisteredVirtualTables: []types.DBVirtualTable{
					{Name: "docs", Module: "fts4"},
					{Name: "notes_ix", Module: "fts4"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`virtual tables "docs" (module fts4), "notes_ix" (module fts4)`,
				"whose module this build of Ptah does not register",
				"a build that registers fts4,",
			},
			wantAbsent: []string{"whose modules this build"},
		},
		{
			// The control for the row above: two DIFFERENT missing modules do
			// take the plural, and both are named.
			name:    "two tables of two modules name both modules",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables: []types.DBTable{{Name: "users"}, {Name: "docs_content"}},
				UnregisteredVirtualTables: []types.DBVirtualTable{
					{Name: "docs", Module: "fts4"},
					{Name: "legacy", Module: "fts3"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				"whose modules this build of Ptah does not register",
				"a build that registers fts3, fts4,",
			},
		},
		{
			name:    "the opt-in lifts the refusal",
			dialect: "sqlite",
			env:     envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "1"),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "docs_content"}, {Name: "users"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr: false,
		},
		{
			// The difference between "set to off" and "unset".
			name:    "an explicit false keeps the refusal",
			dialect: "sqlite",
			env:     envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "false"),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "users"}, {Name: "docs_content"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"does not register"},
		},
		{
			// The other opt-in governs a different question and must not answer
			// this one. Setting it says "drop the virtual table I can see",
			// which is not permission to plan against tables Ptah has said it
			// cannot classify.
			name:    "the drop opt-in does not lift this refusal",
			dialect: "sqlite",
			env:     envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "1"),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "docs", VirtualModule: "fts4"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"does not register"},
		},
		{
			// THE fts5 CONTROL, and the reason this is not a rule about names.
			// fts5 is registered, so SQLite marked its shadow tables, nothing
			// went unclassified, and the #1469 refusal is the one that speaks.
			name:            "a registered module keeps the refusal #1469 landed",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired:         declaring("users"),
			database:        &types.DBSchema{Tables: []types.DBTable{{Name: "docs", VirtualModule: "fts5"}}},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				"the desired schema does not name",
				sqlitevirtual.AllowDropEnvVar,
			},
			wantAbsent: []string{"does not register"},
		},
		{
			// SQLite resolves a module name case-insensitively over ASCII, and
			// records the spelling the statement used. A byte comparison
			// against the lowercase name module_list reports would refuse this
			// perfectly ordinary FTS5 database.
			name:            "a registered module spelled in upper case is not refused as unregistered",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired:         declaringVirtual("docs", "FTS5", "title, body"),
			database:        &types.DBSchema{Tables: []types.DBTable{{Name: "docs", VirtualModule: "FTS5", VirtualArguments: "title, body"}}},
			wantErr:         false,
			wantUnsupported: false,
		},
		{
			// An ADDITION fails differently and certainly. `docs` is virtual on
			// the desired side and absent from the database, so the plan
			// carries CREATE VIRTUAL TABLE, which this build answers with
			// `no such module: fts4` -- measured mid-apply, after the plan had
			// been printed and auto-approved.
			name:            "a desired virtual table this build cannot create is refused",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired:         declaringVirtual("docs", "fts4", "title, body"),
			database:        &types.DBSchema{Tables: []types.DBTable{{Name: "users"}}},
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				"the desired schema adds",
				"no such module: fts4",
				"apply this schema with a build that registers fts4",
			},
		},
		{
			// THE ROW THE THIRD REVIEW FINDING ADDED. Two databases that both
			// already hold the same fts4 index, compared with the opt-in set.
			// No CREATE VIRTUAL TABLE is planned -- the pairing recognizes the
			// matching live declaration -- so the addition refusal must not
			// fire, and the database-side refusal is exactly what the opt-in
			// waives. Refusing here claimed a mid-apply failure that cannot
			// happen and left the opt-in unable to restore the comparison it
			// promises. Measured on the command before the fix:
			// `schema diff` between two identical fts4 databases exited 2.
			name:     "an unregistered module present on both sides is comparable with the opt-in",
			dialect:  "sqlite",
			env:      envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "1"),
			desired:  declaringVirtual("docs", "fts4", "title, body"),
			database: &types.DBSchema{Tables: []types.DBTable{{Name: "docs", VirtualModule: "fts4", VirtualArguments: "title, body"}}},
			wantErr:  false,
		},
		{
			// And the same pair needs no opt-in at all, because nothing in it
			// can be dropped: every live table is named on the desired side, so
			// no DROP TABLE is planned however badly Ptah has misclassified the
			// module's storage. Refusing this was the over-strict half of the
			// same finding.
			name:     "an unregistered module present on both sides needs no opt-in",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired:  declaringVirtual("docs", "fts4", "title, body"),
			database: &types.DBSchema{Tables: []types.DBTable{{Name: "docs", VirtualModule: "fts4", VirtualArguments: "title, body"}}},
			wantErr:  false,
		},
		{
			// THE SCOPING ROW. `--include users` narrows the comparison to a
			// single table the desired side names, so the module's storage is
			// not in it and cannot be dropped. Measured on the command: the
			// same run reports `Schemas are synced, no changes to be made.` at
			// exit 0, and refusing it sent an operator to the opt-in for a run
			// that was already safe.
			//
			// The recorded marker is deliberately still present -- narrowing
			// cannot make the read's statement untrue, and this is exactly the
			// case where the marker must not be read as "refuse".
			name:    "a positive scope that leaves nothing droppable is not refused",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "users"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr: false,
		},
		{
			// A table both sides name and describe differently is NOT refused
			// here, and deliberately so: whether the comparator will change it
			// is the comparator's answer, and computing it at this seam means a
			// second copy of its rules. That case is refused after the diff, by
			// TestValidatePlannedChangesRefusesAChangeItCannotVouchFor.
			name:    "a declared table is left to the post-diff gate",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaringVirtualWithTable(
				"docs", "fts4", "title, body",
				"docs_content", []string{"docid", "c0title"},
			),
			database: &types.DBSchema{
				Tables: []types.DBTable{
					{Name: "docs", VirtualModule: "fts4", VirtualArguments: "title, body"},
					{Name: "docs_content", Columns: []types.DBColumn{
						{Name: "docid"}, {Name: "c0title"}, {Name: "c1body"},
					}},
				},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr: false,
		},
		{
			// The control that keeps the row above from being a hole: add one
			// live table the desired side does not name -- which is what
			// `--exclude docs` leaves behind on a real fts4 database -- and the
			// refusal fires again.
			name:    "one undeclared live table is enough to refuse",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "users"}, {Name: "docs_content"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"does not register"},
		},
		{
			// The opt-in waives the database side and nothing else. An addition
			// this build cannot execute stays refused however it is set,
			// because no value of a variable makes a module exist.
			name:            "the opt-in does not lift the addition refusal",
			dialect:         "sqlite",
			env:             envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "1"),
			desired:         declaringVirtual("docs", "fts4", "title, body"),
			database:        &types.DBSchema{Tables: []types.DBTable{{Name: "users"}}},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"the desired schema adds", "no such module: fts4"},
		},
		{
			// No value of an environment variable makes a module exist, so the
			// desired-side refusal has no escape -- the same rule the kind
			// collision and the changed declaration follow.
			name:            "no opt-in lifts the desired-side refusal",
			dialect:         "sqlite",
			env:             envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "1"),
			desired:         declaringVirtual("docs", "fts4", "title, body"),
			database:        &types.DBSchema{Tables: []types.DBTable{{Name: "users"}}},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"no such module: fts4"},
		},
		{
			// The dialect gate. A MySQL comparison must not be failed by a
			// SQLite subsystem, however the list got there.
			name:    "a non SQLite dialect is not touched",
			dialect: "mysql",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "docs", VirtualModule: "fts4"}},
				UnregisteredVirtualTables: unclassified,
			},
			wantErr: false,
		},
		{
			// THE ROW THE DROP-POLICY FINDING ADDED, on the unregistered-module
			// half. This is the `--exclude docs` shape -- the virtual table gone
			// from Tables and the module's storage left behind under names
			// nothing declares -- which is the run that destroyed the index. A
			// caller that skips drop_table deletes every one of those five DROP
			// TABLE statements before they are rendered, so there is nothing
			// left to destroy and nothing to refuse.
			//
			// Measured on the command with `diff.skip: [drop_table]` in
			// ptah.yaml, on an fts4 database built by a system SQLite that has
			// the module: `ptah schema apply` exited 2 before this change, and
			// the same run with both opt-ins set reported `Schema is synced, no
			// changes to be made.` at exit 0 -- an empty plan the refusal was
			// standing in front of.
			name:    "a caller that skips table drops is not refused for the drops it skips",
			dialect: "sqlite",
			env:     envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired: declaring("users"),
			database: &types.DBSchema{
				Tables: []types.DBTable{
					{Name: "docs_content"},
					{Name: "docs_docsize"},
					{Name: "docs_segdir"},
					{Name: "docs_segments"},
					{Name: "docs_stat"},
					{Name: "users"},
				},
				UnregisteredVirtualTables: unclassified,
			},
			policy:  sqlitevirtual.Policy{SkipDropTable: true},
			wantErr: false,
		},
		{
			// The control that keeps the row above from being "never refuse".
			// `skip drop_table` says nothing about a module this build cannot
			// CREATE: the plan still carries CREATE VIRTUAL TABLE and this build
			// still answers `no such module: fts4`, and no policy makes a module
			// exist.
			name:            "skipping table drops does not lift the addition refusal",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			desired:         declaringVirtual("docs", "fts4", "title, body"),
			database:        &types.DBSchema{Tables: []types.DBTable{{Name: "users"}}},
			policy:          sqlitevirtual.Policy{SkipDropTable: true},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{"the desired schema adds", "no such module: fts4"},
		},
		{
			// And the parse is still owed under the policy. Resolving the opt-in
			// after the policy short-circuit would leave a typo dormant on
			// exactly the projects that configure one (stokaro/ptah#1334).
			name:         "a malformed opt-in is refused under the policy too",
			dialect:      "sqlite",
			env:          envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "maybe"),
			desired:      declaring("users"),
			database:     &types.DBSchema{Tables: []types.DBTable{{Name: "docs_content"}, {Name: "users"}}, UnregisteredVirtualTables: unclassified},
			policy:       sqlitevirtual.Policy{SkipDropTable: true},
			wantErr:      true,
			wantContains: []string{sqlitevirtual.AllowUnregisteredModuleEnvVar, "maybe"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.env(t)

			err := sqlitevirtual.ValidateComparison(tt.dialect, tt.desired, tt.database, tt.policy)

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			c.Assert(errors.Is(err, ptaherr.ErrUnsupportedFeature), qt.Equals, tt.wantUnsupported)
			for _, fragment := range tt.wantContains {
				c.Assert(errorText(err), qt.Contains, fragment)
			}
			for _, fragment := range tt.wantAbsent {
				c.Assert(errorText(err), qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestValidatePlannedChangesRefusesAChangeItCannotVouchFor is the half of the
// guard that only the comparator can answer.
//
// [sqlitevirtual.ValidateComparison] runs before anything is compared, so it can
// ask "is this live table missing from the desired state" -- which is exactly
// the comparator's removal set -- but not "would this table be CHANGED". Two
// database-backed states can name the same table and differ in a column's type,
// nullability, default, generated expression, or a table constraint, and every
// one of those makes the SQLite planner rebuild the table: drop, recreate,
// copy. On a table that is really a module's storage that destroys the index as
// surely as dropping it. Raised in review against a name-only equality check
// that concluded nothing could change.
//
// So the question is asked afterwards, of the diff itself, which cannot drift
// from the rules that produced it.
func TestValidatePlannedChangesRefusesAChangeItCannotVouchFor(t *testing.T) {
	unclassified := []types.DBVirtualTable{{Name: "docs", Module: "fts4"}}
	holdingFTS4 := &types.DBSchema{
		Tables: []types.DBTable{
			{Name: "docs", VirtualModule: "fts4"},
			{Name: "docs_content"},
		},
		UnregisteredVirtualTables: unclassified,
	}

	tests := []struct {
		name            string
		dialect         string
		env             func(testing.TB)
		database        *types.DBSchema
		diff            *difftypes.SchemaDiff
		policy          sqlitevirtual.Policy
		wantErr         bool
		wantUnsupported bool
		wantContains    []string
	}{
		{
			// THE ROW THIS GATE EXISTS FOR. Nothing is dropped; a table both
			// sides name is rebuilt, and it may be the module's.
			name:            "a modified table in an unclassifiable database is refused",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database:        holdingFTS4,
			diff:            modifying("docs_content"),
			wantErr:         true,
			wantUnsupported: true,
			wantContains: []string{
				`the plan changes "docs_content"`,
				`virtual table "docs" (module fts4)`,
				"dropping or rebuilding one of them destroys the index",
				sqlitevirtual.AllowUnregisteredModuleEnvVar,
			},
		},
		{
			// THE ROW THE ADD-COLUMN FINDING ADDED. `ALTER TABLE t ADD COLUMN c`
			// is a statement SQLite has, so the planner emits it in place and
			// drops or rebuilds nothing -- see TestPlannerAddsColumnsAndIndexes
			// in internal/planner/dialects/sqlite. Counting it refused
			// `schema diff --include users` against an fts4 database whose whole
			// plan was `ALTER TABLE "users" ADD COLUMN "email" TEXT;`, and the
			// only escape offered was the opt-in that also permits the drops.
			name:     "a table that only gains a column is not counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff:     addingColumn("users"),
			wantErr:  false,
		},
		{
			// The first control for the row above. The exclusion is about a
			// table diff whose ONLY change is added columns; one rebuilding
			// change beside them still rebuilds the table, added columns and
			// all.
			name:     "a table that gains a column and loses one is still counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "docs_content",
				ColumnsAdded:   []string{"spurious"},
				ColumnsRemoved: []string{"c1body"},
			}}},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// The second control. A constraint recorded ON the table diff also
			// rebuilds -- SQLite has no ALTER for a constraint -- so the
			// exclusion must not read "TablesModified is never counted".
			name:     "a table diff carrying a constraint change is still counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:        "docs_content",
				ColumnsAdded:     []string{"spurious"},
				ConstraintsAdded: []string{"docs_content_chk"},
			}}},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// The third control, and the one that keeps the exclusion from
			// re-opening the constraint route the previous round closed: the
			// same add-column-only table diff, beside a schema-level constraint
			// change on that table. planTableRebuilds derives it from
			// ConstraintsAddedWithTables and rebuilds it, so the gate must too.
			name:     "an added column beside a schema level constraint change is still counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName:    "docs_content",
					ColumnsAdded: []string{"spurious"},
				}},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "docs_content_chk", TableName: "docs_content", Type: "CHECK"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// The constraint-only rebuild. Columns unchanged, so the change is
			// recorded at schema level and TablesModified is empty -- but
			// SQLite has no ALTER for a constraint, so planTableRebuilds
			// derives this table from ConstraintsAddedWithTables and rebuilds
			// it: drop, recreate, copy. Reading only the two table fields let
			// it through, which review caught.
			name:     "a constraint-only rebuild is refused",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "docs_content_chk", TableName: "docs_content", Type: "CHECK"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			name:     "a constraint removal is refused too",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
					{Name: "docs_content_chk", TableName: "docs_content", Type: "CHECK"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// The exclusion, and the control for the two rows above: a
			// constraint on a table the plan CREATES cannot be on storage the
			// module already owns, so adding a table with a constraint beside an
			// index Ptah cannot classify stays ordinary work.
			name:     "a constraint on an added table is not counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TablesAdded: []string{"audit"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "audit_chk", TableName: "audit", Type: "CHECK"},
				},
			},
			wantErr: false,
		},
		{
			// The exclusion is an identity question, not a string one.
			// TablesAdded carries the comparator's spelling while a
			// constraint's TableName comes from the declaration or the catalog,
			// so one can say `main.audit` where the other says `audit`. A raw
			// lookup answers "different object" for one object -- the shape
			// stokaro/ptah#1351 came from -- and the cost is refusing a safe
			// addition.
			name:     "an added table spelled with its schema is still not counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TablesAdded: []string{"main.audit"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "audit_chk", TableName: "audit", Type: "CHECK"},
				},
			},
			wantErr: false,
		},
		{
			// SQLite folds ASCII case, so these two name one table too.
			name:     "an added table spelled in another case is still not counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TablesAdded: []string{"AUDIT"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "audit_chk", TableName: "audit", Type: "CHECK"},
				},
			},
			wantErr: false,
		},
		{
			// The control for both: a constraint on a table the plan does NOT
			// add is still counted, so the identity-aware lookup has not turned
			// the exclusion into "never refuse".
			name:     "a constraint on a table the plan does not add is still counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TablesAdded: []string{"main.audit"},
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "docs_content_chk", TableName: "docs_content", Type: "CHECK"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			name:            "a removed table in an unclassifiable database is refused",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database:        holdingFTS4,
			diff:            removing("docs_stat"),
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_stat"`},
		},
		{
			// THE CONTROL that keeps the gate from being "refuse every fts4
			// database again". A diff that changes nothing cannot touch the
			// module's storage, whatever Ptah believes that storage to be, so
			// the comparison the earlier rounds unblocked stays unblocked.
			name:     "a diff that changes nothing is not refused",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			wantErr:  false,
		},
		{
			// The other control: the same change against a database whose
			// modules are all present is ordinary work.
			name:     "a modified table in a classifiable database is not refused",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: &types.DBSchema{Tables: []types.DBTable{{Name: "docs", VirtualModule: "fts5"}}},
			diff:     modifying("docs"),
			wantErr:  false,
		},
		{
			name:     "the opt-in lifts it",
			dialect:  "sqlite",
			env:      envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "1"),
			database: holdingFTS4,
			diff:     modifying("docs_content"),
			wantErr:  false,
		},
		{
			// The dialect gate. A MySQL plan must not be failed by a SQLite
			// subsystem, however the list got onto the description.
			name:     "a non SQLite dialect is not touched",
			dialect:  "mysql",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff:     modifying("docs_content"),
			wantErr:  false,
		},
		{
			// THE ROW THE DROP-POLICY FINDING ADDED, on the post-diff half. The
			// caller filters TablesRemoved out of this diff after this gate
			// returns, so counting a removal here refuses a plan that will not
			// contain it.
			name:     "a removal is not counted when the caller skips table drops",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff:     removing("docs_stat"),
			policy:   sqlitevirtual.Policy{SkipDropTable: true},
			wantErr:  false,
		},
		{
			// A dropped table's dependent removals go with it, so a constraint
			// whose host is being dropped is not an ALTER on a kept table --
			// both implementations of the policy delete it. Counting the host
			// would refuse the same emptied plan by another route.
			name:     "a constraint on a table the policy keeps from dropping is not counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TablesRemoved: []string{"main.docs_content"},
				ConstraintsRemovedWithTables: []difftypes.ConstraintRemovalInfo{
					{Name: "docs_content_chk", TableName: "docs_content", Type: "CHECK"},
				},
			},
			policy:  sqlitevirtual.Policy{SkipDropTable: true},
			wantErr: false,
		},
		{
			// THE CONTROL for both rows above, and the reason the policy is not
			// simply "return nil". `skip drop_table` filters removals, not
			// modifications: a table both sides name and describe differently is
			// still rebuilt by the SQLite planner -- drop, recreate, copy -- and
			// on a table that is really the module's storage that destroys the
			// index just the same.
			name:            "a rebuild is still refused when the caller skips table drops",
			dialect:         "sqlite",
			env:             envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database:        holdingFTS4,
			diff:            modifying("docs_content"),
			policy:          sqlitevirtual.Policy{SkipDropTable: true},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// The second control: a constraint-only rebuild reaches
			// planTableRebuilds through a schema-level field the drop policy
			// never touches, so it stays refused too.
			name:     "a constraint-only rebuild is still refused when the caller skips table drops",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				ConstraintsAddedWithTables: []difftypes.ConstraintAdditionInfo{
					{Name: "docs_content_chk", TableName: "docs_content", Type: "CHECK"},
				},
			},
			policy:          sqlitevirtual.Policy{SkipDropTable: true},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// THE ROW THE INDEX-AND-TRIGGER FINDING ADDED. `removeIndexes`
			// renders DROP INDEX for a table it is not rebuilding, so this
			// reaches the plan with TablesModified and TablesRemoved both
			// empty and the gate previously saw nothing.
			//
			// Reproduced on the command against an fts4 database this build
			// cannot load, both sides naming the module's storage:
			// `ptah schema diff --from sqlite://live.db --to sqlite://desired.db`
			// planned `DROP INDEX IF EXISTS "docs_content_title_idx";` at
			// exit 0.
			name:     "an index removed from a table in an unclassifiable database is refused",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "docs_content_title_idx", TableName: "docs_content"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// The same finding's trigger half. `removeTriggers` renders
			// DROP TRIGGER unconditionally -- it does not even consult the
			// rebuild set -- so a trigger removal is a statement against a
			// table nothing else in the diff mentions. Measured the same way:
			// `DROP TRIGGER IF EXISTS "docs_content_guard";` at exit 0.
			name:     "a trigger removed from a table in an unclassifiable database is refused",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TriggersRemoved: []difftypes.TriggerRef{
					{TriggerName: "docs_content_guard", TableName: "docs_content"},
				},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// And its replacement half. `modifyTriggers` emits the desired
			// trigger with SetReplace, which puts the existing one out of the
			// way first; the object the module may be maintaining is gone
			// either way.
			name:     "a replaced trigger in an unclassifiable database is refused",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				TriggersModified: []difftypes.TriggerDiff{{
					TriggerName: "docs_content_guard",
					TableName:   "docs_content",
					Changes:     map[string]string{"body": "old -> new"},
				}},
			},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// THE CONTROL that keeps the three rows above from becoming
			// "any index or trigger change is refused". An addition removes
			// nothing, so the harm this gate names cannot come from one, and a
			// database holding an unloadable module must still be able to gain
			// an index.
			name:     "an index or trigger the plan only ADDS is not counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				IndexesAdded: []difftypes.IndexRef{
					{Name: "docs_content_title_idx", TableName: "docs_content"},
				},
				TriggersAdded: []difftypes.TriggerRef{
					{TriggerName: "docs_content_guard", TableName: "docs_content"},
				},
			},
			wantErr: false,
		},
		{
			// The policy row for the index half, and the reason SkipDropIndex
			// exists at all: adding index removals to the counted set without
			// it would have re-opened, on a new field, the over-refusal the
			// drop_table round closed.
			name:     "an index removal is not counted when the caller skips index drops",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "docs_content_title_idx", TableName: "docs_content"},
				},
			},
			policy:  sqlitevirtual.Policy{SkipDropIndex: true},
			wantErr: false,
		},
		{
			// Its control. `skip drop_index` keeps a REPLACEMENT -- an index
			// dropped and recreated under the same name -- because the plan
			// needs the pair, so the table it is aimed at stays counted.
			name:     "an index replacement is still counted when the caller skips index drops",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{
				IndexesRemoved: []difftypes.IndexRef{
					{Name: "docs_content_title_idx", TableName: "docs_content"},
				},
				IndexesAdded: []difftypes.IndexRef{
					{Name: "docs_content_title_idx", TableName: "docs_content"},
				},
			},
			policy:          sqlitevirtual.Policy{SkipDropIndex: true},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// THE ROW THE SKIPPED-COLUMN-DROP FINDING ADDED. A removed column
			// is one of the shapes NeedsTableRebuild reports true for, so this
			// diff was counted as a rebuild -- but `skip drop_column` empties
			// ColumnsRemoved before anything is rendered, leaving a table diff
			// that changes nothing.
			//
			// Reproduced on the command: `ptah migrations generate` with
			// `diff.skip: [drop_table, drop_column]` against an fts4 database,
			// dropping one column from an ordinary `users` table, exited 2 --
			// while the same run with the opt-in set exited 0 and wrote no
			// migration file at all.
			name:     "a column drop is not counted when the caller skips column drops",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "users",
				ColumnsRemoved: []string{"legacy"},
			}}},
			policy:  sqlitevirtual.Policy{SkipDropColumn: true},
			wantErr: false,
		},
		{
			// The control that keeps the row above from reading "a table diff
			// is never counted under this policy". The skip empties
			// ColumnsRemoved and nothing else, so a type change beside it still
			// rebuilds the table -- drop, recreate, copy -- and is still
			// refused. This is the row that fails if the policy is read as a
			// short-circuit rather than as a filter over the diff.
			name:     "a column drop beside a type change is still counted under the same policy",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "docs_content",
				ColumnsRemoved: []string{"c1body"},
				ColumnsModified: []difftypes.ColumnDiff{{
					ColumnName: "c0title",
					Changes:    map[string]string{"type": "TEXT -> INTEGER"},
				}},
			}}},
			policy:          sqlitevirtual.Policy{SkipDropColumn: true},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
		{
			// The second control: without the policy the same column drop is a
			// rebuild the planner really performs, so it stays refused.
			// Measured with `diff.skip: [drop_table]` alone on the fixture
			// above -- exit 2.
			name:     "a column drop with no policy is still counted",
			dialect:  "sqlite",
			env:      envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar),
			database: holdingFTS4,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "docs_content",
				ColumnsRemoved: []string{"c1body"},
			}}},
			wantErr:         true,
			wantUnsupported: true,
			wantContains:    []string{`the plan changes "docs_content"`},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.env(t)

			err := sqlitevirtual.ValidatePlannedChanges(tt.dialect, tt.database, tt.diff, tt.policy)

			c.Assert(err != nil, qt.Equals, tt.wantErr)
			c.Assert(errors.Is(err, ptaherr.ErrUnsupportedFeature), qt.Equals, tt.wantUnsupported)
			for _, fragment := range tt.wantContains {
				c.Assert(errorText(err), qt.Contains, fragment)
			}
		})
	}
}

// TestDiffPolicySkipKindsAreClassified is the census that keeps the next skip
// kind from defaulting to "this gate does not read it".
//
// Three rounds of review on this guard were the same mistake in three places: a
// refusal keyed on a statement the caller's diff policy deletes again before
// anything is rendered. Each was found one field at a time -- drop_table, then
// drop_column, then the index removals the drop_index policy filters -- because
// nothing required the vocabulary to be enumerated. So it is enumerated here:
// every member of [diffpolicy.AllChangeKinds] is either carried by a
// [sqlitevirtual.Policy] field or listed as one this gate reads no field of,
// and a fifth kind fails this test until somebody decides which it is.
//
// The second half is what makes the first half more than bookkeeping: for each
// carried kind, the same diff is refused with the zero policy and admitted with
// the field set, so a Policy field that is declared and never consulted cannot
// pass the census.
func TestDiffPolicySkipKindsAreClassified(t *testing.T) {
	// drop_enum is the deliberate omission: SQLite has no enum type, nothing in
	// a SQLite comparison populates EnumsRemoved, and this gate reads no enum
	// field at all.
	unread := []diffpolicy.ChangeKind{diffpolicy.DropEnum}

	tests := []struct {
		name   string
		kind   diffpolicy.ChangeKind
		diff   *difftypes.SchemaDiff
		policy sqlitevirtual.Policy
	}{
		{
			name:   "drop_table",
			kind:   diffpolicy.DropTable,
			diff:   removing("docs_stat"),
			policy: sqlitevirtual.Policy{SkipDropTable: true},
		},
		{
			name: "drop_column",
			kind: diffpolicy.DropColumn,
			diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
				TableName:      "docs_content",
				ColumnsRemoved: []string{"c1body"},
			}}},
			policy: sqlitevirtual.Policy{SkipDropColumn: true},
		},
		{
			name: "drop_index",
			kind: diffpolicy.DropIndex,
			diff: &difftypes.SchemaDiff{IndexesRemoved: []difftypes.IndexRef{
				{Name: "docs_content_title_idx", TableName: "docs_content"},
			}},
			policy: sqlitevirtual.Policy{SkipDropIndex: true},
		},
	}

	t.Run("every skip kind is classified", func(t *testing.T) {
		c := qt.New(t)

		classified := slices.Clone(unread)
		for _, tt := range tests {
			classified = append(classified, tt.kind)
		}
		slices.Sort(classified)
		all := slices.Clone(diffpolicy.AllChangeKinds())
		slices.Sort(all)

		c.Assert(classified, qt.DeepEquals, all)
	})

	database := &types.DBSchema{
		Tables:                    []types.DBTable{{Name: "docs", VirtualModule: "fts4"}},
		UnregisteredVirtualTables: []types.DBVirtualTable{{Name: "docs", Module: "fts4"}},
	}

	for _, tt := range tests {
		t.Run(tt.name+" is refused without the policy", func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar)(t)

			err := sqlitevirtual.ValidatePlannedChanges("sqlite", database, tt.diff, sqlitevirtual.Policy{})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})

		t.Run(tt.name+" is admitted with the policy", func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(sqlitevirtual.AllowUnregisteredModuleEnvVar)(t)

			err := sqlitevirtual.ValidatePlannedChanges("sqlite", database, tt.diff, tt.policy)

			c.Assert(err, qt.IsNil)
		})
	}
}

// modifying and removing build the one-change diffs this gate reads. The gate
// asks the comparator's answer rather than recomputing it, so a test supplies
// that answer directly.
//
// modifying carries a CHANGED COLUMN rather than an empty TableDiff. The
// difference is the whole subject of the add-column finding: an entry recording
// no change at all, and an entry recording only added columns, are both
// converged without a rebuild, so a fixture that used one of those pinned a
// refusal the planner's own predicate does not justify. `type: TEXT -> INTEGER`
// is the smallest shape SQLite has no ALTER for.
func modifying(table string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName: table,
		ColumnsModified: []difftypes.ColumnDiff{{
			ColumnName: "c0title",
			Changes:    map[string]string{"type": "TEXT -> INTEGER"},
		}},
	}}}
}

// addingColumn builds the diff the SQLite planner converges with a single
// `ALTER TABLE ... ADD COLUMN`.
func addingColumn(table string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
		TableName:    table,
		ColumnsAdded: []string{"email"},
	}}}
}

func removing(table string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{TablesRemoved: []string{table}}
}

// declaringVirtualWithTable builds a desired state holding a virtual table and
// one ordinary table with the named columns, which is what a database URL on
// the desired side produces for a database whose module this build cannot load:
// the virtual table, and the module's storage described as an ordinary one.
func declaringVirtualWithTable(
	virtualName, module, arguments string,
	tableName string,
	columns []string,
) *goschema.Database {
	fields := make([]goschema.Field, 0, len(columns))
	for _, column := range columns {
		fields = append(fields, goschema.Field{StructName: tableName, Name: column})
	}
	return &goschema.Database{
		Tables: []goschema.Table{
			{
				StructName:       virtualName,
				Name:             virtualName,
				VirtualModule:    module,
				VirtualArguments: arguments,
			},
			{StructName: tableName, Name: tableName},
		},
		Fields: fields,
	}
}

// TestValidateToggleResolvesBothOwnedVariables keeps a typo in the newer opt-in
// from staying dormant.
//
// Both toggles this package owns are resolved as soon as the dialect is known,
// for the reason stokaro/ptah#1334 gives: an operator who misspells one and is
// told nothing believes the refusal is unconditional, and finds out on the day
// the condition fires.
func TestValidateToggleResolvesBothOwnedVariables(t *testing.T) {
	tests := []struct {
		name    string
		env     func(testing.TB)
		dialect string
		wantErr string
	}{
		{
			name:    "a malformed unregistered-module opt-in is a configuration error",
			env:     envbooltest.Set(sqlitevirtual.AllowUnregisteredModuleEnvVar, "maybe"),
			dialect: "sqlite",
			wantErr: `invalid boolean value "maybe" for ` + sqlitevirtual.AllowUnregisteredModuleEnvVar,
		},
		{
			name:    "a malformed drop opt-in is still a configuration error",
			env:     envbooltest.Set(sqlitevirtual.AllowDropEnvVar, "yes"),
			dialect: "sqlite",
			wantErr: `invalid boolean value "yes" for ` + sqlitevirtual.AllowDropEnvVar,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			tt.env(t)

			c.Assert(sqlitevirtual.ValidateToggle(tt.dialect), qt.ErrorMatches, tt.wantErr)
			// The same variable on a dialect this subsystem does not own is not
			// this subsystem's business, and failing a MySQL plan over it would
			// be the mistake stokaro/ptah#1334 names.
			c.Assert(sqlitevirtual.ValidateToggle("mysql"), qt.IsNil)
		})
	}
}

// TestReportUnclassifiedNamesWhatTheDescriptionCannotVouchFor covers the read
// surfaces, which refuse nothing and so must say something.
//
// `ptah db read` against an fts4 database emits the virtual table correctly and
// then a CREATE TABLE for each of the module's five storage tables. Replayed,
// those five collide with the index the first statement creates. A reader shown
// that with nothing said cannot tell which half is real.
func TestReportUnclassifiedNamesWhatTheDescriptionCannotVouchFor(t *testing.T) {
	tests := []struct {
		name         string
		schema       *types.DBSchema
		wantContains []string
		wantAbsent   []string
		wantSilent   bool
	}{
		{
			name:       "a nil schema says nothing",
			schema:     nil,
			wantSilent: true,
		},
		{
			name:       "a fully classified database says nothing",
			schema:     &types.DBSchema{Tables: []types.DBTable{{Name: "docs", VirtualModule: "fts5"}}},
			wantSilent: true,
		},
		{
			name: "an unclassified table is named with its module",
			schema: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "docs", VirtualModule: "fts4"}},
				UnregisteredVirtualTables: []types.DBVirtualTable{{Name: "docs", Module: "fts4"}},
			},
			wantContains: []string{
				`virtual table "docs" (module fts4)`,
				"does not register",
				"reports them as ordinary tables",
				sqlitevirtual.AllowUnregisteredModuleEnvVar,
			},
		},
		{
			name: "every unclassified table is named, not just the first",
			schema: &types.DBSchema{
				Tables: []types.DBTable{
					{Name: "docs", VirtualModule: "fts4"},
					{Name: "legacy", VirtualModule: "fts3"},
				},
				UnregisteredVirtualTables: []types.DBVirtualTable{
					{Name: "docs", Module: "fts4"},
					{Name: "legacy", Module: "fts3"},
				},
			},
			wantContains: []string{`"docs" (module fts4)`, `"legacy" (module fts3)`, "fts3, fts4"},
		},
		{
			// Selection runs before the note, and the two projections it can
			// produce need opposite things said. Here the virtual table is gone
			// from the document, so naming it would send a reader looking for a
			// statement that is not there -- but staying silent is worse, since
			// `--exclude docs` leaves the module's storage in the document as
			// ordinary CREATE TABLEs with nothing said. The note keeps the
			// warning and drops the name.
			name: "a projection that dropped the virtual table still warns, without naming it",
			schema: &types.DBSchema{
				Tables:                    []types.DBTable{{Name: "users"}},
				UnregisteredVirtualTables: []types.DBVirtualTable{{Name: "docs", Module: "fts4"}},
			},
			wantContains: []string{
				"this description was narrowed",
				"module fts4",
				"cannot tell whether any of the ordinary tables below are the module's private storage",
			},
			wantAbsent: []string{`virtual table "docs"`},
		},
		{
			// A document with no tables in it has no statements to warn about.
			// Selection can produce one: `--include` naming something the
			// database does not have renders nothing, and a note beside an
			// empty rendering describes a document that does not exist.
			name: "an empty description says nothing",
			schema: &types.DBSchema{
				UnregisteredVirtualTables: []types.DBVirtualTable{{Name: "docs", Module: "fts4"}},
			},
			wantSilent: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			sqlitevirtual.ReportUnclassified(&out, tt.schema)

			c.Assert(out.String() == "", qt.Equals, tt.wantSilent)
			for _, fragment := range tt.wantContains {
				c.Assert(out.String(), qt.Contains, fragment)
			}
			for _, fragment := range tt.wantAbsent {
				c.Assert(out.String(), qt.Not(qt.Contains), fragment)
			}
		})
	}
}

// TestReportUnclassifiedToleratesNoDiagnosticsStream matches how the inspect
// surfaces spell "no diagnostics stream". A note that panicked would fail a
// read that succeeded.
func TestReportUnclassifiedToleratesNoDiagnosticsStream(t *testing.T) {
	sqlitevirtual.ReportUnclassified(nil, &types.DBSchema{
		UnregisteredVirtualTables: []types.DBVirtualTable{{Name: "docs", Module: "fts4"}},
	})
}
