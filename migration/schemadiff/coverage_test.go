package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// The four coverage tests below are all one shape, and the shape is the point.
// Every case is run TWICE against the same schemas: once with the limit
// declared and once without it. Only the second column proves the first one
// means anything -- a suppression that never plans the change would pass the
// first column while being a different, worse defect (stokaro/ptah#1276).

// coverageCase is one difference, asked of a comparison that declared a limit
// covering it and of one that did not.
type coverageCase struct {
	name string
	// desired and database are shared by both runs; only the coverage moves.
	desired  func() *goschema.Database
	database func() *types.DBSchema
	// notDescribed is attached to whichever side the case is about.
	notDescribed coverage.Set
	// onDesired attaches the record to the desired state (gating removals)
	// rather than to the read (gating additions).
	onDesired bool
	// read pulls the one list the case is about out of the diff.
	read func(*difftypes.SchemaDiff) []string
	// wantWithout is what the comparison plans when nothing was declared. It is
	// never empty: a case whose control plans nothing proves nothing.
	wantWithout []string
}

func coverageCases() []coverageCase {
	return []coverageCase{
		{
			// The destructive one. `ptah-compat schema inspect` omits every
			// extension block nothing else in the document names, so applying
			// its own output back to the database it came from planned
			// DROP EXTENSION for an extension that database has.
			name:    "an undescribed extension kind is not a dropped extension",
			desired: func() *goschema.Database { return &goschema.Database{} },
			database: func() *types.DBSchema {
				return &types.DBSchema{Extensions: []types.DBExtension{{Name: "pgcrypto", Schema: "public"}}}
			},
			notDescribed: coverage.Set{}.WithKind(coverage.Extension),
			onDesired:    true,
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.ExtensionsRemoved },
			wantWithout:  []string{"pgcrypto"},
		},
		{
			name:    "an undescribed sequence kind is not a dropped sequence",
			desired: func() *goschema.Database { return &goschema.Database{} },
			database: func() *types.DBSchema {
				return &types.DBSchema{Sequences: []types.DBSequence{{Name: "order_seq", Schema: "public"}}}
			},
			notDescribed: coverage.Set{}.WithKind(coverage.Sequence),
			onDesired:    true,
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.SequencesRemoved },
			wantWithout:  []string{"public.order_seq"},
		},
		{
			// PostgreSQL roles are cluster-scoped, so a read restricted to one
			// database or one schema describes a subset of them by
			// construction. CREATE ROLE for a role that exists fails the
			// migration outright.
			name: "an undescribed role is not a missing role",
			desired: func() *goschema.Database {
				return &goschema.Database{Roles: []goschema.Role{{Name: "admin_user", Login: true}}}
			},
			database:     func() *types.DBSchema { return &types.DBSchema{} },
			notDescribed: coverage.Set{}.WithObject(coverage.Role, "admin_user"),
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.RolesAdded },
			wantWithout:  []string{"admin_user"},
		},
		{
			// The schema reader moving to realm scope made applying a
			// database's own description back to it plan CREATE TABLE for a
			// table that exists, because the read had never looked at the
			// schema holding it.
			name: "a table in an undescribed schema is not a missing table",
			desired: func() *goschema.Database {
				return &goschema.Database{
					Tables: []goschema.Table{{Name: "b", Schema: "extra", StructName: "B"}},
				}
			},
			database:     func() *types.DBSchema { return &types.DBSchema{} },
			notDescribed: coverage.Set{}.WithObject(coverage.Schema, "extra"),
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.TablesAdded },
			wantWithout:  []string{"extra.b"},
		},
	}
}

func TestCoverageSuppressesOnlyWhatWasNotDescribed(t *testing.T) {

	for _, test := range coverageCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, database := test.desired(), test.database()
			desired.NotDescribed = pickDesired(test.onDesired, test.notDescribed)
			database.NotDescribed = pickCurrent(test.onDesired, test.notDescribed)

			c.Assert(test.read(schemadiff.Compare(desired, database)), qt.HasLen, 0)
		})
	}
}

// TestNoCoverageStillPlansTheChange is the control for every row above, run on
// the same schemas with the record removed. Without it a comparator that
// planned nothing at all would pass the suppression test.
func TestNoCoverageStillPlansTheChange(t *testing.T) {

	for _, test := range coverageCases() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired, database := test.desired(), test.database()

			c.Assert(test.read(schemadiff.Compare(desired, database)), qt.DeepEquals, test.wantWithout)
		})
	}
}

// TestCoverageOnTheWrongSideSuppressesNothing pins the asymmetry. The desired
// state's limits gate removals and the read's limits gate additions, and a
// record on the other side must not suppress anything: a document that does not
// describe extensions still says nothing about whether one should be CREATED,
// and that question is the read's to answer.
func TestCoverageOnTheWrongSideSuppressesNothing(t *testing.T) {

	t.Run("a desired-state record does not suppress an addition", func(t *testing.T) {
		c := qt.New(t)
		desired := &goschema.Database{Extensions: []goschema.Extension{{Name: "pgcrypto"}}}
		desired.NotDescribed = coverage.Set{}.WithKind(coverage.Extension)

		diff := schemadiff.Compare(desired, &types.DBSchema{})

		c.Assert(diff.ExtensionsAdded, qt.DeepEquals, []string{"pgcrypto"})
	})

	t.Run("a read record does not suppress a removal", func(t *testing.T) {
		c := qt.New(t)
		database := &types.DBSchema{Extensions: []types.DBExtension{{Name: "pgcrypto", Schema: "public"}}}
		database.NotDescribed = coverage.Set{}.WithKind(coverage.Extension)

		diff := schemadiff.Compare(&goschema.Database{}, database)

		c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"pgcrypto"})
	})
}

// TestCoverageNamesOneObjectOnly pins that a per-object record covers that
// object and nothing beside it. A record that quietly widened to its whole kind
// would pass every suppression assertion above while silencing removals nobody
// asked to silence.
func TestCoverageNamesOneObjectOnly(t *testing.T) {
	c := qt.New(t)

	desired := &goschema.Database{}
	desired.NotDescribed = coverage.Set{}.WithObject(coverage.Extension, "pgcrypto")
	database := &types.DBSchema{Extensions: []types.DBExtension{
		{Name: "pgcrypto", Schema: "public"},
		{Name: "postgis", Schema: "public"},
	}}

	diff := schemadiff.Compare(desired, database)

	c.Assert(diff.ExtensionsRemoved, qt.DeepEquals, []string{"postgis"})
}

// TestUndescribedPolicyIsNotADroppedPolicy is the same pair as the table rows
// above, written out because a planned policy removal carries its table
// alongside its name and so cannot share their `[]string` reader. The compat
// surface omits `policy` blocks with the same rule it omits `extension` blocks,
// and the measured plan dropped a policy the database had.
func TestUndescribedPolicyIsNotADroppedPolicy(t *testing.T) {

	database := func() *types.DBSchema {
		return &types.DBSchema{RLSPolicies: []types.DBRLSPolicy{{Name: "p", Table: "public.guarded"}}}
	}

	t.Run("declared: nothing is planned", func(t *testing.T) {
		c := qt.New(t)
		desired := &goschema.Database{}
		desired.NotDescribed = coverage.Set{}.WithKind(coverage.Policy)

		c.Assert(schemadiff.Compare(desired, database()).RLSPoliciesRemoved, qt.HasLen, 0)
	})

	t.Run("control: undeclared, the drop is still planned", func(t *testing.T) {
		c := qt.New(t)
		diff := schemadiff.Compare(&goschema.Database{}, database())

		c.Assert(diff.RLSPoliciesRemoved, qt.DeepEquals, []difftypes.RLSPolicyRef{
			{PolicyName: "p", TableName: "public.guarded"},
		})
	})
}

func pickDesired(onDesired bool, set coverage.Set) coverage.Set {
	return map[bool]coverage.Set{true: set, false: {}}[onDesired]
}

func pickCurrent(onDesired bool, set coverage.Set) coverage.Set {
	return map[bool]coverage.Set{false: set, true: {}}[onDesired]
}
