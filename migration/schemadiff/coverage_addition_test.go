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

// A record on the CURRENT side says the read never looked. It does not say the
// author did not ask. Reading it as both deleted an explicitly declared object
// from the plan and then reported a synced schema:
//
//	current: // ptah:not-described sequence
//	desired: sequence "s1" { if_not_exists = true }
//	result:  Schemas are synced, no changes to be made.   exit 0, stderr empty
//
// measured on PostgreSQL 17.10 through `ptah-compat schema diff --from
// file://<compat-inspected doc> --to file://<doc declaring citext>`, where the
// same command with the record stripped from --from printed
// `CREATE SEQUENCE IF NOT EXISTS "public"."s1";`.
//
// The gate is kept, because a read that did not look cannot tell a missing
// object from an existing one and `CREATE ROLE` for a role that exists fails
// the migration. What changed is that it now asks whether the creation NEEDS
// that answer: a creation carrying IF NOT EXISTS is correct either way only
// when the guard converges every modeled semantic. A guarded sequence is
// planned. An extension is withheld even with the guard, because an existing
// extension in another schema makes CREATE EXTENSION IF NOT EXISTS a successful
// no-op that leaves desired placement unapplied (stokaro/ptah#1276,
// stokaro/ptah#1441).

// TestAGuardedNonExtensionCreationSurvivesAReadThatDidNotLook pins the generic
// coverage rule independently of extensions, whose installation schema makes
// an IF NOT EXISTS no-op insufficient to converge an unknown current state.
func TestAGuardedNonExtensionCreationSurvivesAReadThatDidNotLook(t *testing.T) {

	tests := []struct {
		name        string
		desired     func() *goschema.Database
		read        func(*difftypes.SchemaDiff) []string
		wantPlanned []string
	}{
		{
			name: "a sequence the desired state declares with if_not_exists",
			desired: func() *goschema.Database {
				return &goschema.Database{
					Sequences: []goschema.Sequence{{Name: "s1", Schema: "public", IfNotExists: true}},
				}
			},
			read:        func(diff *difftypes.SchemaDiff) []string { return diff.SequencesAdded },
			wantPlanned: []string{"public.s1"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &types.DBSchema{}
			database.NotDescribed = coverage.Set{}.WithKind(
				coverage.Extension, coverage.Policy, coverage.Sequence,
			)

			diff, undecided := schemadiff.CompareReportingUndecidedAdditions(test.desired(), database, nil)

			c.Assert(test.read(diff), qt.DeepEquals, test.wantPlanned)
			c.Assert(undecided, qt.HasLen, 0)
		})
	}
}

func TestUnknownCurrentExtensionIsWithheldRegardlessOfCreationGuard(t *testing.T) {

	for _, ifNotExists := range []bool{false, true} {
		t.Run(map[bool]string{false: "unguarded", true: "guarded"}[ifNotExists], func(t *testing.T) {
			c := qt.New(t)
			desired := &goschema.Database{Extensions: []goschema.Extension{{
				Name:        "citext",
				Schema:      "extensions",
				IfNotExists: ifNotExists,
			}}}
			current := &types.DBSchema{}
			current.NotDescribed = coverage.Set{}.WithKind(coverage.Extension)

			diff, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, current, nil)

			c.Assert(diff.ExtensionsAdded, qt.HasLen, 0)
			c.Assert(undecided, qt.DeepEquals, []coverage.Object{{
				Kind: coverage.Extension,
				Name: "citext",
			}})
		})
	}
}

// TestAPolicyAdditionSurvivesAReadThatDidNotLook covers the kind whose guard
// belongs to the planner rather than to the declaration: an RLS policy is
// emitted as `DROP POLICY IF EXISTS` followed by `CREATE POLICY`, measured on
// PostgreSQL 17.10, so the pair converges whether or not the policy is there
// and nothing has to be withheld.
func TestAPolicyAdditionSurvivesAReadThatDidNotLook(t *testing.T) {
	c := qt.New(t)

	desired := &goschema.Database{
		Tables: []goschema.Table{{Name: "guarded", StructName: "Guarded"}},
		RLSPolicies: []goschema.RLSPolicy{
			{Name: "p", Table: "guarded", PolicyFor: "SELECT", UsingExpression: "true"},
		},
	}
	database := &types.DBSchema{Tables: []types.DBTable{{Name: "guarded"}}}
	database.NotDescribed = coverage.Set{}.WithKind(coverage.Policy)

	diff, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, database, nil)

	c.Assert(diff.RLSPoliciesAdded, qt.DeepEquals, []difftypes.RLSPolicyRef{
		{PolicyName: "p", TableName: "guarded"},
	})
	c.Assert(undecided, qt.HasLen, 0)
}

// TestAnUnguardedCreationIsWithheldAndNamed is the other half. Withholding one
// is defensible -- `CREATE SEQUENCE` against a sequence that exists fails the
// migration -- but withholding it in silence is not, so the comparison reports
// what it held back and the surface reads it out.
func TestAnUnguardedCreationIsWithheldAndNamed(t *testing.T) {

	tests := []struct {
		name         string
		desired      func() *goschema.Database
		notDescribed coverage.Set
		read         func(*difftypes.SchemaDiff) []string
		wantWithheld []coverage.Object
	}{
		{
			name: "a sequence declared without if_not_exists",
			desired: func() *goschema.Database {
				return &goschema.Database{Sequences: []goschema.Sequence{{Name: "s1", Schema: "public"}}}
			},
			notDescribed: coverage.Set{}.WithKind(coverage.Sequence),
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.SequencesAdded },
			wantWithheld: []coverage.Object{{Kind: coverage.Sequence, Name: "public.s1"}},
		},
		{
			name: "an extension declared without if_not_exists",
			desired: func() *goschema.Database {
				return &goschema.Database{Extensions: []goschema.Extension{{Name: "citext"}}}
			},
			notDescribed: coverage.Set{}.WithKind(coverage.Extension),
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.ExtensionsAdded },
			wantWithheld: []coverage.Object{{Kind: coverage.Extension, Name: "citext"}},
		},
		{
			name: "a role, which has no conditional creation at all",
			desired: func() *goschema.Database {
				return &goschema.Database{Roles: []goschema.Role{{Name: "admin_user", Login: true}}}
			},
			notDescribed: coverage.Set{}.WithObject(coverage.Role, "admin_user"),
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.RolesAdded },
			wantWithheld: []coverage.Object{{Kind: coverage.Role, Name: "admin_user"}},
		},
		{
			name: "a table in a schema the read never opened",
			desired: func() *goschema.Database {
				return &goschema.Database{
					Tables: []goschema.Table{{Name: "b", Schema: "extra", StructName: "B"}},
				}
			},
			notDescribed: coverage.Set{}.WithObject(coverage.Schema, "extra"),
			read:         func(diff *difftypes.SchemaDiff) []string { return diff.TablesAdded },
			wantWithheld: []coverage.Object{{Kind: coverage.Schema, Name: "extra.b"}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &types.DBSchema{}
			database.NotDescribed = test.notDescribed

			diff, undecided := schemadiff.CompareReportingUndecidedAdditions(test.desired(), database, nil)

			c.Assert(test.read(diff), qt.HasLen, 0)
			c.Assert(undecided, qt.DeepEquals, test.wantWithheld)
		})
	}
}

// TestAnUndeclaredReadPlansEveryAdditionAndWithholdsNothing is the control for
// both tables above, run on the same schemas with the record removed. Without
// it a comparator that planned nothing at all, or one that named every addition
// as withheld, would pass the rows above.
func TestAnUndeclaredReadPlansEveryAdditionAndWithholdsNothing(t *testing.T) {

	tests := []struct {
		name        string
		desired     func() *goschema.Database
		read        func(*difftypes.SchemaDiff) []string
		wantPlanned []string
	}{
		{
			name: "a sequence declared without if_not_exists",
			desired: func() *goschema.Database {
				return &goschema.Database{Sequences: []goschema.Sequence{{Name: "s1", Schema: "public"}}}
			},
			read:        func(diff *difftypes.SchemaDiff) []string { return diff.SequencesAdded },
			wantPlanned: []string{"public.s1"},
		},
		{
			name: "an extension declared without if_not_exists",
			desired: func() *goschema.Database {
				return &goschema.Database{Extensions: []goschema.Extension{{Name: "citext"}}}
			},
			read:        func(diff *difftypes.SchemaDiff) []string { return diff.ExtensionsAdded },
			wantPlanned: []string{"citext"},
		},
		{
			name: "a role",
			desired: func() *goschema.Database {
				return &goschema.Database{Roles: []goschema.Role{{Name: "admin_user", Login: true}}}
			},
			read:        func(diff *difftypes.SchemaDiff) []string { return diff.RolesAdded },
			wantPlanned: []string{"admin_user"},
		},
		{
			name: "a table in another schema",
			desired: func() *goschema.Database {
				return &goschema.Database{
					Tables: []goschema.Table{{Name: "b", Schema: "extra", StructName: "B"}},
				}
			},
			read:        func(diff *difftypes.SchemaDiff) []string { return diff.TablesAdded },
			wantPlanned: []string{"extra.b"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, undecided := schemadiff.CompareReportingUndecidedAdditions(test.desired(), &types.DBSchema{}, nil)

			c.Assert(test.read(diff), qt.DeepEquals, test.wantPlanned)
			c.Assert(undecided, qt.HasLen, 0)
		})
	}
}

// TestWithheldAdditionsAreNotChanges pins the contract the surfaces depend on.
// There is no statement to run for a withheld addition, so counting it as a
// change would make `migrate diff` write a migration file holding nothing --
// worse than the silence it replaces. That is also why it rides beside the diff
// rather than in it: every slice field of SchemaDiff is a category the planner
// renders SQL for (stokaro/ptah#1284). The truth is told on the diagnostics
// stream instead.
func TestWithheldAdditionsAreNotChanges(t *testing.T) {
	c := qt.New(t)

	desired := &goschema.Database{Sequences: []goschema.Sequence{{Name: "s1", Schema: "public"}}}
	database := &types.DBSchema{}
	database.NotDescribed = coverage.Set{}.WithKind(coverage.Sequence)

	diff, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, database, nil)

	c.Assert(undecided, qt.HasLen, 1)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// TestWithheldAdditionsAreOrdered keeps the diagnostic diffable. The withheld
// entries arrive from several comparators and each one builds its planned list
// by ranging over a map, so without an explicit order two runs over the same
// two inputs can print the same warnings in different orders.
func TestWithheldAdditionsAreOrdered(t *testing.T) {
	c := qt.New(t)

	desired := &goschema.Database{
		Extensions: []goschema.Extension{{Name: "citext"}, {Name: "btree_gist"}},
		Sequences: []goschema.Sequence{
			{Name: "b_seq", Schema: "public"},
			{Name: "a_seq", Schema: "public"},
		},
	}
	database := &types.DBSchema{}
	database.NotDescribed = coverage.Set{}.WithKind(coverage.Extension, coverage.Sequence)

	_, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, database, nil)

	c.Assert(undecided, qt.DeepEquals, []coverage.Object{
		{Kind: coverage.Extension, Name: "btree_gist"},
		{Kind: coverage.Extension, Name: "citext"},
		{Kind: coverage.Sequence, Name: "public.a_seq"},
		{Kind: coverage.Sequence, Name: "public.b_seq"},
	})
}

// TestAGuardIsNotAnExcuseToIgnoreARemovalRecord keeps the two halves apart. The
// guard only answers "is this creation safe when the object may already be
// there"; it says nothing about the desired state's own limits, and a removal
// the desired state never claimed to describe must stay suppressed.
func TestAGuardIsNotAnExcuseToIgnoreARemovalRecord(t *testing.T) {
	c := qt.New(t)

	desired := &goschema.Database{}
	desired.NotDescribed = coverage.Set{}.WithKind(coverage.Extension)
	database := &types.DBSchema{Extensions: []types.DBExtension{{Name: "pgcrypto", Schema: "public"}}}

	diff, undecided := schemadiff.CompareReportingUndecidedAdditions(desired, database, nil)

	c.Assert(diff.ExtensionsRemoved, qt.HasLen, 0)
	c.Assert(undecided, qt.HasLen, 0)
}
