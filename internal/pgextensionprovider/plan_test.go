package pgextensionprovider_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgextension"
)

// planned is one step as a test reads it: its payload, notes and phase.
type planned struct {
	Payload ast.ExtensionPayload
	Notes   []string
	Phase   featureplan.Phase
	Effect  plangraph.Action
}

func planning(target string, changes ...schemaext.ChangeRecord) featureplan.Request {
	return featureplan.Request{Target: target, Identifiers: identifier.ForDialect(target), Capabilities: capability.Postgres17(), Changes: changes}
}

func change(name string, value *pgextension.Change) schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: pgextension.Ref(name), Value: value}
}

// steps lists the planned steps in the order the owner's dependencies put
// them, and checks every effect names the extension under the common kind.
func steps(c *qt.C, result featureplan.Result, name string) []planned {
	c.Helper()
	c.Assert(result.Contributions, qt.HasLen, 1)
	plan := must.Must(plangraph.Schedule(c.Context(), result.Contributions[0]))
	var listed []planned
	for _, step := range plan.Steps {
		c.Assert(step.Effects, qt.HasLen, 1)
		c.Assert(step.Effects[0].Subject, qt.DeepEquals, pgextension.Subject(pgextension.Ref(name)))
		listed = append(listed, planned{Payload: step.Payload.Payload, Notes: step.Payload.Notes, Phase: step.Payload.Phase, Effect: step.Effects[0].Action})
	}
	return listed
}

// installed is pg_trgm 1.5 in public, which may move and holds no comment.
func installedTrgm() *pgextension.ObservedExtension {
	return &pgextension.ObservedExtension{Schema: "public", Version: "1.5", Relocatable: true}
}

// TestPlanExtensions_HappyPath pins the statements of each change, all of the
// prerequisite phase: an installation and its comment, a drop with the
// warnings the plan has always carried, and the version, the schema and the
// comment of a surviving extension in that order.
func TestPlanExtensions_HappyPath(t *testing.T) {
	prerequisite := featureplan.PhasePrerequisite
	tests := []struct {
		name   string
		target string
		change *pgextension.Change
		want   []planned
	}{
		{
			name: "an installation with a comment", target: "postgres",
			change: &pgextension.Change{After: &pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true}},
			want: []planned{
				{Payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, Schema: "ext", Version: "1.6", IfNotExists: true}, Phase: prerequisite, Effect: plangraph.Create},
				{Payload: &pgextension.CommentOperation{Name: "pg_trgm", Comment: "trigrams"}, Phase: prerequisite, Effect: plangraph.Alter},
			},
		},
		{
			name: "an installation on CockroachDB", target: "cockroachdb",
			change: &pgextension.Change{After: &pgextension.DesiredExtension{IfNotExists: true}},
			want: []planned{
				{Payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, IfNotExists: true}, Phase: prerequisite, Effect: plangraph.Create},
			},
		},
		{
			name: "a drop", target: "postgres", change: &pgextension.Change{Before: installedTrgm()},
			want: []planned{{Payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Drop}, Phase: prerequisite, Effect: plangraph.Drop, Notes: []string{
				"WARNING: Removing extension 'pg_trgm' may break existing functionality that depends on it",
				"Consider reviewing all database objects that use this extension before proceeding",
				"Extension removal may cascade to dependent objects - review carefully",
				"Remove extension 'pg_trgm' as it's no longer required by the schema",
			}}},
		},
		{
			name: "a version, a move and a comment", target: "yugabytedb",
			change: &pgextension.Change{Before: installedTrgm(), After: &pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams"},
				Schema: "ext", Comment: true},
			want: []planned{
				{Payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.UpdateVersion, Version: "1.6"}, Phase: prerequisite, Effect: plangraph.Alter,
					Notes: []string{"Update extension pg_trgm from 1.5 to 1.6"}},
				{Payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.SetSchema, Schema: "ext"}, Phase: prerequisite, Effect: plangraph.Alter,
					Notes: []string{"Move extension pg_trgm from schema public to ext"}},
				{Payload: &pgextension.CommentOperation{Name: "pg_trgm", Comment: "trigrams"}, Phase: prerequisite, Effect: plangraph.Alter},
			},
		},
		{
			name: "a comment removed", target: "postgres",
			change: &pgextension.Change{Before: &pgextension.ObservedExtension{Schema: "public", Version: "1.5", Comment: new("old")},
				After: &pgextension.DesiredExtension{}, Comment: true},
			want: []planned{{Payload: &pgextension.CommentOperation{Name: "pg_trgm"}, Phase: prerequisite, Effect: plangraph.Alter}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(test.target, change("pg_trgm", test.change))

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Err(request), qt.IsNil)
			c.Assert(steps(c, result, "pg_trgm"), qt.DeepEquals, test.want)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Steps, qt.HasLen, len(test.want))
		})
	}
}

// TestPlanExtensions_RefusesWhatTheServerRefuses pins each refusal and the
// error identity a caller tells it apart by.
func TestPlanExtensions_RefusesWhatTheServerRefuses(t *testing.T) {
	fixed := installedTrgm()
	fixed.Relocatable = false
	tests := []struct {
		name   string
		target string
		change *pgextension.Change
		is     error
		want   string
	}{
		{name: "a lower version", target: "postgres", is: ptaherr.ErrInvalidSchemaDiff,
			change: &pgextension.Change{Before: &pgextension.ObservedExtension{Schema: "public", Version: "1.6"}, After: &pgextension.DesiredExtension{Version: "1.5"}},
			want:   `(?s).*one direction only.*has no update path.*`},
		{name: "a move of a fixed extension", target: "postgres", is: ptaherr.ErrInvalidSchemaDiff,
			change: &pgextension.Change{Before: fixed, After: &pgextension.DesiredExtension{Schema: "ext"}, Schema: "ext"},
			want:   `(?s).*not relocatable.*does not support SET SCHEMA.*`},
		{name: "an installation schema on Spanner", target: "spanner", is: ptaherr.ErrUnsupportedFeature,
			change: &pgextension.Change{After: &pgextension.DesiredExtension{Schema: "ext"}},
			want:   `(?s).*spanner does not support PostgreSQL extension installation schema "ext" for extension "pg_trgm".*`},
		{name: "an update on CockroachDB", target: "cockroachdb", is: ptaherr.ErrUnsupportedFeature,
			change: &pgextension.Change{Before: installedTrgm(), After: &pgextension.DesiredExtension{Version: "1.6"}},
			want:   `(?s).*cockroachdb cannot alter PostgreSQL extension "pg_trgm"; this target has no ALTER EXTENSION.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := planning(test.target, change("pg_trgm", test.change))

			result, err := newRuntime(c).PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Err(request), qt.ErrorIs, test.is)
			c.Assert(result.Err(request), qt.ErrorMatches, test.want)
		})
	}
}

// TestPlanExtensions_DeclaresForARender pins a whole-schema render's
// installation: the statement and its comment, in the prerequisite phase.
func TestPlanExtensions_DeclaresForARender(t *testing.T) {
	c := qt.New(t)
	request := featureplan.DeclarationRequest{Target: "postgres", Identifiers: identifier.ForDialect("postgres"), Capabilities: capability.Postgres17(),
		Objects: []schemaext.Object{declared("pg_trgm", pgextension.DesiredExtension{Schema: "ext", Comment: "trigrams"})}}

	result, err := newRuntime(c).PlanDeclarations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.ValidateOutcome(request), qt.IsNil)
	c.Assert(result.Declarations, qt.HasLen, 1)
	c.Assert(steps(c, featureplan.Result{Contributions: result.Contributions}, "pg_trgm"), qt.DeepEquals, []planned{
		{Payload: &pgextension.Operation{Name: "pg_trgm", Action: pgextension.Create, Schema: "ext"}, Phase: featureplan.PhasePrerequisite, Effect: plangraph.Create},
		{Payload: &pgextension.CommentOperation{Name: "pg_trgm", Comment: "trigrams"}, Phase: featureplan.PhasePrerequisite, Effect: plangraph.Alter},
	})
}
