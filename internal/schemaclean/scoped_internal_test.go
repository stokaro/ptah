package schemaclean

// White-box testing required: a scoped plan reaches its savepoint, its
// writer's dependents and a whole server's listing only through a
// *dbschema.DatabaseConnection, which only the built-in connector registry
// builds, and a test must not register a dialect it invents in the process
// registry. This file drives the connection-free seams the connection path
// calls, with the descriptor and the writer of an invented engine.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/dbreset"
)

// acmeDialect is an engine nobody ships whose cleanup is schema-scoped and
// whose savepoint is spelled as SQL Server spells one, with no release.
func acmeDialect() descriptor.Dialect {
	return descriptor.Dialect{
		Name:        "acme",
		Family:      "acme",
		Placeholder: descriptor.PlaceholderQuestion,
		Identifiers: descriptor.Identifiers{IndexNamespace: "table", IndexNames: "exact", TableNames: "exact", ColumnNames: "exact"},
		Cleanup:     descriptor.Cleanup{SchemaScoped: true, Restrict: true},
		Savepoint:   descriptor.Savepoint{Create: "SAVE TRANSACTION", Rollback: "ROLLBACK TRANSACTION"},
	}
}

// transactionalDDL is a server that rolls schema changes back as a unit.
var transactionalDDL = capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: true})

// TestScopedExecution_BeforeRegistration is the control: a registry that
// does not hold the engine answers the zero descriptor, which runs no scoped
// plan and names no savepoint.
func TestScopedExecution_BeforeRegistration(t *testing.T) {
	c := qt.New(t)
	registry, err := descriptor.NewRegistry()
	c.Assert(err, qt.IsNil)
	described, found := registry.Lookup("acme")

	run, scoped := scopedExecution(described, transactionalDDL)

	c.Assert(found, qt.IsFalse)
	c.Assert(scoped, qt.IsFalse)
	c.Assert(run, qt.Equals, scopedRun{})
}

// TestScopedExecution_AfterRegistration registers the engine and runs a
// scoped plan whose first drop is refused once on a server that rolls schema
// changes back as a unit: each drop is bracketed in the engine's own
// savepoint spelling, and no release is sent.
func TestScopedExecution_AfterRegistration(t *testing.T) {
	c := qt.New(t)
	registry, err := descriptor.NewRegistry(acmeDialect())
	c.Assert(err, qt.IsNil)
	described, found := registry.Lookup("acme")
	c.Assert(found, qt.IsTrue)
	tx := &cleanupRetryTransaction{failQuery: `DROP VIEW "a_base"`, remainingFailures: 1}

	run, scoped := scopedExecution(described, transactionalDDL)
	c.Assert(run.held, qt.IsTrue)
	err = applyScopedPlanChanges(t.Context(), tx, run.savepoint, []Change{
		{Type: ObjectTypeView, Name: "a_base", Cmd: `DROP VIEW "a_base"`},
		{Type: ObjectTypeView, Name: "z_child", Cmd: `DROP VIEW "z_child"`},
	})

	c.Assert(scoped, qt.IsTrue)
	c.Assert(err, qt.IsNil)
	c.Assert(tx.queries, qt.DeepEquals, []string{
		"SAVE TRANSACTION ptah_scoped_cleanup_object",
		`DROP VIEW "a_base"`,
		"ROLLBACK TRANSACTION ptah_scoped_cleanup_object",
		"SAVE TRANSACTION ptah_scoped_cleanup_object",
		`DROP VIEW "z_child"`,
		"SAVE TRANSACTION ptah_scoped_cleanup_object",
		`DROP VIEW "a_base"`,
	})
}

// TestScopedExecution_WithoutTransactionalDDL runs the registered engine's
// scoped plan on a server that commits each schema change as it runs: the
// plan stays scoped, and its drops are not held in a transaction and name no
// savepoint. A set that does not answer the key is read the same way.
func TestScopedExecution_WithoutTransactionalDDL(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
	}{
		{name: "the server commits each schema change", caps: capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false})},
		{name: "the set does not answer the key", caps: capability.Capabilities{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := descriptor.NewRegistry(acmeDialect())
			c.Assert(err, qt.IsNil)
			described, found := registry.Lookup("acme")
			c.Assert(found, qt.IsTrue)

			run, scoped := scopedExecution(described, test.caps)

			c.Assert(scoped, qt.IsTrue)
			c.Assert(run, qt.Equals, scopedRun{})
		})
	}
}

// TestApplyUnheldScopedPlanChanges_HappyPath drops a view that another
// selected view reads, on a server that commits each drop: the refused drop
// is sent again once the view that reads it is gone, and no savepoint is
// sent.
func TestApplyUnheldScopedPlanChanges_HappyPath(t *testing.T) {
	c := qt.New(t)
	executor := &cleanupRetryTransaction{failQuery: `DROP VIEW "a_base"`, remainingFailures: 1}

	err := applyUnheldScopedPlanChanges(t.Context(), executor, []Change{
		{Type: ObjectTypeView, Name: "a_base", Cmd: `DROP VIEW "a_base"`},
		{Type: ObjectTypeView, Name: "z_child", Cmd: `DROP VIEW "z_child"`},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(executor.queries, qt.DeepEquals, []string{
		`DROP VIEW "a_base"`,
		`DROP VIEW "z_child"`,
		`DROP VIEW "a_base"`,
	})
}

// TestApplyUnheldScopedPlanChanges_FailurePath ends a plan whose drop the
// server keeps refusing. The drops the server accepted before the refusal
// stay, so the error names them in the order they ran, or says there are
// none.
func TestApplyUnheldScopedPlanChanges_FailurePath(t *testing.T) {
	const refused = `drop schema object view "a_base": selected dependent still exists\n` +
		`the server commits each drop as it runs; dropped before the failure: `
	tests := []struct {
		name        string
		changes     []Change
		wantErr     string
		wantQueries []string
	}{
		{
			name:        "the first drop is refused",
			changes:     []Change{{Type: ObjectTypeView, Name: "a_base", Cmd: `DROP VIEW "a_base"`}},
			wantErr:     refused + `nothing`,
			wantQueries: []string{`DROP VIEW "a_base"`},
		},
		{
			name: "drops ran before the refusal",
			changes: []Change{
				{Type: ObjectTypeView, Name: "a_base", Cmd: `DROP VIEW "a_base"`},
				{Type: ObjectTypeView, Name: "z_child", Cmd: `DROP VIEW "z_child"`},
				{Type: ObjectTypeTable, Name: "t", Cmd: `DROP TABLE "t"`},
			},
			wantErr:     refused + `view "z_child", table "t"`,
			wantQueries: []string{`DROP VIEW "a_base"`, `DROP VIEW "z_child"`, `DROP TABLE "t"`, `DROP VIEW "a_base"`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			executor := &cleanupRetryTransaction{failQuery: `DROP VIEW "a_base"`, remainingFailures: 9}

			err := applyUnheldScopedPlanChanges(t.Context(), executor, test.changes)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(executor.queries, qt.DeepEquals, test.wantQueries)
		})
	}
}

// acmeDependentsWriter is the engine's writer, which lists the dependents of
// the names it is asked about from its catalog. The embedded writer is never
// called.
type acmeDependentsWriter struct {
	catalog.SchemaWriter
	dependents []dbreset.Dependent
	err        error
	asked      *[][]string
}

func (w acmeDependentsWriter) CleanupDependents(_ context.Context, _ dbreset.Querier, names []string) ([]dbreset.Dependent, error) {
	*w.asked = append(*w.asked, names)
	return w.dependents, w.err
}

// TestRefuseDependents_FailurePath refuses a plan whose selection leaves
// dependents behind, naming each in order and leaving out the one the
// selection drops too. The writer is asked about each selected name once.
func TestRefuseDependents_FailurePath(t *testing.T) {
	c := qt.New(t)
	var asked [][]string
	writer := acmeDependentsWriter{asked: &asked, dependents: []dbreset.Dependent{
		{Source: "users", Name: "user_totals", Kind: "materialized view"},
		{Source: "users", Name: "active_users", Kind: "view"},
		{Source: "users", Name: "posts", Kind: "table"},
	}}

	err := refuseDependents(t.Context(), writer, nil, []Change{
		{Type: ObjectTypeTable, Name: "users"},
		{Type: ObjectTypeForeignKey, Name: ""},
		{Type: ObjectTypeTable, Name: "posts"},
		{Type: ObjectTypeTable, Name: "users"},
	})

	c.Assert(err, qt.ErrorMatches, `scoped cleanup would leave dependents behind: `+
		`materialized view "user_totals" depends on users; view "active_users" depends on users\. `+
		`Add them to the selection so they are dropped together, or widen it to the whole schema`)
	c.Assert(asked, qt.DeepEquals, [][]string{{"users", "posts"}})
}

// TestRefuseDependents_HappyPath leaves the plan to the server where the
// writer lists no dependents, cannot read its catalog, or is not asked.
func TestRefuseDependents_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dependents []dbreset.Dependent
		err        error
		changes    []Change
		wantAsked  [][]string
	}{
		{
			name:       "every dependent is selected",
			dependents: []dbreset.Dependent{{Source: "users", Name: "active_users", Kind: "view"}},
			changes:    []Change{{Type: ObjectTypeTable, Name: "users"}, {Type: ObjectTypeView, Name: "active_users"}},
			wantAsked:  [][]string{{"users", "active_users"}},
		},
		{
			name:      "the catalog cannot be read",
			err:       errors.New("relation pg_rewrite does not exist"),
			changes:   []Change{{Type: ObjectTypeTable, Name: "users"}},
			wantAsked: [][]string{{"users"}},
		},
		{
			name:    "no change names an object",
			changes: []Change{{Type: ObjectTypeTable, Name: " "}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked [][]string
			writer := acmeDependentsWriter{asked: &asked, dependents: test.dependents, err: test.err}

			err := refuseDependents(t.Context(), writer, nil, test.changes)

			c.Assert(err, qt.IsNil)
			c.Assert(asked, qt.DeepEquals, test.wantAsked)
		})
	}
}

// TestRefuseDependents_WriterWithoutDependents leaves the plan to the server
// for a writer that lists no dependents at all.
func TestRefuseDependents_WriterWithoutDependents(t *testing.T) {
	c := qt.New(t)

	err := refuseDependents(t.Context(), struct{ catalog.SchemaWriter }{}, nil, []Change{{Type: ObjectTypeTable, Name: "users"}})

	c.Assert(err, qt.IsNil)
}

// acmeServerCatalog is the catalog strategy of an engine nobody ships that
// also lists what a cleanup of a whole server of it drops.
type acmeServerCatalog struct {
	acmeCatalog
	server []dbreset.CleanupObject
	err    error
}

func (a acmeServerCatalog) ServerObjects(context.Context, dbreset.Querier) ([]dbreset.CleanupObject, error) {
	return a.server, a.err
}

func TestServerObjects_HappyPath(t *testing.T) {
	c := qt.New(t)
	strategy := acmeServerCatalog{server: []dbreset.CleanupObject{
		{Kind: ObjectTypeForeignKey, Schema: "r4", Table: "z", Name: "zfk"},
		{Kind: ObjectTypeSchema, Schema: "r1", Name: "r1"},
	}}

	objects, err := serverObjects(t.Context(), strategy, nil, "acme")

	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.DeepEquals, []Object{
		{Type: ObjectTypeForeignKey, Schema: "r4", Table: "z", Name: "zfk"},
		{Type: ObjectTypeSchema, Schema: "r1", Name: "r1"},
	})
}

func TestServerObjects_FailurePath(t *testing.T) {
	const unlisted = `inspect cleanup databases: the acme writer does not list the databases of a whole server`
	refused := errors.New("access denied")
	tests := []struct {
		name     string
		strategy dbreset.CleanupCatalog
		wantErr  string
	}{
		{name: "a writer with no catalog", strategy: nil, wantErr: unlisted},
		{name: "a catalog that lists no server", strategy: acmeCatalog{}, wantErr: unlisted},
		{
			name:     "a kind the plan cannot name",
			strategy: acmeServerCatalog{server: []dbreset.CleanupObject{{Kind: "tablespace", Name: "ts"}}},
			wantErr:  `inspect schema before cleanup: the acme writer lists tablespace "ts", which the plan cannot name`,
		},
		{name: "a catalog that cannot be read", strategy: acmeServerCatalog{err: refused}, wantErr: `access denied`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			objects, err := serverObjects(t.Context(), test.strategy, nil, "acme")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(objects, qt.IsNil)
		})
	}
}
