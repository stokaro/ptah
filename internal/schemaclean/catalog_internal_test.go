package schemaclean

// White-box testing required: a cleanup reaches a writer's catalog strategy
// only through a *dbschema.DatabaseConnection, which only the built-in
// connector registry builds, so no black-box test can hand it the writer of an
// engine Ptah does not ship. This file drives the connection-free seams the
// connection path calls, with the writer of an invented engine.

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/dbreset"
)

// acmeCatalog is the catalog strategy of an engine nobody ships: it answers
// the objects and view depths it was given, per schema.
type acmeCatalog struct {
	objects map[string][]dbreset.CleanupObject
	depths  []dbreset.ViewDepth
	err     error
}

func (a acmeCatalog) Objects(_ context.Context, _ dbreset.Querier, schema string) ([]dbreset.CleanupObject, error) {
	return a.objects[schema], a.err
}

func (a acmeCatalog) ViewDepths(context.Context, dbreset.Querier, string) ([]dbreset.ViewDepth, error) {
	return a.depths, a.err
}

func (acmeCatalog) LockRelations(context.Context, dbreset.Executor, []dbreset.Relation) error {
	return nil
}

// acmeWriter is that engine's writer, which offers its strategy. The embedded
// writer is never called.
type acmeWriter struct {
	catalog.SchemaWriter
	strategy dbreset.CleanupCatalog
}

func (w acmeWriter) CleanupCatalog() dbreset.CleanupCatalog { return w.strategy }

func TestWriterCatalog(t *testing.T) {
	strategy := &acmeCatalog{}
	tests := []struct {
		name   string
		writer catalog.SchemaWriter
		want   dbreset.CleanupCatalog
	}{
		{name: "a writer that offers a strategy", writer: acmeWriter{strategy: strategy}, want: strategy},
		{name: "a writer that offers none", writer: acmeWriter{}, want: nil},
		{name: "a writer with no catalog", writer: struct{ catalog.SchemaWriter }{}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(writerCatalog(test.writer), qt.Equals, test.want)
		})
	}
}

func TestCatalogObjects_HappyPath(t *testing.T) {
	strategy := acmeCatalog{objects: map[string][]dbreset.CleanupObject{
		"app": {
			{Kind: "sequence", Schema: "app", Table: "users", Name: "users_id_seq", SelectorName: "users_id_seq", Implicit: true,
				Statement: "DROP SEQUENCE app.users_id_seq"},
			{Kind: "event", Name: "nightly"},
		},
		"audit": {{Kind: "procedure", Schema: "audit", Name: "trim(integer)", SelectorName: "trim"}},
	}}
	tests := []struct {
		name     string
		strategy dbreset.CleanupCatalog
		schemas  []string
		want     []Object
	}{
		{
			name:     "every schema of the scope, as plan rows",
			strategy: strategy,
			schemas:  []string{"app", "audit"},
			want: []Object{
				{Type: ObjectTypeSequence, Schema: "app", Table: "users", Name: "users_id_seq", SelectorName: "users_id_seq",
					Implicit: true, Command: "DROP SEQUENCE app.users_id_seq"},
				{Type: ObjectTypeEvent, Name: "nightly"},
				{Type: ObjectTypeProcedure, Schema: "audit", Name: "trim(integer)", SelectorName: "trim"},
			},
		},
		{name: "a schema holding nothing", strategy: strategy, schemas: []string{"empty"}, want: make([]Object, 0)},
		{name: "no strategy", strategy: nil, schemas: []string{"app"}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			objects, err := catalogObjects(c.Context(), test.strategy, nil, "acme", test.schemas)

			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.DeepEquals, test.want)
		})
	}
}

func TestCatalogObjects_FailurePath(t *testing.T) {
	failed := errors.New("catalog unreachable")
	tests := []struct {
		name     string
		strategy dbreset.CleanupCatalog
		wantErr  string
	}{
		{
			name: "a kind the plan has no name for",
			strategy: acmeCatalog{objects: map[string][]dbreset.CleanupObject{
				"app": {{Kind: "sequence", Name: "s"}, {Kind: "widget", Name: "w"}},
			}},
			wantErr: `inspect schema before cleanup: the acme writer lists widget "w", which the plan cannot name`,
		},
		{name: "a failed read", strategy: acmeCatalog{err: failed}, wantErr: "catalog unreachable"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			objects, err := catalogObjects(c.Context(), test.strategy, nil, "acme", []string{"app"})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(objects, qt.IsNil)
		})
	}
}

func TestCatalogViewDepths(t *testing.T) {
	objects := []Object{
		{Type: ObjectTypeView, Name: "v_top"},
		{Type: ObjectTypeMaterializedView, Schema: "app", Name: "mv_mid"},
		{Type: ObjectTypeView, Schema: "app", Name: "v_base"},
		{Type: ObjectTypeView, Schema: "other", Name: "v_top"},
		{Type: ObjectTypeTable, Schema: "app", Name: "v_top"},
	}
	tests := []struct {
		name     string
		strategy dbreset.CleanupCatalog
		want     map[executionObjectIdentity]int
	}{
		{
			name: "the depth of each view of the schema deeper than zero",
			strategy: acmeCatalog{depths: []dbreset.ViewDepth{
				{Kind: "view", Name: "v_top", Depth: 2},
				{Kind: "materialized_view", Name: "mv_mid", Depth: 1},
				{Kind: "view", Name: "v_base", Depth: 0},
			}},
			want: map[executionObjectIdentity]int{
				{objectType: ObjectTypeView, name: "v_top"}:                             2,
				{objectType: ObjectTypeMaterializedView, schema: "app", name: "mv_mid"}: 1,
			},
		},
		{
			name:     "a strategy that reads no view",
			strategy: acmeCatalog{depths: make([]dbreset.ViewDepth, 0)},
			want:     make(map[executionObjectIdentity]int),
		},
		{name: "a strategy that orders nothing", strategy: acmeCatalog{}, want: nil},
		{name: "no strategy", strategy: nil, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			depths, err := catalogViewDepths(c.Context(), test.strategy, nil, "app", objects)

			c.Assert(err, qt.IsNil)
			c.Assert(depths, qt.DeepEquals, test.want)
		})
	}
}
