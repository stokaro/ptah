package tsrender_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/timescaledb/tsast"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsrender"
	"ptah.run/dialect/timescaledb/tsschema"
)

// TestRegistry_HypertableWritesTheCallTheExtensionPublishes pins the shape of
// the statement, which is a function call rather than DDL because TimescaleDB
// has no CREATE HYPERTABLE grammar.
//
// Every argument below is measured on TimescaleDB 2.29.2 / PostgreSQL 17:
//
//	create_hypertable('conditions', by_range('time'))                  -> (1,t)
//	the same call again                                                -> ERROR: already a hypertable
//	… if_not_exists => TRUE                                            -> (1,f), NOTICE
//	by_range('time', INTERVAL '1 day')                                 -> the catalog reports `1 day`
//	… create_default_indexes => FALSE                                  -> no index is created
func TestRegistry_HypertableWritesTheCallTheExtensionPublishes(t *testing.T) {
	tests := []struct {
		name       string
		table      string
		hypertable tsschema.DesiredHypertable
		want       string
	}{
		{
			name: "the default interval", table: "public.readings", hypertable: tsschema.DesiredHypertable{Column: "time"},
			want: `SELECT create_hypertable('"public"."readings"', by_range('time'), create_default_indexes => FALSE);`,
		},
		{
			name: "a declared interval", table: "readings", hypertable: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"},
			want: `SELECT create_hypertable('"readings"', by_range('time', INTERVAL '1 day'), create_default_indexes => FALSE);`,
		},
		{
			name: "guarded", table: "readings", hypertable: tsschema.DesiredHypertable{Column: "time", IfNotExists: true},
			want: `SELECT create_hypertable('"readings"', by_range('time'), if_not_exists => TRUE, create_default_indexes => FALSE);`,
		},
		{
			// The server parses the literal as a name, and an unquoted name
			// folds to lower case: 'Readings' would look for readings, which
			// the quoted CREATE TABLE did not create.
			name: "a mixed-case name", table: "Readings", hypertable: tsschema.DesiredHypertable{Column: "Time"},
			want: `SELECT create_hypertable('"Readings"', by_range('Time'), create_default_indexes => FALSE);`,
		},
		{
			name: "a name already quoted, carrying a double quote", table: `app."odd""name"`, hypertable: tsschema.DesiredHypertable{Column: "ts"},
			want: `SELECT create_hypertable('"app"."odd""name"', by_range('ts'), create_default_indexes => FALSE);`,
		},
		{
			// A single quote is the literal's to escape: the quoted name sits
			// inside a string.
			name: "a name carrying a quote", table: "odd'name", hypertable: tsschema.DesiredHypertable{Column: "ts"},
			want: `SELECT create_hypertable('"odd''name"', by_range('ts'), create_default_indexes => FALSE);`,
		},
		{
			name: "a comment", table: "readings", hypertable: tsschema.DesiredHypertable{Column: "time", Comment: "time series"},
			want: "-- time series\nSELECT create_hypertable('\"readings\"', by_range('time'), create_default_indexes => FALSE);",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := render(&tsast.CreateHypertable{Table: test.table, Hypertable: test.hypertable})

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Equals, test.want)
		})
	}
}

// TestRegistry_AggregateWritesWhatTheExtensionOwns pins the statements of each
// transition.
//
// Every part below is measured on TimescaleDB 2.29.2 / PostgreSQL 17.11:
//
//	CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous) AS … WITH NO DATA
//	                                                       -> CREATE MATERIALIZED VIEW
//	the same without WITH NO DATA, inside a transaction
//	     -> ERROR: CREATE MATERIALIZED VIEW ... WITH DATA cannot run inside a transaction block
//	timescaledb.materialized_only = true                   -> the catalog reports materialized_only t
//	DROP VIEW of an aggregate                              -> ERROR: cannot drop continuous aggregate using DROP VIEW
//	CREATE OR REPLACE MATERIALIZED VIEW                    -> syntax error at or near "MATERIALIZED"
func TestRegistry_AggregateWritesWhatTheExtensionOwns(t *testing.T) {
	stored := &tsschema.ObservedContinuousAggregate{Definition: "SELECT 0;", MaterializedOnly: new(true)}
	tests := []struct {
		name   string
		schema string
		change tsdiff.ContinuousAggregate
		want   string
	}{
		{
			name:   "the default",
			change: tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}},
			want:   "CREATE MATERIALIZED VIEW \"hourly\" WITH (timescaledb.continuous) AS\nSELECT 1\nWITH NO DATA\n;",
		},
		{
			// The body a description read from a server carries: the catalog's
			// `view_definition` ends in a semicolon, and writing it in would put
			// the terminator BEFORE `WITH NO DATA`. The server answers
			// `syntax error at or near "WITH"`.
			name:   "a body carrying the catalog's terminator",
			change: tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1;"}},
			want:   "CREATE MATERIALIZED VIEW \"hourly\" WITH (timescaledb.continuous) AS\nSELECT 1\nWITH NO DATA\n;",
		},
		{
			name:   "in a schema, materialized only, with a comment",
			schema: "metrics",
			change: tsdiff.ContinuousAggregate{After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1", MaterializedOnly: new(true), Comment: "hourly"}},
			want: "-- hourly\nCREATE MATERIALIZED VIEW \"metrics\".\"hourly\" " +
				"WITH (timescaledb.continuous, timescaledb.materialized_only = true) AS\nSELECT 1\nWITH NO DATA\n;",
		},
		{
			name:   "a removal takes the verb the server names",
			change: tsdiff.ContinuousAggregate{Before: stored},
			want:   "DROP MATERIALIZED VIEW IF EXISTS \"hourly\";",
		},
		{
			name:   "a replacement drops before it creates",
			change: tsdiff.ContinuousAggregate{Before: stored, After: &tsschema.DesiredContinuousAggregate{Body: "SELECT 1"}},
			want:   "DROP MATERIALIZED VIEW IF EXISTS \"hourly\";\nCREATE MATERIALIZED VIEW \"hourly\" WITH (timescaledb.continuous) AS\nSELECT 1\nWITH NO DATA\n;",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := render(&tsast.ContinuousAggregate{Schema: test.schema, Name: "hourly", Change: test.change})

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Equals, test.want)
		})
	}
}

// TestRegistry_RefusesOutsideThePostgresFamilyAndInvalidPayloads holds the two
// refusals the handlers own: the target family, and the payload's shape.
func TestRegistry_RefusesOutsideThePostgresFamilyAndInvalidPayloads(t *testing.T) {
	tests := []struct {
		name    string
		target  string
		payload ast.ExtensionPayload
		want    error
	}{
		{name: "another target", target: "mysql", payload: &tsast.CreateHypertable{Table: "t", Hypertable: tsschema.DesiredHypertable{Column: "ts"}}, want: ptaherr.ErrUnsupportedFeature},
		{name: "a hypertable without a column", target: "postgres", payload: &tsast.CreateHypertable{Table: "t"}, want: ptaherr.ErrInvalidSchemaDiff},
		{name: "an aggregate without operands", target: "postgres", payload: &tsast.ContinuousAggregate{Name: "hourly"}, want: ptaherr.ErrInvalidSchemaDiff},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			registry := must.Must(tsrender.Registry())

			statements, err := registry.Render(renderer.ExtensionContext{Target: test.target, Capabilities: timescaleCapabilities()}, ast.StatementExtension, test.payload)

			c.Assert(err, qt.ErrorIs, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// otherSetting stands for another owner's table setting, which the lowering
// leaves for the CREATE TABLE.
type otherSetting struct{ Value string }

func (*otherSetting) Kind() schemaext.Kind { return "example.org/other-setting" }
func (v *otherSetting) Clone() schemaext.Value {
	return &otherSetting{Value: v.Value}
}
func (v *otherSetting) Equal(other schemaext.Value) bool {
	typed, ok := other.(*otherSetting)
	return ok && *typed == *v
}

// TestLowerTableFacets_FollowsCreateTable pins the lowering a PostgreSQL-family
// CREATE TABLE asks for: the hypertable call with the table's name, and the
// facets the statement itself still carries -- every one but the hypertable.
func TestLowerTableFacets_FollowsCreateTable(t *testing.T) {
	hypertable := &tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 day"}
	call := &tsast.CreateHypertable{Table: "public.readings", Hypertable: *hypertable}
	tests := []struct {
		name     string
		facets   schemaext.Facets
		want     []ast.ExtensionPayload
		wantRest schemaext.Facets
	}{
		{name: "no settings"},
		{name: "a hypertable", facets: must.Must(schemaext.NewFacets(hypertable)), want: []ast.ExtensionPayload{call}},
		{
			name:     "another owner's setting beside it",
			facets:   must.Must(schemaext.NewFacets(hypertable, &otherSetting{Value: "kept"})),
			want:     []ast.ExtensionPayload{call},
			wantRest: must.Must(schemaext.NewFacets(&otherSetting{Value: "kept"})),
		},
		{
			name:     "another owner's setting alone",
			facets:   must.Must(schemaext.NewFacets(&otherSetting{Value: "kept"})),
			wantRest: must.Must(schemaext.NewFacets(&otherSetting{Value: "kept"})),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			payloads, rest, err := tsrender.LowerTableFacets("public.readings", test.facets)

			c.Assert(err, qt.IsNil)
			c.Assert(payloads, qt.DeepEquals, test.want)
			c.Assert(rest.Equal(test.wantRest), qt.IsTrue)
		})
	}
}

func render(payload ast.ExtensionPayload) ([]string, error) {
	registry, err := tsrender.Registry()
	if err != nil {
		return nil, err
	}
	return registry.Render(renderer.ExtensionContext{Target: "postgres", Capabilities: timescaleCapabilities()}, ast.StatementExtension, payload)
}

func timescaleCapabilities() capability.Capabilities {
	return capability.Postgres17().With(capability.Hypertables, true).With(capability.ContinuousAggregates, true)
}
