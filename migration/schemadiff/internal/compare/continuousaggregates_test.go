package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// theBody is the SELECT a declaration writes, and theStored is what the catalog
// answers for exactly that declaration.
//
// The pair is measured on TimescaleDB 2.29.2 / PostgreSQL 17.11, and it is the
// whole reason this comparison needs a server: the interval literal became an
// interval cast, the column reference gained quotes, and the GROUP BY key
// written by its output name came back as the expression that name stood for.
const (
	theBody = "SELECT time_bucket('1 hour', time) AS bucket, sensor, avg(value) AS avg_value " +
		"FROM readings GROUP BY bucket, sensor"
	theStored = "  SELECT time_bucket('01:00:00'::interval, \"time\") AS bucket,\n" +
		"     sensor,\n" +
		"     avg(value) AS avg_value\n" +
		"    FROM readings\n" +
		"   GROUP BY (time_bucket('01:00:00'::interval, \"time\")), sensor;"
)

// TestContinuousAggregates_ComparesByTheQualifiedName pins what makes two
// aggregates the same one, and that all three directions are reported.
func TestContinuousAggregates_ComparesByTheQualifiedName(t *testing.T) {
	tests := []struct {
		name        string
		declared    []goschema.ContinuousAggregate
		live        []types.DBContinuousAggregate
		wantAdded   []string
		wantRemoved []string
	}{
		{
			name:      "declared and absent",
			declared:  []goschema.ContinuousAggregate{{Name: "hourly", Body: theBody}},
			wantAdded: []string{"hourly"},
		},
		{
			name:     "declared and present",
			declared: []goschema.ContinuousAggregate{{Name: "hourly", Schema: "public", Body: theBody}},
			live: []types.DBContinuousAggregate{{
				Schema: "public", Name: "hourly", Definition: theStored,
			}},
		},
		{
			// The shape a real round trip has: `schema inspect` scoped to one
			// schema reports the aggregate unqualified, and the document it
			// writes names `schema.public`. Comparing the two strings reported
			// an addition AND a removal for one unchanged object, and the plan
			// created it before dropping it.
			name:     "a qualified declaration of an unqualified row",
			declared: []goschema.ContinuousAggregate{{Name: "hourly", Schema: "public", Body: theBody}},
			live: []types.DBContinuousAggregate{{
				Name: "hourly", Definition: theStored,
			}},
		},
		{
			name: "live and undeclared",
			live: []types.DBContinuousAggregate{{
				Schema: "public", Name: "hourly", Definition: theStored,
			}},
			wantRemoved: []string{"public.hourly"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			declared := &goschema.Database{ContinuousAggregates: test.declared}
			live := &types.DBSchema{ContinuousAggregates: test.live}

			compare.ContinuousAggregates(declared, live, diff, compare.CoverageOf(declared, live), nil,
				postgresSemantics())

			c.Assert(diff.ContinuousAggregatesAdded, qt.DeepEquals, test.wantAdded)
			c.Assert(diff.ContinuousAggregatesRemoved, qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.ContinuousAggregatesModified, qt.HasLen, 0)
		})
	}
}

// TestContinuousAggregates_TheBodyIsComparedOnlyThroughTheServer is the row
// that decides whether an unchanged schema replans itself forever.
//
// The declared body and the stored body are the SAME aggregate, and they share
// no long substring. Without a resolved spelling the body is not comparable at
// all, and reporting a difference would drop and recreate the aggregate on
// every run -- each drop discarding the materialization it exists to keep.
func TestContinuousAggregates_TheBodyIsComparedOnlyThroughTheServer(t *testing.T) {
	tests := []struct {
		name   string
		bodies map[string]config.ContinuousAggregateBody
		want   []difftypes.ContinuousAggregateDiff
	}{
		{
			name: "no resolver ran",
		},
		{
			name:   "the server refused the declaration",
			bodies: map[string]config.ContinuousAggregateBody{"hourly": {}},
		},
		{
			name: "the server normalized it to what the catalog holds",
			bodies: map[string]config.ContinuousAggregateBody{
				"hourly": {Body: theStored, Resolved: true},
			},
		},
		{
			name: "the server normalized it to something else",
			bodies: map[string]config.ContinuousAggregateBody{
				"hourly": {
					Body:     "  SELECT time_bucket('1 day'::interval, \"time\") AS bucket\n    FROM readings;",
					Resolved: true,
				},
			},
			want: []difftypes.ContinuousAggregateDiff{{
				Name: "hourly", OldBody: theStored, NewBody: theBody,
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			declared := &goschema.Database{ContinuousAggregates: []goschema.ContinuousAggregate{
				{Name: "hourly", Body: theBody},
			}}
			live := &types.DBSchema{ContinuousAggregates: []types.DBContinuousAggregate{
				{Name: "hourly", Definition: theStored},
			}}

			compare.ContinuousAggregates(declared, live, diff, compare.CoverageOf(declared, live), test.bodies,
				postgresSemantics())

			c.Assert(diff.ContinuousAggregatesModified, qt.DeepEquals, test.want)
			c.Assert(diff.ContinuousAggregatesAdded, qt.HasLen, 0)
			c.Assert(diff.ContinuousAggregatesRemoved, qt.HasLen, 0)
		})
	}
}

// TestContinuousAggregates_TheOptionIsComparedWithoutOne is the complement: an
// attribute the catalog reports as the declaration writes it needs no server,
// and leaving the body uncompared must not leave the whole object uncompared.
func TestContinuousAggregates_TheOptionIsComparedWithoutOne(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}

	declared := &goschema.Database{ContinuousAggregates: []goschema.ContinuousAggregate{
		{Name: "hourly", Body: theBody, MaterializedOnly: new(true)},
	}}
	live := &types.DBSchema{ContinuousAggregates: []types.DBContinuousAggregate{
		{Name: "hourly", Definition: theStored, MaterializedOnly: false},
	}}

	compare.ContinuousAggregates(declared, live, diff, compare.CoverageOf(declared, live), nil,
		postgresSemantics())

	c.Assert(diff.ContinuousAggregatesModified, qt.DeepEquals, []difftypes.ContinuousAggregateDiff{{
		Name: "hourly", OldBody: theStored, NewBody: theBody,
		OldMaterializedOnly: false, NewMaterializedOnly: true,
	}})
}

// TestContinuousAggregates_ADescriptionThatCouldNotSayItDoesNotDropIt is the
// coverage half.
//
// A format with no way to name a continuous aggregate still describes the
// hypertable underneath it, so its silence looks like a complete description
// with one object left out -- and the drop that silence would plan discards a
// materialization no rollback rebuilds.
func TestContinuousAggregates_ADescriptionThatCouldNotSayItDoesNotDropIt(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	live := &types.DBSchema{ContinuousAggregates: []types.DBContinuousAggregate{{
		Schema: "public", Name: "hourly", Definition: theStored,
	}}}
	silent := &goschema.Database{NotDescribed: coverage.Set{}.With(coverage.Object{
		Kind:       coverage.ContinuousAggregate,
		Reason:     coverage.Unsupported,
		Provenance: coverage.DerivedFromFact,
	})}

	compare.ContinuousAggregates(silent, live, diff, compare.CoverageOf(silent, live), nil,
		postgresSemantics())

	c.Assert(diff.ContinuousAggregatesRemoved, qt.HasLen, 0)

	// The control: a description that COULD have named one still removes.
	speaking := &goschema.Database{}
	plain := &difftypes.SchemaDiff{}
	compare.ContinuousAggregates(speaking, live, plain, compare.CoverageOf(speaking, live), nil,
		postgresSemantics())
	c.Assert(plain.ContinuousAggregatesRemoved, qt.DeepEquals, []string{"public.hourly"})
}

// postgresSemantics is the identifier rule the comparison runs under: an
// unqualified name resolves to the default schema, which is what makes a
// document naming `schema.public` and a read reporting none the same object.
func postgresSemantics() identifier.Semantics {
	semantics := identifier.ForDialect(platform.Postgres)
	semantics.DefaultSchema = "public"
	return semantics
}
