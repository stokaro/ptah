package schemadiff_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/timescaledb/tsdiff"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
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

// TestTimescaleHypertables_ComparesByTheTableItPartitions pins what makes two
// hypertables the same one.
//
// A hypertable has no name of its own -- `timescaledb_information.hypertables`
// is keyed by the relation -- so its settings belong to the table, and an
// unqualified declaration names the same table a qualified catalog row does.
// Comparing the raw strings would report an addition and a removal on every run
// against a server whose read reports the schema (stokaro/ptah#1026).
func TestTimescaleHypertables_ComparesByTheTableItPartitions(t *testing.T) {
	tests := []struct {
		name     string
		declared *tsschema.DesiredHypertable
		schema   string
		live     *tsschema.ObservedHypertable
		want     []string
	}{
		{
			name:     "declared and absent",
			declared: &tsschema.DesiredHypertable{Column: "time"},
			want:     []string{`hypertable conditions: ordinary -> "time" ""`},
		},
		{
			// The declaration leaves the schema off and the catalog reports it,
			// which is what a Go-annotated schema against PostgreSQL looks like.
			name:     "an unqualified declaration of a qualified row",
			declared: &tsschema.DesiredHypertable{Column: "time"},
			schema:   "public",
			live:     &tsschema.ObservedHypertable{Column: "time", Dimensions: 1},
		},
		{
			name:   "live and undeclared",
			schema: "public",
			live:   &tsschema.ObservedHypertable{Column: "time", Dimensions: 1},
			want:   []string{`hypertable conditions: "time" "" -> ordinary`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareTimescale(c, hypertableDocument(test.declared, tsschema.CompleteCoverage),
				hypertableRead(test.schema, test.live, nil))

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// TestTimescaleHypertables_AnOmittedIntervalIsNotAChange is the row that would
// otherwise make every apply plan the same change forever.
//
// An empty declared interval takes TimescaleDB's own default -- 7 days for a
// timestamptz column, measured on 2.29.2 -- and the catalog then reports what
// the server chose. Comparing that against the empty declaration would report a
// difference on every run for a declaration that asked for whatever the server
// picked.
func TestTimescaleHypertables_AnOmittedIntervalIsNotAChange(t *testing.T) {
	tests := []struct {
		name     string
		declared tsschema.DesiredHypertable
		want     []string
	}{
		{name: "no interval declared", declared: tsschema.DesiredHypertable{Column: "time"}},
		{name: "the declared interval matches", declared: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "7 DAYS"}},
		{
			name:     "the declared interval differs",
			declared: tsschema.DesiredHypertable{Column: "time", ChunkInterval: "1 hour"},
			want:     []string{`hypertable conditions: "time" "7 days" -> "time" "1 hour"`},
		},
		{
			name:     "the dimension moved",
			declared: tsschema.DesiredHypertable{Column: "recorded_at"},
			want:     []string{`hypertable conditions: "time" "7 days" -> "recorded_at" ""`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareTimescale(c, hypertableDocument(new(test.declared), tsschema.CompleteCoverage),
				hypertableRead("public", &tsschema.ObservedHypertable{Column: "time", ChunkInterval: "7 days", Dimensions: 1}, nil))

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// TestTimescaleHypertables_ASourceThatCannotSayItKeepsIt is the coverage half,
// and the one that costs the most when it is wrong.
//
// A format with no way to say a table is partitioned describes an ordinary
// table -- complete on its face, and wrong. Reading that silence as intent
// would report a removal the planner then refuses, so an operator applying a
// YAML document against a TimescaleDB server would be told their schema cannot
// be applied at all. The control is a source that enrolls the model: there the
// same silence is a request.
func TestTimescaleHypertables_ASourceThatCannotSayItKeepsIt(t *testing.T) {
	tests := []struct {
		name     string
		coverage func(schemaext.Representation) (schemaext.Coverage, error)
		want     []string
	}{
		{name: "a format with no hypertable spelling", coverage: noTimescaleCoverage},
		{name: "a format that has one", coverage: tsschema.CompleteCoverage, want: []string{`hypertable conditions: "time" "" -> ordinary`}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareTimescale(c, hypertableDocument(nil, test.coverage),
				hypertableRead("public", &tsschema.ObservedHypertable{Column: "time", Dimensions: 1}, nil))

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// undescribedHypertableRead is the conditions table as a read reports a
// hypertable whose dimension the catalog did not report: no settings, and a
// limit that says why.
func undescribedHypertableRead() *catalog.Database {
	current := hypertableRead("", nil, nil)
	current.FeatureCoverage = must.Must(schemaext.NewCoverage(schemaext.Observed, current.FeatureCoverage.KindRecords(),
		[]schemaext.SubjectCoverage{{Kind: tsschema.HypertableKind, Subject: tableSubject("conditions"),
			Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the catalog reported no dimension for this hypertable"}}}))
	return current
}

// TestTimescaleHypertables_ARowTheReadCouldNotDescribeIsUndecided pins the
// current-side limit: a hypertable whose dimension the catalog did not report
// carries no settings, and the read records why. A declaration either way is a
// decision about settings nobody read -- partitioning it would plan
// create_hypertable on a table that already is one, and leaving it ordinary
// would plan nothing for a hypertable the document says is not one -- so the
// comparison is incomplete.
func TestTimescaleHypertables_ARowTheReadCouldNotDescribeIsUndecided(t *testing.T) {
	tests := []struct {
		name     string
		declared *tsschema.DesiredHypertable
	}{
		{name: "the declaration partitions it", declared: &tsschema.DesiredHypertable{Column: "time"}},
		{name: "the declaration leaves it ordinary"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareTimescale(c, hypertableDocument(test.declared, tsschema.CompleteCoverage), undescribedHypertableRead())

			c.Assert(err, qt.ErrorMatches, `schema comparison is incomplete: .*table "conditions".*this table's hypertable settings were not fully inspected`)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestTimescaleHypertables_ASourceWithoutHypertablesDecidesNothingAboutOne is
// the control on the refusal above: a source that cannot describe hypertables
// made no decision about the undescribed one, so nothing is refused or planned.
func TestTimescaleHypertables_ASourceWithoutHypertablesDecidesNothingAboutOne(t *testing.T) {
	c := qt.New(t)

	diff, err := compareTimescale(c, hypertableDocument(nil, noTimescaleCoverage), undescribedHypertableRead())

	c.Assert(err, qt.IsNil)
	c.Assert(timescaleChanges(diff), qt.HasLen, 0)
}

// TestTimescaleAggregates_ComparesByTheQualifiedName pins what makes two
// aggregates the same one, and that both lifecycle directions are reported.
func TestTimescaleAggregates_ComparesByTheQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		declared []schemaext.Object
		live     []schemaext.Object
		want     []string
	}{
		{
			name:     "declared and absent",
			declared: []schemaext.Object{declaredAggregate("", tsschema.DesiredContinuousAggregate{Body: theBody})},
			want:     []string{"aggregate hourly: create"},
		},
		{
			name:     "declared and present",
			declared: []schemaext.Object{declaredAggregate("public", tsschema.DesiredContinuousAggregate{Body: theBody})},
			live:     []schemaext.Object{liveAggregate("public", false)},
		},
		{
			// The shape a real round trip has: `schema inspect` scoped to one
			// schema reports the aggregate unqualified, and the document it
			// writes names `schema.public`. Comparing the two strings reported
			// an addition AND a removal for one unchanged object, and the plan
			// created it before dropping it.
			name:     "a qualified declaration of an unqualified row",
			declared: []schemaext.Object{declaredAggregate("public", tsschema.DesiredContinuousAggregate{Body: theBody})},
			live:     []schemaext.Object{liveAggregate("", false)},
		},
		{
			name: "live and undeclared",
			live: []schemaext.Object{liveAggregate("public", false)},
			want: []string{"aggregate public.hourly: drop"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareTimescale(c, aggregateDocument(test.declared, tsschema.CompleteCoverage),
				hypertableRead("public", nil, test.live))

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// TestTimescaleAggregates_TheBodyIsComparedOnlyThroughTheServer is the row that
// decides whether an unchanged schema replans itself forever.
//
// The declared body and the stored body are the SAME aggregate, and they share
// no long substring. Without a normalization attached by a server the body is
// not comparable at all, and reporting a difference would drop and recreate
// the aggregate on every run -- each drop discarding the materialization it
// exists to keep.
func TestTimescaleAggregates_TheBodyIsComparedOnlyThroughTheServer(t *testing.T) {
	tests := []struct {
		name       string
		normalized *tsschema.NormalizedBody
		want       []string
	}{
		{name: "no server normalized it"},
		{name: "the server normalized it to what the catalog holds", normalized: &tsschema.NormalizedBody{Body: theStored}},
		{
			name:       "the server normalized it to something else",
			normalized: &tsschema.NormalizedBody{Body: "  SELECT time_bucket('1 day'::interval, \"time\") AS bucket\n    FROM readings;"},
			want:       []string{"aggregate hourly: replace"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared := declaredAggregate("", tsschema.DesiredContinuousAggregate{Body: theBody, Normalized: test.normalized})
			diff, err := compareTimescale(c, aggregateDocument([]schemaext.Object{declared}, tsschema.CompleteCoverage),
				hypertableRead("", nil, []schemaext.Object{liveAggregate("", false)}))

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// TestTimescaleAggregates_TheOptionIsComparedWithoutAServer is the complement:
// an attribute the catalog reports as the declaration writes it needs no
// server, and leaving the body uncompared must not leave the whole object
// uncompared. An option the declaration left out takes the server's default,
// which is not a constant, so it is never a difference.
func TestTimescaleAggregates_TheOptionIsComparedWithoutAServer(t *testing.T) {
	tests := []struct {
		name     string
		declared *bool
		want     []string
	}{
		{name: "the option is left out"},
		{name: "the option matches", declared: new(false)},
		{name: "the option differs", declared: new(true), want: []string{"aggregate hourly: replace"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared := declaredAggregate("", tsschema.DesiredContinuousAggregate{Body: theBody, MaterializedOnly: test.declared})
			diff, err := compareTimescale(c, aggregateDocument([]schemaext.Object{declared}, tsschema.CompleteCoverage),
				hypertableRead("", nil, []schemaext.Object{liveAggregate("", false)}))

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// TestTimescaleAggregates_ASourceThatCannotSayItDoesNotDropIt is the coverage
// half.
//
// A format with no way to name a continuous aggregate still describes the
// hypertable underneath it, so its silence looks like a complete description
// with one object left out -- and the drop that silence would plan discards a
// materialization no rollback rebuilds.
func TestTimescaleAggregates_ASourceThatCannotSayItDoesNotDropIt(t *testing.T) {
	tests := []struct {
		name     string
		coverage func(schemaext.Representation) (schemaext.Coverage, error)
		want     []string
	}{
		{name: "a format with no aggregate spelling", coverage: noTimescaleCoverage},
		{name: "a format that has one", coverage: tsschema.CompleteCoverage, want: []string{"aggregate public.hourly: drop"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := compareTimescale(c, aggregateDocument(nil, test.coverage),
				hypertableRead("public", nil, []schemaext.Object{liveAggregate("public", false)}))

			c.Assert(err, qt.IsNil)
			c.Assert(timescaleChanges(diff), qt.DeepEquals, test.want)
		})
	}
}

// TestTimescaleAggregates_ADeclaredRelationCannotTakeAnAggregatesName holds
// the refusal a declared table, view or materialized view meets when a live
// continuous aggregate holds its name.
//
// An aggregate holds its name as a relation -- pg_class reports relkind 'v' --
// so the statement creating the declaration would answer `relation ... already
// exists` halfway through the script. The refusal does not depend on whether
// the source can describe aggregates: a source that cannot leaves the
// aggregate alone, and its declaration still collides with it. The most common
// shape is an aggregate written as a materialized view.
func TestTimescaleAggregates_ADeclaredRelationCannotTakeAnAggregatesName(t *testing.T) {
	tests := []struct {
		name     string
		declare  func(*schemamodel.Database)
		coverage func(schemaext.Representation) (schemaext.Coverage, error)
		want     string
	}{
		{
			name: "a table",
			declare: func(db *schemamodel.Database) {
				db.Tables = append(db.Tables, schemamodel.Table{StructName: "H", Name: "hourly"})
			},
			coverage: noTimescaleCoverage,
			want:     `.*declared table "hourly" is a TimescaleDB continuous aggregate on this server, materializing public.readings.*rename the table.*`,
		},
		{
			name: "a view",
			declare: func(db *schemamodel.Database) {
				db.Views = append(db.Views, schemamodel.View{Name: "hourly", Body: "SELECT 1"})
			},
			coverage: noTimescaleCoverage,
			want:     `.*declared view "hourly" is a TimescaleDB continuous aggregate on this server.*rename the view.*`,
		},
		{
			name: "a materialized view",
			declare: func(db *schemamodel.Database) {
				db.MaterializedViews = append(db.MaterializedViews, schemamodel.MaterializedView{Name: "hourly", Body: "SELECT 1"})
			},
			coverage: tsschema.CompleteCoverage,
			want:     `.*declared materialized view "hourly" is a TimescaleDB continuous aggregate on this server.*rename the materialized view.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := aggregateDocument(nil, test.coverage)
			test.declare(desired)

			diff, err := compareTimescale(c, desired, hypertableRead("", nil, []schemaext.Object{liveAggregate("", false)}))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestTimescaleAggregates_AnotherRelationNameIsNotRefused is the control on
// the refusal above: a view of another name is compared as any view is.
func TestTimescaleAggregates_AnotherRelationNameIsNotRefused(t *testing.T) {
	c := qt.New(t)
	desired := aggregateDocument(nil, noTimescaleCoverage)
	desired.Views = append(desired.Views, schemamodel.View{Name: "other", Body: "SELECT 1"})

	diff, err := compareTimescale(c, desired, hypertableRead("", nil, []schemaext.Object{liveAggregate("", false)}))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.ViewsAdded.Names(), qt.DeepEquals, []string{"other"})
}

func compareTimescale(c *qt.C, desired *schemamodel.Database, current *catalog.Database) (*difftypes.SchemaDiff, error) {
	c.Helper()
	caps := capability.Postgres17().With(capability.Hypertables, true).With(capability.ContinuousAggregates, true)
	return schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current,
		catalog.ServerInfo{Dialect: platform.Postgres, Capabilities: caps}, nil, must.Must(builtin.New()))
}

// hypertableDocument declares the conditions table, with the given hypertable
// settings when they are not nil, under the given source coverage.
func hypertableDocument(declared *tsschema.DesiredHypertable, coverage func(schemaext.Representation) (schemaext.Coverage, error)) *schemamodel.Database {
	table := schemamodel.Table{StructName: "C", Name: "conditions"}
	if declared != nil {
		table.Facets = must.Must(schemaext.NewFacets(declared))
	}
	return &schemamodel.Database{
		Tables:          []schemamodel.Table{table},
		Fields:          []schemamodel.Field{{StructName: "C", Name: "time", Type: "TIMESTAMPTZ", Primary: true}},
		FeatureCoverage: must.Must(coverage(schemaext.Desired)),
	}
}

func aggregateDocument(objects []schemaext.Object, coverage func(schemaext.Representation) (schemaext.Coverage, error)) *schemamodel.Database {
	desired := hypertableDocument(nil, coverage)
	desired.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	return desired
}

// hypertableRead is the conditions table as a PostgreSQL read reports it, in
// schema, with its hypertable settings when they are not nil and the
// aggregates the read found. A read always enrolls both TimescaleDB models.
func hypertableRead(schema string, live *tsschema.ObservedHypertable, aggregates []schemaext.Object) *catalog.Database {
	table := catalog.Table{Schema: schema, Name: "conditions", Columns: []catalog.Column{{
		Name: "time", DataType: "timestamp with time zone", UDTName: "timestamptz", IsPrimaryKey: true,
	}}}
	if live != nil {
		table.Facets = must.Must(schemaext.NewFacets(live))
	}
	return &catalog.Database{
		Tables:          []catalog.Table{table},
		FeatureObjects:  must.Must(schemaext.NewObjects(aggregates...)),
		FeatureCoverage: must.Must(tsschema.CompleteCoverage(schemaext.Observed)),
	}
}

func declaredAggregate(schema string, aggregate tsschema.DesiredContinuousAggregate) schemaext.Object {
	return tsschema.DesiredContinuousAggregateObject(schema, "hourly", aggregate)
}

func liveAggregate(schema string, materializedOnly bool) schemaext.Object {
	return tsschema.ObservedContinuousAggregateObject(schema, "hourly", tsschema.ObservedContinuousAggregate{
		Definition: theStored, MaterializedOnly: &materializedOnly, HypertableSchema: "public", HypertableName: "readings",
	})
}

// noTimescaleCoverage is the coverage of a format that has no spelling for
// either TimescaleDB model: it enrolls neither.
func noTimescaleCoverage(schemaext.Representation) (schemaext.Coverage, error) {
	return schemaext.Coverage{}, nil
}

// timescaleChanges renders every TimescaleDB change of a diff as one line a
// table row can list. A refused comparison has no diff and no line.
func timescaleChanges(diff *difftypes.SchemaDiff) []string {
	if diff == nil {
		return nil
	}
	records := append([]schemaext.ChangeRecord(nil), diff.FeatureChanges...)
	for _, table := range diff.TablesModified {
		records = append(records, table.FeatureChanges...)
	}
	var lines []string
	for _, record := range records {
		switch change := record.Value.(type) {
		case *tsdiff.Hypertable:
			lines = append(lines, fmt.Sprintf("hypertable %s: %s -> %s", record.Subject.Name.Source, observedPartitioning(change.Before), declaredPartitioning(change.After)))
		case *tsdiff.ContinuousAggregate:
			lines = append(lines, fmt.Sprintf("aggregate %s: %s", tsschema.QualifiedName(record.Subject), aggregateAction(change)))
		}
	}
	return lines
}

func observedPartitioning(value *tsschema.ObservedHypertable) string {
	if value == nil {
		return "ordinary"
	}
	return fmt.Sprintf("%q %q", value.Column, value.ChunkInterval)
}

func declaredPartitioning(value *tsschema.DesiredHypertable) string {
	if value == nil {
		return "ordinary"
	}
	return fmt.Sprintf("%q %q", value.Column, value.ChunkInterval)
}

func aggregateAction(change *tsdiff.ContinuousAggregate) string {
	switch {
	case change.Before == nil:
		return "create"
	case change.After == nil:
		return "drop"
	default:
		return "replace"
	}
}

// tableSubject names a table of the default schema the way a PostgreSQL read
// does.
func tableSubject(name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres)).TableParts("", name)
}
