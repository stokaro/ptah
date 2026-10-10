package sqlitetable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/sqlite/sqlitetable"
)

func decode(c *qt.C, properties map[string]string) ([]schemaext.Value, error) {
	c.Helper()
	return sqlitetable.PropertyService{}.DecodeProperties(c.Context(), schemaext.PropertyDecodeRequest{
		Target: platform.SQLite, Format: schemaext.TablePlatformProperties,
		Fragments: []schemaext.PropertyFragment{{Kind: sqlitetable.TableKind, Properties: properties}},
	})
}

// TestPropertyService_DecodeProperties_HappyPath reads the two platform
// properties: `true` turns an option on, and `false` and an empty value leave
// it off.
func TestPropertyService_DecodeProperties_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		want       sqlitetable.Options
	}{
		{name: "both on", properties: map[string]string{"strict": "true", "without_rowid": "true"},
			want: sqlitetable.Options{Strict: true, WithoutRowID: true}},
		{name: "one on", properties: map[string]string{"without_rowid": "true"}, want: sqlitetable.Options{WithoutRowID: true}},
		{name: "false is off", properties: map[string]string{"strict": "false", "without_rowid": "false"}},
		{name: "empty states nothing", properties: map[string]string{"strict": ""}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := decode(c, test.properties)

			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Value{&sqlitetable.DesiredTable{Options: test.want}})
		})
	}
}

// TestPropertyService_DecodeProperties_FailurePath refuses a key the owner
// does not have and a value that is not true or false, rather than reading
// a typo as an option left out.
func TestPropertyService_DecodeProperties_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		want       string
	}{
		{name: "an unknown key", properties: map[string]string{"stricter": "true"}, want: `.*unknown SQLite table property "stricter"`},
		{name: "not a boolean", properties: map[string]string{"strict": "yes"}, want: `.*SQLite table property strict is "yes", not true or false`},
		{name: "another spelling of true", properties: map[string]string{"without_rowid": "1"}, want: `.*without_rowid is "1", not true or false`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := decode(c, test.properties)

			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(values, qt.IsNil)
		})
	}
}

// TestPropertyService_EncodeProperties writes an option that is on as `true`
// and leaves one that is off unwritten.
func TestPropertyService_EncodeProperties(t *testing.T) {
	c := qt.New(t)

	fragments, err := sqlitetable.PropertyService{}.EncodeProperties(c.Context(), schemaext.PropertyEncodeRequest{
		Target: platform.SQLite, Format: schemaext.TablePlatformProperties,
		Values: []schemaext.Value{&sqlitetable.DesiredTable{Options: sqlitetable.Options{WithoutRowID: true}}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.DeepEquals, []schemaext.PropertyFragment{{Kind: sqlitetable.TableKind, Properties: map[string]string{"without_rowid": "true"}}})
}

// TestTableCodecs_RoundTrip writes only the options that are on, and reads
// back exactly what it wrote.
func TestTableCodecs_RoundTrip(t *testing.T) {
	c := qt.New(t)
	codec := sqlitetable.TableCodecs()[0]
	declared := &sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true}}

	data, err := codec.Encode(declared)
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"strict":true}`)
	decoded, err := codec.Decode(data)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded, qt.DeepEquals, schemaext.Value(declared))
}

// TestTableCodecs_RefusesAnOptionWrittenAsFalse refuses a spelling the
// encoder never writes.
func TestTableCodecs_RefusesAnOptionWrittenAsFalse(t *testing.T) {
	c := qt.New(t)

	decoded, err := sqlitetable.TableCodecs()[0].Decode([]byte(`{"strict":false}`))

	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(decoded, qt.IsNil)
}

// TestCompareService_PlansNoChange returns the declarations as they were and
// no change: SQLite has no statement that turns either option on or off for a
// table that exists.
func TestCompareService_PlansNoChange(t *testing.T) {
	c := qt.New(t)
	table := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "t", Normalized: "t"}}
	desired := schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: table,
		Values: must.Must(schemaext.NewFacets(&sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true}}))}}}
	current := schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: table,
		Values: must.Must(schemaext.NewFacets(&sqlitetable.ObservedTable{}))}}}

	result, err := sqlitetable.CompareService{}.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{
		Target: platform.SQLite, Kinds: []schemaext.Kind{sqlitetable.TableKind}, Desired: desired, Current: current,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Changes, qt.HasLen, 0)
	c.Assert(result.Desired.Records, qt.HasLen, 1)
}

// TestPlanService_AccountsForEveryTableAction gives a table the options ride
// with a receipt for each common action, and refuses a change, which no
// comparison produces.
func TestPlanService_AccountsForEveryTableAction(t *testing.T) {
	c := qt.New(t)
	table := objectidentity.ID{Kind: objectidentity.KindTable, Name: objectidentity.Part{Source: "t", Normalized: "t"}}
	request := featureplan.Request{Target: platform.SQLite, ParentKinds: []schemaext.Kind{sqlitetable.TableKind}}
	for _, action := range []featureplan.ParentAction{featureplan.CreateTable, featureplan.AlterTable, featureplan.DropTable, featureplan.RebuildTable} {
		request.Tables = append(request.Tables, featureplan.Table{Subject: table, Action: action})
	}

	result, err := sqlitetable.PlanService{}.PlanFeatures(c.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Parents, qt.HasLen, 4)
	c.Assert(result.Diagnostics, qt.HasLen, 0)

	refused, err := sqlitetable.PlanService{}.PlanFeatures(c.Context(), featureplan.Request{Target: platform.SQLite,
		Changes: []schemaext.ChangeRecord{{Subject: table, Value: &sqlitetable.Change{Before: &sqlitetable.ObservedTable{}, After: &sqlitetable.DesiredTable{}}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(refused.Diagnostics, qt.HasLen, 1)
	c.Assert(refused.Diagnostics[0].Problem.Message, qt.Equals, "SQLite table options change only when their table is created")
}

// TestTableOptions reads the declared options a renderer writes, and refuses
// a facet of another kind on a SQLite table.
func TestTableOptions(t *testing.T) {
	c := qt.New(t)
	declared := &sqlitetable.DesiredTable{Options: sqlitetable.Options{Strict: true, WithoutRowID: true}}

	options, err := sqlitetable.TableOptions(must.Must(schemaext.NewFacets(declared)))
	c.Assert(err, qt.IsNil)
	c.Assert(options, qt.DeepEquals, declared)

	none, err := sqlitetable.TableOptions(schemaext.Facets{})
	c.Assert(err, qt.IsNil)
	c.Assert(none, qt.IsNil)
}

// TestServices_RefuseAnotherTarget refuses every stage on a target that has
// neither option.
func TestServices_RefuseAnotherTarget(t *testing.T) {
	c := qt.New(t)

	_, err := sqlitetable.PlanService{}.PlanFeatures(c.Context(), featureplan.Request{Target: platform.Postgres})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
}

// TestReportService_CountsEachOption counts a table once per option it is
// created with.
func TestReportService_CountsEachOption(t *testing.T) {
	c := qt.New(t)

	reports, err := sqlitetable.ReportService{}.ReportValues(c.Context(), schemaext.ReportingRequest{
		Representation: schemaext.Observed,
		Values:         []schemaext.Value{&sqlitetable.ObservedTable{Options: sqlitetable.Options{Strict: true}}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(reports, qt.DeepEquals, []schemaext.ValueReport{{Kind: sqlitetable.TableKind, Counts: []schemaext.MetricCount{
		{Name: "sqlite_strict_tables", Value: 1}, {Name: "sqlite_without_rowid_tables", Value: 0},
	}}})
}
