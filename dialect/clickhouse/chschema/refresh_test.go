package chschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func fullSchedule() chschema.Schedule {
	return chschema.Schedule{
		Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE",
		DependsOn: []string{"analytics.b", "analytics.a"}, Append: true,
	}
}

// Both representations keep every clause as written, including the order of
// the dependencies, and an omitted clause stays off the wire.
func TestRefreshValuesRoundTripAsWritten(t *testing.T) {
	for _, test := range []struct {
		name     string
		schedule chschema.Schedule
		wire     string
	}{
		{"minimal", chschema.Schedule{Mode: chschema.RefreshAfter, Interval: "90 SECOND"}, `{"mode":"AFTER","interval":"90 SECOND"}`},
		{"every clause", fullSchedule(), `{"mode":"EVERY","interval":"1 DAY","offset":"2 HOUR","randomize":"30 MINUTE","depends_on":["analytics.b","analytics.a"],"append":true}`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := &chschema.DesiredRefresh{Schedule: test.schedule}
			observed := must.Must(desired.Observed())
			c.Assert(observed.Desired(), qt.DeepEquals, desired)
			for index, value := range []schemaext.Value{desired, observed} {
				codec := chschema.RefreshCodecs()[index]
				encoded, err := codec.Canonical(value)
				c.Assert(err, qt.IsNil)
				c.Assert(string(encoded), qt.Equals, test.wire)
				decoded, err := codec.Decode(encoded)
				c.Assert(err, qt.IsNil)
				c.Assert(decoded, qt.DeepEquals, value)
			}
		})
	}
}

// The codecs are strict: a field the model does not have, a missing required
// clause, a null and a schedule the server would refuse are all refused, so a
// document cannot carry a schedule the owner would read differently.
func TestRefreshCodecsRefuseValuesTheServerWouldNot(t *testing.T) {
	for _, wire := range []string{
		`null`,
		`{}`,
		`{"mode":"EVERY"}`,
		`{"interval":"1 HOUR"}`,
		`{"mode":"SOMETIMES","interval":"1 HOUR"}`,
		`{"mode":"EVERY","interval":" "}`,
		`{"mode":"AFTER","interval":"1 HOUR","offset":"5 MINUTE"}`,
		`{"mode":"EVERY","interval":"1 HOUR","depends_on":[""]}`,
		`{"mode":"EVERY","interval":"1 HOUR","append":null}`,
		`{"mode":"EVERY","interval":"1 HOUR","strategy":"manual"}`,
		`{"mode":"EVERY","interval":"1 HOUR","mode":"AFTER"}`,
	} {
		for _, codec := range chschema.RefreshCodecs() {
			t.Run(string(codec.Representation)+" "+wire, func(t *testing.T) {
				c := qt.New(t)
				decoded, err := codec.Decode(json.RawMessage(wire))
				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(decoded, qt.IsNil)
			})
		}
	}
}

// Validation names the model whose value is wrong, which is how a source or
// reader error says where the bad schedule came from.
func TestRefreshValidationIdentifiesTheModel(t *testing.T) {
	c := qt.New(t)
	err := chschema.ValidateObservedRefresh(&chschema.ObservedRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshAfter, Interval: "1 HOUR", Offset: "1 MINUTE"}})
	var invalid *schemaext.InvalidModelError
	c.Assert(err, qt.ErrorAs, &invalid)
	c.Assert(invalid.Kind, qt.Equals, chschema.RefreshKind)
	c.Assert(invalid.Representation, qt.Equals, schemaext.Observed)
	c.Assert(chschema.ValidateDesiredRefresh(nil), qt.ErrorIs, schemaext.ErrInvalidValue)
	_, err = (*chschema.DesiredRefresh)(nil).Observed()
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert((*chschema.ObservedRefresh)(nil).Desired(), qt.IsNil)
}

// Clause is what follows REFRESH in a CREATE and in MODIFY REFRESH, in the
// order ClickHouse's grammar takes the clauses.
func TestRefreshClauseFollowsTheServerGrammar(t *testing.T) {
	for _, test := range []struct {
		name     string
		schedule chschema.Schedule
		want     string
	}{
		{"minimal", chschema.Schedule{Mode: chschema.RefreshAfter, Interval: "30 MINUTE"}, "AFTER 30 MINUTE"},
		{"every clause", fullSchedule(), "EVERY 1 DAY OFFSET 2 HOUR RANDOMIZE FOR 30 MINUTE DEPENDS ON analytics.b, analytics.a APPEND"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.schedule.Clause(), qt.Equals, test.want)
		})
	}
}

// Equality compares the schedule as written: intervals are not read here, and
// dependency order counts, so the owner's comparison, not the model, decides
// which spellings are one schedule.
func TestRefreshEqualityComparesClausesAsWritten(t *testing.T) {
	for _, test := range []struct {
		name  string
		other chschema.Schedule
	}{
		{"another interval spelling", chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "24 HOUR", Offset: "2 HOUR", Randomize: "30 MINUTE", DependsOn: []string{"analytics.b", "analytics.a"}, Append: true}},
		{"dependencies reordered", chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE", DependsOn: []string{"analytics.a", "analytics.b"}, Append: true}},
		{"without APPEND", chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 DAY", Offset: "2 HOUR", Randomize: "30 MINUTE", DependsOn: []string{"analytics.b", "analytics.a"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(fullSchedule().Equal(test.other), qt.IsFalse)
			c.Assert((&chschema.DesiredRefresh{Schedule: fullSchedule()}).Equal(&chschema.DesiredRefresh{Schedule: test.other}), qt.IsFalse)
		})
	}
	c := qt.New(t)
	c.Assert(fullSchedule().Equal(fullSchedule()), qt.IsTrue)
	c.Assert((&chschema.ObservedRefresh{Schedule: fullSchedule()}).Equal(&chschema.DesiredRefresh{Schedule: fullSchedule()}), qt.IsFalse)
}

// A view's schedule survives the facet envelope with its target binding, and
// a clone shares no dependency list with its source.
func TestRefreshRegistryPreservesScopeAndIndependence(t *testing.T) {
	c := qt.New(t)
	registry := must.Must(engine.New(engine.Provider{ID: "example.org/clickhouse", Codecs: chschema.RefreshCodecs()})).Codecs()
	declared := &chschema.DesiredRefresh{Schedule: fullSchedule()}
	facets := must.Must(must.Must(schemaext.NewFacets(declared)).WithTargetScope(chschema.RefreshKind, "clickhouse"))

	encoded, err := registry.EncodeFacets(t.Context(), schemaext.Desired, facets)
	c.Assert(err, qt.IsNil)
	decoded, err := registry.DecodeFacets(t.Context(), schemaext.Desired, encoded)
	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Equal(facets), qt.IsTrue)
	c.Assert(decoded.TargetScope(chschema.RefreshKind), qt.DeepEquals, []string{"clickhouse"})

	clone := declared.Clone().(*chschema.DesiredRefresh)
	clone.DependsOn[0] = "mutated"
	c.Assert(declared.DependsOn[0], qt.Equals, "analytics.b")
}

// Coverage records what a source or reader knows about schedules: the whole
// kind, and the views that depart from it.
func TestRefreshCoverageRecordsKindAndSubjectKnowledge(t *testing.T) {
	c := qt.New(t)
	view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "totals")
	other := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "other")
	coverage, err := chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unreadable clause"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(coverage.Lookup(chschema.RefreshKind, view).State, qt.Equals, schemaext.Unrepresentable)
	c.Assert(coverage.Lookup(chschema.RefreshKind, other).State, qt.Equals, schemaext.Complete)
	_, err = chschema.RefreshCoverage(schemaext.Change, schemaext.Knowledge{State: schemaext.Complete}, nil)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
}
