package chcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chcompare"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine"
)

func viewStorageRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: chschema.Owner, Targets: []engine.Target{{Name: "clickhouse"}}, Codecs: chschema.ViewStorageCodecs(),
		FacetComparisons: []engine.FacetComparison{{Target: "clickhouse", OwnerKinds: []objectidentity.Kind{objectidentity.KindMatView},
			Kinds: []schemaext.Kind{chschema.ViewStorageKind}, Service: chcompare.ViewStorageService{}}},
	}))
}

func storedView() objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "totals")
}

// viewStorageCoverage claims the kind for every view in one representation and
// records the given knowledge of the stored view, when there is any.
func viewStorageCoverage(representation schemaext.Representation, knowledge *schemaext.Knowledge) schemaext.Coverage {
	registry := must.Must(schemaext.NewRegistry(
		schemaext.OwnedCodec{Owner: chschema.Owner, Codec: chschema.ViewStorageCodecs()[0]},
		schemaext.OwnedCodec{Owner: chschema.Owner, Codec: chschema.ViewStorageCodecs()[1]},
	))
	model, _ := registry.Identity(chschema.ViewStorageKind, representation)
	var subjects []schemaext.SubjectCoverage
	if knowledge != nil {
		subjects = []schemaext.SubjectCoverage{{Kind: chschema.ViewStorageKind, Subject: storedView(), Knowledge: *knowledge}}
	}
	return must.Must(schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}, subjects))
}

func viewStorageRequest(desired, current *schemaext.Knowledge, owner schemaext.ParentState) schemaext.FacetComparisonRequest {
	owner.Subject = storedView()
	return schemaext.FacetComparisonRequest{
		Target: "clickhouse", Kinds: []schemaext.Kind{chschema.ViewStorageKind},
		Owners:  []schemaext.ParentState{owner},
		Desired: schemaext.FacetState{Coverage: viewStorageCoverage(schemaext.Desired, desired)},
		Current: schemaext.FacetState{Coverage: viewStorageCoverage(schemaext.Observed, current)},
	}
}

var keyedStorage = schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the view stores its rows with ENGINE = MergeTree ORDER BY c, which Ptah does not model"}

// A view either side could not describe is undecided, with that side's reason,
// so a comparison never reads it as synchronized.
func TestViewStorageComparisonReportsUndescribedStorageUndecided(t *testing.T) {
	for _, test := range []struct {
		name             string
		desired, current *schemaext.Knowledge
		reason           string
	}{
		{name: "the read", current: &keyedStorage, reason: "the read could not describe the ClickHouse materialized view's storage: " + keyedStorage.Reason},
		{name: "the desired source", desired: &keyedStorage, reason: "the desired source could not describe the ClickHouse materialized view's storage: " + keyedStorage.Reason},
		{name: "both", desired: &keyedStorage, current: &keyedStorage, reason: "the read could not describe the ClickHouse materialized view's storage: " + keyedStorage.Reason},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := viewStorageRequest(test.desired, test.current, schemaext.ParentState{Desired: true, Current: true})
			result, err := viewStorageRuntime().CompareFacets(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: chschema.ViewStorageKind, Subject: storedView(), Reason: test.reason}})
		})
	}
}

// A view both sides describe compares as nothing, and so does a view only one
// side holds: its creation or removal belongs to the common view.
func TestViewStorageComparisonLeavesDescribedViewsAlone(t *testing.T) {
	for _, test := range []struct {
		name    string
		current *schemaext.Knowledge
		owner   schemaext.ParentState
	}{
		{name: "described on both sides", owner: schemaext.ParentState{Desired: true, Current: true}},
		{name: "removed", current: &keyedStorage, owner: schemaext.ParentState{Current: true}},
		{name: "created", owner: schemaext.ParentState{Desired: true}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := viewStorageRuntime().CompareFacets(t.Context(), viewStorageRequest(nil, test.current, test.owner))
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.HasLen, 0)
		})
	}
}

func TestViewStorageComparisonRefusesAnotherTarget(t *testing.T) {
	c := qt.New(t)
	request := viewStorageRequest(nil, &keyedStorage, schemaext.ParentState{Desired: true, Current: true})
	request.Target = "postgres"
	result, err := (chcompare.ViewStorageService{}).CompareFacets(t.Context(), request)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result, qt.DeepEquals, schemaext.FacetComparisonResult{})
}

// The kind carries no value, so no codec encodes one.
func TestViewStorageCodecsRefuseEveryValue(t *testing.T) {
	for _, codec := range chschema.ViewStorageCodecs() {
		t.Run(string(codec.Representation), func(t *testing.T) {
			c := qt.New(t)
			encoded, err := codec.Encode(codec.Prototype)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(encoded, qt.IsNil)
			decoded, err := codec.Decode([]byte(`{}`))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
