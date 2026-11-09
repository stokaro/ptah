//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/engine"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/testutils"
	"ptah.run/migration/safety"
	"ptah.run/provider-sdk/provider"
	transport "ptah.run/provider-sdk/runtime"
)

const (
	countedChange      = schemaext.Kind("example.org/fixture/retention-change")
	countedFacetChange = schemaext.Kind("example.org/fixture/retention-facet-change")
)

// comparisonCountsSpec builds the fixture command that counts what it compares,
// with the given environment.
func comparisonCountsSpec(c *qt.C, environment ...string) providerhost.Spec {
	c.Helper()
	binary := testutils.BuildFixtureProvider(c, "comparison-counts")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	return providerhost.Spec{Executable: binary, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture", Environment: environment}
}

// driftReport is the part of a JSON drift report these tests read.
type driftReport struct {
	Drift    bool `json:"drift"`
	Findings []struct {
		Category string `json:"category"`
		Count    int    `json:"count"`
	} `json:"findings"`
	Inventory []safety.InventoryEntry `json:"inventory"`
}

// categories names the report's findings.
func (r driftReport) categories() []string {
	names := make([]string, len(r.Findings))
	for i, finding := range r.Findings {
		names[i] = finding.Category
	}
	return names
}

// providerDrift runs `schema drift` against the provider the environment
// configures, over a schema that declares the retention objects in source.
func providerDrift(c *qt.C, source string, environment ...string) driftReport {
	c.Helper()
	f := newProviderCLI(c, comparisonCountsSpec(c, environment...))
	dir := filepath.Join(filepath.Dir(f.schemaPath), "schema")
	writeSQLSchema(c, dir, map[string]string{"schema.sql": source})
	stdout, stderr, _ := f.run(c, c.Context(), "drift", "--schema-file", dir, "--format", "json")
	var report driftReport
	c.Assert(json.Unmarshal([]byte(stdout), &report), qt.IsNil, qt.Commentf("stdout: %s\nstderr: %s", stdout, stderr))
	return report
}

// TestProviderDriftCountsWhatTheProviderCompares runs the drift report against
// a provider that counts. An object both sides hold counts on each whether or
// not it differs, one only the schema declares counts as desired, and one only
// the database holds counts as observed.
func TestProviderDriftCountsWhatTheProviderCompares(t *testing.T) {
	tests := []struct {
		name      string
		source    string
		held      string
		wantDrift bool
		want      []safety.InventoryEntry
	}{
		{
			name:   "each side holds the same objects",
			source: "CREATE RETENTION same SECONDS 60;\nCREATE RETENTION other SECONDS 60;\n",
			held:   "same=60,other=60",
			want:   []safety.InventoryEntry{{Kind: string(countedChange), Desired: 2, Observed: 2}},
		},
		{
			name:      "objects differ, and each side holds one of its own",
			source:    "CREATE RETENTION same SECONDS 60;\nCREATE RETENTION changed SECONDS 60;\nCREATE RETENTION created SECONDS 60;\nCREATE RETENTION added SECONDS 60;\n",
			held:      "same=60,changed=120,dropped=60",
			wantDrift: true,
			want:      []safety.InventoryEntry{{Kind: string(countedChange), Desired: 4, Observed: 3}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report := providerDrift(c, test.source, "FIXTURE_HELD_RETENTIONS="+test.held)

			c.Assert(report.Drift, qt.Equals, test.wantDrift)
			c.Assert(report.Inventory, qt.DeepEquals, test.want)
		})
	}
}

// TestProviderDriftLeavesOutAProviderThatPredatesTheCounts runs the same
// objects against a provider that offers no counts. The comparison ran, as its
// finding shows, and the inventory says nothing about its kind: a reader never
// learns that a side holds none of a kind nobody counted.
func TestProviderDriftLeavesOutAProviderThatPredatesTheCounts(t *testing.T) {
	c := qt.New(t)

	report := providerDrift(c, "CREATE RETENTION changed SECONDS 60;\n", "FIXTURE_HELD_RETENTIONS=changed=120", "FIXTURE_UNCOUNTED=1")

	c.Assert(report.Drift, qt.IsTrue)
	c.Assert(report.categories(), qt.Contains, "feature_changes:"+string(countedChange))
	c.Assert(report.Inventory, qt.HasLen, 0)
}

// engineFeatures is the feature list of the fixture's engine as a host that
// lists the counts holds it, which is the profile the host captures a value
// under.
func engineFeatures(c *qt.C, spec providerhost.Spec) []string {
	c.Helper()
	client, err := transport.Start(c.Context(), transport.Options{Executable: spec.Executable, SHA256: spec.SHA256, ProviderID: spec.ProviderID,
		Environment: spec.Environment, HostFeatures: []string{provider.ComparisonCountsFeature}})
	c.Assert(err, qt.IsNil)
	defer client.Close()
	return client.Description().GetEngines()[0].GetFeatures()
}

// countedObjects is what a comparison of the fixture's retention objects
// receives: names on each side, and complete knowledge of the kind on both.
func countedObjects(c *qt.C, spec providerhost.Spec, desired, current []string, exclusions ...string) ([]schemaext.ObjectCount, int) {
	c.Helper()
	features := engineFeatures(c, spec)
	host, err := providerhost.Start(c.Context(), spec)
	c.Assert(err, qt.IsNil)
	c.Cleanup(host.Close)
	runtime, err := engine.New(host.Provider())
	c.Assert(err, qt.IsNil)
	complete := schemaext.Knowledge{State: schemaext.Complete}
	objects := func(representation schemaext.Representation, names []string) schemaext.ObjectState {
		captured := make([]schemaext.Object, 0, len(names))
		for _, name := range names {
			subject := objectidentity.ID{Kind: objectidentity.Kind(comparisonNative), Name: objectidentity.Part{Source: name, Normalized: name}}
			captured = append(captured, schemaext.Object{Ref: subject, Value: profiledModel(c, runtime, features, representation, comparisonNative, subject, `{"seconds":60}`)})
		}
		state, err := schemaext.NewObjects(captured...)
		c.Assert(err, qt.IsNil)
		return schemaext.ObjectState{Objects: state, Coverage: comparisonCoverage(c, runtime, comparisonNative, representation, complete)}
	}
	request := schemaext.ObjectComparisonRequest{Target: "fixture", Identifiers: comparisonRules(), Desired: objects(schemaext.Desired, desired), Current: objects(schemaext.Observed, current)}
	for _, name := range exclusions {
		request.Exclusions = append(request.Exclusions, schemaext.Exclusion{Kind: coverage.Kind(comparisonNative), Name: name})
	}
	result, err := runtime.CompareObjects(c.Context(), request)
	c.Assert(err, qt.IsNil)
	return result.Counts, len(result.Changes)
}

// TestProviderComparisonCountsThroughTheProcessBoundary reads the counts a
// comparison states as the engine runtime holds them. An excluded object is
// counted on neither side, whichever side holds it. A batch that captures no
// value still counts, because the host offers the engine's features to a batch
// no value set a profile for.
func TestProviderComparisonCountsThroughTheProcessBoundary(t *testing.T) {
	tests := []struct {
		name             string
		desired, current []string
		exclusions       []string
		want             schemaext.ObjectCount
		wantChanges      int
	}{
		{name: "matched, added and removed", desired: []string{"same", "created"}, current: []string{"same", "dropped"},
			want: schemaext.ObjectCount{Kind: countedChange, Matched: 1, Added: 1, Removed: 1}, wantChanges: 2},
		{name: "an object both sides hold, excluded", desired: []string{"same", "kept"}, current: []string{"same", "kept"}, exclusions: []string{"kept"},
			want: schemaext.ObjectCount{Kind: countedChange, Matched: 1}},
		{name: "an object only the schema declares, excluded", desired: []string{"same", "created"}, current: []string{"same"}, exclusions: []string{"created"},
			want: schemaext.ObjectCount{Kind: countedChange, Matched: 1}},
		{name: "an object only the database holds, excluded", desired: []string{"same"}, current: []string{"same", "dropped"}, exclusions: []string{"dropped"},
			want: schemaext.ObjectCount{Kind: countedChange, Matched: 1}},
		{name: "a batch with no captured value", want: schemaext.ObjectCount{Kind: countedChange}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			counts, changes := countedObjects(c, comparisonCountsSpec(c), test.desired, test.current, test.exclusions...)

			c.Assert(counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
			c.Assert(changes, qt.Equals, test.wantChanges)
		})
	}
}

// TestProviderComparisonWithoutCountsStatesNoZero compares through a provider
// that predates the counts. No count reaches the runtime, not even one of
// zeros, so the kind has no inventory entry.
func TestProviderComparisonWithoutCountsStatesNoZero(t *testing.T) {
	c := qt.New(t)

	counts, changes := countedObjects(c, comparisonCountsSpec(c, "FIXTURE_UNCOUNTED=1"), []string{"created"}, nil)

	c.Assert(counts, qt.IsNil)
	c.Assert(changes, qt.Equals, 1)
}

// TestProviderFacetComparisonCountsThroughTheProcessBoundary counts the tables
// a facet stands for. A facet the run binds to another target is an exclusion:
// its table is counted on neither side, though the database holds a facet there.
func TestProviderFacetComparisonCountsThroughTheProcessBoundary(t *testing.T) {
	tests := []struct {
		name    string
		boundTo string
		want    schemaext.ObjectCount
	}{
		{name: "a facet bound to this target", boundTo: "fixture", want: schemaext.ObjectCount{Kind: countedFacetChange, Matched: 2}},
		{name: "a facet bound to another target", boundTo: "other", want: schemaext.ObjectCount{Kind: countedFacetChange, Matched: 1}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spec := comparisonCountsSpec(c)
			features := engineFeatures(c, spec)
			host, err := providerhost.Start(c.Context(), spec)
			c.Assert(err, qt.IsNil)
			c.Cleanup(host.Close)
			runtime, err := engine.New(host.Provider())
			c.Assert(err, qt.IsNil)
			rules := comparisonRules()
			builder := objectidentity.NewBuilder(rules)
			compared, bindable := builder.TableParts("main", "compared"), builder.TableParts("main", "bindable")
			facets := func(representation schemaext.Representation, subject objectidentity.ID) schemaext.Facets {
				values, err := schemaext.NewFacets(profiledModel(c, runtime, features, representation, comparisonFacet, subject, `{"seconds":60}`))
				c.Assert(err, qt.IsNil)
				return values
			}
			scoped, err := facets(schemaext.Desired, bindable).WithTargetScope(comparisonFacet, test.boundTo)
			c.Assert(err, qt.IsNil)
			selection, err := schemaext.NewTargetSelection("fixture")
			c.Assert(err, qt.IsNil)
			declared, err := scoped.ForTarget(selection)
			c.Assert(err, qt.IsNil)
			complete := schemaext.Knowledge{State: schemaext.Complete}

			result, err := runtime.CompareFacets(c.Context(), schemaext.FacetComparisonRequest{Target: "fixture", Identifiers: rules,
				Owners: []schemaext.ParentState{{Subject: compared, Desired: true, Current: true}, {Subject: bindable, Desired: true, Current: true}},
				Desired: schemaext.FacetState{Coverage: comparisonCoverage(c, runtime, comparisonFacet, schemaext.Desired, complete), Records: []schemaext.FacetRecord{
					{Subject: bindable, Values: declared}, {Subject: compared, Values: facets(schemaext.Desired, compared)}}},
				Current: schemaext.FacetState{Coverage: comparisonCoverage(c, runtime, comparisonFacet, schemaext.Observed, complete), Records: []schemaext.FacetRecord{
					{Subject: bindable, Values: facets(schemaext.Observed, bindable)}, {Subject: compared, Values: facets(schemaext.Observed, compared)}}}})

			c.Assert(err, qt.IsNil)
			c.Assert(result.Counts, qt.DeepEquals, []schemaext.ObjectCount{test.want})
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}
