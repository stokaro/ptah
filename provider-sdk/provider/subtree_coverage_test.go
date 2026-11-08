package provider_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/proto"

	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// withSubtrees lists schema.subtree-coverage on the provider and on its one
// engine, which declares / as its schema separator.
func withSubtrees(d *protocol.Description) *protocol.Description {
	d.Features = append(d.Features, provider.SchemaSubtreeCoverageFeature)
	d.Engines[0].Features = append(d.Engines[0].Features, provider.SchemaSubtreeCoverageFeature)
	d.Engines[0].Identifiers.SchemaSeparator = "/"
	return d
}

func namespacePart(value string) *protocol.NamespaceComponent {
	return &protocol.NamespaceComponent{Source: value, Normalized: value}
}

// subtreeCoverage claims every schema of database production complete below
// its root, and the subtree of schema hidden unknown.
func subtreeCoverage() (*protocol.Description, *protocol.ModelCoverage) {
	description, coverage := modelCoverage()
	root := &protocol.ModelNamespace{Database: namespacePart("production"), Catalog: &protocol.NamespaceComponent{}, Schema: &protocol.NamespaceComponent{}, SchemaSubtree: true}
	hidden := proto.CloneOf(root)
	hidden.Schema = namespacePart("hidden")
	coverage.Kinds[0].Knowledge = &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "outside the database"}
	coverage.Kinds[0].Regions = []*protocol.ModelNamespaceKnowledge{
		{Scope: root, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}},
		{Scope: hidden, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "a hidden directory is not read"}},
	}
	return withSubtrees(description), coverage
}

func TestSchemaWithin(t *testing.T) {
	for _, test := range []struct {
		name, region, subject, separator string
		want                             bool
	}{
		{name: "the root covers a nested schema", region: "", subject: "a/b", separator: "/", want: true},
		{name: "the root covers itself", region: "", subject: "", separator: "/", want: true},
		{name: "a schema covers itself", region: "a", subject: "a", separator: "/", want: true},
		{name: "a schema covers its child", region: "a", subject: "a/b", separator: "/", want: true},
		{name: "a schema covers a deeper descendant", region: "a", subject: "a/b/c", separator: "/", want: true},
		{name: "a schema does not cover a sibling sharing its prefix", region: "a", subject: "ab", separator: "/", want: false},
		{name: "a schema does not cover its parent", region: "a/b", subject: "a", separator: "/", want: false},
		{name: "no separator nests nothing", region: "a", subject: "a/b", separator: "", want: false},
		{name: "no separator leaves the root alone", region: "", subject: "a", separator: "", want: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(provider.SchemaWithin(test.region, test.subject, test.separator), qt.Equals, test.want)
		})
	}
}

// A negotiated read may claim a subtree complete and a subtree below it
// unknown, and a schema's subtree is another region than the schema alone.
func TestSubtreeCoverage_HappyPath(t *testing.T) {
	c := qt.New(t)
	description, coverage := subtreeCoverage()
	exact := proto.CloneOf(coverage.Kinds[0].Regions[1])
	exact.Scope.SchemaSubtree = false
	coverage.Kinds[0].Regions = append(coverage.Kinds[0].Regions, exact)
	original := proto.CloneOf(coverage)

	err := provider.ValidateModelCoverage(description, "fixture", coverage)

	c.Assert(err, qt.IsNil)
	c.Assert(provider.ValidateDescription(description, nil), qt.IsNil)
	c.Assert(proto.Equal(coverage, original), qt.IsTrue)
}

func TestSubtreeCoverage_FailurePath(t *testing.T) {
	without := func(features []string) []string {
		return slices.DeleteFunc(slices.Clone(features), func(f string) bool { return f == provider.SchemaSubtreeCoverageFeature })
	}
	for _, test := range []struct {
		name    string
		mutate  func(*protocol.Description, *protocol.ModelCoverage) *protocol.Description
		wantErr string
	}{
		{name: "a provider that does not list the feature", wantErr: `.*a subtree region needs schema.subtree-coverage`,
			mutate: func(d *protocol.Description, _ *protocol.ModelCoverage) *protocol.Description {
				d.Features = without(d.Features)
				return d
			}},
		{name: "an engine that does not list it", wantErr: `.*a subtree region needs schema.subtree-coverage`,
			mutate: func(d *protocol.Description, _ *protocol.ModelCoverage) *protocol.Description {
				d.Engines[0].Features = without(d.Engines[0].Features)
				return d
			}},
		{name: "a host that does not list it", wantErr: `.*a subtree region needs schema.subtree-coverage`,
			mutate: func(d *protocol.Description, _ *protocol.ModelCoverage) *protocol.Description {
				return provider.NegotiatedDescription(d, nil)
			}},
		{name: "a region that names no schema", wantErr: `.*a subtree region names no schema`,
			mutate: func(d *protocol.Description, coverage *protocol.ModelCoverage) *protocol.Description {
				coverage.Kinds[0].Regions[0].Scope.Schema = nil
				return d
			}},
		{name: "the same subtree twice", wantErr: `.*duplicate normalized model namespace`,
			mutate: func(d *protocol.Description, coverage *protocol.ModelCoverage) *protocol.Description {
				coverage.Kinds[0].Regions = append(coverage.Kinds[0].Regions, proto.CloneOf(coverage.Kinds[0].Regions[1]))
				return d
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, coverage := subtreeCoverage()
			description = test.mutate(description, coverage)

			err := provider.ValidateModelCoverage(description, "fixture", coverage)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestSchemaSeparatorDescription_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*protocol.Description)
		wantErr string
	}{
		{name: "the feature without a separator", wantErr: `.*schema.subtree-coverage requires a schema separator in the engine's identifier rules`,
			mutate: func(d *protocol.Description) { d.Engines[0].Identifiers.SchemaSeparator = "" }},
		{name: "a separator without the feature", wantErr: `.*a schema separator requires schema.subtree-coverage`,
			mutate: func(d *protocol.Description) {
				d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.SchemaSubtreeCoverageFeature })
			}},
		{name: "the engine's feature without the provider's", wantErr: `.*engine schema.subtree-coverage requires the provider's`,
			mutate: func(d *protocol.Description) {
				d.Features = slices.DeleteFunc(d.Features, func(f string) bool { return f == provider.SchemaSubtreeCoverageFeature })
			}},
		{name: "a separator of two characters", wantErr: `.*a schema separator is one character that is not a letter, a digit or a space`,
			mutate: func(d *protocol.Description) { d.Engines[0].Identifiers.SchemaSeparator = "//" }},
		{name: "a letter", wantErr: `.*a schema separator is one character that is not a letter, a digit or a space`,
			mutate: func(d *protocol.Description) { d.Engines[0].Identifiers.SchemaSeparator = "a" }},
		{name: "a digit", wantErr: `.*a schema separator is one character that is not a letter, a digit or a space`,
			mutate: func(d *protocol.Description) { d.Engines[0].Identifiers.SchemaSeparator = "1" }},
		{name: "a space", wantErr: `.*a schema separator is one character that is not a letter, a digit or a space`,
			mutate: func(d *protocol.Description) { d.Engines[0].Identifiers.SchemaSeparator = " " }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, _ := subtreeCoverage()
			test.mutate(description)

			err := provider.ValidateDescription(description, nil)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// A host that does not list the feature gets neither the feature nor the
// separator, so its description still validates; one that lists it gets both.
func TestSubtreeCoverageIsTwoWay(t *testing.T) {
	c := qt.New(t)
	description, _ := subtreeCoverage()

	unlisted := provider.NegotiatedDescription(description, nil)
	listed := provider.NegotiatedDescription(description, []string{provider.SchemaSubtreeCoverageFeature})

	c.Assert(provider.TwoWayFeatures(), qt.Contains, provider.SchemaSubtreeCoverageFeature)
	c.Assert(unlisted.GetFeatures(), qt.Not(qt.Contains), provider.SchemaSubtreeCoverageFeature)
	c.Assert(unlisted.GetEngines()[0].GetFeatures(), qt.Not(qt.Contains), provider.SchemaSubtreeCoverageFeature)
	c.Assert(unlisted.GetEngines()[0].GetIdentifiers().GetSchemaSeparator(), qt.Equals, "")
	c.Assert(provider.ValidateDescription(unlisted, nil), qt.IsNil)
	c.Assert(listed.GetEngines()[0].GetFeatures(), qt.Contains, provider.SchemaSubtreeCoverageFeature)
	c.Assert(listed.GetEngines()[0].GetIdentifiers().GetSchemaSeparator(), qt.Equals, "/")
	c.Assert(description.GetEngines()[0].GetIdentifiers().GetSchemaSeparator(), qt.Equals, "/")
}

// A filtered read's regions fit inside the exact namespaces its selectors
// bind, so it sends no subtree region even where subtrees are negotiated.
func TestFilteredInspectionRefusesASubtreeRegion(t *testing.T) {
	c := qt.New(t)
	description, request, snapshot := filteredInspection()
	withSubtrees(description)
	snapshot.Coverage.Kinds[0].Regions[0].Scope.SchemaSubtree = true

	values, err := provider.InspectionValues(description, request, snapshot)

	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*a filtered inspection sends no subtree region`)
	c.Assert(values, qt.IsNil)
}

// subtreeComparison is a comparison whose desired coverage claims the subtree
// of the root schema complete, under / as the comparison's separator.
func subtreeComparison() (*protocol.Description, *protocol.CompareModelsRequest) {
	description, request, _ := comparisonExchange()
	withSubtrees(description)
	request.Identifiers.Rules.SchemaSeparator = "/"
	request.Desired.Coverage.Kinds[0].Sources[0].Regions = []*protocol.ComparisonRegion{{
		Scope:     &protocol.ComparisonNamespace{Targets: []string{"fixture"}, Schema: &protocol.ComparisonPart{}, SchemaSubtree: true},
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE},
	}}
	return description, request
}

func TestComparisonSubtreeRegion_HappyPath(t *testing.T) {
	c := qt.New(t)
	description, request := subtreeComparison()

	err := provider.ValidateComparisonRequest(description, request)

	c.Assert(err, qt.IsNil)
}

func TestComparisonSubtreeRegion_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		mutate  func(*protocol.Description, *protocol.CompareModelsRequest)
		wantErr string
	}{
		{name: "rules without a separator", wantErr: `.*a subtree region needs schema.subtree-coverage`,
			mutate: func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
				r.Identifiers.Rules.SchemaSeparator = ""
			}},
		{name: "an engine that does not list the feature", wantErr: `.*a subtree region needs schema.subtree-coverage`,
			mutate: func(d *protocol.Description, _ *protocol.CompareModelsRequest) {
				d.Engines[0].Features = slices.DeleteFunc(d.Engines[0].Features, func(f string) bool { return f == provider.SchemaSubtreeCoverageFeature })
			}},
		{name: "a region that names no schema", wantErr: `.*a subtree region names no schema`,
			mutate: func(_ *protocol.Description, r *protocol.CompareModelsRequest) {
				r.Desired.Coverage.Kinds[0].Sources[0].Regions[0].Scope.Schema = nil
			}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request := subtreeComparison()
			test.mutate(description, request)

			err := provider.ValidateComparisonRequest(description, request)

			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// subtreePlanning is a planning request whose captured objects carry a
// complete subtree region over the root, under identifiers with separator.
func subtreePlanning(separator string) (*protocol.Description, *protocol.PlanFeaturesRequest) {
	description, request, _ := planningObjectsExchange()
	withSubtrees(description)
	request.Identifiers.Rules.SchemaSeparator = separator
	request.CurrentObjects.Coverage.Kinds[0].Sources[0].Regions = []*protocol.ComparisonRegion{{
		Scope:     &protocol.ComparisonNamespace{Targets: []string{"fixture"}, Schema: &protocol.ComparisonPart{}, SchemaSubtree: true},
		Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE},
	}}
	return description, request
}

// A planning request's captured objects are checked under the request's own
// identifiers, so a subtree region a read claimed reaches the planner where
// they carry the separator.
func TestPlanningCarriesASubtreeRegion_HappyPath(t *testing.T) {
	c := qt.New(t)
	description, request := subtreePlanning("/")

	err := provider.ValidatePlanningRequest(description, request)

	c.Assert(err, qt.IsNil)
}

// Where the identifiers carry no separator, the subtree region is refused.
func TestPlanningCarriesASubtreeRegion_FailurePath(t *testing.T) {
	c := qt.New(t)
	description, request := subtreePlanning("")

	err := provider.ValidatePlanningRequest(description, request)

	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(err, qt.ErrorMatches, `.*a subtree region needs schema.subtree-coverage`)
}
