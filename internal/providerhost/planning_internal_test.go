package providerhost

// White-box testing required: captured wire operands and frozen ownership are
// private adapter state, not observable in the public planning result.

import (
	"context"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

func planningFixture(c *qt.C) (modelPlanner, *resolvedModel) {
	target := yamlIdentityTarget()
	key := modelKey{kind: "example.org/fixture/value", representation: schemaext.Desired}
	contract := &modelContract{key: key, signature: "fixture", engines: map[string]*protocol.ValueModel{"fixture": target.engine.ValueModels[0]}}
	target.host.models = map[modelKey]*modelContract{key: contract}
	owner := &protocol.Reference{Engine: "fixture", Kind: "table", Name: "records"}
	value, err := contract.resolved(modelWire{
		Format: 1,
		Engine: "fixture",
	}, &protocol.NormalizedValue{Value: &protocol.Envelope{Kind: string(key.kind), Owner: owner, Payload: []byte("{}")}, NormalizedOwner: owner, CanonicalFormat: "fixture", CanonicalProjection: []byte("{}")})
	c.Assert(err, qt.IsNil)
	return modelPlanner{target: target, declaration: &protocol.ModelPlanning{Id: "plan"}}, value
}

func planningExcludedFacet(c *qt.C) schemaext.Facets {
	_, value := planningFixture(c)
	facets, err := schemaext.NewFacets(value)
	c.Assert(err, qt.IsNil)
	facets, err = facets.WithTargetScope(value.Kind(), "other")
	c.Assert(err, qt.IsNil)
	target, err := schemaext.NewTargetSelection("fixture")
	c.Assert(err, qt.IsNil)
	facets, err = facets.ForTarget(target)
	c.Assert(err, qt.IsNil)
	return facets
}

func planningParent() objectidentity.ID {
	return objectidentity.ID{
		Kind:     objectidentity.KindTable,
		Database: objectidentity.Part{Source: "database", Normalized: "database"},
		Catalog:  objectidentity.Part{Source: "catalog", Normalized: "catalog"},
		Schema:   objectidentity.Part{Source: "Main", Normalized: "main", Quoted: true},
		Name:     objectidentity.Part{Source: "Records", Normalized: "records", Quoted: true},
	}
}

func TestPlanningRequestRetainsCapturedCommonFacts(t *testing.T) {
	c := qt.New(t)
	service, _ := planningFixture(c)
	parent := planningParent()
	flag := capability.All()[0]
	request := featureplan.Request{
		Target:       "fixture",
		Identifiers:  identifier.Semantics{TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable},
		Capabilities: capability.Of(map[capability.Capability]bool{flag: false}),
		DatabasePath: "/tenant",
		ParentKinds:  []schemaext.Kind{"example.org/fixture/value"},
		Types:        featureplan.TypeContext{Desired: []schemaext.DefinedType{{ID: parent, References: []string{`"Main"."Status"`}}}},
		Tables: []featureplan.Table{
			{
				Subject: parent,
				Action:  featureplan.AlterTable,
				Desired: schemacapture.TableDeclaration{
					Table:   schemamodel.Table{Name: "Records", Schema: "Main", PrimaryKey: []string{"id"}},
					Fields:  []schemamodel.Field{{Name: "id", Type: "text", DefaultSet: true, Enum: []string{"new", "old"}}},
					Indexes: []schemamodel.Index{{Name: "by_id", Parts: []schemamodel.IndexPart{{Name: "id", Prefix: "0008", Desc: true}}, StorageParams: map[string]string{"fillfactor": "80"}}},
				},
				Current: schemacapture.TableObservation{
					Table: catalog.Table{Name: "Records", Schema: "Main", RowStatsUnknown: true, Columns: []catalog.Column{{Name: "id", DataType: "text", ColumnDefault: new(""), NumericScale: new(0)}}},
				},
			},
		},
	}
	wire, err := service.planningRequest(request)
	c.Assert(err, qt.IsNil)
	c.Assert(wire.Capabilities, qt.DeepEquals, map[string]bool{string(flag): false})
	c.Assert(wire.DatabasePath, qt.Equals, "/tenant")
	c.Assert(boundIdentity(wire.Tables[0].Subject), qt.DeepEquals, parent)
	c.Assert(wire.Tables[0].Action, qt.Equals, protocol.PlanningTable_ALTER)
	c.Assert(wire.Tables[0].Desired.Columns[0].DefaultSet, qt.IsTrue)
	c.Assert(wire.Tables[0].Desired.Columns[0].EnumValues, qt.DeepEquals, []string{"new", "old"})
	c.Assert(wire.Tables[0].Desired.Indexes[0].Parts[0].Prefix, qt.Equals, "0008")
	c.Assert(wire.Tables[0].Desired.Indexes[0].Parts[0].Descending, qt.IsTrue)
	c.Assert(wire.Tables[0].Current.Columns[0].DefaultExpression, qt.DeepEquals, new(""))
	c.Assert(wire.Tables[0].Current.Columns[0].NumericScale, qt.DeepEquals, new(int64(0)))
	c.Assert(wire.Tables[0].Current.Columns[0].NumericPrecision, qt.IsNil)
	c.Assert(wire.Tables[0].Current.RowStatsUnknown, qt.IsTrue)
	wire.Tables[0].Desired.Columns[0].EnumValues[0] = "changed"
	wire.Tables[0].Desired.Indexes[0].StorageParameters["fillfactor"] = "99"
	*wire.Tables[0].Current.Columns[0].DefaultExpression = "changed"
	wire.DesiredTypes[0].References[0] = "changed"
	c.Assert(request.Tables[0].Desired.Fields[0].Enum[0], qt.Equals, "new")
	c.Assert(request.Tables[0].Desired.Indexes[0].StorageParams["fillfactor"], qt.Equals, "80")
	c.Assert(*request.Tables[0].Current.Table.Columns[0].ColumnDefault, qt.Equals, "")
	c.Assert(request.Types.Desired[0].References[0], qt.Equals, `"Main"."Status"`)
}

func TestPlanningFacetsUseAuthoritativePrimaryKeySlot(t *testing.T) {
	c := qt.New(t)
	facets := planningExcludedFacet(c)
	for _, source := range []schemacapture.TableDeclaration{
		{Table: schemamodel.Table{Name: "Records", PrimaryKeyFacets: facets}},
		{Table: schemamodel.Table{Name: "Records"}, Constraints: []schemamodel.Constraint{{Type: " primary key ", Facets: facets}}},
	} {
		capture := &planningCapture{}
		records, err := capture.declaredFacets(planningParent(), source)
		c.Assert(err, qt.IsNil)
		c.Assert(records, qt.HasLen, 1)
		c.Assert(records[0].Subject, qt.DeepEquals, objectidentity.PrimaryKeyOf(planningParent()))
		c.Assert(records[0].Values.TargetScope("example.org/fixture/value"), qt.DeepEquals, []string{"other"})
	}
}

func TestPlanningRefusesFacetsOutsideAuthoritativePrimaryKeySlot(t *testing.T) {
	c := qt.New(t)
	facets := planningExcludedFacet(c)
	for _, source := range []schemacapture.TableDeclaration{
		{Table: schemamodel.Table{Name: "Records", PrimaryKeyFacets: facets}, Constraints: []schemamodel.Constraint{{Type: "PRIMARY KEY"}}},
		{Table: schemamodel.Table{Name: "Records"}, Constraints: []schemamodel.Constraint{{Type: "PRIMARY KEY"}, {Type: "PRIMARY KEY", Facets: facets}}},
	} {
		capture := &planningCapture{}
		records, err := capture.declaredFacets(planningParent(), source)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(records, qt.IsNil)
	}
	capture := &planningCapture{}
	records, err := capture.observedFacets(planningParent(), schemacapture.TableObservation{Constraints: []catalog.Constraint{{Type: "PRIMARY KEY"}, {Type: "PRIMARY KEY", Facets: facets}}})
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(records, qt.IsNil)
}

func TestPlanningColumnRetainsExactOperandsAndExclusions(t *testing.T) {
	c := qt.New(t)
	capture := &planningCapture{}
	facets := planningExcludedFacet(c)
	source := &ast.ColumnNode{
		Name:               "id",
		Type:               "text",
		TypeRawSQL:         true,
		TypeIsDeclaredText: true,
		EnumType:           true,
		Default:            &ast.DefaultValue{ValueSet: true},
		ForeignKey:         &ast.ForeignKeyRef{Table: "parents", Columns: []string{"id", "tenant"}, Deferrable: true, Initially: "DEFERRED", Match: "FULL", Facets: facets},
		Facets:             facets,
	}
	column, err := capture.columnOperand(source)
	c.Assert(err, qt.IsNil)
	c.Assert(column.Default, qt.IsNotNil)
	c.Assert(column.Default.ValueSet, qt.IsTrue)
	c.Assert(column.Default.Value, qt.Equals, "")
	c.Assert(column.TypeRawSql, qt.IsTrue)
	c.Assert(column.TypeIsDeclaredText, qt.IsTrue)
	c.Assert(column.EnumType, qt.IsTrue)
	c.Assert(column.Facets[0].Value, qt.IsNil)
	c.Assert(column.Facets[0].Targets, qt.DeepEquals, []string{"other"})
	c.Assert(column.ForeignKey.Facets[0].Targets, qt.DeepEquals, []string{"other"})
	c.Assert(column.ForeignKey.Deferrable, qt.IsTrue)
	column.ForeignKey.Columns[0] = "changed"
	column.ForeignKey.Facets[0].Targets[0] = "changed"
	c.Assert(source.ForeignKey.Columns[0], qt.Equals, "id")
	c.Assert(source.ForeignKey.Facets.TargetScope("example.org/fixture/value"), qt.DeepEquals, []string{"other"})
	source.Default = nil
	column, err = capture.columnOperand(source)
	c.Assert(err, qt.IsNil)
	c.Assert(column.Default, qt.IsNil)
}

func TestPlanningCaptureRefusesUnsupportedFacts(t *testing.T) {
	c := qt.New(t)
	capture := &planningCapture{}
	_, err := capture.declaration(planningParent(), schemacapture.TableDeclaration{Fields: []schemamodel.Field{{Name: "id", Overrides: map[string]map[string]string{"fixture": {"type": "text"}}}}})
	c.Assert(err, qt.ErrorMatches, `(?s).*Field.Overrides.*`)
	_, err = capture.table(featureplan.Table{Desired: schemacapture.TableDeclaration{Fields: []schemamodel.Field{{Name: "orphan"}}}})
	c.Assert(err, qt.ErrorMatches, `(?s).*TableDeclaration.Fields.*`)
}

func TestPlanningCommonMetadataRejectsUnknownEnums(t *testing.T) {
	for _, source := range []featureplan.CommonStep{
		{Transaction: plangraph.Transaction("invalid")},
		{Impact: schemaext.Effect{Impact: schemaext.Impact("invalid")}},
		{Effects: []plangraph.Effect{{Action: plangraph.Action("invalid")}}},
	} {
		c := qt.New(t)
		result, err := planningMetadata(source)
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(result, qt.IsNil)
	}
}

func TestPlanningOwnershipChangesFrozenContracts(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ModelPlanning)
	}{
		{"service", func(d *protocol.ModelPlanning) { d.Id = "other" }},
		{"changes", func(d *protocol.ModelPlanning) { d.ChangeKinds[0] = "other" }},
		{"parents", func(d *protocol.ModelPlanning) { d.ParentKinds[0] = "other" }},
		{"operations", func(d *protocol.ModelPlanning) { d.OperationKinds[0] = "other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := yamlIdentityTarget()
			target.engine.Planning = []*protocol.ModelPlanning{{Id: "plan", ChangeKinds: []string{"change"}, ParentKinds: []string{"parent"}, OperationKinds: []string{"operation"}}}
			contract := &modelContract{engines: map[string]*protocol.ValueModel{"fixture": target.engine.ValueModels[0]}}
			before, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			definition, err := target.host.modelDefinition(contract)
			c.Assert(err, qt.IsNil)
			test.mutate(target.engine.Planning[0])
			after, err := target.identity(nil)
			c.Assert(err, qt.IsNil)
			changed, err := target.host.modelDefinition(contract)
			c.Assert(err, qt.IsNil)
			c.Assert(after.ContractSHA256, qt.Not(qt.Equals), before.ContractSHA256)
			c.Assert(string(changed), qt.Not(qt.Equals), string(definition))
		})
	}
}

func TestPlanningContractIgnoresDeclarationOrder(t *testing.T) {
	c := qt.New(t)
	first := &protocol.ModelPlanning{Id: "a", ChangeKinds: []string{"b", "a"}, ParentKinds: []string{"d", "c"}, OperationKinds: []string{"f", "e"}}
	second := &protocol.ModelPlanning{Id: "b"}
	before := planningContracts([]*protocol.ModelPlanning{first, second})
	reordered := proto.CloneOf(first)
	reordered.ChangeKinds, reordered.ParentKinds, reordered.OperationKinds = []string{"a", "b"}, []string{"c", "d"}, []string{"e", "f"}
	c.Assert(planningContracts([]*protocol.ModelPlanning{second, reordered}), qt.DeepEquals, before)
}

func TestPlanningDiagnosticPreservesInputZeroAndKind(t *testing.T) {
	c := qt.New(t)
	change := boundPlanningDiagnostic(&protocol.PlanningDiagnostic{
		Code:    protocol.PlanningDiagnostic_INVALID_SCHEMA,
		Kind:    "table",
		Object:  "records",
		Feature: "retention",
		Message: "Cannot plan",
		Input:   &protocol.PlanningDiagnostic_Change{Change: 0},
	})
	c.Assert(change.Change, qt.DeepEquals, new(0))
	c.Assert(change.Parent, qt.IsNil)
	c.Assert(change.Problem, qt.DeepEquals, schemavalidation.Diagnostic{Code: schemavalidation.InvalidSchema, Kind: "table", Object: "records", Feature: "retention", Message: "Cannot plan"})
	parent := boundPlanningDiagnostic(&protocol.PlanningDiagnostic{Input: &protocol.PlanningDiagnostic_Parent{Parent: 0}})
	c.Assert(parent.Parent, qt.DeepEquals, new(0))
	c.Assert(parent.Change, qt.IsNil)
}

func TestPlanningRequestPreservesCoverageSourcesAndExclusions(t *testing.T) {
	c := qt.New(t)
	service, value := planningFixture(c)
	contract := service.target.host.models[modelKey{kind: value.Kind(), representation: schemaext.Desired}]
	complete, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete})
	c.Assert(err, qt.IsNil)
	unknown, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Uninspected, Reason: "Capture did not inspect this namespace"})
	c.Assert(err, qt.IsNil)
	knowledge, err := complete.Intersect(unknown)
	c.Assert(err, qt.IsNil)
	coverage, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{
		{Model: schemaext.CodecIdentity{Owner: service.target.host.description.ProviderId, Kind: value.Kind(), Representation: schemaext.Desired, Version: 1, Definition: contract.signature}, Knowledge: knowledge},
		{Model: schemaext.CodecIdentity{Owner: "example.org/other", Kind: "example.org/other/facet", Representation: schemaext.Desired, Version: 1, Definition: contract.signature}, Knowledge: complete},
	}, nil)
	c.Assert(err, qt.IsNil)
	capture := &planningCapture{comparisonCapture: comparisonCapture{target: service.target, request: &protocol.CompareModelsRequest{Engine: "fixture"}}}
	declaration, err := capture.declaration(planningParent(), schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "Records", Facets: planningExcludedFacet(c)}, FeatureCoverage: coverage})
	c.Assert(err, qt.IsNil)
	c.Assert(coverage.KindRecords(), qt.HasLen, 2)
	c.Assert(declaration.Features.Objects.Coverage.Kinds, qt.HasLen, 1)
	c.Assert(declaration.Features.Facets.Coverage.Kinds, qt.HasLen, 1)
	c.Assert(declaration.Features.Objects.Coverage.Kinds[0].Sources, qt.HasLen, 2)
	c.Assert(declaration.Features.Facets.Coverage.Kinds[0].Sources, qt.HasLen, 2)
	c.Assert(declaration.Features.Facets.Records[0].Value, qt.IsNil)
	c.Assert(declaration.Features.Facets.Records[0].Targets, qt.DeepEquals, []string{"other"})
	restored, err := service.target.boundCoverage(schemaext.Desired, declaration.Features.Facets.Coverage)
	c.Assert(err, qt.IsNil)
	c.Assert(restored.Lookup(value.Kind(), planningParent()).State, qt.Equals, schemaext.Uninspected)
}

func TestPlanningCoverageRoutingRetainsOwnedIdentityRefusals(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*schemaext.CodecIdentity)
	}{
		{"unknown owned model", func(model *schemaext.CodecIdentity) { model.Kind = "example.org/fixture/unknown" }},
		{"changed definition", func(model *schemaext.CodecIdentity) { model.Definition = "sha256:changed" }},
		{"changed owner", func(model *schemaext.CodecIdentity) { model.Owner = "example.org/other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service, value := planningFixture(c)
			contract := service.target.host.models[modelKey{kind: value.Kind(), representation: schemaext.Desired}]
			model := schemaext.CodecIdentity{Owner: service.target.host.description.ProviderId, Kind: value.Kind(), Representation: schemaext.Desired, Version: 1, Definition: contract.signature}
			test.change(&model)
			complete, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete})
			c.Assert(err, qt.IsNil)
			coverage, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{{Model: model, Knowledge: complete}}, nil)
			c.Assert(err, qt.IsNil)
			capture := &planningCapture{comparisonCapture: comparisonCapture{target: service.target, request: &protocol.CompareModelsRequest{Engine: "fixture"}}}
			_, err = capture.declaration(planningParent(), schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "Records"}, FeatureCoverage: coverage})
			c.Assert(err, qt.ErrorIs, schemaext.ErrIncompatibleCodec)
		})
	}
}

func TestPlanningCoverageRoutesOnlyTheSelectedEngine(t *testing.T) {
	c := qt.New(t)
	service, value := planningFixture(c)
	current := service.target.host.models[modelKey{kind: value.Kind(), representation: schemaext.Desired}]
	const otherKind schemaext.Kind = "example.org/fixture/other-engine"
	other := *current
	other.key = modelKey{kind: otherKind, representation: schemaext.Desired}
	other.engines = map[string]*protocol.ValueModel{"other": {Kind: string(otherKind)}}
	service.target.host.models[other.key] = &other
	complete, err := schemaext.NewScopedKnowledge(schemaext.Knowledge{State: schemaext.Complete})
	c.Assert(err, qt.IsNil)
	coverage, err := schemaext.NewCoverage(schemaext.Desired, []schemaext.KindCoverage{
		{Model: schemaext.CodecIdentity{Owner: service.target.host.description.ProviderId, Kind: value.Kind(), Representation: schemaext.Desired, Version: 1, Definition: current.signature}, Knowledge: complete},
		{Model: schemaext.CodecIdentity{Owner: service.target.host.description.ProviderId, Kind: otherKind, Representation: schemaext.Desired, Version: 1, Definition: other.signature}, Knowledge: complete},
	}, nil)
	c.Assert(err, qt.IsNil)
	capture := &planningCapture{comparisonCapture: comparisonCapture{target: service.target, request: &protocol.CompareModelsRequest{Engine: "fixture"}}}
	declaration, err := capture.declaration(planningParent(), schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "Records"}, FeatureCoverage: coverage})
	c.Assert(err, qt.IsNil)
	c.Assert(declaration.Features.Facets.Coverage.Kinds, qt.HasLen, 1)
	c.Assert(declaration.Features.Facets.Coverage.Kinds[0].Model.Kind, qt.Equals, string(value.Kind()))
	c.Assert(coverage.KindRecords(), qt.HasLen, 2)
}

func TestPlanningRequestRefusesMixedCaptureProfiles(t *testing.T) {
	c := qt.New(t)
	service, value := planningFixture(c)
	facets, err := schemaext.NewFacets(value)
	c.Assert(err, qt.IsNil)
	changed := value.cloneModel()
	changed.wire.Features = []string{"other-profile"}
	other, err := schemaext.NewFacets(changed)
	c.Assert(err, qt.IsNil)
	request := featureplan.Request{
		Target:      "fixture",
		Identifiers: identifier.Semantics{TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable},
		Tables: []featureplan.Table{
			{Subject: planningParent(), Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "Records", Facets: facets}, Fields: []schemamodel.Field{{Name: "id", Type: "text", Facets: other}}}},
		},
	}
	wire, err := service.planningRequest(request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(wire, qt.IsNil)
	request.Tables[0].Desired.Fields[0].Facets = facets
	wire, err = service.planningRequest(request)
	c.Assert(err, qt.IsNil)
	c.Assert(wire.TargetFeatures, qt.HasLen, 0)
	c.Assert(wire.Tables[0].Desired.Features.Facets.Records, qt.HasLen, 2)
}

func TestPlanningReplyBindsOperationsAndGraphMetadata(t *testing.T) {
	for _, test := range []struct {
		name  string
		wire  protocol.PlanningOperation_Phase
		phase featureplan.Phase
	}{
		{"dependent", protocol.PlanningOperation_DEPENDENT, featureplan.PhaseDependent},
		{"lifecycle", protocol.PlanningOperation_LIFECYCLE, featureplan.PhaseLifecycle},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			service, value := planningFixture(c)
			key := modelKey{kind: "example.org/fixture/operation", representation: schemaext.Operation}
			service.target.host.models[key] = &modelContract{key: key, signature: "operation", engines: map[string]*protocol.ValueModel{"fixture": {Kind: string(key.kind), Representation: protocol.ValueInput_OPERATION}}}
			operation := proto.CloneOf(value.normalized)
			operation.Value.Kind = string(key.kind)
			operation.OperationSemantics = &protocol.OperationSemantics{Impact: protocol.ChangeSemantics_DESTRUCTIVE, Reason: "Drops retained state"}
			parent := comparisonIdentity(planningParent())
			id := &protocol.StepIdentity{Owner: "example.org/fixture", Name: "rewrite"}
			source := &protocol.StepIdentity{Owner: "ptah.run/common", Name: "add-column"}
			reply := &protocol.PlannedFeatures{
				Complete: true,
				Contributions: []*protocol.PlanningContribution{
					{
						Owner: id.Owner,
						Steps: []*protocol.PlanningStep{
							{
								Id:        id,
								Operation: &protocol.PlanningOperation{Value: operation, Role: protocol.NativeOperation_ALTER_TABLE, Parent: parent, Notes: []string{"Owner requires this order"}, Phase: test.wire},
								Metadata:  &protocol.PlanningMetadata{Transaction: protocol.PlanningMetadata_REQUIRED, Placement: protocol.PlanningMetadata_EARLY, Impact: protocol.ChangeSemantics_DESTRUCTIVE, Reason: "Drops retained state", Effects: []*protocol.PlanningEffect{{Subject: parent, Action: protocol.PlanningEffect_ALTER}}},
							},
						},
						Dependencies: []*protocol.PlanningDependency{{Before: source, After: id}},
					},
				},
				Rewrites: []*protocol.PlanningRewrite{{Sources: []*protocol.StepIdentity{source}, Replacement: id}},
				Changes:  []*protocol.PlanningReceipt{{Subject: parent, Kind: "example.org/fixture/change", Strategy: "Rewrite", Steps: []*protocol.StepIdentity{id}}},
				Parents: []*protocol.PlanningParentReceipt{
					{Action: protocol.PlanningTable_ALTER, Receipt: &protocol.PlanningReceipt{Subject: parent, Kind: "example.org/fixture/value", Strategy: "Preserved", Steps: []*protocol.StepIdentity{id}}},
				},
			}
			result, err := service.target.planningReply(&protocol.PlanFeaturesRequest{Engine: "fixture", TargetFeatures: []string{"captured"}}, reply)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			step := result.Contributions[0].Steps[0]
			c.Assert(step.Transaction, qt.Equals, plangraph.TransactionRequired)
			c.Assert(step.Placement, qt.Equals, plangraph.PlacementEarly)
			c.Assert(step.Impact, qt.DeepEquals, schemaext.Effect{Impact: schemaext.Destructive, Reason: "Drops retained state"})
			c.Assert(step.Effects, qt.DeepEquals, []plangraph.Effect{{Subject: planningParent(), Action: plangraph.Alter}})
			c.Assert(step.Payload.Role, qt.Equals, ast.AlterExtension)
			c.Assert(step.Payload.Phase, qt.Equals, test.phase)
			c.Assert(step.Payload.Parent, qt.DeepEquals, planningParent())
			c.Assert(step.Payload.Payload.(*resolvedOperation).model.wire.Features, qt.DeepEquals, []string{"captured"})
			c.Assert(result.Parents[0].Action, qt.Equals, featureplan.AlterTable)
			c.Assert(result.Rewrites[0].Sources, qt.DeepEquals, []plangraph.StepID{{Owner: source.Owner, Name: source.Name}})
			c.Assert(result.Contributions[0].Dependencies[0].Before, qt.DeepEquals, result.Rewrites[0].Sources[0])
			c.Assert(result.Changes[0].Steps[0], qt.DeepEquals, step.ID)
			reply.Contributions[0].Steps[0].Operation.Notes[0] = "changed"
			reply.Contributions[0].Steps[0].Operation.Value.Value.Payload[0] = 'x'
			c.Assert(step.Payload.Notes, qt.DeepEquals, []string{"Owner requires this order"})
			c.Assert(string(step.Payload.Payload.(*resolvedOperation).model.wire.Payload), qt.Equals, "{}")
		})
	}
}

// commonPlanRecorder observes the production host request without a process.
// Transport behavior is covered by integration/providercli.
type commonPlanRecorder struct {
	*engine.Runtime
	request featureplan.Request
}

func (r *commonPlanRecorder) PlanFeatures(_ context.Context, request featureplan.Request) (featureplan.Result, error) {
	r.request = request
	return featureplan.Result{Complete: true}, nil
}

func TestCommonPlanRetainsUnchangedContextAndExactColumnOperand(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonExact, ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	builder := objectidentity.NewBuilder(semantics)
	parent := builder.TableParts("main", "Orders.Log")
	unchanged := schemapreparation.Table{Subject: builder.TableParts("main", "unchanged"),
		Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "unchanged", Schema: "main"}, Fields: []schemamodel.Field{{Name: "kept", Type: "INTEGER"}}},
		Current: schemacapture.TableObservation{Table: catalog.Table{Name: "unchanged", Schema: "main", Comment: "captured"}},
	}
	modified := difftypes.TableDiff{TableName: `main."Orders.Log"`,
		Desired:      schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "Orders.Log", Schema: "main"}, Fields: []schemamodel.Field{{Name: "added", Type: "INTEGER"}}},
		Current:      schemacapture.TableObservation{Table: catalog.Table{Name: "Orders.Log", Schema: "main", Columns: []catalog.Column{{Name: "removed", DataType: "TEXT", ColumnDefault: new("captured")}}}},
		ColumnsAdded: difftypes.ColumnChanges{{Name: "added", Type: "TEXT"}}, ColumnsRemoved: difftypes.ColumnChanges{{Name: "removed", Type: "TEXT"}},
	}
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &semantics, CurrentDatabasePath: "/captured", TablesModified: []difftypes.TableDiff{modified},
		TablePreparation: &schemapreparation.Capture{Prepared: []schemapreparation.Table{unchanged}}}
	base, err := engine.New()
	c.Assert(err, qt.IsNil)
	runtime := &commonPlanRecorder{Runtime: base}
	capabilities := capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: false})
	nodes, err := (commonPlanner{target: "fixture", options: planext.Options{Capabilities: capabilities}, assessParents: true, commonTables: true}).GenerateMigrationAST(c.Context(), runtime, diff)
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 2)
	c.Assert(runtime.request.DatabasePath, qt.Equals, "/captured")
	c.Assert(runtime.request.Capabilities, qt.DeepEquals, capabilities)
	c.Assert(runtime.request.Tables, qt.HasLen, 2)
	c.Assert(runtime.request.Tables[0].Action, qt.Equals, featureplan.ParentAction(""))
	c.Assert(runtime.request.Tables[0].Current.Table.Comment, qt.Equals, "captured")
	c.Assert(runtime.request.Tables[1].Action, qt.Equals, featureplan.AlterTable)
	c.Assert(runtime.request.Tables[1].Subject, qt.DeepEquals, parent)
	c.Assert(runtime.request.Tables[1].Desired.Fields[0].Type, qt.Equals, "INTEGER")
	index := slices.IndexFunc(runtime.request.CommonSteps, func(step featureplan.CommonStep) bool { return step.AddedColumn != nil })
	c.Assert(index, qt.Not(qt.Equals), -1)
	operand := runtime.request.CommonSteps[index].AddedColumn
	c.Assert(operand.Type, qt.Equals, "TEXT")
	c.Assert(operand.Name, qt.Equals, "added")
	c.Assert(nodes[0].(*ast.AlterTableNode).Operations[0], qt.Satisfies, func(op ast.AlterOperation) bool { _, ok := op.(*ast.AddColumnOperation); return ok })
	c.Assert(nodes[1].(*ast.AlterTableNode).Operations[0], qt.Satisfies, func(op ast.AlterOperation) bool { _, ok := op.(*ast.DropColumnOperation); return ok })
	runtime.request.Tables[0].Desired.Fields[0].Type = "changed"
	c.Assert(diff.TablePreparation.Prepared[0].Desired.Fields[0].Type, qt.Equals, "INTEGER")
}

func TestCommonPlanningContextRefusesAmbiguousParents(t *testing.T) {
	for _, test := range []struct {
		name  string
		names []string
	}{
		{"duplicate parent", []string{"same", "same"}},
		{"name disagrees", []string{"other"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}
			for _, name := range test.names {
				diff.TablesAdded = append(diff.TablesAdded, difftypes.TableCreation{Name: name, Table: schemamodel.Table{Name: "same"}})
			}
			request, names, err := (commonPlanner{target: "fixture"}).planningContext(diff)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(request, qt.DeepEquals, featureplan.Request{})
			c.Assert(names, qt.IsNil)
		})
	}
}

// phasePlanRuntime returns an early operation with no resource dependency, so
// its position measures the host's phase window alone.
type phasePlanRuntime struct {
	*engine.Runtime
	phase featureplan.Phase
}

func (r *phasePlanRuntime) PlanFeatures(context.Context, featureplan.Request) (featureplan.Result, error) {
	return featureplan.Result{Complete: true, Contributions: []plangraph.Contribution[featureplan.Operation]{{
		Owner: "example.org/phase", Steps: []plangraph.Step[featureplan.Operation]{{
			ID:        plangraph.StepID{Owner: "example.org/phase", Name: "native"},
			Payload:   featureplan.Operation{Role: ast.StatementExtension, Payload: &creationOperation{name: "native"}, Phase: r.phase},
			Placement: plangraph.PlacementEarly,
		}},
	}}}, nil
}

func TestCommonPlannerKeepsDependentPhaseBetweenCommonChanges(t *testing.T) {
	for _, test := range []struct {
		phase          featureplan.Phase
		native, create int
	}{
		{featureplan.PhaseDependent, 1, 0},
		{featureplan.PhaseLifecycle, 0, 1},
	} {
		t.Run(string(test.phase), func(t *testing.T) {
			c := qt.New(t)
			base, err := engine.New()
			c.Assert(err, qt.IsNil)
			rules := identifier.ForDialect("sqlite")
			diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
				TablesAdded:   []difftypes.TableCreation{{Name: "new", Table: schemamodel.Table{Name: "new"}, Fields: []schemamodel.Field{{Name: "id", Type: "INTEGER"}}}},
				TablesRemoved: difftypes.TableRemovals{{Name: "old", Current: schemacapture.TableObservation{Table: catalog.Table{Name: "old"}}}},
			}
			nodes, err := (commonPlanner{target: "fixture", commonTables: true}).GenerateMigrationAST(c.Context(), &phasePlanRuntime{Runtime: base, phase: test.phase}, diff)
			c.Assert(err, qt.IsNil)
			c.Assert(nodes, qt.HasLen, 3)
			c.Assert(nodes[test.native].(*ast.ExtensionStatement).Payload.(*creationOperation).name, qt.Equals, "native")
			c.Assert(nodes[test.create].(*ast.CreateTableNode).Name, qt.Equals, "new")
			c.Assert(nodes[2].(*ast.DropTableNode).Name, qt.Equals, "old")
		})
	}
}
