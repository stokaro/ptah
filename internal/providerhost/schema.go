package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/planext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/core/selectext"
	"ptah.run/engine"
)

// Provider returns services for the explicitly selected target declarations.
// No built-in renderer, planner, or capability preset supplies missing behavior.
func (h *Host) Provider() engine.Provider {
	result := engine.Provider{ID: h.description.GetProviderId(), Codecs: slices.Clone(h.modelCodecs), YAML: h.yamlExtension(), Annotations: h.annotationExtensions(), SQL: h.sqlExtensions(),
		HCL: h.hclExtensions()}
	for _, codec := range result.Codecs {
		contract := h.models[modelKey{kind: codec.Prototype.Kind(), representation: codec.Representation}]
		if contract.displayName != "" {
			result.Reporting = append(result.Reporting, engine.Reporting{Representation: contract.key.representation,
				Definitions: []schemaext.ReportDefinition{{Kind: contract.key.kind, DisplayName: contract.displayName, Metrics: contract.metricDefinitions()}},
				Service:     modelReporter{host: h, contract: contract}})
		}
	}
	for i := range result.Codecs {
		result.Codecs[i].Definition = slices.Clone(result.Codecs[i].Definition)
	}
	if len(result.Codecs) > 0 {
		result.ModelDecoder = &schemaext.BatchDecoder{Decode: h.decodeModels}
	}
	if h.selections != nil {
		result.Selections = []selectext.Extension{h.selections.extension}
	}
	for _, declared := range h.description.GetEngines() {
		selected := h.targets[declared.GetId()]
		result.Targets = append(result.Targets, selected.registration())
		selected.registerComparisons(&result)
		selected.registerPlanning(&result)
		selected.registerReversals(&result)
		selected.registerDeclarations(&result)
		selected.registerContextBindings(&result)
		if selected.supportsLint() {
			result.Lint = append(result.Lint, selected.lint())
		}
		if slices.Contains(h.description.GetFeatures(), provider.ConvertValuesFeature) {
			if kinds := h.conversionKinds(declared.GetId()); len(kinds) > 0 {
				result.Conversions = append(result.Conversions, engine.Conversion{Target: declared.GetId(), Kinds: kinds, Service: selected})
			}
		}
	}
	return result
}

func (t *target) registration() engine.Target {
	result := engine.Target{Name: t.engine.GetId(), Aliases: slices.Clone(t.engine.GetAliases()), Rendering: t, Splitter: t, Data: t.dataServices(),
		Planner: func(options planext.Options) planext.Planner {
			return commonPlanner{selections: t.host.selections, options: options, transactions: t.supportsTransactions(), target: t.engine.GetId(),
				commonTables: t.supportsCommonTables(), commonObjects: len(t.commonKinds()) > 0, comments: t.commonKinds()[protocol.CommonObjectSupport_COMMENT],
				indexes: t.offers(provider.ObservedIndexesFeature), renames: t.offers(provider.IndexRenamesFeature),
				observedKeys: t.offers(provider.ObservedPrimaryKeysFeature), keys: t.offers(provider.PrimaryKeyChangesFeature),
				observedConstraints: t.offers(provider.ObservedConstraintsFeature), constraints: t.offers(provider.ConstraintChangesFeature),
				observedUnique: t.offers(provider.ObservedUniqueConstraintsFeature), namespaces: t.carriesNamespaces(), assessParents: t.assessesParents()}
		},
	}
	if t.offers(provider.DatabaseContextFeature) {
		result.DatabaseContext = t
	}
	if t.supportsCommonTables() {
		result.SchemaRendering, result.Validation = t, t
		result.Preparation, result.Creations = t, t
	}
	if t.rendersAuthoredSchemas() {
		result.SchemaRendering = t
	}
	if slices.Contains(t.engine.GetFeatures(), provider.ValidateSchemaFeature) {
		result.Validation = t
	}
	if slices.Contains(t.engine.GetFeatures(), provider.PrepareTablesFeature) {
		result.Preparation = t
	}
	if slices.Contains(t.engine.GetFeatures(), provider.ProjectTableCreationsFeature) {
		result.Creations = t
	}
	if slices.Contains(t.engine.GetFeatures(), provider.ColumnsFeature) {
		result.Comparison.ColumnService = columnService{target: t}
	}
	if t.offers(provider.CheckEquivalenceFeature) {
		result.Comparison.Checks = checkService{target: t}
	}
	if t.offers(provider.ObservedConstraintsFeature) {
		result.Constraints = constraintProjection{}
	}
	result.Comparison.Namespaces = t.carriesNamespaces()
	if t.offers(provider.ObservedIndexesFeature) {
		result.Comparison.Indexes = observedIndexRules{methods: t.offers(provider.IndexMethodsFeature), facets: t.offers(provider.ObservedIndexFacetsFeature),
			nulls: t.offers(provider.IndexNullsOrderFeature)}
	}
	result.Lowering = providerLowering{target: t, uniqueIndexes: t.offers(provider.ObservedIndexesFeature)}
	if t.offers(provider.IndexMethodsFeature) {
		result.ReadBack = indexKindReadBack{}
	}
	if t.supportsLint() {
		result.Annotator = t
	}
	if len(t.commonKinds()) > 0 {
		result.Spelling = commonSpeller{target: t}
	}
	return result
}

// commonDeclaration is a table declaration as the common table message holds
// it: without its comments on an engine that carries them on the
// common-object wire, where a step of their own sets them. An engine that
// does not carry them keeps them, and commonTable refuses them.
func (t *target) commonDeclaration(declaration schemacapture.TableDeclaration) schemacapture.TableDeclaration {
	if t.commonKinds()[protocol.CommonObjectSupport_COMMENT] {
		declaration, _ = creationComments(declaration)
	}
	return declaration
}

// assessesParents reports an engine with a planning service that owns table
// settings, which the host hands every table it creates, changes or drops.
func (t *target) assessesParents() bool {
	return slices.ContainsFunc(t.engine.GetPlanning(), func(assignment *protocol.ModelPlanning) bool { return len(assignment.GetParentKinds()) > 0 })
}

// rendersAuthoredSchemas reports an engine whose whole schema renders through
// its provider's planning, as a plan that creates every declared object: one
// with declaration services, or one whose provider owns the lifecycle of its
// tables, which leaves the host no common table to render. A schema rendering
// of either needs schema validation; renderDeclarations names it when it is
// missing.
func (t *target) rendersAuthoredSchemas() bool {
	return len(t.engine.GetDeclarations()) > 0 || (!t.supportsCommonTables() && t.assessesParents())
}

// supportsCommonTables gates shared table interpretation, not native services.
// Rendering and planning also use this contract because they accept both common
// table changes and provider-owned operations.
func (t *target) supportsCommonTables() bool {
	return slices.Contains(t.engine.GetFeatures(), provider.CommonTableIdentityFeature)
}

func (t *target) requireCommonTables() error {
	if !t.supportsCommonTables() {
		return unsupported("common table operations without " + provider.CommonTableIdentityFeature)
	}
	return nil
}

func (t *target) RenderSchema(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	if request.Schema == nil {
		return renderer.SchemaResult{}, fmt.Errorf("provider schema is required")
	}
	// A render creates the enum columns a plan creates; see enumColumns.
	schema, err := t.enumColumns(request.Schema)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	request.Schema = schema
	if t.rendersAuthoredSchemas() {
		return t.renderDeclarations(ctx, request)
	}
	if _, err := t.declaredCommonObjects(request.Schema); err != nil {
		return renderer.SchemaResult{}, err
	}
	nodes, err := t.commonSchema(request.Schema)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	rendered, err := t.Render(ctx, renderer.Request{Target: request.Target, Capabilities: request.Capabilities, Nodes: nodes, Form: renderer.FormText})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	fragments, err := rendered.SQLFragments()
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	return renderer.SchemaResult{Complete: true, Statements: fragments}, nil
}

func (t *target) ValidateSchema(ctx context.Context, request schemavalidation.Request) (schemavalidation.Result, error) {
	if slices.Contains(t.engine.GetFeatures(), provider.ValidateSchemaFeature) {
		return t.validateOwnedSchema(ctx, request)
	}
	_, err := t.RenderSchema(ctx, renderer.SchemaRequest{Target: request.Target, Capabilities: request.Capabilities, Identifiers: request.Identifiers, Schema: request.Schema})
	if err != nil {
		return schemavalidation.Result{}, err
	}
	return schemavalidation.Result{Complete: true}, nil
}

func (t *target) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	if slices.Contains(t.engine.GetFeatures(), provider.PrepareTablesFeature) {
		return t.prepareOwnedTables(ctx, request)
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	if request.Target != t.engine.GetId() {
		return schemapreparation.Result{}, fmt.Errorf("provider preparation target changed")
	}
	result := request.Clone()
	for i := range result.Tables {
		table := &result.Tables[i]
		node, err := commonTable(t.commonDeclaration(table.Desired))
		if err != nil {
			return schemapreparation.Result{}, err
		}
		for j := range table.Desired.Fields {
			table.Desired.Fields[j].Primary = node.Columns[j].Primary
		}
		table.ColumnPrimaryKeysPrepared = true
	}
	return schemapreparation.Result{Complete: true, Tables: result.Tables}, nil
}

// ProjectTableCreations selects owned prediction or the explicit common
// identity contract. Neither policy falls back to a built-in engine.
//
// The common contract answers from the declaration alone, so it refuses facts
// other than the engine's own, which it could not honor. The engine's own are
// what the conversion of a source reads from capability.ForDialect once a
// provider or its first session has stated them, and they change nothing here.
func (t *target) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	if slices.Contains(t.engine.GetFeatures(), provider.ProjectTableCreationsFeature) {
		return t.projectOwnedTableCreations(ctx, request)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	if request.Target != t.engine.GetId() || !request.Capabilities.Equal(capability.ForDialect(t.engine.GetId())) {
		return schemaprojection.TableCreationResult{}, unsupported("creation target metadata")
	}
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, table := range request.Tables {
		node, err := commonTable(t.commonDeclaration(table.Declaration))
		if err != nil {
			return schemaprojection.TableCreationResult{}, err
		}
		created := schemaprojection.TableCreation{Subject: table.Subject, ColumnPrimaryKeysPrepared: true}
		for _, column := range node.Columns {
			if column.Primary {
				created.ColumnPrimaryKeys = append(created.ColumnPrimaryKeys, column.Name)
			}
		}
		result.Tables = append(result.Tables, created)
	}
	return result, nil
}
