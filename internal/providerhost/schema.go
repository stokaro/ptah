package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/planext"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemaprojection"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
)

// Provider returns services for the explicitly selected target declarations.
// No built-in renderer, planner, or capability preset supplies missing behavior.
func (h *Host) Provider() engine.Provider {
	result := engine.Provider{ID: h.description.GetProviderId(), Codecs: slices.Clone(h.modelCodecs), YAML: h.yamlExtension(), Annotations: h.annotationExtensions(), SQL: h.sqlExtensions()}
	for _, codec := range result.Codecs {
		contract := h.models[modelKey{kind: codec.Prototype.Kind(), representation: codec.Representation}]
		if contract.displayName != "" {
			result.Reporting = append(result.Reporting, engine.Reporting{Representation: contract.key.representation,
				Definitions: []schemaext.ReportDefinition{{Kind: contract.key.kind, DisplayName: contract.displayName}}, Service: modelReporter{contract: contract}})
		}
	}
	for i := range result.Codecs {
		result.Codecs[i].Definition = slices.Clone(result.Codecs[i].Definition)
	}
	if len(result.Codecs) > 0 {
		result.ModelDecoder = &schemaext.BatchDecoder{Decode: h.decodeModels}
	}
	for _, declared := range h.description.GetEngines() {
		selected := h.targets[declared.GetId()]
		result.Targets = append(result.Targets, selected.registration())
		selected.registerComparisons(&result)
		selected.registerPlanning(&result)
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
			return commonPlanner{options: options, transactions: t.supportsTransactions(), target: t.engine.GetId(),
				commonTables:  t.supportsCommonTables(),
				assessParents: slices.ContainsFunc(t.engine.GetPlanning(), func(assignment *protocol.ModelPlanning) bool { return len(assignment.GetParentKinds()) > 0 })}
		},
	}
	if t.supportsCommonTables() {
		result.SchemaRendering, result.Validation = t, t
		result.Preparation, result.Creations = t, t
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
	if t.supportsLint() {
		result.Annotator = t
	}
	return result
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
	nodes, err := commonSchema(request.Schema, t.engine.GetDefaultSchema())
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	rendered, err := t.Render(ctx, renderer.Request{Target: request.Target, Capabilities: request.Capabilities, Nodes: nodes})
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
		node, err := commonTable(table.Desired)
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
func (t *target) ProjectTableCreations(ctx context.Context, request schemaprojection.TableCreationRequest) (schemaprojection.TableCreationResult, error) {
	if slices.Contains(t.engine.GetFeatures(), provider.ProjectTableCreationsFeature) {
		return t.projectOwnedTableCreations(ctx, request)
	}
	if err := ctx.Err(); err != nil {
		return schemaprojection.TableCreationResult{}, err
	}
	if request.Target != t.engine.GetId() || request.Capabilities.Len() != 0 {
		return schemaprojection.TableCreationResult{}, unsupported("creation target metadata")
	}
	result := schemaprojection.TableCreationResult{Complete: true}
	for _, table := range request.Tables {
		node, err := commonTable(table.Declaration)
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
