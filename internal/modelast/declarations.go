package modelast

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/featureops"
)

// Lowering supplies the selected owner runtime and target facts. CommonMetadata
// is a local batch adapter for the host's common AST vocabulary. It returns one
// metadata record per node; the host assigns identities and retains common
// ordering. Nil leaves those footprints unknown. It never grants a missing
// declaration owner permission to drop an object.
type Lowering struct {
	Context        context.Context
	Runtime        featureplan.DeclarationRuntime
	Capabilities   capability.Capabilities
	CommonMetadata func(context.Context, []ast.Node) ([]featureplan.CommonStep, error)
}

// DeclarationServiceError identifies a failed owner call. Schema adapters must
// propagate it as a service failure rather than inventing a completed refusal.
// Completed refusals remain the typed schema errors produced by the reply.
type DeclarationServiceError struct{ Cause error }

// Error describes the failed owner call.
func (e *DeclarationServiceError) Error() string { return e.Cause.Error() }

// Unwrap preserves the provider's original failure identity.
func (e *DeclarationServiceError) Unwrap() error { return e.Cause }

func walkDeclarations(database schemamodel.Database, target string, lowering Lowering, visit func(ast.Node) error) error {
	objects, err := database.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Parent.Empty() }).All()
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return walkCommonDatabase(database, target, visit)
	}
	if err := schemaext.RequireRuntime(lowering.Context, lowering.Runtime); err != nil {
		return fmt.Errorf("%w: standalone feature lowering: %w", ptaherr.ErrUnsupportedFeature, err)
	}
	// Capture graph units through one common walk. An owner may require a
	// position before, between, or after them; streaming SQL before scheduling
	// would make a late conflict leave a successful prefix.
	var nodes []ast.Node
	if err := walkCommonDatabase(database, target, func(node ast.Node) error {
		nodes = append(nodes, node)
		return lowering.Context.Err()
	}); err != nil {
		return err
	}
	common, metadata, err := declarationCommonGraph(lowering, nodes)
	if err != nil {
		return err
	}
	semantics := identifier.ForDialect(target)
	request := featureplan.DeclarationRequest{Target: target, Identifiers: semantics, Capabilities: lowering.Capabilities,
		Objects: objects, CommonSteps: metadata}
	names := make(map[objectidentity.Key]string, len(database.Tables))
	builder := objectidentity.NewBuilder(semantics)
	for _, table := range database.Tables {
		// A table without a name is no relation an owner can order itself
		// against, and the common render answers for it on its own.
		if table.Name == "" {
			continue
		}
		request.Tables = append(request.Tables, schemacapture.DeclareTable(&database, table, semantics))
		names[builder.TableParts(table.Schema, table.Name).Key()] = renderTableName(table, target)
	}
	reply, err := lowering.Runtime.PlanDeclarations(lowering.Context, request)
	if err != nil {
		return &DeclarationServiceError{Cause: err}
	}
	if err := reply.Err(request); err != nil {
		return err
	}
	contributions := []plangraph.Contribution[[]ast.Node]{common}
	for _, source := range reply.Contributions {
		contribution := plangraph.Contribution[[]ast.Node]{Owner: source.Owner, Dependencies: slices.Clone(source.Dependencies)}
		for _, step := range source.Steps {
			payload, err := featureops.Nodes(step.Payload, names)
			if err != nil {
				return err
			}
			contribution.Steps = append(contribution.Steps, plangraph.Step[[]ast.Node]{ID: step.ID,
				Payload: payload, Effects: step.Effects, Transaction: step.Transaction, Impact: step.Impact, Placement: step.Placement})
		}
		contributions = append(contributions, contribution)
	}
	lifecycle, err := plangraph.LifecycleDependencies(lowering.Context, contributions...)
	if err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	contributions[0].Dependencies = append(contributions[0].Dependencies, lifecycle...)
	plan, err := plangraph.Schedule(lowering.Context, contributions...)
	if err != nil {
		return fmt.Errorf("%w: %w", ptaherr.ErrInvalidSchemaDiff, err)
	}
	for _, step := range plan.Steps {
		if err := lowering.Context.Err(); err != nil {
			return err
		}
		if err := visitDatabaseNodes(visit, step.Payload...); err != nil {
			return err
		}
	}
	return lowering.Context.Err()
}

func declarationCommonGraph(lowering Lowering, nodes []ast.Node) (plangraph.Contribution[[]ast.Node], []featureplan.CommonStep, error) {
	common := plangraph.Contribution[[]ast.Node]{Owner: "ptah.run/schema-creation"}
	metadata := make([]featureplan.CommonStep, len(nodes))
	if lowering.CommonMetadata != nil {
		var err error
		metadata, err = lowering.CommonMetadata(lowering.Context, nodes)
		if err != nil {
			return common, nil, err
		}
		if len(metadata) != len(nodes) {
			return common, nil, fmt.Errorf("%w: common creation metadata changed the node count", schemaext.ErrInvalidValue)
		}
	}
	for i, node := range nodes {
		step := metadata[i].Clone()
		step.ID = plangraph.StepID{Owner: common.Owner, Name: fmt.Sprintf("common/%06d", i)}
		metadata[i] = step
		common.Steps = append(common.Steps, plangraph.Step[[]ast.Node]{ID: step.ID,
			Payload: []ast.Node{node}, Effects: step.Effects, Transaction: step.Transaction, Impact: step.Impact})
		if i > 0 {
			common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: metadata[i-1].ID, After: step.ID})
		}
	}
	return common, metadata, nil
}
