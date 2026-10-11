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
	"ptah.run/internal/deporder"
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
	// DatabasePath is the absolute path of the database a lowered description
	// was read from, such as /local, and empty for a declaration.
	DatabasePath string
	// GrantPath names a grant's object in the render of a description read
	// from the database at databasePath, such as YDB's absolute path for a
	// grant on the database itself. It is called only where DatabasePath is
	// known. Nil keeps every grant's name, as every target but YDB does.
	GrantPath func(objectType, objectName, databasePath string) string
	// ResolvesBodyAtCreate reports whether the target resolves the names in a
	// routine's body when the routine is created, which orders the routine
	// after everything its body reads ([deporder.BodyResolution]). Nil orders
	// every routine by its signature alone, as every target but the
	// PostgreSQL family does.
	ResolvesBodyAtCreate deporder.BodyResolution
}

// grantName binds GrantPath to the lowering's database path, and is nil where
// either is missing.
func (l Lowering) grantName() grantName {
	if l.GrantPath == nil || l.DatabasePath == "" {
		return nil
	}
	return func(objectType, objectName string) string {
		return l.GrantPath(objectType, objectName, l.DatabasePath)
	}
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
	objects, tables, err := declaredObjects(database, target, lowering)
	if err != nil {
		return err
	}
	if len(objects) == 0 {
		return walkCommonDatabase(database, target, lowering.grantName(), lowering.ResolvesBodyAtCreate, visit, nil)
	}
	if err := schemaext.RequireRuntime(lowering.Context, lowering.Runtime); err != nil {
		return fmt.Errorf("%w: standalone feature lowering: %w", ptaherr.ErrUnsupportedFeature, err)
	}
	// Capture graph units through one common walk. An owner may require a
	// position before, between, or after them; streaming SQL before scheduling
	// would make a late conflict leave a successful prefix.
	var nodes []ast.Node
	typeWindow := 0
	if err := walkCommonDatabase(tables, target, lowering.grantName(), lowering.ResolvesBodyAtCreate, func(node ast.Node) error {
		nodes = append(nodes, node)
		return lowering.Context.Err()
	}, func() { typeWindow = len(nodes) }); err != nil {
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
			placed, err := phaseDependencies(step.ID, step.Payload.Phase, nodes, metadata, typeWindow)
			if err != nil {
				return err
			}
			contribution.Dependencies = append(contribution.Dependencies, placed...)
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

// phaseDependencies places a declaration step of phase among the common
// steps. A whole-schema render orders an owner's steps by their dependencies
// alone, except a prerequisite, which it runs after the schema statements
// and before the rest: an object every other object may use; and a type,
// which it runs at typeWindow, after the enums and before the routines that
// name a type and the tables, as a migration plan does. It has no window for
// any other phase.
func phaseDependencies(
	step plangraph.StepID, phase featureplan.Phase, nodes []ast.Node, metadata []featureplan.CommonStep, typeWindow int,
) ([]plangraph.Dependency, error) {
	switch phase {
	case featureplan.PhaseDefault:
		return nil, nil
	case featureplan.PhasePrerequisite:
		return prerequisiteDependencies(nodes, metadata, step), nil
	case featureplan.PhaseType:
		return windowDependencies(metadata, typeWindow, step), nil
	default:
		return nil, fmt.Errorf("%w: declaration step %s/%s asks for the %q phase, which a schema render has no window for",
			schemaext.ErrInvalidValue, step.Owner, step.Name, phase)
	}
}

// prerequisiteDependencies places step after the schema statements a render
// opens with and before its first other statement, nodes and metadata being
// the common sequence and its steps.
func prerequisiteDependencies(nodes []ast.Node, metadata []featureplan.CommonStep, step plangraph.StepID) []plangraph.Dependency {
	schemas := 0
	for schemas < len(nodes) {
		if _, ok := nodes[schemas].(*ast.CreateSchemaNode); !ok {
			break
		}
		schemas++
	}
	return windowDependencies(metadata, schemas, step)
}

// windowDependencies places step after the common step before position and
// before the one at it, metadata being the common steps in order.
func windowDependencies(metadata []featureplan.CommonStep, position int, step plangraph.StepID) []plangraph.Dependency {
	var dependencies []plangraph.Dependency
	if position > 0 {
		dependencies = append(dependencies, plangraph.Dependency{Before: metadata[position-1].ID, After: step})
	}
	if position < len(metadata) {
		dependencies = append(dependencies, plangraph.Dependency{Before: step, After: metadata[position].ID})
	}
	return dependencies
}

// declaredObjects selects the objects their owners plan as declarations, and
// returns the schema the common walk renders: the standalone objects, and a
// table's child whose kind the runtime declares. Such a child is its owner's to
// create, in a step scheduled against the common ones, so the schema left for
// the common walk does not hold it and its CREATE TABLE does not carry it; any
// other child is created with its table.
func declaredObjects(database schemamodel.Database, target string, lowering Lowering) ([]schemaext.Object, schemamodel.Database, error) {
	child := func(ref objectidentity.ID) bool {
		return !ref.Parent.Empty() && lowering.Runtime != nil && lowering.Runtime.DeclaresKind(target, schemaext.Kind(ref.Kind))
	}
	objects, err := database.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Parent.Empty() || child(ref) }).All()
	if err != nil {
		return nil, schemamodel.Database{}, err
	}
	tables := database
	tables.FeatureObjects = database.FeatureObjects.Select(func(ref objectidentity.ID) bool { return !child(ref) })
	return objects, tables, nil
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
