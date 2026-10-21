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
	sequence, err := captureCommonSequence(tables, target, lowering, objects)
	if err != nil {
		return err
	}
	nodes, typeStart, typeEnd := sequence.nodes, sequence.typeStart, sequence.typeEnd
	common, metadata := declarationCommonGraph(nodes, sequence.metadata)
	semantics := identifier.ForDialect(target)
	request := featureplan.DeclarationRequest{Target: target, Identifiers: semantics, Capabilities: lowering.Capabilities,
		Objects: objects, CommonSteps: metadata, Types: schemaext.DefinedTypes(database.FeatureObjects)}
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
			placed, err := phaseDependencies(step.ID, step.Payload.Phase, nodes, metadata, typeStart, typeEnd)
			if err != nil {
				return err
			}
			contribution.Dependencies = append(contribution.Dependencies, placed...)
			payload, err := featureops.Nodes(step.Payload, names, step.Transaction)
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
	if err := visitDatabaseNodes(visit, sequence.trailing...); err != nil {
		return err
	}
	return lowering.Context.Err()
}

// commonSequence is the common statements of a render, each with its
// metadata, the span the types take in it, and the permission statements that
// go after every scheduled statement instead.
type commonSequence struct {
	nodes              []ast.Node
	metadata           []featureplan.CommonStep
	typeStart, typeEnd int
	trailing           []ast.Node
}

// captureCommonSequence walks the common statements of tables once, as graph
// units: an owner may require a position before, between or after them, and
// streaming SQL before scheduling would make a late conflict leave a
// successful prefix. objects are the declared objects the owners create.
func captureCommonSequence(tables schemamodel.Database, target string, lowering Lowering, objects []schemaext.Object) (commonSequence, error) {
	var sequence commonSequence
	if err := walkCommonDatabase(tables, target, lowering.grantName(), lowering.ResolvesBodyAtCreate, func(node ast.Node) error {
		sequence.nodes = append(sequence.nodes, node)
		return lowering.Context.Err()
	}, func(start bool) {
		if start {
			sequence.typeStart = len(sequence.nodes)
		} else {
			sequence.typeEnd = len(sequence.nodes)
		}
	}); err != nil {
		return commonSequence{}, err
	}
	metadata, err := commonMetadata(lowering, sequence.nodes)
	if err != nil {
		return commonSequence{}, err
	}
	sequence.metadata = metadata
	return sequence.withoutDeclaredObjectPermissions(objects), nil
}

// withoutDeclaredObjectPermissions takes out of the sequence each GRANT and
// REVOKE that reads an object an owner declares, such as a YDB topic, into
// trailing, in their order, to be written after every scheduled statement.
// The type span moves with the statements.
//
// The owner decides where its object is created, and one creates it after
// every common statement: YDB's streaming query owner, so the query reads the
// schema the rest leaves. A permission statement inside the common sequence
// would then come before the object it names. Nothing depends on a GRANT or
// a REVOKE, and the end of the script is after both its object and its
// grantee.
func (s commonSequence) withoutDeclaredObjectPermissions(objects []schemaext.Object) commonSequence {
	declared := make(map[objectidentity.Key]bool, len(objects))
	for _, object := range objects {
		declared[object.Ref.Key()] = true
	}
	kept := commonSequence{typeStart: s.typeStart, typeEnd: s.typeEnd}
	for i, node := range s.nodes {
		if !permissionOnDeclaredObject(node, s.metadata[i].Effects, declared) {
			kept.nodes = append(kept.nodes, node)
			kept.metadata = append(kept.metadata, s.metadata[i])
			continue
		}
		kept.trailing = append(kept.trailing, node)
		if i < s.typeStart {
			kept.typeStart--
		}
		if i < s.typeEnd {
			kept.typeEnd--
		}
	}
	return kept
}

// permissionOnDeclaredObject reports whether node is a GRANT or a REVOKE whose
// effects read one of the declared objects.
func permissionOnDeclaredObject(node ast.Node, effects []plangraph.Effect, declared map[objectidentity.Key]bool) bool {
	switch node.(type) {
	case *ast.GrantPrivilegeNode, *ast.RevokePrivilegeNode:
	default:
		return false
	}
	return slices.ContainsFunc(effects, func(effect plangraph.Effect) bool {
		return effect.Action == plangraph.Read && declared[effect.Subject.Key()]
	})
}

// phaseDependencies places a declaration step of phase among the common
// steps. A whole-schema render orders an owner's steps by their dependencies
// alone, except a prerequisite, which it runs after the schema statements
// and before the rest: an object every other object may use; and a type,
// which shares the common type span, after the enums and before routines that
// name a type and the tables, as a migration plan does. It has no window for
// any other phase.
func phaseDependencies(
	step plangraph.StepID, phase featureplan.Phase, nodes []ast.Node, metadata []featureplan.CommonStep, typeStart, typeEnd int,
) ([]plangraph.Dependency, error) {
	switch phase {
	case featureplan.PhaseDefault:
		return nil, nil
	case featureplan.PhasePrerequisite:
		return prerequisiteDependencies(nodes, metadata, step), nil
	case featureplan.PhaseType:
		return spanDependencies(metadata, typeStart, typeEnd, step), nil
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
	return spanDependencies(metadata, position, position, step)
}

func spanDependencies(metadata []featureplan.CommonStep, start, end int, step plangraph.StepID) []plangraph.Dependency {
	var dependencies []plangraph.Dependency
	if start > 0 {
		dependencies = append(dependencies, plangraph.Dependency{Before: metadata[start-1].ID, After: step})
	}
	if end < len(metadata) {
		dependencies = append(dependencies, plangraph.Dependency{Before: step, After: metadata[end].ID})
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

// commonMetadata is the metadata lowering gives each common node, one record
// per node, and empty records where it gives none.
func commonMetadata(lowering Lowering, nodes []ast.Node) ([]featureplan.CommonStep, error) {
	if lowering.CommonMetadata == nil {
		return make([]featureplan.CommonStep, len(nodes)), nil
	}
	metadata, err := lowering.CommonMetadata(lowering.Context, nodes)
	if err != nil {
		return nil, err
	}
	if len(metadata) != len(nodes) {
		return nil, fmt.Errorf("%w: common creation metadata changed the node count", schemaext.ErrInvalidValue)
	}
	return metadata, nil
}

// declarationCommonGraph gives each common node, with its metadata, an
// identity and a step, each after the one before it.
func declarationCommonGraph(nodes []ast.Node, metadata []featureplan.CommonStep) (plangraph.Contribution[[]ast.Node], []featureplan.CommonStep) {
	common := plangraph.Contribution[[]ast.Node]{Owner: "ptah.run/schema-creation"}
	identified := make([]featureplan.CommonStep, len(nodes))
	for i, node := range nodes {
		step := metadata[i].Clone()
		step.ID = plangraph.StepID{Owner: common.Owner, Name: fmt.Sprintf("common/%06d", i)}
		identified[i] = step
		common.Steps = append(common.Steps, plangraph.Step[[]ast.Node]{ID: step.ID,
			Payload: []ast.Node{node}, Effects: step.Effects, Transaction: step.Transaction, Impact: step.Impact})
		if i > 0 {
			common.Dependencies = append(common.Dependencies, plangraph.Dependency{Before: identified[i-1].ID, After: step.ID})
		}
	}
	return common, identified
}
