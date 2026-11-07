package providerhost

import (
	"context"
	"fmt"
	"slices"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/planext"
	"ptah.run/core/renderer"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemapreparation"
	"ptah.run/core/schemavalidation"
	"ptah.run/engine"
	"ptah.run/internal/modelast"
	"ptah.run/internal/planner/featurehost"
	"ptah.run/internal/tableref"
)

// authoredPlanner joins already validated declaration contributions with the
// parent creation plan. It asks neither service to compare an observed state.
type authoredPlanner struct {
	*engine.Runtime
	objects []schemaext.Object
	tables  []schemacapture.TableDeclaration
}

func (p authoredPlanner) PlanFeatures(ctx context.Context, request featureplan.Request) (featureplan.Result, error) {
	parents, err := p.Runtime.PlanFeatures(ctx, request)
	if err != nil {
		return featureplan.Result{}, err
	}
	if err := parents.Err(request); err != nil {
		return featureplan.Result{}, err
	}
	declared := featureplan.DeclarationRequest{Target: request.Target, Identifiers: request.Identifiers, Capabilities: request.Capabilities,
		Objects: p.objects, Tables: p.tables, Types: request.Types.Desired, CommonSteps: request.CommonSteps}
	objects, err := p.Runtime.PlanDeclarations(ctx, declared)
	if err != nil {
		return featureplan.Result{}, err
	}
	if err := objects.Err(declared); err != nil {
		return featureplan.Result{}, err
	}
	parents.Contributions = append(parents.Contributions, objects.Contributions...)
	return parents, nil
}

func (t *target) renderDeclarations(ctx context.Context, request renderer.SchemaRequest) (renderer.SchemaResult, error) {
	if !t.negotiated(provider.ValidateSchemaFeature) {
		return renderer.SchemaResult{}, unsupported("authored schema rendering requires schema validation")
	}
	checked, err := t.validateOwnedSchema(ctx, schemavalidation.Request{Target: request.Target, Capabilities: request.Capabilities, Identifiers: request.Identifiers, Schema: request.Schema})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	if err := checked.Err(request.Target); err != nil {
		return renderer.SchemaResult{}, err
	}
	// The process adapter's declarations and parent planners come from the same
	// frozen description. This explicit registry supplies no built-in services.
	runtime, err := engine.New(t.host.Provider())
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	// Render the same desired shape that comparison reads. In particular, a
	// UNIQUE constraint on an engine without constraints becomes the index that
	// holds it, including the index's comment.
	lowered, err := runtime.LowerDesired(ctx, schemapreparation.LoweringRequest{Target: request.Target,
		Desired: request.Schema, Capabilities: request.Capabilities, Semantics: request.Identifiers})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	request.Schema = lowered
	child := func(ref objectidentity.ID) bool {
		return !ref.Parent.Empty() && runtime.DeclaresKind(request.Target, schemaext.Kind(ref.Kind))
	}
	objects, err := request.Schema.FeatureObjects.Select(func(ref objectidentity.ID) bool { return ref.Parent.Empty() || child(ref) }).All()
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	var tables []schemacapture.TableDeclaration
	for _, table := range request.Schema.Tables {
		tables = append(tables, schemacapture.DeclareTable(request.Schema, table, request.Identifiers))
	}
	// A named child assigned to a declaration planner is created separately.
	// Its full definition remains in the declaration context, not the parent
	// CREATE TABLE payload, so the two services cannot create it twice.
	parents := *request.Schema
	parents.FeatureObjects = request.Schema.FeatureObjects.Select(func(ref objectidentity.ID) bool { return !child(ref) })
	parentRequest := request
	parentRequest.Schema = &parents
	creation, err := t.authoredCreationContext(parentRequest)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	metadata, err := commonPlanningMetadata(creation.plan, creation.nodes, t.host.selections)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	metadata, trailing := creation.takeDeclaredObjectPermissions(metadata, objects)
	plan, nodes := creation.plan, creation.nodes
	graph, err := featurehost.NewGraph("ptah.run/provider-declarations", nodes, metadata, featurehost.Windows{
		Creation: creation.creationStart, Dependent: len(nodes), Removal: len(nodes),
		Prerequisites: &featurehost.PrerequisiteWindows{Creation: 0, Removal: len(nodes)},
		Types:         &featurehost.TypeWindows{Creation: featurehost.Span{}, Removal: featurehost.Span{Start: len(nodes), End: len(nodes)}},
		Lifecycle:     &featurehost.Span{Start: 0, End: len(nodes)},
	})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	plan.CommonSteps = graph.CommonSteps()
	features, err := featurehost.Plan(ctx, authoredPlanner{Runtime: runtime, objects: objects, tables: tables}, plan, creation.names,
		featureplan.PhaseDependent, featureplan.PhasePrerequisite, featureplan.PhaseType, featureplan.PhaseLifecycle)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	ordered, err := graph.Schedule(ctx, features)
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	if err := rejectUnclaimedChanges(ordered); err != nil {
		return renderer.SchemaResult{}, err
	}
	ordered = append(ordered, trailing...)
	rendered, err := t.Render(ctx, renderer.Request{Target: request.Target, Capabilities: request.Capabilities, Nodes: ordered, Form: renderer.FormText})
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	statements, err := rendered.SQLFragments()
	if err != nil {
		return renderer.SchemaResult{}, err
	}
	return renderer.SchemaResult{Complete: true, Statements: statements, Omissions: rendered.Omissions}, nil
}

// takeDeclaredObjectPermissions takes out of the common statements that follow
// the tables each GRANT and REVOKE whose effects read an object a declaration
// planner creates, one of objects. It returns the metadata of the statements
// it keeps, and the permissions it took in their order, which the render
// writes after every scheduled statement, as a built-in engine's schema render
// does ([modelast.DeclaredObjects]). A declaration planner places its
// creations among the common steps and may place one after all of them, as
// YDB's provider places a coordination node. A permission on that object kept
// among the other grants would come before the object, or would hold the
// grants after it until the object exists. Nothing depends on a GRANT or a
// REVOKE, and the end of the script follows both its object and its grantee.
// A render whose permissions read no declared object keeps every statement in
// place.
func (c *authoredCreation) takeDeclaredObjectPermissions(metadata []featureplan.CommonStep, objects []schemaext.Object) ([]featureplan.CommonStep, []ast.Node) {
	declared := modelast.NewDeclaredObjects(objects)
	nodes, kept := slices.Clone(c.nodes[:c.creationStart]), slices.Clone(metadata[:c.creationStart])
	var trailing []ast.Node
	for i := c.creationStart; i < len(c.nodes); i++ {
		if declared.Permits(c.nodes[i], metadata[i].Effects) {
			trailing = append(trailing, c.nodes[i])
			continue
		}
		nodes, kept = append(nodes, c.nodes[i]), append(kept, metadata[i])
	}
	c.nodes = nodes
	return kept, trailing
}

// authoredCreation keeps complete captured tables, including indexes and
// triggers, beside placeholders whose provider must replace them. It does not
// construct a current catalog or assert absence in one.
type authoredCreation struct {
	plan          featureplan.Request
	names         map[objectidentity.Key]string
	nodes         []ast.Node
	creationStart int
}

func (t *target) authoredCreationContext(request renderer.SchemaRequest) (authoredCreation, error) {
	source := request.Schema
	namespaces, err := t.declaredNamespaceCreations(source)
	if err != nil {
		return authoredCreation{}, err
	}
	plan := featureplan.Request{Target: request.Target, Identifiers: request.Identifiers, Capabilities: request.Capabilities, DatabasePath: request.DatabasePath,
		Types:  featureplan.TypeContext{Desired: schemaext.DefinedTypes(source.FeatureObjects)},
		Policy: featureplan.Policy{TableRebuildRequest: planext.DefaultTableRebuildRequest}}
	names := make(map[objectidentity.Key]string, len(source.Tables))
	builder := objectidentity.NewBuilder(request.Identifiers)
	var creations difftypes.TableChanges
	captured := make(map[string]schemacapture.TableDeclaration, len(source.Tables))
	for _, table := range source.Tables {
		declaration := schemacapture.DeclareTable(source, table, request.Identifiers)
		name := tableref.CanonicalExact(table.Schema, table.Name)
		if _, found := captured[name]; found {
			return authoredCreation{}, fmt.Errorf("provider schema contains duplicate tables")
		}
		captured[name] = declaration
		creations = append(creations, difftypes.TableCreation{Name: name, Table: declaration.Table, Fields: declaration.Fields, Enums: declaration.Enums,
			Constraints: declaration.Constraints, OwnedObjects: declaration.OwnedObjects, FeatureCoverage: declaration.FeatureCoverage, DependsOn: slices.Clone(source.Dependencies[name])})
	}
	ordered := creations.InDependencyOrder()
	if len(ordered) != len(creations) {
		return authoredCreation{}, fmt.Errorf("provider schema has invalid table dependencies")
	}
	before, after := commonObjectCreations(source, t.engine.GetId())
	bindGrantDatabasePath(before, request.DatabasePath)
	bindGrantDatabasePath(after, request.DatabasePath)
	nodes := slices.Concat(namespaces, before)
	for _, creation := range ordered {
		subject := builder.TableParts(creation.Table.Schema, creation.Table.Name)
		plan.Tables = append(plan.Tables, featureplan.Table{Subject: subject, Desired: captured[creation.Name], Action: featureplan.CreateTable})
		names[subject.Key()] = creation.Name
		steps, err := t.authoredTableNodes(captured[creation.Name])
		if err != nil {
			return authoredCreation{}, err
		}
		nodes = append(nodes, steps...)
	}
	creationStart := len(nodes)
	nodes = append(nodes, after...)
	return authoredCreation{plan: plan, names: names, nodes: nodes, creationStart: creationStart}, nil
}

// authoredTableNodes retains every common operand when the host owns table
// creation. A provider-owned table remains a placeholder until parent planning
// replaces it using the complete declaration in the same request.
func (t *target) authoredTableNodes(declaration schemacapture.TableDeclaration) ([]ast.Node, error) {
	var comments []ast.Node
	if t.commonKinds()[protocol.CommonObjectSupport_COMMENT] {
		declaration, comments = creationComments(declaration)
	}
	if !t.supportsCommonTables() {
		return append([]ast.Node{&pendingTableCreation{name: tableref.CanonicalExact(declaration.Table.Schema, declaration.Table.Name)}}, comments...), nil
	}
	node, err := commonTable(declaration)
	if err != nil {
		return nil, err
	}
	return append([]ast.Node{node}, comments...), nil
}
