package providerhost

import (
	"fmt"
	"slices"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/internal/tableref"
)

// carriesNamespaces reports whether the target's engine carries its
// namespaces on the common-object wire: its provider and the engine list the
// feature and the engine declares the SCHEMA kind. A read then lists every
// schema of its scope, and a plan creates and drops schemas through the
// provider.
func (t *target) carriesNamespaces() bool {
	return t.offers(provider.CommonNamespacesFeature) && t.commonKinds()[protocol.CommonObjectSupport_SCHEMA]
}

// namespaceRemoval stands for the removal of a schema a plan reverses the
// creation of. The common AST has no node for it, and only the provider
// renders it; see target.namespaceOperation.
type namespaceRemoval struct{ name string }

func (n *namespaceRemoval) Accept(ast.Visitor) error {
	return unsupported("removing schema " + n.name + " outside a provider plan")
}

// namespaceSteps are the creations of the schemas a plan needs and the
// removals of the schemas it reverses, for a provider that carries
// namespaces. A schema is created when the comparison adds it, because the
// declaration names it and the read does not hold it, and when an object the
// plan creates is in it and the read does not hold it, as the PostgreSQL
// planner creates the schema of a created table. The schema the connection
// resolved, the engine's default one and a schema the read holds get no
// creation. Removals come only from reversing a creation and run last; see
// difftypes.SchemaDiff.SchemasRemoved. A planner whose provider does not
// carry namespaces plans none.
func (p commonPlanner) namespaceSteps(diff *difftypes.SchemaDiff) (creations, removals []ast.Node) {
	if !p.namespaces {
		return nil, nil
	}
	semantics := diff.EffectiveIdentifierSemantics(p.target)
	held := make(map[string]bool, len(diff.HeldSchemas)+1)
	for _, schema := range diff.HeldSchemas {
		held[semantics.TableIdentityKey(schema)] = true
	}
	held[semantics.TableIdentityKey(semantics.DefaultSchema)] = true
	needed := make([]string, 0, len(diff.SchemasAdded))
	need := func(schema string) {
		schema = strings.TrimSpace(schema)
		key := semantics.TableIdentityKey(schema)
		if schema == "" || held[key] {
			return
		}
		held[key] = true
		needed = append(needed, schema)
	}
	for _, schema := range diff.SchemasAdded {
		need(schema.Name)
	}
	for _, name := range slices.Concat(diff.TablesAdded.Names(), diff.ViewsAdded.Names(), diff.MaterializedViewsAdded.Names()) {
		if ref, ok := tableref.Parse(name); ok && ref.Qualified {
			need(ref.Schema)
		}
	}
	for _, sequence := range diff.SequencesAdded {
		need(sequence.Schema)
	}
	slices.Sort(needed)
	for _, schema := range needed {
		node := schemaprecondition.Node(schema, diff.DeclaredSchemas, semantics)
		node.IfNotExists = false
		creations = append(creations, node)
	}
	for _, schema := range diff.SchemasRemoved {
		removals = append(removals, &namespaceRemoval{name: schema})
	}
	return creations, removals
}

// namespaceEffect is the footprint of a schema creation or removal.
func namespaceEffect(builder objectidentity.Builder, node ast.Node) (objectidentity.ID, plangraph.Action, bool) {
	switch typed := node.(type) {
	case *ast.CreateSchemaNode:
		return namespaceIdentity(builder, typed.Name), plangraph.Create, true
	case *namespaceRemoval:
		return namespaceIdentity(builder, typed.name), plangraph.Drop, true
	}
	return objectidentity.ID{}, "", false
}

// namespaceIdentity is the identity of the schema name, compared as the
// engine compares the schema part of a table's name.
func namespaceIdentity(builder objectidentity.Builder, name string) objectidentity.ID {
	return objectidentity.ID{Kind: objectidentity.KindSchema, Name: builder.TableParts("", name).Name}
}

// namespaceOperation is the operand of a schema creation or removal on the
// common-object wire. A schema's character set and collation have no place
// on the wire, so a declaration that states them is refused rather than
// created without them.
func (t *target) namespaceOperation(node ast.Node) (*protocol.CommonObjectChange, *protocol.Reference, error) {
	switch typed := node.(type) {
	case *ast.CreateSchemaNode:
		object, err := t.namespaceObject(typed.Name)
		if err != nil {
			return nil, nil, err
		}
		if typed.Charset != "" || typed.Collate != "" {
			return nil, nil, unsupported(fmt.Sprintf("the character set or collation of schema %q", typed.Name))
		}
		creation := &protocol.NamespaceCreation{Name: typed.Name, Comment: typed.Comment}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_CreateSchema{CreateSchema: creation}}, object, nil
	case *namespaceRemoval:
		object, err := t.namespaceObject(typed.name)
		if err != nil {
			return nil, nil, err
		}
		return &protocol.CommonObjectChange{Operation: &protocol.CommonObjectChange_DropSchema{DropSchema: object}}, object, nil
	default:
		return nil, nil, nil
	}
}

// namespaceObject is the reference of the schema name on the target's engine,
// or the refusal of a schema step on an engine that does not carry
// namespaces.
func (t *target) namespaceObject(name string) (*protocol.Reference, error) {
	if !t.carriesNamespaces() {
		return nil, t.uncarriedSchema(name)
	}
	return &protocol.Reference{Engine: t.engine.GetId(), Kind: provider.NamespaceKind, Name: name}, nil
}

// uncarriedSchema is the refusal of schema name on an engine that does not
// carry namespaces, naming the schema, the engine and the feature it lacks.
func (t *target) uncarriedSchema(name string) error {
	return unsupported(fmt.Sprintf("schema %q on the %s engine, whose provider does not carry namespaces (%s)",
		name, t.engine.GetId(), provider.CommonNamespacesFeature))
}

// refuseUncarriedSchemas refuses, on an engine that does not carry namespaces,
// a declared schema nothing hands to the provider.
//
// A plan creates a schema only through the namespace steps of an engine that
// carries namespaces (see commonPlanner.namespaceSteps), and a comparison
// adds none for any other engine. There a declared schema reaches the provider
// only as the schema of a table, view, materialized view, sequence or provider
// object in it, and the provider decides what the schema means: YDB, whose
// schema is a directory, creates it with the first object in it. A declared
// schema in which neither the declaration nor the read has any object reaches
// the provider through nothing, so without this refusal a plan would leave it
// out and report the declaration synced. A schema nested below it, on an
// engine whose schemas nest, holds it too.
//
// The engine's default schema and the schema the session resolved exist, and a
// schema the declaration leaves undescribed claims nothing, so none of them is
// refused. Without a read nothing is refused: the declaration is then being
// described for a caller that compares no database, such as a document
// converted to stand for one, and the comparison that plans it passes the read.
func (t *target) refuseUncarriedSchemas(desired *schemamodel.Database, current *catalog.Database, semantics identifier.Semantics) error {
	if desired == nil || current == nil || len(desired.Schemas) == 0 || t.carriesNamespaces() {
		return nil
	}
	held := heldSchemas{keys: make(map[string]bool), semantics: semantics, separator: t.schemaSeparator()}
	held.hold(t.engine.GetDefaultSchema())
	held.hold(semantics.DefaultSchema)
	held.declaration(desired)
	held.read(current)
	for _, schema := range desired.Schemas {
		if held.holds(schema.Name) || !desired.NotDescribed.DescribesSchema(schema.Name) {
			continue
		}
		return t.uncarriedSchema(schema.Name)
	}
	return nil
}

// heldSchemas are the schemas that hold an object, keyed as the engine
// compares the schema part of a table's name. On an engine whose schemas nest,
// a schema holds what a schema below it holds.
type heldSchemas struct {
	keys      map[string]bool
	semantics identifier.Semantics
	separator string
}

func (h heldSchemas) hold(schema string) {
	schema = strings.TrimSpace(schema)
	h.keys[h.semantics.TableIdentityKey(schema)] = true
	for h.separator != "" {
		cut := strings.LastIndex(schema, h.separator)
		if cut <= 0 {
			return
		}
		schema = schema[:cut]
		h.keys[h.semantics.TableIdentityKey(schema)] = true
	}
}

func (h heldSchemas) holds(schema string) bool {
	return h.keys[h.semantics.TableIdentityKey(strings.TrimSpace(schema))]
}

// declaration holds the schema of each table, view, materialized view,
// sequence and provider object desired declares.
func (h heldSchemas) declaration(desired *schemamodel.Database) {
	for _, table := range desired.Tables {
		h.hold(table.Schema)
	}
	for _, name := range slices.Concat(viewNames(desired.Views), materializedViewNames(desired.MaterializedViews)) {
		if ref, ok := tableref.Parse(name); ok && ref.Qualified {
			h.hold(ref.Schema)
		}
	}
	for _, sequence := range desired.Sequences {
		h.hold(sequence.Schema)
	}
	for _, ref := range desired.FeatureObjects.Refs() {
		h.hold(ref.Schema.Source)
	}
}

// read holds the schema of each table, view, materialized view, sequence and
// provider object the read reports.
func (h heldSchemas) read(current *catalog.Database) {
	for _, table := range current.Tables {
		h.hold(table.Schema)
	}
	for _, view := range current.Views {
		h.hold(view.Schema)
	}
	for _, view := range current.MatViews {
		h.hold(view.Schema)
	}
	for _, sequence := range current.Sequences {
		h.hold(sequence.Schema)
	}
	for _, ref := range current.FeatureObjects.Refs() {
		h.hold(ref.Schema.Source)
	}
}

// declaredNamespaceCreations creates the schemas a whole declaration needs,
// for a render from nothing: each schema it declares other than the engine's
// default one and, where the engine carries namespaces, each schema one of its
// tables, views or sequences is in. A declared schema other than the default
// on an engine without namespaces is refused, since nothing could create it.
func (t *target) declaredNamespaceCreations(source *schemamodel.Database) ([]ast.Node, error) {
	defaultSchema := t.engine.GetDefaultSchema()
	created := map[string]bool{defaultSchema: true, "": true}
	var nodes []ast.Node
	for _, namespace := range source.Schemas {
		if namespace.Name == defaultSchema {
			if err := onlyFields(namespace, "Name"); err != nil {
				return nil, err
			}
			continue
		}
		if !t.carriesNamespaces() {
			return nil, t.uncarriedSchema(namespace.Name)
		}
		if err := onlyFields(namespace, "Name", "Comment"); err != nil {
			return nil, err
		}
		if !created[namespace.Name] {
			created[namespace.Name] = true
			nodes = append(nodes, &ast.CreateSchemaNode{Name: namespace.Name, Comment: namespace.Comment})
		}
	}
	if !t.carriesNamespaces() {
		return nodes, nil
	}
	var named []string
	for _, table := range source.Tables {
		named = append(named, table.Schema)
	}
	for _, sequence := range source.Sequences {
		named = append(named, sequence.Schema)
	}
	for _, name := range slices.Concat(viewNames(source.Views), materializedViewNames(source.MaterializedViews)) {
		if ref, ok := tableref.Parse(name); ok && ref.Qualified {
			named = append(named, ref.Schema)
		}
	}
	slices.Sort(named)
	for _, schema := range slices.Compact(named) {
		schema = strings.TrimSpace(schema)
		if !created[schema] {
			created[schema] = true
			nodes = append(nodes, &ast.CreateSchemaNode{Name: schema})
		}
	}
	return nodes, nil
}

func viewNames(views []schemamodel.View) []string {
	names := make([]string, len(views))
	for i, view := range views {
		names[i] = view.Name
	}
	return names
}

func materializedViewNames(views []schemamodel.MaterializedView) []string {
	names := make([]string, len(views))
	for i, view := range views {
		names[i] = view.Name
	}
	return names
}
