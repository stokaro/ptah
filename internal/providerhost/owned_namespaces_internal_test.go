package providerhost

// White-box testing required: the schema steps a plan needs, the catalog
// schemas a read produces and the wire operand of a schema step are private
// adapter state, and they decide whether a declared schema is created, kept
// out of a plan because the read holds it, or refused. The subprocess tests
// in integration/providercli cover the engine.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"google.golang.org/protobuf/testing/protocmp"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// uncarriedApp is the refusal of schema app on the fixture engine, which does
// not carry namespaces.
const uncarriedApp = `unsupported feature: the experimental provider contract does not support schema "app" on the fixture engine, ` +
	`whose provider does not carry namespaces \(schema\.common-namespaces\)`

// namespacePlanner plans for a provider that owns the table lifecycle and
// carries its namespaces.
func namespacePlanner() commonPlanner {
	return commonPlanner{target: "fixture", assessParents: true, namespaces: true}
}

// namespaceTarget is a target whose engine carries comments and namespaces
// on a negotiated common-object wire.
func namespaceTarget() *target {
	result := commonObjectTarget(protocol.CommonObjectSupport_COMMENT, protocol.CommonObjectSupport_SCHEMA)
	result.engine.Features = append(result.engine.Features, provider.CommonNamespacesFeature)
	result.host.description.Features = append(result.host.description.Features, provider.CommonNamespacesFeature)
	return result
}

// createdNames are the names of the schemas nodes create, with their
// comments, in order.
func createdNames(nodes []ast.Node) [][2]string {
	var names [][2]string
	for _, node := range nodes {
		created := node.(*ast.CreateSchemaNode)
		names = append(names, [2]string{created.Name, created.Comment})
	}
	return names
}

// A plan creates the schema the comparison adds and the schema of an object it
// creates, and none the read holds or the connection resolved.
func TestNamespaceStepsCreateWhatThePlanNeeds_HappyPath(t *testing.T) {
	c := qt.New(t)
	rules := ownedIndexRules()
	diff := &difftypes.SchemaDiff{IdentifierSemantics: &rules,
		SchemasAdded:    []schemamodel.Schema{{Name: "empty_one"}},
		DeclaredSchemas: []schemamodel.Schema{{Name: "app", Comment: "Application"}, {Name: "empty_one"}},
		HeldSchemas:     []string{"main", "kept"},
		TablesAdded: difftypes.TableChanges{{Name: "app.t", Table: schemamodel.Table{Name: "t", Schema: "app"}},
			{Name: "kept.u", Table: schemamodel.Table{Name: "u", Schema: "kept"}}, {Name: "v", Table: schemamodel.Table{Name: "v"}}},
		SequencesAdded: []schemamodel.Sequence{{Name: "ids", Schema: "counters"}},
	}

	creations, removals := namespacePlanner().namespaceSteps(diff)

	c.Assert(createdNames(creations), qt.DeepEquals, [][2]string{{"app", "Application"}, {"counters", ""}, {"empty_one", ""}})
	c.Assert(creations[0].(*ast.CreateSchemaNode).IfNotExists, qt.IsFalse)
	c.Assert(removals, qt.HasLen, 0)
}

// A rollback removes the schema its forward plan created, last.
func TestNamespaceStepsRemoveWhatARollbackRemoves_HappyPath(t *testing.T) {
	c := qt.New(t)
	rules := ownedIndexRules()

	creations, removals := namespacePlanner().namespaceSteps(&difftypes.SchemaDiff{IdentifierSemantics: &rules, SchemasRemoved: []string{"app"}})

	c.Assert(creations, qt.HasLen, 0)
	c.Assert(removals, qt.HasLen, 1)
	c.Assert(*removals[0].(*namespaceRemoval), qt.Equals, namespaceRemoval{name: "app"})
}

// The schemas a read found are context for every provider planner, and a
// schema change is a step only of a planner whose provider carries namespaces.
func TestNamespaceFields_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name    string
		planner commonPlanner
		diff    *difftypes.SchemaDiff
	}{
		{"held schemas without namespaces", commonPlanner{target: "fixture", assessParents: true}, &difftypes.SchemaDiff{HeldSchemas: []string{"main"}}},
		{"an added schema", namespacePlanner(), &difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{{Name: "app"}}}},
		{"a removed schema", namespacePlanner(), &difftypes.SchemaDiff{SchemasRemoved: []string{"app"}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := row.planner.commonObjectSteps(row.diff)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestNamespaceFields_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{"an added schema", &difftypes.SchemaDiff{SchemasAdded: []schemamodel.Schema{{Name: "app"}}}, `.*does not support SchemaDiff\.SchemasAdded`},
		{"a removed schema", &difftypes.SchemaDiff{SchemasRemoved: []string{"app"}}, `.*does not support SchemaDiff\.SchemasRemoved`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := commonPlanner{target: "fixture", assessParents: true}.commonObjectSteps(row.diff)
			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}

// A schema step reaches the provider as a typed operand on the common-object
// wire.
func TestNamespaceChange_HappyPath(t *testing.T) {
	for _, row := range []struct {
		name string
		node ast.Node
		want *protocol.Change
	}{
		{"a creation with a comment", &ast.CreateSchemaNode{Name: "app", Comment: "Application"},
			&protocol.Change{Object: ref(provider.NamespaceKind, "", "app"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_CreateSchema{CreateSchema: &protocol.NamespaceCreation{Name: "app", Comment: "Application"}}}}}},
		{"a removal", &namespaceRemoval{name: "app"},
			&protocol.Change{Object: ref(provider.NamespaceKind, "", "app"), Operation: &protocol.Change_CommonObject{CommonObject: &protocol.CommonObjectChange{
				Operation: &protocol.CommonObjectChange_DropSchema{DropSchema: ref(provider.NamespaceKind, "", "app")}}}}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			change, err := namespaceTarget().change(row.node)
			c.Assert(err, qt.IsNil)
			c.Assert(change, qt.CmpEquals(protocmp.Transform()), row.want)
		})
	}
}

func TestNamespaceChange_FailurePath(t *testing.T) {
	withoutNamespaces := commonObjectTarget(everyCommonKind...)
	for _, row := range []struct {
		name   string
		target *target
		node   ast.Node
		want   string
	}{
		{"a creation on an engine without namespaces", withoutNamespaces, &ast.CreateSchemaNode{Name: "app"}, uncarriedApp},
		{"a removal on an engine without namespaces", withoutNamespaces, &namespaceRemoval{name: "app"}, uncarriedApp},
		{"a character set", namespaceTarget(), &ast.CreateSchemaNode{Name: "app", Charset: "utf8mb4"},
			`.*does not support the character set or collation of schema "app"`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			change, err := row.target.change(row.node)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(change, qt.IsNil)
		})
	}
}

// A read's namespaces become the catalog's schemas, and a comment on one is
// the schema's; a read that leaves them uninspected records that the schemas
// are not described.
func TestNamespacesRead_HappyPath(t *testing.T) {
	t.Run("listed namespaces", func(t *testing.T) {
		c := qt.New(t)
		result := &catalog.Database{}
		observed := &protocol.ObservedCommonObjects{
			Namespaces: []*protocol.ObservedNamespace{{Identity: ref(provider.NamespaceKind, "", "main")}, {Identity: ref(provider.NamespaceKind, "", "app")}},
			Comments:   []*protocol.ObjectComment{{Object: ref(provider.NamespaceKind, "", "app"), Text: "Application"}},
			Coverage: []*protocol.CommonObjectCoverage{{Kind: protocol.CommonObjectSupport_SCHEMA, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}},
				{Kind: protocol.CommonObjectSupport_COMMENT, Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_COMPLETE}}},
		}
		c.Assert(reader{}.commonObjects(result, observed), qt.IsNil)
		c.Assert(result.Schemas, qt.DeepEquals, []catalog.Schema{{Name: "main"}, {Name: "app", Comment: "Application"}})
		c.Assert(result.NotDescribed.DescribesSchema("app"), qt.IsTrue)
	})
	t.Run("uninspected namespaces", func(t *testing.T) {
		c := qt.New(t)
		result := &catalog.Database{}
		observed := &protocol.ObservedCommonObjects{Coverage: []*protocol.CommonObjectCoverage{{Kind: protocol.CommonObjectSupport_SCHEMA,
			Knowledge: &protocol.ModelKnowledge{State: protocol.ModelKnowledge_UNINSPECTED, Reason: "the read was scoped"}}}}
		c.Assert(reader{}.commonObjects(result, observed), qt.IsNil)
		c.Assert(result.Schemas, qt.HasLen, 0)
		c.Assert(result.NotDescribed.DescribesSchema("app"), qt.IsFalse)
	})
}

// A render from nothing creates the schemas a declaration names, and refuses
// a declared schema on an engine that cannot create one.
func TestDeclaredNamespaceCreations_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "main"}, {Name: "app", Comment: "Application"}},
		Tables: []schemamodel.Table{{Name: "t", Schema: "app"}, {Name: "u", Schema: "audit"}, {Name: "v"}},
		Views:  []schemamodel.View{{Name: "reports.daily", Body: "SELECT 1"}}}

	nodes, err := namespaceTarget().declaredNamespaceCreations(source)

	c.Assert(err, qt.IsNil)
	c.Assert(createdNames(nodes), qt.DeepEquals, [][2]string{{"app", "Application"}, {"audit", ""}, {"reports", ""}})
	nodes, err = commonObjectTarget(everyCommonKind...).declaredNamespaceCreations(&schemamodel.Database{Tables: source.Tables})
	c.Assert(err, qt.IsNil)
	c.Assert(nodes, qt.HasLen, 0)
}

func TestDeclaredNamespaceCreations_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name   string
		target *target
		source *schemamodel.Database
		want   string
	}{
		{"an engine without namespaces", commonObjectTarget(everyCommonKind...), &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}}},
			uncarriedApp},
		{"a character set", namespaceTarget(), &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app", Charset: "utf8mb4"}}},
			`.*does not support Schema\.Charset`},
		{"a comment on the default schema", namespaceTarget(), &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "main", Comment: "Default"}}},
			`.*does not support Schema\.Comment`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			nodes, err := row.target.declaredNamespaceCreations(row.source)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(nodes, qt.IsNil)
		})
	}
}

// heldNode is a provider object: the schema in its identity is all a check of
// the schemas that hold an object reads.
type heldNode struct{}

func (heldNode) Kind() schemaext.Kind { return "example.org/node" }

func (heldNode) Clone() schemaext.Value { return heldNode{} }

func (heldNode) Equal(other schemaext.Value) bool { return other == heldNode{} }

// nodeIn is a provider object named n in schema.
func nodeIn(c *qt.C, schema string) schemaext.Objects {
	c.Helper()
	objects, err := schemaext.NewObjects(schemaext.Object{Ref: objectidentity.ID{Kind: "example.org/node",
		Schema: objectidentity.Part{Source: schema, Normalized: schema}, Name: objectidentity.Part{Source: "n", Normalized: "n"}}, Value: heldNode{}})
	c.Assert(err, qt.IsNil)
	return objects
}

// nestedTarget is a target without namespaces whose schemas nest, a/b below a.
func nestedTarget() *target {
	result := commonObjectTarget(everyCommonKind...)
	result.engine.Identifiers = &protocol.IdentifierRules{SchemaSeparator: "/"}
	return result
}

// appSchema declares schema app.
func appSchema() []schemamodel.Schema { return []schemamodel.Schema{{Name: "app"}} }

// A declared schema is kept on an engine that carries namespaces, and on one
// that does not where something exists in it or reaches the provider through
// it: the default schema, the schema the session resolved, a schema the
// declaration or the read has an object in, a schema below which another one
// holds an object, and one the declaration leaves undescribed. A lowering with
// no read compares nothing and refuses nothing.
func TestRefuseUncarriedSchemas_HappyPath(t *testing.T) {
	c := qt.New(t)
	rules := ownedIndexRules()
	resolved := ownedIndexRules()
	resolved.DefaultSchema = "app"
	withoutNamespaces := commonObjectTarget(everyCommonKind...)
	for _, row := range []struct {
		name      string
		target    *target
		desired   *schemamodel.Database
		current   *catalog.Database
		semantics identifier.Semantics
	}{
		{name: "an engine that carries namespaces", target: namespaceTarget(), desired: &schemamodel.Database{Schemas: appSchema()}, current: &catalog.Database{}, semantics: rules},
		{name: "no read", target: withoutNamespaces, desired: &schemamodel.Database{Schemas: appSchema()}, semantics: rules},
		{name: "the default schema", target: withoutNamespaces, current: &catalog.Database{}, desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "main"}}}, semantics: rules},
		{name: "the schema the session resolved", target: withoutNamespaces, current: &catalog.Database{}, desired: &schemamodel.Database{Schemas: appSchema()}, semantics: resolved},
		{name: "a declared table", target: withoutNamespaces, current: &catalog.Database{}, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema(), Tables: []schemamodel.Table{{Name: "t", Schema: "app"}}}},
		{name: "a declared view", target: withoutNamespaces, current: &catalog.Database{}, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema(), Views: []schemamodel.View{{Name: "app.v", Body: "SELECT 1"}}}},
		{name: "a declared materialized view", target: withoutNamespaces, current: &catalog.Database{}, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema(), MaterializedViews: []schemamodel.MaterializedView{{Name: "app.m", Body: "SELECT 1"}}}},
		{name: "a declared sequence", target: withoutNamespaces, current: &catalog.Database{}, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema(), Sequences: []schemamodel.Sequence{{Name: "ids", Schema: "app"}}}},
		{name: "a declared provider object", target: withoutNamespaces, current: &catalog.Database{}, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema(), FeatureObjects: nodeIn(c, "app")}},
		{name: "a table the read reports", target: withoutNamespaces, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema()}, current: &catalog.Database{Tables: []catalog.Table{{Name: "t", Schema: "app"}}}},
		{name: "a view the read reports", target: withoutNamespaces, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema()}, current: &catalog.Database{Views: []catalog.View{{Name: "v", Schema: "app"}}}},
		{name: "a materialized view the read reports", target: withoutNamespaces, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema()}, current: &catalog.Database{MatViews: []catalog.MaterializedView{{Name: "m", Schema: "app"}}}},
		{name: "a sequence the read reports", target: withoutNamespaces, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema()}, current: &catalog.Database{Sequences: []catalog.Sequence{{Name: "ids", Schema: "app"}}}},
		{name: "a provider object the read reports", target: withoutNamespaces, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema()}, current: &catalog.Database{FeatureObjects: nodeIn(c, "app")}},
		{name: "a table in a nested schema", target: nestedTarget(), current: &catalog.Database{}, semantics: rules,
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "a"}, {Name: "a/b"}}, Tables: []schemamodel.Table{{Name: "t", Schema: "a/b"}}}},
		{name: "an undescribed schema", target: withoutNamespaces, current: &catalog.Database{}, semantics: rules,
			desired: &schemamodel.Database{Schemas: appSchema(), NotDescribed: coverage.Set{}.WithObject(coverage.Schema, "app")}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(row.target.refuseUncarriedSchemas(row.desired, row.current, row.semantics), qt.IsNil)
		})
	}
}

// A declared schema that nothing on either side is in is refused on an engine
// that does not carry namespaces, by its name and the feature the engine
// lacks: no plan step would create it. An object in a schema of a similar
// name, or below the schema on an engine whose schemas do not nest, does not
// hold it.
func TestRefuseUncarriedSchemas_FailurePath(t *testing.T) {
	rules := ownedIndexRules()
	withoutNamespaces := commonObjectTarget(everyCommonKind...)
	for _, row := range []struct {
		name    string
		target  *target
		desired *schemamodel.Database
		current *catalog.Database
		want    string
	}{
		{name: "an empty schema", target: withoutNamespaces, current: &catalog.Database{}, desired: &schemamodel.Database{Schemas: appSchema()}, want: uncarriedApp},
		{name: "a table of another schema", target: withoutNamespaces,
			desired: &schemamodel.Database{Schemas: appSchema(), Tables: []schemamodel.Table{{Name: "t", Schema: "apps"}}},
			current: &catalog.Database{Tables: []catalog.Table{{Name: "u", Schema: "main"}}}, want: uncarriedApp},
		{name: "a table below it on an engine whose schemas do not nest", target: withoutNamespaces, current: &catalog.Database{},
			desired: &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "app"}, {Name: "app/b"}}, Tables: []schemamodel.Table{{Name: "t", Schema: "app/b"}}},
			want:    uncarriedApp},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(row.target.refuseUncarriedSchemas(row.desired, row.current, rules), qt.ErrorMatches, row.want)
		})
	}
}
