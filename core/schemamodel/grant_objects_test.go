package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// TestGrantObjectKinds_HappyPath pins the kinds of scheme object a grant
// names by OnObject, each with the attribute it is declared with, and the
// lookups both ways. A kind is matched without regard to case or spacing, as
// Canonicalize writes it.
func TestGrantObjectKinds_HappyPath(t *testing.T) {
	c := qt.New(t)

	kinds := schemamodel.GrantObjectKinds()
	kinds[0].Kind = "changed"

	c.Assert(schemamodel.GrantObjectKinds(), qt.DeepEquals, []schemamodel.GrantObjectKind{
		{Kind: "TOPIC", Attribute: "on_topic"},
		{Kind: "COORDINATION NODE", Attribute: "on_coordination_node"},
		{Kind: "SECRET", Attribute: "on_secret"},
		{Kind: "EXTERNAL DATA SOURCE", Attribute: "on_external_data_source"},
		{Kind: "EXTERNAL TABLE", Attribute: "on_external_table"},
		{Kind: "ASYNC REPLICATION", Attribute: "on_async_replication"},
		{Kind: "TRANSFER", Attribute: "on_transfer"},
		{Kind: "STREAMING QUERY", Attribute: "on_streaming_query"},
		{Kind: "RESOURCE POOL", Attribute: "on_resource_pool"},
	})
	for _, kind := range schemamodel.GrantObjectKinds() {
		byAttribute, attributed := schemamodel.GrantObjectKindOf(kind.Attribute)
		byKind, kinded := schemamodel.GrantObjectAttribute(" " + kind.Kind + " ")
		c.Assert([]any{byAttribute, attributed, byKind, kinded}, qt.DeepEquals, []any{kind.Kind, true, kind.Attribute, true})
	}
	attribute, ok := schemamodel.GrantObjectAttribute("coordination   node")
	c.Assert([]any{attribute, ok}, qt.DeepEquals, []any{"on_coordination_node", true})
}

// TestGrantObjectKinds_FailurePath pins what names no kind: an attribute of
// another target and a kind no grant names.
func TestGrantObjectKinds_FailurePath(t *testing.T) {
	c := qt.New(t)
	kind, kinded := schemamodel.GrantObjectKindOf("on_table")
	attribute, attributed := schemamodel.GrantObjectAttribute("TABLE")
	c.Assert([]any{kind, kinded, attribute, attributed}, qt.DeepEquals, []any{"", false, "", false})
}

// TestGrant_ObjectTarget pins how a grant on a scheme object is canonicalized
// and keyed: the kind upper-cased with its spacing folded, and a key that
// keeps a topic and a secret of one name apart, and apart from a table.
func TestGrant_ObjectTarget(t *testing.T) {
	c := qt.New(t)
	topic := schemamodel.Grant{Role: " app ", Privileges: []string{"select row"}, OnObject: " app.events ", ObjectKind: " coordination\tnode "}
	topic.Canonicalize()

	c.Assert(topic, qt.DeepEquals, schemamodel.Grant{Role: "app", Privileges: []string{"SELECT ROW"},
		OnObject: "app.events", ObjectKind: "COORDINATION NODE"})
	c.Assert([]string{
		topic.TargetKey(),
		schemamodel.Grant{OnObject: "app.events", ObjectKind: "secret"}.TargetKey(),
		schemamodel.Grant{OnTable: "app.events"}.TargetKey(),
	}, qt.DeepEquals, []string{"COORDINATION NODE app.events", "SECRET app.events", "TABLE app.events"})
}
