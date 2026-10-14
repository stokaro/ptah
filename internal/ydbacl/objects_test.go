package ydbacl_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/ydbacl"
)

// TestSchemeObjects_HappyPath pins each kind of scheme object a grant names
// beyond the database, a directory and a table: the object type a grant
// carries, the owner's model kind, the capability key a target holds the kind
// under, and the owner's identity of an object a grant names, by a reference
// and by its absolute path under the database. Every kind the schema model
// lists is one of them, so a declaration of any of them reaches a reader that
// reads it.
func TestSchemeObjects_HappyPath(t *testing.T) {
	tests := []struct {
		objectType string
		kind       schemaext.Kind
		key        capability.Capability
		ref        objectidentity.ID
	}{
		{objectType: schemamodel.GrantObjectTopic, kind: ydbtopic.Kind, key: capability.Topics,
			ref: ydbtopic.Ref("app/eu", "events")},
		{objectType: schemamodel.GrantObjectCoordinationNode, kind: ydbcoordination.Kind, key: capability.CoordinationNodes,
			ref: ydbcoordination.Ref("app/eu", "events")},
		{objectType: schemamodel.GrantObjectSecret, kind: ydbsecret.Kind, key: capability.Secrets,
			ref: ydbsecret.Ref("app/eu", "events")},
		{objectType: schemamodel.GrantObjectExternalDataSource, kind: ydbexternal.SourceKind, key: capability.ExternalDataSources,
			ref: ydbexternal.SourceRef("app/eu", "events")},
		{objectType: schemamodel.GrantObjectExternalTable, kind: ydbexternal.TableKind, key: capability.ExternalDataSources,
			ref: ydbexternal.TableRef("app/eu", "events")},
	}
	listed := make([]string, 0, len(tests))
	for _, kind := range schemamodel.GrantObjectKinds() {
		listed = append(listed, kind.Kind)
	}
	covered := make([]string, 0, len(tests))
	for _, test := range tests {
		covered = append(covered, test.objectType)
		t.Run(test.objectType, func(t *testing.T) {
			c := qt.New(t)
			objectType, typed := ydbacl.ObjectType(test.kind)
			key, keyed := ydbacl.ObjectCapability(test.objectType)
			byReference, referenced := ydbacl.ObjectRef(test.objectType, "app/eu.events", "")
			byPath, pathed := ydbacl.ObjectRef(test.objectType, "/local/app/eu/events", "/local/")
			named, name, ok := ydbacl.ObjectName(test.ref)

			c.Assert(ydbacl.IsSchemeObject(test.objectType), qt.IsTrue)
			c.Assert([]any{objectType, typed}, qt.DeepEquals, []any{test.objectType, true})
			c.Assert([]any{key, keyed}, qt.DeepEquals, []any{test.key, true})
			c.Assert([]any{byReference, referenced}, qt.DeepEquals, []any{test.ref, true})
			c.Assert([]any{byPath, pathed}, qt.DeepEquals, []any{test.ref, true})
			c.Assert([]any{named, name, ok}, qt.DeepEquals, []any{test.objectType, "app/eu.events", true})
		})
	}
	c := qt.New(t)
	c.Assert(covered, qt.DeepEquals, listed)
}

// TestSchemeObjects_FailurePath pins what names no owned scheme object: the
// database, a directory and a table, a kind no owner has, an absolute path
// outside the database or with no database to read it against, and an
// identity of a kind no grant names.
func TestSchemeObjects_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		objectType string
		objectName string
		root       string
	}{
		{name: "a table", objectType: "TABLE", objectName: "app.events"},
		{name: "a directory", objectType: "SCHEMA", objectName: "app"},
		{name: "a kind no owner has", objectType: "QUEUE", objectName: "app.events"},
		{name: "a path outside the database", objectType: "TOPIC", objectName: "/other/app/events", root: "/local"},
		{name: "a path with no database", objectType: "TOPIC", objectName: "/local/app/events"},
		{name: "the database itself", objectType: "TOPIC", objectName: "/local/", root: "/local"},
		{name: "no name", objectType: "TOPIC", objectName: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref, ok := ydbacl.ObjectRef(test.objectType, test.objectName, test.root)
			c.Assert(ok, qt.IsFalse)
			c.Assert(ref, qt.Equals, objectidentity.ID{})
		})
	}
	t.Run("an identity of a kind no grant names", func(t *testing.T) {
		c := qt.New(t)
		objectType, name, ok := ydbacl.ObjectName(ydbreplication.ReplicationRef("", "x"))
		c.Assert([]any{objectType, name, ok}, qt.DeepEquals, []any{"", "", false})
	})
	t.Run("the table kind", func(t *testing.T) {
		c := qt.New(t)
		key, ok := ydbacl.ObjectCapability("TABLE")
		c.Assert([]any{key, ok, ydbacl.IsSchemeObject("TABLE")}, qt.DeepEquals, []any{capability.Capability(""), false, false})
	})
}

// TestDescribesSchema pins the permissions whose holders a new secret gives
// ydb.granular.describe_schema, measured on 26.2.1.14 by granting each
// permission alone on a directory and creating a secret in it.
func TestDescribesSchema(t *testing.T) {
	c := qt.New(t)
	describing := slices.DeleteFunc(ydbacl.Permissions(), func(permission string) bool {
		return !ydbacl.DescribesSchema(permission)
	})
	c.Assert(describing, qt.DeepEquals, []string{
		"ydb.generic.full", "ydb.generic.full_legacy", "ydb.generic.list", "ydb.generic.read",
		"ydb.generic.use", "ydb.generic.use_legacy", "ydb.granular.describe_schema",
	})
}
