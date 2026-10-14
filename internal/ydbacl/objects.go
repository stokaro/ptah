package ydbacl

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/tableref"
)

// The object kinds a grant names beyond the database, a directory and a
// table: scheme objects their YDB owners manage, each with an access list of
// its own. A grant carries them as [schemamodel.Grant.ObjectKind] spells them.
//
// Measured on 25.1.4.7 and 26.2.1.14: GRANT and REVOKE name each by its path
// as they name a table, and take every permission [Permissions] lists. The
// description of each reports the entries in its own access list, which a
// directory listing does not. CREATE OR REPLACE of an external object, ALTER
// TOPIC, ALTER SECRET, a change of a coordination node, ALTER ASYNC
// REPLICATION, ALTER TRANSFER, ALTER RESOURCE POOL and ALTER STREAMING QUERY
// keep the entries; a DROP takes them with the object, so an object dropped
// and created again holds none. A streaming query is measured on 26.2.1.14
// alone, the line that has them.
//
// A resource pool belongs to the database rather than to a directory, and
// its path is [ResourcePoolDirectory] and its name: GRANT on the name alone
// answers `Path does not exist`.
const (
	// ObjectTopic is a standalone topic.
	ObjectTopic = schemamodel.GrantObjectTopic
	// ObjectCoordinationNode is a coordination node.
	ObjectCoordinationNode = schemamodel.GrantObjectCoordinationNode
	// ObjectSecret is a secret.
	ObjectSecret = schemamodel.GrantObjectSecret
	// ObjectExternalDataSource is an external data source.
	ObjectExternalDataSource = schemamodel.GrantObjectExternalDataSource
	// ObjectExternalTable is an external table.
	ObjectExternalTable = schemamodel.GrantObjectExternalTable
	// ObjectAsyncReplication is an async replication.
	ObjectAsyncReplication = schemamodel.GrantObjectAsyncReplication
	// ObjectTransfer is a transfer.
	ObjectTransfer = schemamodel.GrantObjectTransfer
	// ObjectStreamingQuery is a streaming query.
	ObjectStreamingQuery = schemamodel.GrantObjectStreamingQuery
	// ObjectResourcePool is a resource pool, which a grant names by the pool's
	// name alone.
	ObjectResourcePool = schemamodel.GrantObjectResourcePool
)

// ResourcePoolDirectory is the directory, relative to the database root, that
// holds every resource pool's scheme object.
const ResourcePoolDirectory = ".metadata/workload_manager/pools"

// schemeObject is one owned kind a grant names: the object type a grant
// carries, the capability key a target holds the kind under, and the owner's
// model kind and identity. directory is set for a kind that belongs to the
// database rather than to a directory, and is where its objects are; a grant
// names such an object by its name alone.
type schemeObject struct {
	objectType string
	key        capability.Capability
	kind       schemaext.Kind
	ref        func(schema, name string) objectidentity.ID
	directory  string
}

// schemeObjects is every owned kind a grant names. Each target capability is
// the one under which the reader describes the kind, which is also when it
// reads the kind's access list.
var schemeObjects = []schemeObject{
	{ObjectTopic, capability.Topics, ydbtopic.Kind, ydbtopic.Ref, ""},
	{ObjectCoordinationNode, capability.CoordinationNodes, ydbcoordination.Kind, ydbcoordination.Ref, ""},
	{ObjectSecret, capability.Secrets, ydbsecret.Kind, ydbsecret.Ref, ""},
	{ObjectExternalDataSource, capability.ExternalDataSources, ydbexternal.SourceKind, ydbexternal.SourceRef, ""},
	{ObjectExternalTable, capability.ExternalDataSources, ydbexternal.TableKind, ydbexternal.TableRef, ""},
	{ObjectAsyncReplication, capability.AsyncReplication, ydbreplication.ReplicationKind, ydbreplication.ReplicationRef, ""},
	{ObjectTransfer, capability.Transfers, ydbreplication.TransferKind, ydbreplication.TransferRef, ""},
	{ObjectStreamingQuery, capability.StreamingQueries, ydbstreaming.Kind, ydbstreaming.Ref, ""},
	{ObjectResourcePool, capability.ResourcePools, ydbworkload.PoolKind, poolRef, ResourcePoolDirectory},
}

// poolRef is the identity of the resource pool name; a pool has no schema.
func poolRef(_, name string) objectidentity.ID { return ydbworkload.PoolRef(name) }

// databaseObjectName is the name of the object of a database-wide kind that
// objectName names, the name alone or the absolute path under root, and false
// for anything else: a pool is in no directory of its own.
func (o schemeObject) databaseObjectName(objectName, root string) (string, bool) {
	name := objectName
	if strings.HasPrefix(objectName, "/") {
		database := strings.TrimRight(root, "/")
		relative, under := strings.CutPrefix(objectName, database+"/"+o.directory+"/")
		if database == "" || !under {
			return "", false
		}
		name = relative
	}
	return name, name != "" && !strings.Contains(name, "/")
}

// schemeObjectOf is the owned kind objectType names, matched without regard
// to case or spacing.
func schemeObjectOf(objectType string) (schemeObject, bool) {
	normalized := strings.Join(strings.Fields(strings.ToUpper(objectType)), " ")
	for _, object := range schemeObjects {
		if object.objectType == normalized {
			return object, true
		}
	}
	return schemeObject{}, false
}

// IsSchemeObject reports whether objectType names a scheme object an owner
// manages, rather than the database, a directory or a table.
func IsSchemeObject(objectType string) bool {
	_, ok := schemeObjectOf(objectType)
	return ok
}

// ObjectCapability returns the capability key a target needs before a grant
// may name an object of objectType, and false for a type that needs none of
// its own: the database, a directory and a table.
func ObjectCapability(objectType string) (capability.Capability, bool) {
	object, ok := schemeObjectOf(objectType)
	return object.key, ok
}

// ObjectType returns the object type a grant carries for the owner model kind
// kind, and false for a kind no grant names.
func ObjectType(kind schemaext.Kind) (string, bool) {
	object, ok := schemeObjectOfKind(kind)
	return object.objectType, ok
}

// schemeObjectOfKind is the owned kind whose owner model kind is kind.
func schemeObjectOfKind(kind schemaext.Kind) (schemeObject, bool) {
	for _, object := range schemeObjects {
		if object.kind == kind {
			return object, true
		}
	}
	return schemeObject{}, false
}

// ObjectRef returns the owner's identity of the object a grant on objectType
// names by objectName, and false when objectType is not an owned kind or the
// name names no object. objectName is a reference qualified by its directory,
// as a table's is, or an absolute path under root, the absolute path of the
// database, which a plan writes where the line resolves no relative one. A
// resource pool is named by its name alone. The planner orders a GRANT and a
// REVOKE against the owner's statements on this identity.
func ObjectRef(objectType, objectName, root string) (objectidentity.ID, bool) {
	object, ok := schemeObjectOf(objectType)
	if !ok {
		return objectidentity.ID{}, false
	}
	if object.directory != "" {
		name, named := object.databaseObjectName(objectName, root)
		if !named {
			return objectidentity.ID{}, false
		}
		return object.ref("", name), true
	}
	if strings.HasPrefix(objectName, "/") {
		database := strings.TrimRight(root, "/")
		relative, under := strings.CutPrefix(objectName, database+"/")
		if database == "" || !under || relative == "" {
			return objectidentity.ID{}, false
		}
		schema, name := "", relative
		if slash := strings.LastIndex(relative, "/"); slash >= 0 {
			schema, name = relative[:slash], relative[slash+1:]
		}
		return object.ref(schema, name), name != ""
	}
	ref, parsed := tableref.Parse(objectName)
	if !parsed || ref.Name == "" {
		return objectidentity.ID{}, false
	}
	return object.ref(strings.Trim(ref.Schema, "/"), ref.Name), true
}

// ObjectName is how a grant names the object at ref, an identity of one of
// the owned kinds, together with the object type it carries, and false for
// a ref of any other kind.
func ObjectName(ref objectidentity.ID) (objectType, objectName string, ok bool) {
	object, ok := schemeObjectOfKind(schemaext.Kind(ref.Kind))
	if !ok {
		return "", "", false
	}
	if object.directory != "" {
		return object.objectType, ref.Name.Source, true
	}
	return object.objectType, tableref.Canonical(ref.Schema.Source, ref.Name.Source), true
}
