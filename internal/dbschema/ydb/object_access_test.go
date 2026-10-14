package ydb_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/ydb-platform/ydb-go-genproto/draft/protos/Ydb_View"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Coordination"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Table"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Topic"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	ydbschema "ptah.run/internal/dbschema/ydb"
)

// ownedObjectsSource is a directory holding one object of each kind an owner
// manages and a grant can name, each described with the entry its service
// answers with: an owner, and permission entries of its own. A secret has no
// description, so its entry is the one DescribePath answers with. The
// directory's own entry carries the effective permissions a secret created in
// it copies: METADATA-READERS through ydb.generic.list, readers through
// ydb.granular.describe_schema, and writers through a permission that does not
// carry it.
func ownedObjectsSource() fakeSource {
	external := externalSource()
	external.directories = map[string][]*Ydb_Scheme.Entry{
		"/local": {entry("ext", Ydb_Scheme.Entry_DIRECTORY)},
		"/local/ext": {
			entry("events", Ydb_Scheme.Entry_EXTERNAL_TABLE),
			entry("locks", Ydb_Scheme.Entry_COORDINATION_NODE),
			entry("pg", Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE),
			entry("pg_password", Ydb_Scheme.Entry_SECRET),
			entry("s3", Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE),
			entry("stream", Ydb_Scheme.Entry_TOPIC),
		},
	}
	external.sources["/local/ext/pg"].Self = owned("app", permission("readers", "ydb.granular.select_row"))
	external.external["/local/ext/events"].Self = owned("app", permission("readers", "ydb.generic.read"))
	topic := standaloneTopic()
	topic.Self = owned("app", permission("readers", "ydb.granular.select_row", "ydb.generic.list"))
	external.topics = map[string]*Ydb_Topic.DescribeTopicResult{"/local/ext/stream": topic}
	external.nodes = map[string]*Ydb_Coordination.DescribeNodeResult{
		"/local/ext/locks": {Self: owned("root@builtin", permission("writers", "ydb.generic.write")), Config: &Ydb_Coordination.Config{}},
	}
	external.paths = map[string]*Ydb_Scheme.Entry{
		"/local/ext/pg_password": owned("app",
			permission("METADATA-READERS", "ydb.granular.describe_schema"),
			permission("readers", "ydb.granular.describe_schema", "ydb.granular.select_row"),
			permission("writers", "ydb.granular.describe_schema")),
	}
	external.selves = map[string]*Ydb_Scheme.Entry{"/local/ext": {
		Owner: "root@builtin",
		EffectivePermissions: []*Ydb_Scheme.Permissions{
			permission("METADATA-READERS", "ydb.generic.list"),
			permission("readers", "ydb.granular.describe_schema"),
			permission("writers", "ydb.generic.write"),
		},
	}}
	return external
}

// TestReader_ReadsThePermissionsOfOwnedObjects reads the owner and every
// permission entry of a topic, a coordination node, a secret, an external data
// source and an external table, one grant per permission name, as it reads a
// table's. A secret holds a ydb.granular.describe_schema entry for each subject
// that may describe its directory from the moment YDB creates it, so those are
// implicit; the same permission held by a subject that may not describe the
// directory is a grant like any other.
func TestReader_ReadsThePermissionsOfOwnedObjects(t *testing.T) {
	c := qt.New(t)
	caps := capability.YDB262().With(capability.ExternalDataSources, true)

	db, err := ydbschema.NewReaderFromSource(ownedObjectsSource(), "/local", caps).ReadSchemaContext(context.Background())

	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "readers", Privilege: "ydb.generic.read", ObjectType: "EXTERNAL TABLE", Schema: "ext", ObjectName: "events"},
		{Role: "writers", Privilege: "ydb.generic.write", ObjectType: "COORDINATION NODE", Schema: "ext", ObjectName: "locks"},
		{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "EXTERNAL DATA SOURCE", Schema: "ext", ObjectName: "pg"},
		{Role: "METADATA-READERS", Privilege: "ydb.granular.describe_schema", ObjectType: "SECRET", Schema: "ext",
			ObjectName: "pg_password", Implicit: true},
		{Role: "readers", Privilege: "ydb.granular.describe_schema", ObjectType: "SECRET", Schema: "ext",
			ObjectName: "pg_password", Implicit: true},
		{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "SECRET", Schema: "ext", ObjectName: "pg_password"},
		{Role: "writers", Privilege: "ydb.granular.describe_schema", ObjectType: "SECRET", Schema: "ext", ObjectName: "pg_password"},
		{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "TOPIC", Schema: "ext", ObjectName: "stream"},
		{Role: "readers", Privilege: "ydb.generic.list", ObjectType: "TOPIC", Schema: "ext", ObjectName: "stream"},
	})
	c.Assert(db.ObjectOwners, qt.DeepEquals, []catalog.ObjectOwner{
		{Kind: "schema", Name: "ext", Owner: "root@builtin"},
		{Kind: "external table", Schema: "ext", Name: "events", Owner: "app"},
		{Kind: "coordination node", Schema: "ext", Name: "locks", Owner: "root@builtin"},
		{Kind: "external data source", Schema: "ext", Name: "pg", Owner: "app"},
		{Kind: "secret", Schema: "ext", Name: "pg_password", Owner: "app"},
		{Kind: "topic", Schema: "ext", Name: "stream", Owner: "app"},
	})
}

// TestReader_ReadsThePermissionsOfViewsAndReplications reads the owner and
// the permission entries of a view, an async replication and a transfer from
// the descriptions their services answer with. A grant on a view is reported
// as a grant on a table, the shape a declaration names a view with, and its
// owner row says it is a view.
func TestReader_ReadsThePermissionsOfViewsAndReplications(t *testing.T) {
	c := qt.New(t)
	source := replicationSource()
	source.directories["/local"] = append(source.directories["/local"], entry("active_users", Ydb_Scheme.Entry_VIEW))
	source.tables["/local/active_users"] = &Ydb_Table.DescribeTableResult{}
	source.views = map[string]*Ydb_View.DescribeViewResult{"/local/active_users": {
		QueryText: "SELECT id FROM order_log", Self: owned("app", permission("readers", "ydb.granular.select_row"))}}
	source.replications["/local/mirror"].Self = owned("root@builtin", permission("replicators", "ydb.generic.full"))
	source.transfers["/local/ingest"].Self = owned("app",
		permission("readers", "ydb.generic.read"), permission("writers", "ydb.generic.write", "ydb.generic.list"))

	db := readFrom(c, source)

	c.Assert(db.Grants, qt.DeepEquals, []catalog.Grant{
		{Role: "readers", Privilege: "ydb.granular.select_row", ObjectType: "TABLE", ObjectName: "active_users"},
		{Role: "readers", Privilege: "ydb.generic.read", ObjectType: "TRANSFER", ObjectName: "ingest"},
		{Role: "writers", Privilege: "ydb.generic.write", ObjectType: "TRANSFER", ObjectName: "ingest"},
		{Role: "writers", Privilege: "ydb.generic.list", ObjectType: "TRANSFER", ObjectName: "ingest"},
		{Role: "replicators", Privilege: "ydb.generic.full", ObjectType: "ASYNC REPLICATION", ObjectName: "mirror"},
	})
	c.Assert(db.ObjectOwners, qt.DeepEquals, []catalog.ObjectOwner{
		{Kind: "view", Name: "active_users", Owner: "app"},
		{Kind: "transfer", Name: "ingest", Owner: "app"},
		{Kind: "async replication", Name: "mirror", Owner: "root@builtin"},
	})
}

// TestReader_ReadsNoPermissionsOfObjectsItDoesNotDescribe reads the access
// lists of owned objects only where it describes them: not outside the scope
// of a scoped read, and not a secret's on a line without secrets, where the
// secret is recorded as unread and its entry never asked for. The fake answers
// a request for an entry it does not hold with an error, so a read that asked
// would fail.
func TestReader_ReadsNoPermissionsOfObjectsItDoesNotDescribe(t *testing.T) {
	tests := []struct {
		name    string
		caps    capability.Capabilities
		schemas []string
	}{
		{name: "out of scope", caps: capability.YDB262().With(capability.ExternalDataSources, true), schemas: []string{"other"}},
		{name: "a secret on a line without secrets", caps: capability.YDB253()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			source := ownedObjectsSource()
			source.directories["/local/ext"] = []*Ydb_Scheme.Entry{entry("pg_password", Ydb_Scheme.Entry_SECRET)}
			source.directories["/local"] = append(source.directories["/local"], entry("other", Ydb_Scheme.Entry_DIRECTORY))
			source.directories["/local/other"] = nil
			source.paths = nil
			reader := ydbschema.NewReaderFromSource(source, "/local", test.caps)
			reader.SetSchemas(test.schemas)

			db, err := reader.ReadSchemaContext(context.Background())

			c.Assert(err, qt.IsNil)
			c.Assert(db.Grants, qt.HasLen, 0)
		})
	}
}

// TestReader_ObjectPermissions_FailurePath fails a read whose request for a
// secret's entry fails, as a failed description of any other object fails it.
func TestReader_ObjectPermissions_FailurePath(t *testing.T) {
	c := qt.New(t)
	source := ownedObjectsSource()
	source.paths = nil
	caps := capability.YDB262().With(capability.ExternalDataSources, true)

	db, err := ydbschema.NewReaderFromSource(source, "/local", caps).ReadSchemaContext(context.Background())

	c.Assert(err, qt.ErrorMatches, `described path /local/ext/pg_password, which the fixture does not hold`)
	c.Assert(db, qt.IsNil)
}
