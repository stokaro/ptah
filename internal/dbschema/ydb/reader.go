package ydb

import (
	"context"
	"errors"
	"fmt"
	"path"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"
	ydbsdk "github.com/ydb-platform/ydb-go-sdk/v3"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/revisiontable"
	"ptah.run/internal/tableref"
	"ptah.run/internal/ydbcoordination"
	"ptah.run/internal/ydburl"
)

// Reader describes a YDB database's row tables, views, topics, async
// replications, transfers and access model.
//
// A Ptah schema is a directory on YDB, and "" is the database root, so a table
// at /local/app/users is table users in schema app. The reader walks the whole
// tree with the scheme service, skipping every directory whose name begins
// with a dot (.sys, .metadata, .tmp, .sys_health, ...), and describes each row
// table with the table service, each view with the view service, each topic
// with the topic service, and each async replication and transfer with the
// replication service. A changefeed's topic sits under its table rather than
// in a directory, so the walk does not meet it as a topic.
//
// A table an async replication writes is recorded rather than described: YDB
// marks one with the `__async_replica` attribute and keeps it read-only, and
// the replication owns it. A replication and a transfer are recorded rather
// than described on a cluster that does not serve the replication API.
//
// The access model is read where YDB keeps it. The owner and the permission
// entries of the database, of each directory and of each table come with the
// scheme service's own answer for the object, so they cover exactly what the
// read walks; the users, groups and memberships come from .sys/auth_users,
// .sys/auth_groups and .sys/auth_group_members, the only place YDB reports
// them. A password is never read: the query names the columns it reads, and
// the password hash is not one of them.
//
// A view is read only on a server with [capability.Views], and a topic only on
// one with [capability.Topics]; every YDB line Ptah measured has both. On a
// server without the key, the object is recorded like the objects below, so a
// plan never meets one the renderer would refuse. A table's TTL is read as its
// row deletion policy.
//
// It describes each coordination node with the coordination service, except
// Ptah's own lock node at the root ([LockNode]), which it leaves out of every
// read as it leaves out the migrator's tables. A node whose name starts with a
// dot is the server's, like a dot-directory, and is recorded as not described.
//
// An object it meets and Ptah does not model -- a column table, a topic of the
// older persistent queue kind, and the rest of [coverage]'s YDB kinds -- is
// recorded in [catalog.Database.NotDescribed] by its path, as is a table
// setting such as a changefeed or a TTL run interval. An object or an index
// kind the reader does not know is refused by name rather than read as the
// nearest known one.
type Reader struct {
	open     func(context.Context) (Source, func(), error)
	database string
	// realm is set when the root the reader reads is a dev realm's
	// directory rather than a database: Ptah's lock node and the dev realms
	// live at the root of a database, so a realm's root holds neither.
	realm   bool
	caps    capability.Capabilities
	schemas []string
}

// NewReader returns a reader that reads root, an absolute path in the database
// driver is connected to, on a server with caps. root is the database itself,
// or the directory of the dev realm a URL named; see [Connection.Root].
func NewReader(driver *ydbsdk.Driver, root string, caps capability.Capabilities) *Reader {
	return &Reader{
		open: func(ctx context.Context) (Source, func(), error) {
			return newGRPCSource(ctx, driver)
		},
		database: "/" + strings.Trim(root, "/"),
		realm:    strings.Trim(root, "/") != strings.Trim(driver.Name(), "/"),
		caps:     caps.Clone(),
	}
}

// NewReaderFromSource returns a reader that asks source about database, an
// absolute path such as /local, and reads it as [NewReader] reads its root.
func NewReaderFromSource(source Source, database string, caps capability.Capabilities) *Reader {
	return &Reader{
		open: func(context.Context) (Source, func(), error) {
			return source, func() {}, nil
		},
		database: "/" + strings.Trim(database, "/"),
		caps:     caps.Clone(),
	}
}

// SetSchemas limits the read to the directories named, each relative to the
// database root, with "" for the root itself. A directory's subdirectories are
// schemas of their own and are not included by naming it. An empty list reads
// every directory.
func (r *Reader) SetSchemas(schemas []string) {
	if len(schemas) == 0 {
		r.schemas = nil
		return
	}
	r.schemas = make([]string, 0, len(schemas))
	for _, schema := range schemas {
		r.schemas = append(r.schemas, strings.Trim(schema, "/"))
	}
}

// ReadSchema reads the database under context.Background().
func (r *Reader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

// ReadSchemaContext reads the database. Tables come out ordered by schema and
// name, and each table's columns in the order the table declares them.
func (r *Reader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	source, end, err := r.open(ctx)
	if err != nil {
		return nil, err
	}
	defer end()

	db := &catalog.Database{DatabasePath: "/" + strings.Trim(r.database, "/")}
	if err := r.walk(ctx, source, "", db); err != nil {
		return nil, err
	}
	if err := r.principals(ctx, source, db); err != nil {
		return nil, err
	}
	if err := r.resourcePools(ctx, source, db); err != nil {
		return nil, err
	}
	return db, nil
}

// walk reads the directory schema, relative to the database root, and the
// directories under it.
func (r *Reader) walk(ctx context.Context, source Source, schema string, db *catalog.Database) error {
	self, entries, err := source.ListDirectory(ctx, r.absolute(schema, ""))
	if err != nil {
		return err
	}
	r.directoryAccess(schema, self, db)
	slices.SortFunc(entries, func(a, b *Ydb_Scheme.Entry) int { return strings.Compare(a.GetName(), b.GetName()) })
	for _, entry := range entries {
		if err := r.entry(ctx, source, schema, entry, db); err != nil {
			return err
		}
	}
	return nil
}

// LockNode is the coordination node, at the database root, whose semaphores
// are Ptah's locks (see internal/dblock). The reader leaves it out of every
// schema, as it leaves out the migrator's tables: it is Ptah's bookkeeping,
// and a plan that dropped it would only have the next run create it again.
// It leaves out ydburl.RealmDirectory at the root for the same reason.
const LockNode = ydbcoordination.LockNode

// entry reads one directory entry.
func (r *Reader) entry(
	ctx context.Context,
	source Source,
	schema string,
	entry *Ydb_Scheme.Entry,
	db *catalog.Database,
) error {
	name := entry.GetName()
	switch entry.GetType() {
	case Ydb_Scheme.Entry_DIRECTORY:
		return r.directory(ctx, source, schema, name, db)
	case Ydb_Scheme.Entry_TABLE:
		return r.tableEntry(ctx, source, schema, name, db)
	case Ydb_Scheme.Entry_VIEW, Ydb_Scheme.Entry_TOPIC, Ydb_Scheme.Entry_REPLICATION, Ydb_Scheme.Entry_TRANSFER:
		if described, err := r.keyedEntry(ctx, source, schema, entry, db); described || err != nil {
			return err
		}
	case Ydb_Scheme.Entry_DATABASE:
		// Another database whose root sits under this one. It is not part
		// of the database this connection reads.
		return nil
	case Ydb_Scheme.Entry_SYS_VIEW:
		// A system view outside a dot-directory belongs to the server too.
		return nil
	case Ydb_Scheme.Entry_COORDINATION_NODE:
		return r.coordinationNode(ctx, source, schema, name, db)
	}
	if !r.inScope(schema) {
		return nil
	}
	kind, known := unmodeledEntries[entry.GetType()]
	if !known {
		return fmt.Errorf("YDB object %s is a %s, which Ptah does not read; "+
			"remove it from the database or read a schema that does not hold it",
			r.absolute(schema, name), entryTypeName(entry.GetType()))
	}
	db.NotDescribed = db.NotDescribed.With(unmodeled(kind, schema, name))
	return nil
}

// tableEntry describes the row table name in the directory schema, or records
// it: a table an async replication writes is the replication's, and YDB
// keeps it read-only, so it is recorded rather than described. A table
// another operation drops while the read runs is left out, as one dropped
// before it would be; see [describeListedTable].
func (r *Reader) tableEntry(ctx context.Context, source Source, schema, name string, db *catalog.Database) error {
	if !r.inScope(schema) || revisiontable.IsDefault(name) {
		// The migrator's own tables are its bookkeeping, not the
		// schema, as every other reader treats its revision tables.
		// The tag table is one of them: measured on 26.2.1.14, a read
		// of the migrations directory listed it, so a scoped plan
		// would drop it.
		return nil
	}
	described, err := describeListedTable(ctx, source, r.absolute(schema, ""), name)
	if errors.Is(err, errTableGone) {
		return nil
	}
	if err != nil {
		return err
	}
	if isReplica(described.GetAttributes()) {
		db.NotDescribed = db.NotDescribed.With(replicaTable(schema, name))
		return nil
	}
	return r.table(ctx, source, schema, name, described, db)
}

// keyedEntries are the kinds of entry the reader describes on a target with
// the capability each names, and records rather than describes on one
// without it.
var keyedEntries = map[Ydb_Scheme.Entry_Type]capability.Capability{
	Ydb_Scheme.Entry_VIEW:        capability.Views,
	Ydb_Scheme.Entry_TOPIC:       capability.Topics,
	Ydb_Scheme.Entry_REPLICATION: capability.AsyncReplication,
	Ydb_Scheme.Entry_TRANSFER:    capability.Transfers,
}

// keyedEntry describes an entry of a kind [keyedEntries] names, and reports
// false when the target lacks the kind's key or the entry is out of scope, so
// the caller records the entry rather than describing it.
func (r *Reader) keyedEntry(
	ctx context.Context,
	source Source,
	schema string,
	entry *Ydb_Scheme.Entry,
	db *catalog.Database,
) (bool, error) {
	if !r.caps.Has(keyedEntries[entry.GetType()]) || !r.inScope(schema) {
		return false, nil
	}
	name := entry.GetName()
	switch entry.GetType() {
	case Ydb_Scheme.Entry_VIEW:
		described, err := source.DescribeView(ctx, r.absolute(schema, name))
		if err != nil {
			return true, err
		}
		return true, r.view(schema, name, described, db)
	case Ydb_Scheme.Entry_TOPIC:
		return true, r.topic(ctx, source, schema, name, db)
	case Ydb_Scheme.Entry_REPLICATION:
		return true, r.replication(ctx, source, schema, name, db)
	default:
		return true, r.transfer(ctx, source, schema, name, db)
	}
}

// directory reads the directory name in schema, unless it belongs to the
// server or to the dev realms.
func (r *Reader) directory(ctx context.Context, source Source, schema, name string, db *catalog.Database) error {
	if strings.HasPrefix(name, ".") {
		// .sys, .metadata, .tmp and every other dot-directory belong to the
		// server.
		return nil
	}
	if schema == "" && name == ydburl.RealmDirectory {
		// The dev realms runs create here are theirs, and no part of the
		// database's schema.
		return nil
	}
	return r.walk(ctx, source, path.Join(schema, name), db)
}

// unmodeledEntries maps each scheme entry type Ptah does not model to the
// coverage kind it is recorded under. A view, a topic, an async replication
// and a transfer are here for a server without [capability.Views],
// [capability.Topics], [capability.AsyncReplication] or [capability.Transfers],
// whose reader records them rather than describing them.
var unmodeledEntries = map[Ydb_Scheme.Entry_Type]coverage.Kind{
	Ydb_Scheme.Entry_VIEW:                 coverage.View,
	Ydb_Scheme.Entry_TOPIC:                coverage.Topic,
	Ydb_Scheme.Entry_PERS_QUEUE_GROUP:     coverage.Topic,
	Ydb_Scheme.Entry_COLUMN_TABLE:         coverage.ColumnTable,
	Ydb_Scheme.Entry_COLUMN_STORE:         coverage.ColumnTable,
	Ydb_Scheme.Entry_SEQUENCE:             coverage.Sequence,
	Ydb_Scheme.Entry_REPLICATION:          coverage.Replication,
	Ydb_Scheme.Entry_TRANSFER:             coverage.Transfer,
	Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE: coverage.ExternalDataSource,
	Ydb_Scheme.Entry_EXTERNAL_TABLE:       coverage.ExternalTable,
	Ydb_Scheme.Entry_SECRET:               coverage.Secret,
	Ydb_Scheme.Entry_RESOURCE_POOL:        coverage.ResourcePool,
	EntryStreamingQuery:                   coverage.StreamingQuery,
}

// EntryStreamingQuery is the scheme entry type of a streaming query, which the
// pinned protocol buffers do not name. Measured on 26.2.1.14 with
// EnableExternalDataSources on: ListDirectory reports a streaming query, at
// the root and in a directory alike, as type 26. A read that met one refused
// the whole database as an object of an unknown type.
const EntryStreamingQuery Ydb_Scheme.Entry_Type = 26

// coordinationNode describes the coordination node name in the directory
// schema. Ptah's lock node at the root of a database is left out, and a node
// whose name starts with a dot is the server's and recorded as not described.
func (r *Reader) coordinationNode(ctx context.Context, source Source, schema, name string, db *catalog.Database) error {
	if schema == "" && name == LockNode && !r.realm {
		return nil
	}
	if !r.inScope(schema) {
		return nil
	}
	if strings.HasPrefix(name, ".") {
		db.NotDescribed = db.NotDescribed.With(unmodeled(coverage.CoordinationNode, schema, name))
		return nil
	}
	absolute := r.absolute(schema, name)
	described, err := source.DescribeCoordinationNode(ctx, absolute)
	if err != nil {
		return err
	}
	node, err := decodeCoordinationNode(schema, name, described)
	if err != nil {
		return fmt.Errorf("YDB coordination node %s: %w", absolute, err)
	}
	db.CoordinationNodes = append(db.CoordinationNodes, node)
	return nil
}

// entryTypeName names a scheme entry type, including one the pinned protocol
// buffers do not know.
func entryTypeName(entryType Ydb_Scheme.Entry_Type) string {
	if name, known := Ydb_Scheme.Entry_Type_name[int32(entryType)]; known {
		return name
	}
	return fmt.Sprintf("scheme entry of type %d", int32(entryType))
}

// unmodeled is the record of one object Ptah does not model.
func unmodeled(kind coverage.Kind, schema, name string) coverage.Object {
	return coverage.Object{
		Kind:       kind,
		Name:       tableref.Canonical(schema, name),
		Reason:     coverage.Unsupported,
		Provenance: coverage.Observed,
	}
}

// TableColumns describes the row table name in the directory schema, relative
// to the database root, and returns its columns in the order the table
// declares them.
//
// It is the migrator's question about its own tables, which [Reader.ReadSchema]
// leaves out, and it reads only the one path. exists is false when nothing
// holds the name, including when the directory that would hold it does not
// exist; an object of another kind under the name is an error, because a
// table cannot be created there.
func (r *Reader) TableColumns(ctx context.Context, schema, name string) (columns []string, exists bool, err error) {
	source, end, err := r.open(ctx)
	if err != nil {
		return nil, false, err
	}
	defer end()

	_, entries, err := source.ListDirectory(ctx, r.absolute(schema, ""))
	if isSchemeError(err) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	index := slices.IndexFunc(entries, func(entry *Ydb_Scheme.Entry) bool { return entry.GetName() == name })
	if index < 0 {
		return nil, false, nil
	}
	if entryType := entries[index].GetType(); entryType != Ydb_Scheme.Entry_TABLE {
		return nil, false, fmt.Errorf("YDB object %s is a %s, not a row table", r.absolute(schema, name),
			entryTypeName(entryType))
	}
	described, err := source.DescribeTable(ctx, r.absolute(schema, name))
	if err != nil {
		return nil, false, err
	}
	for _, column := range described.GetColumns() {
		columns = append(columns, column.GetName())
	}
	return columns, true, nil
}

// inScope reports whether the read covers the directory schema.
func (r *Reader) inScope(schema string) bool {
	return r.schemas == nil || slices.Contains(r.schemas, schema)
}

// absolute is the path of name in the directory schema; an empty name is the
// directory itself.
func (r *Reader) absolute(schema, name string) string {
	return path.Join(r.database, schema, name)
}
