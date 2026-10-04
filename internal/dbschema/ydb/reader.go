package ydb

import (
	"context"
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
)

// Reader describes a YDB database's row tables.
//
// A Ptah schema is a directory on YDB, and "" is the database root, so a table
// at /local/app/users is table users in schema app. The reader walks the whole
// tree with the scheme service, skipping every directory whose name begins
// with a dot (.sys, .metadata, .tmp, .sys_health, ...), and describes each row
// table with the table service. It never reads a system view.
//
// An object it meets and Ptah does not model -- a view, a topic, a column
// table, a coordination node, and the rest of [coverage]'s YDB kinds -- is
// recorded in [catalog.Database.NotDescribed] by its path, as is a table
// setting such as a TTL or a changefeed. The access model is recorded as a
// whole kind, because the reader does not read it. An object or an index kind
// the reader does not know is refused by name rather than read as the nearest
// known one.
type Reader struct {
	open     func(context.Context) (Source, func(), error)
	database string
	caps     capability.Capabilities
	schemas  []string
}

// NewReader returns a reader for the database driver is connected to, on a
// server with caps.
func NewReader(driver *ydbsdk.Driver, caps capability.Capabilities) *Reader {
	return &Reader{
		open: func(ctx context.Context) (Source, func(), error) {
			return newGRPCSource(ctx, driver)
		},
		database: driver.Name(),
		caps:     caps.Clone(),
	}
}

// NewReaderFromSource returns a reader that asks source about database, an
// absolute path such as /local.
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

	db := &catalog.Database{}
	if err := r.walk(ctx, source, "", db); err != nil {
		return nil, err
	}
	// The access model lives in .sys/auth_* and in each object's ACL, and the
	// reader reads neither, so the description holds no users, groups or
	// grants whatever the database has.
	db.NotDescribed = db.NotDescribed.With(
		coverage.Object{Kind: coverage.Role, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromTarget},
		coverage.Object{Kind: coverage.Grant, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromTarget},
	)
	return db, nil
}

// walk reads the directory schema, relative to the database root, and the
// directories under it.
func (r *Reader) walk(ctx context.Context, source Source, schema string, db *catalog.Database) error {
	entries, err := source.ListDirectory(ctx, r.absolute(schema, ""))
	if err != nil {
		return err
	}
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
const LockNode = "ptah_locks"

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
		if strings.HasPrefix(name, ".") {
			// .sys, .metadata, .tmp and every other dot-directory belong to
			// the server.
			return nil
		}
		return r.walk(ctx, source, path.Join(schema, name), db)
	case Ydb_Scheme.Entry_TABLE:
		if !r.inScope(schema) || revisiontable.IsDefault(name) {
			// The migrator's own tables are its bookkeeping, not the
			// schema, as every other reader treats its revision tables.
			// The tag table is one of them: measured on 26.2.1.14, a read
			// of the migrations directory listed it, so a scoped plan
			// would drop it.
			return nil
		}
		described, err := source.DescribeTable(ctx, r.absolute(schema, name))
		if err != nil {
			return err
		}
		return r.table(ctx, source, schema, name, described, db)
	case Ydb_Scheme.Entry_DATABASE:
		// Another database whose root sits under this one. It is not part
		// of the database this connection reads.
		return nil
	case Ydb_Scheme.Entry_SYS_VIEW:
		// A system view outside a dot-directory belongs to the server too.
		return nil
	case Ydb_Scheme.Entry_COORDINATION_NODE:
		if schema == "" && name == LockNode {
			return nil
		}
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

// unmodeledEntries maps each scheme entry type Ptah does not model to the
// coverage kind it is recorded under.
var unmodeledEntries = map[Ydb_Scheme.Entry_Type]coverage.Kind{
	Ydb_Scheme.Entry_VIEW:                 coverage.View,
	Ydb_Scheme.Entry_TOPIC:                coverage.Topic,
	Ydb_Scheme.Entry_PERS_QUEUE_GROUP:     coverage.Topic,
	Ydb_Scheme.Entry_COLUMN_TABLE:         coverage.ColumnTable,
	Ydb_Scheme.Entry_COLUMN_STORE:         coverage.ColumnTable,
	Ydb_Scheme.Entry_COORDINATION_NODE:    coverage.CoordinationNode,
	Ydb_Scheme.Entry_SEQUENCE:             coverage.Sequence,
	Ydb_Scheme.Entry_REPLICATION:          coverage.Replication,
	Ydb_Scheme.Entry_TRANSFER:             coverage.Transfer,
	Ydb_Scheme.Entry_EXTERNAL_DATA_SOURCE: coverage.ExternalDataSource,
	Ydb_Scheme.Entry_EXTERNAL_TABLE:       coverage.ExternalTable,
	Ydb_Scheme.Entry_SECRET:               coverage.Secret,
	Ydb_Scheme.Entry_RESOURCE_POOL:        coverage.ResourcePool,
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

	entries, err := source.ListDirectory(ctx, r.absolute(schema, ""))
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
