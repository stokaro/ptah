package atlassource

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/atlasurl"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/migrationreplay"
	"github.com/stokaro/ptah/internal/schemafile"
	"github.com/stokaro/ptah/migration/migrator"
)

// revisionTableName is the Atlas revision table filtered out of replayed
// dev-database state, mirroring `atlas migrate diff` behavior.
const revisionTableName = "atlas_schema_revisions"

// ResolveOptions configures resolution of one classified desired-state set.
type ResolveOptions struct {
	// Dialect pins the SQL dialect used to parse local schema files and to
	// validate database-backed sources.
	Dialect string
	// DialectFlag names the flag that pinned Dialect ("--url" for schema
	// apply; "--dev-url", "--from", or "--to" for schema diff). It is used in
	// dialect-mismatch errors.
	DialectFlag string
	// DevURL is the dev database URL used to replay migration-directory
	// sources.
	DevURL string
}

// State is one resolved desired-state. Resolution closes every connection it
// opens, so a State is pure data.
type State struct {
	// Kind is the concrete source kind the state was resolved from.
	Kind Kind
	// Schema is the desired-state schema IR.
	Schema *goschema.Database
	// DB is the introspected database state backing Schema for database and
	// migration-directory sources; nil for local schema files.
	DB *dbschematypes.DBSchema
	// DefaultSchema is the schema that owns unqualified objects for database
	// and migration-directory sources ("public" for PostgreSQL, the database
	// name for MySQL-family targets, "main" for SQLite); empty for local
	// schema files. Schema-scope filtering uses it to resolve unqualified
	// object names.
	DefaultSchema string
}

// Resolve materializes the set's desired state. Local schema files load
// exactly as before this resolver existed; database URLs are introspected
// live; migration directories are replayed on the dev database and the result
// is introspected.
func (s Set) Resolve(ctx context.Context, opts ResolveOptions) (State, error) {
	switch s.Kind {
	case KindLocalFile:
		schema, err := schemafile.LoadAll(s.rawURLs(), schemafile.Options{Dialect: opts.Dialect})
		if err != nil {
			return State{}, err
		}
		return State{Kind: s.Kind, Schema: schema}, nil
	case KindDatabase:
		return s.resolveDatabase(ctx, opts)
	case KindMigrationDir:
		return s.resolveMigrationDir(ctx, opts)
	default:
		return State{}, fmt.Errorf("%s: unresolved %s desired-state source", s.Flag, s.Kind)
	}
}

func (s Set) rawURLs() []string {
	urls := make([]string, 0, len(s.Sources))
	for _, source := range s.Sources {
		urls = append(urls, source.Raw)
	}
	return urls
}

func (s Set) resolveDatabase(ctx context.Context, opts ResolveOptions) (State, error) {
	if err := s.ensureDialect(opts); err != nil {
		return State{}, err
	}
	conn, err := dbschema.ConnectToDatabase(ctx, s.Sources[0].Raw)
	if err != nil {
		return State{}, fmt.Errorf("connect to %s database: %w", s.Flag, err)
	}
	defer dbschema.CloseAndWarn(conn)

	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return State{}, fmt.Errorf("read %s database schema: %w", s.Flag, err)
	}
	return State{
		Kind:          s.Kind,
		Schema:        dbschematogo.ConvertDBSchemaToGoSchema(schema),
		DB:            schema,
		DefaultSchema: conn.Info().Schema,
	}, nil
}

// ensureDialect rejects database sources whose URL scheme resolves to a
// different dialect than the pinned one, before any connection is opened.
func (s Set) ensureDialect(opts ResolveOptions) error {
	implied := s.ImpliedDialect()
	pinned := platform.NormalizeDialect(opts.Dialect)
	if pinned == "" {
		pinned = opts.Dialect
	}
	if implied == "" || pinned == "" || implied == pinned {
		return nil
	}
	return fmt.Errorf("%s database dialect %q does not match %s dialect %q", s.Flag, implied, opts.DialectFlag, pinned)
}

func (s Set) resolveMigrationDir(ctx context.Context, opts ResolveOptions) (State, error) {
	source := s.Sources[0]
	devURL := strings.TrimSpace(opts.DevURL)
	if err := s.EnsureDevDatabase(devURL); err != nil {
		return State{}, err
	}
	if isDockerURL(devURL) {
		return State{}, errors.New("docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for migration SQL replay")
	}
	if err := s.ensureDevDialect(devURL, opts); err != nil {
		return State{}, err
	}
	if err := VerifyMigrationDir(source.Path); err != nil {
		return State{}, err
	}

	conn, err := dbschema.ConnectToDatabase(ctx, devURL)
	if err != nil {
		return State{}, fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	if err := migrationreplay.ReplayOnConnection(ctx, conn, source.Path, migrator.MigrationDirFormatAtlas); err != nil {
		return State{}, fmt.Errorf("%s %q: %w", s.Flag, source.Raw, err)
	}
	schema, err := dbschema.ReadSchemaWithSchemas(conn, nil)
	if err != nil {
		return State{}, fmt.Errorf("read dev database schema: %w", err)
	}
	schema = WithoutRevisionTable(schema)
	return State{
		Kind:          s.Kind,
		Schema:        dbschematogo.ConvertDBSchemaToGoSchema(schema),
		DB:            schema,
		DefaultSchema: conn.Info().Schema,
	}, nil
}

func (s Set) ensureDevDialect(devURL string, opts ResolveOptions) error {
	devDialect, err := atlasurl.DialectFromURL(devURL)
	if err != nil {
		return err
	}
	pinned := platform.NormalizeDialect(opts.Dialect)
	if devDialect == "" || pinned == "" || devDialect == pinned {
		return nil
	}
	return fmt.Errorf("--dev-url dialect %q does not match %s dialect %q", devDialect, opts.DialectFlag, pinned)
}

// VerifyMigrationDir mirrors `atlas migrate diff` checksum handling for a
// migration directory used as a desired-state or inspection source: a missing
// atlas.sum is tolerated, an invalid one fails before replay.
func VerifyMigrationDir(dir string) error {
	result, err := migratesum.VerifyDirWithFormat(dir, migrator.MigrationDirFormatAtlas)
	if errors.Is(err, migratesum.ErrSumFileMissing) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("migration directory checksum verification failed: %w", err)
	}
	if !result.OK() {
		return fmt.Errorf("migration directory checksum verification failed:\n%s", result.Describe())
	}
	return nil
}

// WithoutRevisionTable returns a copy of schema with the Atlas revision table
// (and its indexes and constraints) removed, so replayed dev-database state
// only exposes the migrations' own objects.
func WithoutRevisionTable(schema *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	if schema == nil {
		return &dbschematypes.DBSchema{}
	}
	out := *schema
	out.Tables = filterByTable(out.Tables, func(table dbschematypes.DBTable) bool {
		return !strings.EqualFold(table.Name, revisionTableName)
	})
	out.Indexes = filterByTable(out.Indexes, func(index dbschematypes.DBIndex) bool {
		return !strings.EqualFold(index.TableName, revisionTableName)
	})
	out.Constraints = filterByTable(out.Constraints, func(constraint dbschematypes.DBConstraint) bool {
		return !strings.EqualFold(constraint.TableName, revisionTableName)
	})
	return &out
}

func filterByTable[T any](values []T, keep func(T) bool) []T {
	out := make([]T, 0, len(values))
	for _, value := range values {
		if keep(value) {
			out = append(out, value)
		}
	}
	return out
}

func isDockerURL(rawURL string) bool {
	parsed, err := url.Parse(rawURL)
	return err == nil && parsed.Scheme == "docker"
}
