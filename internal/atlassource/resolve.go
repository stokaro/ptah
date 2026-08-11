package atlassource

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/schemasource"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/internal/schemascope"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	// revisionTableName is the Atlas revision table filtered out of replayed
	// dev-database state, mirroring `atlas migrate diff` behavior.
	revisionTableName = "atlas_schema_revisions"
)

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
	// SchemaScope and SchemaScopeFlag limit an HCL desired state to one schema;
	// see [go.5x5.cz/ptah/internal/schemafile.Options]. They are passed in
	// rather than derived from DevURL here because a verb with a target URL --
	// `schema apply` -- is limited by either one, and the caller is the only
	// layer that knows which flags it has.
	SchemaScope     string
	SchemaScopeFlag string
	// ConnectTimeout bounds opening a database-backed source and reading its
	// initial connection metadata. A zero value leaves the caller's context
	// deadline unchanged.
	ConnectTimeout time.Duration
	// DevLockHeld tells migration-directory resolution that the caller already
	// holds the dev database realm lock across a larger operation.
	DevLockHeld bool
	// Schemas restricts introspection of database-backed sources (live database
	// URLs and replayed migration directories) to the named schema scopes.
	// Repeated and comma-separated values union deterministically; empty reads
	// the connection's default scope.
	//
	// Resolution must read what the caller asked for: a post-hoc filter over a
	// universe that was never introspected selects nothing, and nothing is
	// indistinguishable from an empty database.
	Schemas []string
	// IgnoreUnknownHCLNames accepts and drops schema-HCL names Ptah does not
	// model instead of refusing the file. Off by default: it belongs to the
	// Atlas-compatible command tree, which reads files written for another
	// tool, and not to Ptah's own commands, where an unmodeled name is a typo
	// worth naming. See [go.5x5.cz/ptah/internal/schemafile.Options].
	IgnoreUnknownHCLNames bool
	// Vars supplies values for the `variable` blocks of an HCL schema file, as
	// `--var` spells them. See [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// ValidateSchema applies a caller-selected policy after any source kind is
	// fully materialized and before the resolved state is returned. Nil accepts
	// every schema. The callback is interface-neutral: compatibility adapters
	// select policy without making this shared resolver depend on a CLI layer.
	ValidateSchema func(*goschema.Database) error
	// ValidateMigrationSource applies a caller-selected policy to the stable,
	// checksum-verified snapshot of a migration-directory source before the dev
	// database is connected to or reset. Nil accepts every migration body.
	ValidateMigrationSource func(fs.FS) error
	// ValidateLocalSchemaSource applies a caller-selected policy to each local
	// schema path before it is parsed or a dev database is opened. Nil accepts
	// every source format supported by schemafile.
	ValidateLocalSchemaSource func(string) error
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
// is introspected; external schema programs run without a shell and their
// standard output is parsed as the desired schema.
func (s Set) Resolve(ctx context.Context, opts ResolveOptions) (State, error) {
	if s.Kind == KindLocalFile && opts.ValidateLocalSchemaSource != nil {
		for _, source := range s.Sources {
			if err := opts.ValidateLocalSchemaSource(source.Path); err != nil {
				return State{}, err
			}
		}
	}
	state, err := s.resolve(ctx, opts)
	if err != nil {
		return State{}, err
	}
	if opts.ValidateSchema != nil {
		if err := opts.ValidateSchema(state.Schema); err != nil {
			return State{}, err
		}
	}
	return state, nil
}

func (s Set) resolve(ctx context.Context, opts ResolveOptions) (State, error) {
	switch s.Kind {
	case KindLocalFile:
		schema, err := schemafile.LoadAll(s.rawURLs(), schemafile.Options{
			Dialect:               opts.Dialect,
			IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
			SchemaScope:           opts.SchemaScope,
			SchemaScopeFlag:       opts.SchemaScopeFlag,
			Vars:                  opts.Vars,
		})
		if err != nil {
			return State{}, err
		}
		return State{Kind: s.Kind, Schema: schema}, nil
	case KindDatabase:
		return s.resolveDatabase(ctx, opts)
	case KindMigrationDir:
		return s.resolveMigrationDir(ctx, opts)
	case KindExternalSchema:
		return s.resolveExternalSchema(ctx, opts)
	default:
		return State{}, fmt.Errorf("%s: unresolved %s desired-state source", s.Flag, s.Kind)
	}
}

// resolveExternalSchema runs the classified external schema program and parses
// its standard output into the desired schema IR. The classification gate has
// already been passed; execution itself delegates to the same schemasource
// runner the native external_schema path uses (no shell, bounded output,
// stderr surfaced on failure).
func (s Set) resolveExternalSchema(ctx context.Context, opts ResolveOptions) (State, error) {
	command := s.Sources[0].Command
	command.Dialect = opts.Dialect
	schema, err := schemasource.Run(ctx, command)
	if err != nil {
		return State{}, fmt.Errorf("%s %q: %w", s.Flag, s.Sources[0].Raw, err)
	}
	return State{Kind: s.Kind, Schema: schema}, nil
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
	conn, err := connectDatabase(ctx, s.Sources[0].Raw, opts.ConnectTimeout)
	if err != nil {
		return State{}, fmt.Errorf("connect to %s database: %w", s.Flag, err)
	}
	defer dbschema.CloseAndWarn(conn)

	// Which schemas this read covers is [schemascope.ReadNames]'s decision, not
	// this function's. Deriving it here is what made a database URL describe the
	// connected schema alone while `schema inspect` described the whole realm,
	// and the comparator -- which cannot tell a schema nobody read from a schema
	// nobody wants -- planned `DROP TABLE "extra"."b" CASCADE` off the
	// difference (stokaro/ptah#1276).
	names, err := schemascope.ReadNames(ctx, conn.Info(), opts.Schemas, conn)
	if err != nil {
		return State{}, fmt.Errorf("read %s database schema: %w", s.Flag, err)
	}
	schema, err := dbschema.ReadSchemaWithSchemas(conn, names)
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
	snapshot, err := CaptureVerifiedMigrationDir(source.Path)
	if err != nil {
		return State{}, err
	}
	if opts.ValidateMigrationSource != nil {
		if err := opts.ValidateMigrationSource(snapshot); err != nil {
			return State{}, err
		}
	}

	conn, err := connectDatabase(ctx, devURL, opts.ConnectTimeout)
	if err != nil {
		return State{}, fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	var state State
	replay := migrationreplay.WithReplayedSnapshot
	if opts.DevLockHeld {
		replay = migrationreplay.WithReplayedSnapshotLocked
	}
	if err := replay(
		ctx,
		conn,
		snapshot,
		migrator.MigrationDirFormatAtlas,
		func(replayConn *dbschema.DatabaseConnection) error {
			// Same decision, same owner: a migration directory that creates a
			// second schema describes it, and a read scoped to the dev
			// connection's own schema would report the replay as having created
			// nothing there (stokaro/ptah#1276).
			names, err := schemascope.ReadNames(ctx, replayConn.Info(), opts.Schemas, replayConn)
			if err != nil {
				return fmt.Errorf("read dev database schema: %w", err)
			}
			schema, err := dbschema.ReadSchemaWithSchemas(replayConn, names)
			if err != nil {
				return fmt.Errorf("read dev database schema: %w", err)
			}
			schema = WithoutRevisionTable(schema)
			state = State{
				Kind:          s.Kind,
				Schema:        dbschematogo.ConvertDBSchemaToGoSchema(schema),
				DB:            schema,
				DefaultSchema: replayConn.Info().Schema,
			}
			return nil
		},
	); err != nil {
		return State{}, fmt.Errorf("%s %q: %w", s.Flag, source.Raw, err)
	}
	if err := ctx.Err(); err != nil {
		return State{}, err
	}
	return state, nil
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

func connectDatabase(
	ctx context.Context,
	rawURL string,
	timeout time.Duration,
) (*dbschema.DatabaseConnection, error) {
	if timeout <= 0 {
		return dbschema.ConnectToDatabase(ctx, rawURL)
	}
	connectCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return dbschema.ConnectToDatabase(connectCtx, rawURL)
}

// VerifyMigrationDir mirrors `atlas migrate diff` checksum handling for a
// migration directory used as a desired-state or inspection source: a missing
// atlas.sum is tolerated, an invalid one fails before replay.
func VerifyMigrationDir(dir string) error {
	return verifyMigrationFS(os.DirFS(dir))
}

// CaptureVerifiedMigrationDir returns one stable migration-directory snapshot
// after applying the same checksum policy as [VerifyMigrationDir]. Callers that
// inspect policy and then replay the directory use the returned filesystem so
// both operations see the same bytes.
func CaptureVerifiedMigrationDir(dir string) (fs.FS, error) {
	snapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	if err != nil {
		return nil, fmt.Errorf("capture migration directory: %w", err)
	}
	if err := verifyMigrationFS(snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func verifyMigrationFS(fsys fs.FS) error {
	result, err := migratesum.VerifyWithFormat(fsys, migrator.MigrationDirFormatAtlas)
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
