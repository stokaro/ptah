package atlassource

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"strings"
	"time"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemasource"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasregistry"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/migratesum"
	"ptah.run/internal/migrationreplay"
	"ptah.run/internal/migrationsnapshot"
	"ptah.run/internal/ociartifact"
	"ptah.run/internal/schemaartifact"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/schemascope"
	"ptah.run/internal/schemaselection"
	"ptah.run/migration/migrationfile"
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
	// DialectFromServer says Dialect was read from a connected server rather
	// than from a URL scheme. A database source is then held to it once the
	// source is connected, because only then are two servers being compared:
	// a MySQL-family scheme cannot tell MySQL from MariaDB, and a server can.
	// With a Dialect read from a scheme, the source is compared by its scheme
	// alone.
	DialectFromServer bool
	// DevURL is the dev database URL used to replay migration-directory
	// sources.
	DevURL string
	// DevServerDisposable is the operator's declaration that the server
	// DevURL names is the run's own, as
	// [ptah.run/internal/devdocker.DisposableServerDeclared] resolved it. A
	// migration-directory replay then runs the statements whose effect reaches
	// past the dev database.
	DevServerDisposable bool
	// SchemaScope and SchemaScopeFlag limit an HCL desired state to one schema;
	// see [ptah.run/internal/schemafile.Options]. They are passed in
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
	// worth naming. See [ptah.run/internal/schemafile.Options].
	IgnoreUnknownHCLNames bool
	// ReportIgnored receives a warning line per name dropped under
	// IgnoreUnknownHCLNames. See
	// [ptah.run/internal/schemafile.Options.ReportIgnored]: the tolerance
	// and the reporting travel together, because matching a documented
	// tolerance in silence is the part stokaro/ptah#1709 named.
	ReportIgnored io.Writer
	// Vars supplies values for the `variable` blocks of an HCL schema file, as
	// `--var` spells them. See [ptah.run/internal/schemafile.Options].
	Vars []string
	// ValidateSchema applies a caller-selected policy after any source kind is
	// fully materialized and before the resolved state is returned. Nil accepts
	// every schema. The callback is interface-neutral: compatibility adapters
	// select policy without making this shared resolver depend on a CLI layer.
	ValidateSchema func(*schemamodel.Database) error
	// ValidateInspectedSchema replaces ValidateSchema for database-backed and
	// replayed migration-directory states. It lets an adapter distinguish
	// authored desired content from objects supplied by the target server.
	ValidateInspectedSchema func(*schemamodel.Database) error
	// ValidateInspectedDatabase applies a caller-selected policy while a live
	// database source or replayed migration-directory dev database is still
	// open. The schema list is the exact scope introspected into the returned
	// state. Nil performs no supplemental catalog work.
	ValidateInspectedDatabase func(*dbschema.DatabaseConnection, []string) error
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
	Schema *schemamodel.Database
	// DB is the introspected database state backing Schema for database and
	// migration-directory sources; nil for local schema files.
	DB *catalog.Database
	// DefaultSchema is the schema that owns unqualified objects for database
	// and migration-directory sources ("public" for PostgreSQL, the database
	// name for MySQL-family targets, "main" for SQLite); empty for local
	// schema files. Schema-scope filtering uses it to resolve unqualified
	// object names.
	DefaultSchema string
	// RealmScoped reports that a database source described the whole realm
	// rather than the one schema its URL named. It travels with DefaultSchema
	// because the two answer one question between them: which schema owns an
	// unqualified object, and whether an exclude pattern's leading segment is
	// that schema or an object in it (stokaro/ptah#1703). False for a local
	// schema file, which names no URL to be scoped by.
	RealmScoped bool
}

// Resolve materializes the set's desired state. Local schema files load
// exactly as before this resolver existed; database URLs are introspected
// live; migration directories are replayed on the dev database and the result
// is introspected; external schema programs run without a shell and their
// standard output is parsed as the desired schema.
func (s Set) Resolve(ctx context.Context, opts ResolveOptions) (State, error) {
	var resolved State
	err := s.ResolveHolding(ctx, opts, func(state State, _ *dbschema.DatabaseConnection) error {
		resolved = state
		return nil
	})
	if err != nil {
		return State{}, err
	}
	return resolved, nil
}

// HoldFunc receives a resolved state together with the connection that
// produced it, while that connection is still open.
//
// The connection is the source database's own for [KindDatabase], and the
// session the migration directory was replayed on for [KindMigrationDir],
// with the replayed schema still in place. Every other kind has no server
// behind it, and the connection is nil. It must not escape the call: it is
// closed, and a replayed dev database is cleaned, when the call returns.
type HoldFunc func(state State, conn *dbschema.DatabaseConnection) error

// ResolveHolding resolves the set as [Set.Resolve] does, validation
// included, and hands the state to hold before the connection that produced
// it is closed.
//
// It exists for a comparison that has to ask the server that stored a state
// how that server spells what the other side declares. After [Set.Resolve]
// returns, the database it read is out of reach, and a replayed directory's
// schema has been dropped from the dev database.
//
// An error hold returns is returned as it is, without the resolution context
// a resolution error carries; a resolution error is returned before hold runs.
func (s Set) ResolveHolding(ctx context.Context, opts ResolveOptions, hold HoldFunc) error {
	if err := s.ValidateLocalSchemaSources(opts.ValidateLocalSchemaSource); err != nil {
		return err
	}
	var holdErr error
	err := s.resolve(ctx, opts, func(state State, conn *dbschema.DatabaseConnection) error {
		validateSchema := opts.ValidateSchema
		if state.DB != nil && opts.ValidateInspectedSchema != nil {
			validateSchema = opts.ValidateInspectedSchema
		}
		if validateSchema != nil {
			if err := validateSchema(state.Schema); err != nil {
				return err
			}
		}
		// Kept apart from the resolution error: a replay wraps whatever its
		// callback returns with the source's name, and the caller's own
		// failure is not a failure to read the source.
		holdErr = hold(state, conn)
		return nil
	})
	if err != nil {
		return err
	}
	return holdErr
}

func (s Set) resolve(ctx context.Context, opts ResolveOptions, finish HoldFunc) error {
	switch s.Kind {
	case KindLocalFile:
		schema, err := schemafile.LoadSources(s.SchemaFileSources(), schemafile.Options{
			Dialect:               opts.Dialect,
			IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
			ReportIgnored:         opts.ReportIgnored,
			SchemaScope:           opts.SchemaScope,
			SchemaScopeFlag:       opts.SchemaScopeFlag,
			Vars:                  opts.Vars,
		})
		if err != nil {
			return err
		}
		return finish(State{Kind: s.Kind, Schema: schema}, nil)
	case KindDatabase:
		return s.resolveDatabase(ctx, opts, finish)
	case KindMigrationDir:
		return s.resolveMigrationDir(ctx, opts, finish)
	case KindExternalSchema:
		state, err := s.resolveExternalSchema(ctx, opts)
		if err != nil {
			return err
		}
		return finish(state, nil)
	case KindRemoteSchema:
		state, err := s.resolveRemoteSchema(ctx)
		if err != nil {
			return err
		}
		return finish(state, nil)
	default:
		return fmt.Errorf("%s: unresolved %s desired-state source", s.Flag, s.Kind)
	}
}

// resolveRemoteSchema pulls the schema artifact a `data "remote_schema"` block
// names and returns the schema it carries.
//
// The artifact records the schema IR itself, so there is nothing to
// materialize: no temporary file, no re-parse, and no dialect guess. Ptah
// pushes these with `schema push`, so this is the read half of a capability the
// repository already had -- distributing a desired state through an ordinary
// registry, with tags, digests and ordinary registry auth, and with no hosted
// service in the path (stokaro/ptah#1210).
func (s Set) resolveRemoteSchema(ctx context.Context) (State, error) {
	reference := s.Sources[0].OCIReference
	plainHTTP, err := atlasregistry.PlainHTTP.Resolve()
	if err != nil {
		return State{}, fmt.Errorf("%s %q: reading the registry transport setting: %w",
			s.Flag, reference, err)
	}
	client, err := ociartifact.NewClient(ociartifact.ClientOptions{PlainHTTP: plainHTTP})
	if err != nil {
		return State{}, fmt.Errorf("%s %q: opening the registry client: %w", s.Flag, reference, err)
	}
	// Pull validates the artifact type, the format annotation and the single
	// expected file, and parses the schema, so it returns either a schema or an
	// error.
	artifact, err := schemaartifact.Pull(ctx, client, reference)
	if err != nil {
		return State{}, fmt.Errorf("%s %q: %w", s.Flag, reference, err)
	}
	return State{Kind: s.Kind, Schema: artifact.Database}, nil
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

// SchemaFileSources carries each classified local-file source into the loader
// together with its own variable scope, so a file selected by an atlas.hcl
// `data "hcl_schema"` block sees that block's `vars` and nothing else.
//
// Exported because [Set.Resolve] is not the only loader of a classified set:
// `schema inspect` materializes its sources on the dev database itself, and
// reading the URLs out of the set would drop the scope on the way.
func (s Set) SchemaFileSources() []schemafile.Source {
	sources := make([]schemafile.Source, 0, len(s.Sources))
	for _, source := range s.Sources {
		sources = append(sources, schemafile.Source{
			URL:        source.Raw,
			VarValues:  source.VarValues,
			VarsScoped: source.VarsScoped,
		})
	}
	return sources
}

func (s Set) resolveDatabase(ctx context.Context, opts ResolveOptions, finish HoldFunc) error {
	if err := s.ensureDialect(opts); err != nil {
		return err
	}
	conn, err := connectDatabase(ctx, s.Sources[0].Raw, opts.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("connect to %s database: %w", s.Flag, err)
	}
	defer dbschema.CloseAndWarn(conn)
	if err := s.ensureServerDialect(conn, opts); err != nil {
		return err
	}

	// Which schemas this read covers is [schemascope.ReadNames]'s decision, not
	// this function's. Deriving it here is what made a database URL describe the
	// connected schema alone while `schema inspect` described the whole realm,
	// and the comparator -- which cannot tell a schema nobody read from a schema
	// nobody wants -- planned `DROP TABLE "extra"."b" CASCADE` off the
	// difference (stokaro/ptah#1276).
	names, err := schemascope.ReadNames(ctx, conn.Info(), opts.Schemas, conn)
	if err != nil {
		return fmt.Errorf("read %s database schema: %w", s.Flag, err)
	}
	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, names)
	if err != nil {
		return fmt.Errorf("read %s database schema: %w", s.Flag, err)
	}
	if opts.ValidateInspectedDatabase != nil {
		if err := opts.ValidateInspectedDatabase(conn, names); err != nil {
			return err
		}
	}
	return finish(State{
		Kind:          s.Kind,
		Schema:        dbschematogo.ConvertDBSchemaToGoSchema(schema, conn.Info().Dialect),
		DB:            schema,
		DefaultSchema: conn.Info().Schema,
		RealmScoped:   schemaselection.Realm(conn.Info().Dialect, conn.Info().URL, conn.Info().Schema),
	}, conn)
}

// ensureDialect rejects database sources whose URL scheme cannot name a
// server of the pinned dialect, before any connection is opened. The
// comparison is atlasurl.SchemeDialectMatches's: a MySQL-family scheme does
// not say whether the server is MySQL or MariaDB.
func (s Set) ensureDialect(opts ResolveOptions) error {
	implied := s.ImpliedDialect()
	pinned := platform.NormalizeDialect(opts.Dialect)
	if pinned == "" {
		pinned = opts.Dialect
	}
	if implied == "" || pinned == "" || atlasurl.SchemeDialectMatches(implied, pinned) {
		return nil
	}
	return fmt.Errorf("%s database dialect %q does not match %s dialect %q", s.Flag, implied, opts.DialectFlag, pinned)
}

// ensureServerDialect holds a connected database source to a dialect read from
// a server, which is when two servers are compared. The pinned community
// binary v1.3.0 refuses a MySQL dev database for a MariaDB `--to` database and
// the reverse on `migrate diff`, whatever the spelling: measured, its plan for
// the pair changes the schema's collation, which a plan scoped to one schema
// may not do. Accepting the pair because both schemes are in one family would
// be looser than it (stokaro/ptah#3756).
func (s Set) ensureServerDialect(conn *dbschema.DatabaseConnection, opts ResolveOptions) error {
	if !opts.DialectFromServer {
		return nil
	}
	server := platform.NormalizeDialect(conn.Info().Dialect)
	pinned := platform.NormalizeDialect(opts.Dialect)
	if server == "" || pinned == "" || server == pinned {
		return nil
	}
	return fmt.Errorf("%s database dialect %q does not match %s dialect %q", s.Flag, server, opts.DialectFlag, pinned)
}

func (s Set) resolveMigrationDir(ctx context.Context, opts ResolveOptions, finish HoldFunc) error {
	source := s.Sources[0]
	devURL := strings.TrimSpace(opts.DevURL)
	if err := s.EnsureDevDatabase(devURL); err != nil {
		return err
	}
	// The dialect check runs on the URL AS WRITTEN, before any container is
	// started: `docker://postgres/16/dev` names its dialect in the text, so a
	// mismatch with the pinned dialect is answerable without provisioning, and
	// answering it first keeps a refused run from paying for a container it
	// would immediately throw away.
	if err := s.ensureDevDialect(devURL, opts); err != nil {
		return err
	}
	// The operator's spelling decides whether this is a docker URL at all; the
	// normalization above is this path's, and it applies to the answer. See
	// [devdocker.Parse]: a leading space is not a docker URL with whitespace on
	// it, it is a value the pinned binary cannot parse.
	resolved, releaseDev, err := devdocker.Resolve(ctx, opts.DevURL, devdocker.Options{
		DeclaredDisposable: opts.DevServerDisposable,
	})
	if err != nil {
		return err
	}
	defer releaseDev()
	devURL = strings.TrimSpace(resolved)
	snapshot := s.migrationSnapshot
	if snapshot == nil {
		snapshot, err = s.captureMigrationSource()
		if err != nil {
			return err
		}
		if opts.ValidateMigrationSource != nil {
			if err := opts.ValidateMigrationSource(snapshot); err != nil {
				return err
			}
		}
	}

	conn, err := connectDatabase(ctx, devURL, opts.ConnectTimeout)
	if err != nil {
		return fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	var finishErr error
	replay := migrationreplay.WithReplayedSnapshot
	if opts.DevLockHeld {
		replay = migrationreplay.WithReplayedSnapshotLocked
	}
	if err := replay(
		ctx,
		conn,
		snapshot,
		migrationfile.DirFormatAtlas,
		func(replayConn *dbschema.DatabaseConnection) error {
			state, err := s.DevState(ctx, replayConn, opts)
			if err != nil {
				return err
			}
			// Finished here rather than after the replay returns, because the
			// replay's cleanup drops the schema this session holds, and a
			// caller holding the state may need to ask the server about it.
			finishErr = finish(state, replayConn)
			return nil
		},
	); err != nil {
		return errors.Join(finishErr, fmt.Errorf("%s %q: %w", s.Flag, source.Raw, err))
	}
	if finishErr != nil {
		return finishErr
	}
	return ctx.Err()
}

// DevState reads what a dev database session holds after the set's source
// was replayed or materialized on it, as the state of that source.
//
// The read covers the schemas [schemascope.ReadNames] selects rather than the
// dev connection's own schema alone: a directory or a schema file that creates
// a second schema describes it, and a read scoped to the connection's schema
// would report it as having created nothing there (stokaro/ptah#1276). The
// revision table is left out, and opts.ValidateInspectedDatabase runs on the
// session. opts.ValidateInspectedSchema does not; the caller runs it.
func (s Set) DevState(ctx context.Context, conn *dbschema.DatabaseConnection, opts ResolveOptions) (State, error) {
	names, err := schemascope.ReadNames(ctx, conn.Info(), opts.Schemas, conn)
	if err != nil {
		return State{}, fmt.Errorf("read dev database schema: %w", err)
	}
	schema, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, names)
	if err != nil {
		return State{}, fmt.Errorf("read dev database schema: %w", err)
	}
	schema = WithoutRevisionTable(schema)
	if opts.ValidateInspectedDatabase != nil {
		if err := opts.ValidateInspectedDatabase(conn, names); err != nil {
			return State{}, err
		}
	}
	return State{
		Kind:          s.Kind,
		Schema:        dbschematogo.ConvertDBSchemaToGoSchema(schema, conn.Info().Dialect),
		DB:            schema,
		DefaultSchema: conn.Info().Schema,
		RealmScoped:   schemaselection.Realm(conn.Info().Dialect, conn.Info().URL, conn.Info().Schema),
	}, nil
}

func (s Set) ensureDevDialect(devURL string, opts ResolveOptions) error {
	devDialect, err := atlasurl.DialectFromURL(devURL)
	if err != nil {
		return err
	}
	pinned := platform.NormalizeDialect(opts.Dialect)
	if devDialect == "" || pinned == "" || atlasurl.SchemeDialectMatches(devDialect, pinned) {
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

// CaptureVerifiedMigrationDir returns one stable migration-directory snapshot
// after applying the checksum policy `atlas migrate diff` applies: a missing
// atlas.sum is tolerated, an invalid one fails before replay. Callers that
// inspect policy and then replay the directory use the returned filesystem so
// both operations see the same bytes.
func CaptureVerifiedMigrationDir(dir string) (fs.FS, error) {
	return CaptureVerifiedMigrationFS(os.DirFS(dir))
}

// CaptureVerifiedMigrationFS is [CaptureVerifiedMigrationDir] for a directory
// that has no local path.
//
// A `migration.dir` naming a registry reference is one: the project loader
// registers a lazily-pulled filesystem, and the fetch happens on the first read
// this capture performs (stokaro/ptah#1215).
func CaptureVerifiedMigrationFS(fsys fs.FS) (fs.FS, error) {
	snapshot, err := migrationsnapshot.CaptureStable(fsys)
	if err != nil {
		return nil, fmt.Errorf("capture migration directory: %w", err)
	}
	if err := verifyMigrationFS(snapshot); err != nil {
		return nil, err
	}
	return snapshot, nil
}

func verifyMigrationFS(fsys fs.FS) error {
	result, err := migratesum.VerifyWithFormat(fsys, migrationfile.DirFormatAtlas)
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
func WithoutRevisionTable(schema *catalog.Database) *catalog.Database {
	if schema == nil {
		return &catalog.Database{}
	}
	out := *schema
	out.Tables = filterByTable(out.Tables, func(table catalog.Table) bool {
		return !strings.EqualFold(table.Name, revisionTableName)
	})
	out.Indexes = filterByTable(out.Indexes, func(index catalog.Index) bool {
		return !strings.EqualFold(index.TableName, revisionTableName)
	})
	out.Constraints = filterByTable(out.Constraints, func(constraint catalog.Constraint) bool {
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
