package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"slices"
	"strings"
	"time"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlasfilter"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/devclean"
	"go.5x5.cz/ptah/internal/devdocker"
	"go.5x5.cz/ptah/internal/devlock"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/rolescope"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/migrator"
)

const inspectDevCleanupTimeout = 30 * time.Second

// InspectSourceOptions configures URL-driven Atlas schema inspection.
type InspectSourceOptions struct {
	// URL is the raw --url inspection source: a database URL, a local schema
	// file, a migration directory, or an env:// reference.
	URL string
	// DevURL is the dev database used to evaluate non-database sources. The
	// dev database is reset destructively before the source is materialized
	// on it.
	DevURL string
	// Schemas restricts inspection to the named schema scopes.
	Schemas []string
	// Include positively selects the top-level resources inspection keeps,
	// with Atlas-style selectors. Empty keeps every inspected resource.
	Include []string
	// Exclude filters inspected resources with Atlas-style selectors.
	Exclude []string
	// Format is the Atlas --format value or Go template.
	Format string
	// Diagnostics receives non-fatal rendering diagnostics.
	Diagnostics io.Writer
	// SuppressRoleCoverageNote omits the Ptah-only role coverage note while
	// preserving every other selector and safety diagnostic.
	SuppressRoleCoverageNote bool
	// ProjectEnv expands env:// references through the evaluated atlas.hcl
	// environment.
	ProjectEnv atlassource.ProjectEnv
	// ConnectTimeout bounds the initial database connection attempts. Zero
	// disables the bound.
	ConnectTimeout time.Duration
	// Vars supplies values for HCL schema-file `variable` blocks, as `--var`
	// spells them; see [go.5x5.cz/ptah/internal/schemafile.Options].
	Vars []string
	// IgnoreUnknownHCLNames is the Atlas-compatible surface's unknown-name
	// policy; see [go.5x5.cz/ptah/internal/atlassource.ResolveOptions].
	IgnoreUnknownHCLNames bool
	// ValidateDesiredSchema applies a caller-selected policy to authored local
	// schema sources before the dev database is reset. Live database and
	// migration-directory inspection are descriptions, not authored desired
	// schema documents, and do not use this hook.
	ValidateDesiredSchema func(*goschema.Database) error
	// ValidateRenderedVirtualTables applies to the SQLite virtual tables whose
	// module declaration the chosen rendering dropped.
	ValidateRenderedVirtualTables func(names []string) error
	// ValidateInspectedSchema applies after live or dev-database introspection,
	// before output or file exports.
	ValidateInspectedSchema func(*goschema.Database) error
	// PrepareInspectedSchema normalizes and validates the exact introspected
	// database snapshot that rendering and file exports consume.
	PrepareInspectedSchema func(*dbschematypes.DBSchema) (*dbschematypes.DBSchema, error)
	// ValidateLiveObject applies to catalog-only live or dev-database objects
	// before output or file exports. Nil avoids supplemental catalog reads.
	ValidateLiveObject func(LiveSchemaObject) error
	// ValidateMigrationSource applies to the stable migration-directory
	// snapshot before the dev database is connected to or reset.
	ValidateMigrationSource func(fs.FS) error
	// ValidateLocalSchemaSource applies to each local schema path before parsing
	// or dev-database work.
	ValidateLocalSchemaSource func(string) error
	// OmitAtlasRefusedBlocks is the Atlas-compatible surface's block-type
	// policy for rendered HCL; see [InspectOptions].
	OmitAtlasRefusedBlocks bool
	// CompatibilityHCLFraming is the Atlas-compatible surface's independent
	// single-document HCL framing policy; see [InspectOptions].
	CompatibilityHCLFraming bool
	// DevURLDiagnostic lets the Atlas-compatible surface answer a --dev-url in
	// this package's own words before the shared resolution reaches it, and is
	// nil on every native path.
	//
	// It exists because the two surfaces owe different sentences for the same
	// value: the community binary reports a driver problem, and native Ptah
	// names the flag and quotes what the operator actually typed. Consulted here
	// rather than at the caller because only this function knows the source
	// turned out to need a dev database at all -- a database --url never reaches
	// it, which is the scope the community binary was measured to use.
	DevURLDiagnostic func(string) error
}

// ValidateInspectOptions runs every check [InspectSource] performs before it
// looks at the source at all: the output format, then the --exclude selectors,
// then the --include ones, in that order.
//
// It is exported so a caller that must do work BEFORE handing the source over
// can run these first and keep the argument error the operator would otherwise
// have got. `ptah schema inspect` is that caller: an `oci://` --schema-file is
// pulled from a registry before the source can be classified, and without this
// an invalid --format or a missing --dev-url would be reported as a registry
// dial failure after an authenticated network round trip — or after a timeout,
// on a registry that is merely slow. Local argument errors should not depend on
// a network.
//
// The order is part of the contract, not an implementation detail: it decides
// which message an invocation wrong in two ways receives, and the compat
// surface is measured against the pinned binary on exactly that.
func ValidateInspectOptions(opts InspectSourceOptions) error {
	if _, err := NormalizeInspectFormat(opts.Format); err != nil {
		return err
	}
	if err := atlasfilter.ValidateExcludeSelectors(opts.Exclude); err != nil {
		return err
	}
	// Malformed or unsupported --include selectors fail before any database is
	// contacted and before the dev database is reset.
	return atlasfilter.ValidateIncludeSelectors(opts.Include)
}

// ValidateNonDatabaseInspectPreconditions is [ValidateInspectOptions] plus the
// dev-database requirement, for a source already known not to be a database
// URL.
//
// A non-database source is materialized on --dev-url, so an absent one is an
// argument error rather than something to discover later. Callers that must
// fetch the source before it can be classified use this to answer that error
// without fetching anything. [InspectSource] reaches the same predicate through
// its own path, so the two cannot disagree about the message.
func ValidateNonDatabaseInspectPreconditions(opts InspectSourceOptions) error {
	if err := ValidateInspectOptions(opts); err != nil {
		return err
	}
	if err := refuseInspectDevURL(opts.DevURL, opts.DevURLDiagnostic); err != nil {
		return err
	}
	return refuseInspectDevURLForm(opts.DevURL)
}

// refuseInspectDevURLForm answers every dev database URL verdict that is
// decidable from the URL text alone.
//
// It exists because stokaro/ptah#1468 turned `docker://` from a value this
// inspection refused into one it PROVISIONS, and the provisioning happens deep
// inside [inspectOnDev] -- after the source has been fetched. That moved a whole
// family of purely local argument errors back behind the registry, which is the
// defect stokaro/ptah#1496 fixed for --format, --include and --connect-timeout.
// Measured on a build of that merge: `--dev-url docker://nosuchengine/16/dev`
// with a local --schema-file answers `unsupported docker --dev-url engine
// "nosuchengine"`, and the same mistake with an oci:// --schema-file answered a
// registry dial failure instead. The rerouting-parameter refusal -- the one that
// stops a dev URL pointing the connection at a database the run then resets --
// was behind the registry too.
//
// The two checks are the two the run reaches anyway, in the order it reaches
// them: [atlassource.PinDialect] pins the dialect off --dev-url before any
// source is loaded, and [devdocker.Parse] is the first thing
// [devdocker.Provision] does. They are CALLED here rather than restated, so the
// message an operator sees is the same one either spelling of the invocation
// produces, and a verdict either function learns later is answered in front of
// the registry without this function being edited. That is the same reason
// resolveInspectLocals exists on the calling command: a hand-maintained list of
// the checks somebody remembered drifts, and has drifted, twice.
//
// It is separate from [refuseInspectDevURL] because that one owns the verdicts
// the two surfaces word differently; these are text facts both surfaces share.
//
// The value is NOT trimmed here. It is normalized once, by the command, so that
// every consumer judges the same bytes; trimming again would let the next
// caller reintroduce the disagreement that normalization removed.
func refuseInspectDevURLForm(devURL string) error {
	// No sets are passed: a caller reaching this has a source that is not a
	// database URL, so it contributes no dialect, and this computes exactly the
	// --dev-url half inspectOnDev computes from the same value.
	if _, _, err := atlassource.PinDialect(devURL); err != nil {
		return err
	}
	if !devdocker.IsURL(devURL) {
		return nil
	}
	_, err := devdocker.Parse(devURL)
	return err
}

// InspectSource classifies the --url inspection source and renders it with
// Atlas-compatible formatting. Database URLs are introspected directly. Local
// schema files and migration directories are evaluated on the --dev-url dev
// database, mirroring Atlas: the dev database is reset, the source is
// materialized on it (schema files executed, migration directories replayed),
// and the result is introspected so the output is normalized by a real
// database of the target dialect.
func InspectSource(ctx context.Context, opts InspectSourceOptions) (string, error) {
	if err := ValidateInspectOptions(opts); err != nil {
		return "", err
	}
	set, err := atlassource.ClassifySet("--url", []string{opts.URL}, opts.ProjectEnv)
	if err != nil {
		return "", err
	}

	inspectOpts := InspectOptions{
		DevURL:                   opts.DevURL,
		Schemas:                  opts.Schemas,
		Include:                  opts.Include,
		Exclude:                  opts.Exclude,
		Format:                   opts.Format,
		Diagnostics:              opts.Diagnostics,
		SuppressRoleCoverageNote: opts.SuppressRoleCoverageNote,
		OmitAtlasRefusedBlocks:   opts.OmitAtlasRefusedBlocks,
		CompatibilityHCLFraming:  opts.CompatibilityHCLFraming,
		PrepareSchema:            opts.PrepareInspectedSchema,
		ValidateSchema:           opts.ValidateInspectedSchema,
		ValidateLiveObject:       opts.ValidateLiveObject,

		ValidateRenderedVirtualTables: opts.ValidateRenderedVirtualTables,
	}
	if set.Kind == atlassource.KindDatabase {
		conn, err := connectInspectSource(ctx, set.Sources[0].Raw, opts.ConnectTimeout)
		if err != nil {
			return "", fmt.Errorf("connect to --url: %w", err)
		}
		defer dbschema.CloseAndWarn(conn)
		return Inspect(ctx, conn, inspectOpts)
	}
	return inspectOnDev(ctx, set, opts, inspectOpts)
}

// refuseInspectDevURL answers a dev database URL this inspection cannot use.
//
// The two verdicts are ordered as measured: an absent value first, then
// whatever the calling surface wants to say about the remainder. Only the
// caller can supply that last one, because the two surfaces owe different
// sentences for the same value; see [InspectSourceOptions.DevURLDiagnostic].
//
// A `docker://` value used to be refused here outright. It is now provisioned
// instead, by [devdocker.Resolve] further down, and the verdicts that remain
// for one are read from the URL text by [refuseInspectDevURLForm] and by
// [devdocker.Parse] itself, in the pinned binary's own words.
//
// The value arrives as the operator wrote it and is normalized here for these
// two verdicts only. What must NOT be normalized is the value handed to the
// provisioner: see [devdocker.Parse] for the leading space that is not
// whitespace around a docker URL but a value with no scheme at all.
func refuseInspectDevURL(devURL string, surfaceDiagnostic func(string) error) error {
	trimmed := strings.TrimSpace(devURL)
	if trimmed == "" {
		// Atlas parity: `atlas schema inspect -u file://...` without a dev
		// database fails with exactly this message.
		return errors.New("--dev-url cannot be empty")
	}
	if surfaceDiagnostic == nil {
		return nil
	}
	return surfaceDiagnostic(trimmed)
}

// inspectOnDev evaluates a local-file or migration-directory inspection
// source on the dev database and renders the introspected result.
func inspectOnDev(
	ctx context.Context,
	set atlassource.Set,
	opts InspectSourceOptions,
	inspectOpts InspectOptions,
) (string, error) {
	if err := refuseInspectDevURL(opts.DevURL, opts.DevURLDiagnostic); err != nil {
		return "", err
	}
	devURL := strings.TrimSpace(opts.DevURL)
	dialect, _, err := atlassource.PinDialect(devURL, set)
	if err != nil {
		return "", err
	}

	// Load and verify the source before the dev database is touched, so bad
	// sources fail without a destructive reset.
	var desired *goschema.Database
	var migrationSnapshot fs.FS
	switch set.Kind {
	case atlassource.KindLocalFile:
		if err := validateInspectLocalSources(set, opts.ValidateLocalSchemaSource); err != nil {
			return "", err
		}
		// The source URL is the file itself, so --dev-url is the only URL that
		// can limit this run to a schema.
		schemaScope, schemaScopeFlag := schemafile.ScopeFromURLs(devURL, "", "")
		// LoadSources rather than the URL list: a source that came from an
		// atlas.hcl `data "hcl_schema"` block carries that block's `vars`, and
		// reading the raw URLs back out of the set would drop them. Measured on
		// the pinned Atlas community binary v1.3.0, `schema inspect --env local
		// --url env://src` against a file with a required variable the block
		// supplies is exit 0 there and was exit 1 here, `missing value for
		// required variable "tenant"`.
		desired, err = schemafile.LoadSources(set.SchemaFileSources(), schemafile.Options{
			Dialect:               dialect,
			IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
			ReportIgnored:         opts.Diagnostics,
			SchemaScope:           schemaScope,
			SchemaScopeFlag:       schemaScopeFlag,
			Vars:                  opts.Vars,
		})
		if err != nil {
			return "", err
		}
	case atlassource.KindExternalSchema, atlassource.KindRemoteSchema:
		// Both resolve to a schema IR rather than to files: an external program
		// prints one, and a registry artifact records one. Neither has a local
		// path for the file loader above to read.
		state, err := set.Resolve(ctx, atlassource.ResolveOptions{
			Dialect:                   dialect,
			DialectFlag:               "--dev-url",
			ValidateLocalSchemaSource: opts.ValidateLocalSchemaSource,
		})
		if err != nil {
			return "", err
		}
		desired = state.Schema
	case atlassource.KindMigrationDir:
		migrationSnapshot, err = prepareInspectMigrationSnapshot(
			set.Sources[0].Path,
			opts.ValidateMigrationSource,
		)
		if err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("--url: unresolved %s inspection source", set.Kind)
	}
	if err := validateInspectDesiredSchema(desired, opts.ValidateDesiredSchema); err != nil {
		return "", err
	}

	// The container is started here and not at the top of the function: every
	// refusal above is answerable from the URL text and the sources alone, and
	// paying two seconds of container start for a run that fails on a bad
	// schema file would be a worse answer to the same question.
	//
	// The value handed over is the operator's, not the trimmed copy above. A
	// leading space is not whitespace around a docker URL, it is a value with no
	// scheme at all, and the pinned binary answers it `parse open url: first
	// path segment in URL cannot contain colon` at exit 1. Normalizing first
	// promoted exactly that into a started container and an exit 0 -- the one
	// direction compatibility policy (a) forbids. The normalization this path
	// wants still happens; it happens to the answer.
	resolved, releaseDev, err := devdocker.Resolve(ctx, opts.DevURL, devdocker.Options{})
	if err != nil {
		return "", err
	}
	defer releaseDev()
	devURL = strings.TrimSpace(resolved)

	devConn, err := connectInspectSource(ctx, devURL, opts.ConnectTimeout)
	if err != nil {
		return "", fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(devConn)
	devConn.SchemaWriter().SetDryRun(false)

	switch set.Kind {
	case atlassource.KindMigrationDir:
		var rendered string
		err := migrationreplay.WithReplayedSnapshot(
			ctx,
			devConn,
			migrationSnapshot,
			migrator.MigrationDirFormatAtlas,
			func(replayConn *dbschema.DatabaseConnection) error {
				schema, validatedOpts, err := readValidatedInspectDevSchema(ctx, replayConn, inspectDevReadOptions{
					inspect:         inspectOpts,
					withoutRevision: true,
				})
				if err != nil {
					return err
				}
				rendered, err = renderInspectSchema(
					schema,
					replayConn.Info(),
					validatedOpts,
				)
				return err
			},
		)
		if err != nil {
			return "", fmt.Errorf("--url %q: %w", set.Sources[0].Raw, err)
		}
		return rendered, nil
	case atlassource.KindLocalFile, atlassource.KindExternalSchema, atlassource.KindRemoteSchema:
		var rendered string
		err := withMaterializedDevSchema(
			ctx,
			devConn,
			desired,
			opts.Diagnostics,
			func(materializedConn *dbschema.DatabaseConnection) error {
				schema, validatedOpts, err := readValidatedInspectDevSchema(ctx, materializedConn, inspectDevReadOptions{
					inspect: inspectOpts,
				})
				if err != nil {
					return err
				}
				rendered, err = renderInspectSchema(schema, materializedConn.Info(), validatedOpts)
				return err
			},
		)
		if err != nil {
			return "", err
		}
		return rendered, nil
	}
	return "", fmt.Errorf("--url: unresolved %s inspection source", set.Kind)
}

func validateInspectLocalSources(set atlassource.Set, validate func(string) error) error {
	if validate == nil {
		return nil
	}
	for _, source := range set.Sources {
		if err := validate(source.Path); err != nil {
			return err
		}
	}
	return nil
}

func validateInspectDesiredSchema(desired *goschema.Database, validate func(*goschema.Database) error) error {
	if desired == nil || validate == nil {
		return nil
	}
	return validate(desired)
}

func prepareInspectMigrationSnapshot(
	dir string,
	validate func(fs.FS) error,
) (fs.FS, error) {
	snapshot, err := atlassource.CaptureVerifiedMigrationDir(dir)
	if err != nil {
		return nil, err
	}
	if validate != nil {
		if err := validate(snapshot); err != nil {
			return nil, err
		}
	}
	return snapshot, nil
}

func withMaterializedDevSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	desired *goschema.Database,
	diag io.Writer,
	consume func(*dbschema.DatabaseConnection) error,
) (resultErr error) {
	lock, err := devlock.Acquire(ctx, devConn, 0)
	if err != nil {
		return fmt.Errorf("acquire schema inspection dev database lock: %w", err)
	}
	defer func() {
		if releaseErr := lock.Release(); releaseErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf("release schema inspection dev database lock: %w", releaseErr),
			)
		}
	}()

	return devConn.WithSession(ctx, func(materializedConn *dbschema.DatabaseConnection) (resultErr error) {
		defer func() {
			cleanupCtx, cancel := context.WithTimeout(
				context.WithoutCancel(ctx),
				inspectDevCleanupTimeout,
			)
			defer cancel()
			if cleanupErr := devclean.DatabaseRealm(cleanupCtx, materializedConn); cleanupErr != nil {
				resultErr = errors.Join(
					resultErr,
					fmt.Errorf("clean dev database after schema inspection: %w", cleanupErr),
				)
			}
		}()
		if err := devclean.DatabaseRealm(ctx, materializedConn); err != nil {
			return fmt.Errorf("reset dev database: %w", err)
		}
		if err := materializeOnDev(ctx, materializedConn, desired, diag); err != nil {
			return err
		}
		return consume(materializedConn)
	})
}

// materializeOnDev executes the desired schema's ordered CREATE statements on
// an already-reset dev database, minus the roles that database's server
// already has.
func materializeOnDev(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	desired *goschema.Database,
	diag io.Writer,
) error {
	info := devConn.Info()
	desired, err := devMaterializableSchema(devConn, desired, diag)
	if err != nil {
		return err
	}
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(desired, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("render schema DDL for dev database: %w", err)
	}
	if err := executeApplyStatements(ctx, devConn.Writer(), statements); err != nil {
		return fmt.Errorf("materialize schema on dev database: %w", err)
	}
	return nil
}

// devMaterializableSchema returns the desired state with the roles the dev
// database's server already has taken out of it, and reports which ones on
// diag.
//
// The reset that precedes materialization empties the dev DATABASE, and a role
// is not in it -- roles are server-scoped, so a role any database on that
// server ever created is still there afterwards and `CREATE ROLE` for it fails
// at SQLSTATE 42710. That is what made Ptah's own inspect output unreplayable
// against a clean sibling database in the same cluster (stokaro/ptah#1267);
// see [rolescope.RolesToCreateOnDev] for the measurement and for why the role
// is skipped rather than refused or altered.
//
// The extra read is why this is guarded on the desired state declaring a role
// at all: a document with no role block is the common case, and it pays
// nothing. The read is the dev database's default scope rather than the
// caller's --schema selection on purpose. Only the ROLE lists are consulted,
// and their union is the set of roles the server has whatever schemas are in
// scope: what the selection moves is which of them are described, not which of
// them exist. See [dbschematypes.DBSchema.RolesOutOfScope].
//
// The desired state is copied rather than edited. Which roles one dev database
// happened to have is a fact about that database, not about the document, and
// the value belongs to a caller that may still use it -- retry against another
// dev database, or report what the source declared.
func devMaterializableSchema(
	devConn *dbschema.DatabaseConnection,
	desired *goschema.Database,
	diag io.Writer,
) (*goschema.Database, error) {
	if desired == nil || len(desired.Roles) == 0 {
		return desired, nil
	}
	devSchema, err := dbschema.ReadSchemaWithSchemas(devConn, nil)
	if err != nil {
		return nil, fmt.Errorf("read dev database roles: %w", err)
	}
	create, alreadyOnServer := rolescope.RolesToCreateOnDev(
		desired.Roles,
		slices.Concat(devSchema.Roles, devSchema.RolesOutOfScope),
	)
	if len(alreadyOnServer) == 0 {
		return desired, nil
	}
	rolescope.ReportNotCreatedOnDev(diag, alreadyOnServer)
	materializable := *desired
	materializable.Roles = create
	return &materializable, nil
}

// readInspectDevSchema reads back a source that was materialized on the dev
// database. It resolves scope exactly the way a database source does, so
// `schema inspect -u file://…` describes the same set of schemas the same URL
// would describe as a target.
type inspectDevReadOptions struct {
	inspect         InspectOptions
	withoutRevision bool
}

func readValidatedInspectDevSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	opts inspectDevReadOptions,
) (*dbschematypes.DBSchema, InspectOptions, error) {
	schema, names, err := readInspectSchemaWithNames(ctx, devConn, opts.inspect.Schemas)
	if err != nil {
		return nil, InspectOptions{}, fmt.Errorf("read dev database schema: %w", err)
	}
	if opts.withoutRevision {
		schema = atlassource.WithoutRevisionTable(schema)
	}
	schema, err = prepareInspectSchema(schema, opts.inspect.PrepareSchema)
	if err != nil {
		return nil, InspectOptions{}, err
	}
	if err := validateInspectSchema(schema, opts.inspect.ValidateSchema); err != nil {
		return nil, InspectOptions{}, err
	}
	if err := ValidateLiveObjects(devConn, names, opts.inspect.ValidateLiveObject); err != nil {
		return nil, InspectOptions{}, err
	}
	validatedOpts := opts.inspect
	validatedOpts.PrepareSchema = nil
	validatedOpts.ValidateSchema = nil
	return schema, validatedOpts, nil
}

// connectInspectSource opens one source connection, bounding only the initial
// connection attempt by timeout: [dbschema.ConnectToDatabase] uses its
// context for the verification ping and metadata queries, not for the
// returned connection's lifetime.
func connectInspectSource(
	ctx context.Context,
	url string,
	timeout time.Duration,
) (*dbschema.DatabaseConnection, error) {
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return dbschema.ConnectToDatabase(ctx, url)
}
