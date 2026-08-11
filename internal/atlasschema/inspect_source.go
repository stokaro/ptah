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
	// ValidateInspectedSchema applies after live or dev-database introspection,
	// before output or file exports.
	ValidateInspectedSchema func(*goschema.Database) error
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
}

// InspectSource classifies the --url inspection source and renders it with
// Atlas-compatible formatting. Database URLs are introspected directly. Local
// schema files and migration directories are evaluated on the --dev-url dev
// database, mirroring Atlas: the dev database is reset, the source is
// materialized on it (schema files executed, migration directories replayed),
// and the result is introspected so the output is normalized by a real
// database of the target dialect.
func InspectSource(ctx context.Context, opts InspectSourceOptions) (string, error) {
	if _, err := NormalizeInspectFormat(opts.Format); err != nil {
		return "", err
	}
	if err := atlasfilter.ValidateExcludeSelectors(opts.Exclude); err != nil {
		return "", err
	}
	// Malformed or unsupported --include selectors fail before any database is
	// contacted and before the dev database is reset.
	if err := atlasfilter.ValidateIncludeSelectors(opts.Include); err != nil {
		return "", err
	}
	set, err := atlassource.ClassifySet("--url", []string{opts.URL}, opts.ProjectEnv)
	if err != nil {
		return "", err
	}

	inspectOpts := InspectOptions{
		DevURL:                  opts.DevURL,
		Schemas:                 opts.Schemas,
		Include:                 opts.Include,
		Exclude:                 opts.Exclude,
		Format:                  opts.Format,
		Diagnostics:             opts.Diagnostics,
		OmitAtlasRefusedBlocks:  opts.OmitAtlasRefusedBlocks,
		CompatibilityHCLFraming: opts.CompatibilityHCLFraming,
		ValidateSchema:          opts.ValidateInspectedSchema,
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

// inspectOnDev evaluates a local-file or migration-directory inspection
// source on the dev database and renders the introspected result.
func inspectOnDev(
	ctx context.Context,
	set atlassource.Set,
	opts InspectSourceOptions,
	inspectOpts InspectOptions,
) (string, error) {
	devURL := strings.TrimSpace(opts.DevURL)
	if devURL == "" {
		// Atlas parity: `atlas schema inspect -u file://...` without a dev
		// database fails with exactly this message.
		return "", errors.New("--dev-url cannot be empty")
	}
	if isDockerSimulationURL(devURL) {
		return "", errors.New("docker --dev-url values are accepted by Atlas, but Ptah requires a directly connectable dev database URL for schema inspection")
	}
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
		desired, err = schemafile.LoadAll(sourceRawURLs(set), schemafile.Options{
			Dialect:               dialect,
			IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
			SchemaScope:           schemaScope,
			SchemaScopeFlag:       schemaScopeFlag,
			Vars:                  opts.Vars,
		})
		if err != nil {
			return "", err
		}
	case atlassource.KindExternalSchema:
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
				schema, err := readInspectDevSchema(ctx, replayConn, opts.Schemas)
				if err != nil {
					return err
				}
				rendered, err = renderInspectSchema(
					atlassource.WithoutRevisionTable(schema),
					replayConn.Info(),
					inspectOpts,
				)
				return err
			},
		)
		if err != nil {
			return "", fmt.Errorf("--url %q: %w", set.Sources[0].Raw, err)
		}
		return rendered, nil
	case atlassource.KindLocalFile, atlassource.KindExternalSchema:
		var rendered string
		err := withMaterializedDevSchema(
			ctx,
			devConn,
			desired,
			opts.Diagnostics,
			func(materializedConn *dbschema.DatabaseConnection) error {
				schema, err := readInspectDevSchema(ctx, materializedConn, opts.Schemas)
				if err != nil {
					return err
				}
				rendered, err = renderInspectSchema(schema, materializedConn.Info(), inspectOpts)
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
func readInspectDevSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	schemas []string,
) (*dbschematypes.DBSchema, error) {
	schema, err := readInspectSchema(ctx, devConn, schemas)
	if err != nil {
		return nil, fmt.Errorf("read dev database schema: %w", err)
	}
	return schema, nil
}

func sourceRawURLs(set atlassource.Set) []string {
	urls := make([]string, 0, len(set.Sources))
	for _, source := range set.Sources {
		urls = append(urls, source.Raw)
	}
	return urls
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
