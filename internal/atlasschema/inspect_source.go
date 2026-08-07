package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"io"
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
	// OmitAtlasRefusedBlocks is the Atlas-compatible surface's block-type
	// policy for rendered HCL; see [InspectOptions].
	OmitAtlasRefusedBlocks bool
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
		DevURL:                 opts.DevURL,
		Schemas:                opts.Schemas,
		Include:                opts.Include,
		Exclude:                opts.Exclude,
		Format:                 opts.Format,
		Diagnostics:            opts.Diagnostics,
		OmitAtlasRefusedBlocks: opts.OmitAtlasRefusedBlocks,
	}
	if set.Kind == atlassource.KindDatabase {
		conn, err := connectInspectSource(ctx, set.Sources[0].Raw, opts.ConnectTimeout)
		if err != nil {
			return "", fmt.Errorf("connect to --url: %w", err)
		}
		defer dbschema.CloseAndWarn(conn)
		return Inspect(conn, inspectOpts)
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
	switch set.Kind {
	case atlassource.KindLocalFile:
		desired, err = schemafile.LoadAll(sourceRawURLs(set), schemafile.Options{
			Dialect:               dialect,
			IgnoreUnknownHCLNames: opts.IgnoreUnknownHCLNames,
			Vars:                  opts.Vars,
		})
		if err != nil {
			return "", err
		}
	case atlassource.KindExternalSchema:
		state, err := set.Resolve(ctx, atlassource.ResolveOptions{
			Dialect:     dialect,
			DialectFlag: "--dev-url",
		})
		if err != nil {
			return "", err
		}
		desired = state.Schema
	case atlassource.KindMigrationDir:
		if err := atlassource.VerifyMigrationDir(set.Sources[0].Path); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("--url: unresolved %s inspection source", set.Kind)
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
		err := migrationreplay.WithReplayedDirectory(
			ctx,
			devConn,
			set.Sources[0].Path,
			migrator.MigrationDirFormatAtlas,
			func(replayConn *dbschema.DatabaseConnection) error {
				schema, err := readInspectDevSchema(replayConn, opts.Schemas)
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
			func(materializedConn *dbschema.DatabaseConnection) error {
				schema, err := readInspectDevSchema(materializedConn, opts.Schemas)
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

func withMaterializedDevSchema(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	desired *goschema.Database,
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
		if err := materializeOnDev(ctx, materializedConn, desired); err != nil {
			return err
		}
		return consume(materializedConn)
	})
}

// materializeOnDev executes the desired schema's ordered CREATE statements on
// an already-reset dev database.
func materializeOnDev(
	ctx context.Context,
	devConn *dbschema.DatabaseConnection,
	desired *goschema.Database,
) error {
	info := devConn.Info()
	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(desired, info.Dialect, info.Capabilities)
	if err != nil {
		return fmt.Errorf("render schema DDL for dev database: %w", err)
	}
	if err := executeApplyStatements(ctx, devConn.Writer(), statements); err != nil {
		return fmt.Errorf("materialize schema on dev database: %w", err)
	}
	return nil
}

func readInspectDevSchema(
	devConn *dbschema.DatabaseConnection,
	schemas []string,
) (*dbschematypes.DBSchema, error) {
	schema, err := dbschema.ReadSchemaWithSchemas(devConn, SplitSchemaNames(schemas))
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
