package generator

import (
	"context"
	"fmt"
	"maps"
	"strings"
	"time"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/convert/dbschematogo"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// BaselineShadowVerifyOptions configures shadow verification before metadata
// baselining.
type BaselineShadowVerifyOptions struct {
	ShadowDatabaseURL string
	TargetConn        *dbschema.DatabaseConnection
	MigrationsDir     string
	Version           int64
	Dialect           string
	Capabilities      capability.Capabilities
	CompareOptions    *config.CompareOptions
	Schemas           []string
	ProviderOptions   []migrator.FSProviderOption
	ConnectTimeout    time.Duration
}

// VerifyBaselineShadow replays migrations up to Version on the shadow database
// and compares the resulting schema with the target database.
func VerifyBaselineShadow(ctx context.Context, opts BaselineShadowVerifyOptions) error {
	if opts.TargetConn == nil {
		return fmt.Errorf("baseline shadow check failed: target database connection is required")
	}
	connectCtx, cancelConnect := baselineShadowConnectContext(ctx, opts.ConnectTimeout)
	shadowConn, err := dbschema.ConnectToDatabase(connectCtx, opts.ShadowDatabaseURL)
	cancelConnect()
	if err != nil {
		return fmt.Errorf("baseline shadow check failed: connect to shadow database: %w", err)
	}
	defer dbschema.CloseAndWarn(shadowConn)

	if !sameDialect(opts.Dialect, shadowConn.Info().Dialect) {
		return fmt.Errorf(
			"baseline shadow check failed: shadow database dialect %q does not match target dialect %q",
			shadowConn.Info().Dialect,
			opts.Dialect,
		)
	}
	if opts.Capabilities != nil && !maps.Equal(opts.Capabilities, shadowConn.Info().Capabilities) {
		return fmt.Errorf("baseline shadow check failed: shadow database capabilities do not match target %s capabilities", opts.Dialect)
	}
	if err := shadowConn.SchemaWriter().DropAllTables(); err != nil {
		return fmt.Errorf("baseline shadow check failed: drop all objects: %w", err)
	}
	if err := resetBaselineShadowSchemas(ctx, shadowConn, opts.Schemas); err != nil {
		return err
	}

	migrations, err := loadPriorMigrations(opts.MigrationsDir, opts.ProviderOptions...)
	if err != nil {
		return fmt.Errorf("baseline shadow check failed: load migrations: %w", err)
	}
	migrations = migrationsAtOrBelow(migrations, opts.Version)
	if len(migrations) == 0 {
		return fmt.Errorf("baseline shadow check failed: no migrations found at or below version %d", opts.Version)
	}

	mig := migrator.NewMigrator(shadowConn, migrator.NewRegisteredMigrationProvider(migrations...))
	if err := mig.MigrateUp(ctx); err != nil {
		if description := describeReplayError(err); description != "" {
			return fmt.Errorf("baseline shadow check failed: %s", description)
		}
		return fmt.Errorf("baseline shadow check failed: replay migrations: %w", err)
	}
	if err := dropBaselineShadowMetadata(ctx, shadowConn, mig.MigrationsTableIdentifier()); err != nil {
		return err
	}

	targetSchema, err := dbschema.ReadSchemaWithSchemas(opts.TargetConn, opts.Schemas)
	if err != nil {
		return fmt.Errorf("baseline shadow check failed: read target schema: %w", err)
	}
	shadowSchema, err := dbschema.ReadSchemaWithSchemas(shadowConn, opts.Schemas)
	if err != nil {
		return fmt.Errorf("baseline shadow check failed: read shadow schema: %w", err)
	}
	compareOpts := withDialect(opts.CompareOptions, opts.Dialect)
	diff := schemadiff.CompareWithOptions(dbschematogo.ConvertDBSchemaToGoSchema(shadowSchema), targetSchema, compareOpts)
	if !diff.HasChanges() {
		return nil
	}
	return fmt.Errorf("baseline shadow check failed: %s", describeShadowDiff(diff))
}

func baselineShadowConnectContext(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(ctx)
	}
	return context.WithTimeout(ctx, timeout)
}

func migrationsAtOrBelow(migrations []*migrator.Migration, version int64) []*migrator.Migration {
	out := make([]*migrator.Migration, 0, len(migrations))
	for _, migration := range migrations {
		if migration.Version <= version {
			out = append(out, migration)
		}
	}
	return out
}

func resetBaselineShadowSchemas(ctx context.Context, conn *dbschema.DatabaseConnection, schemas []string) error {
	if conn.Info().Dialect != "postgres" {
		return nil
	}
	for _, schema := range schemas {
		if schema == "" || schema == "public" {
			continue
		}
		_, err := conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoteBaselinePostgresIdentifier(schema)+" CASCADE")
		if err != nil {
			return fmt.Errorf("baseline shadow check failed: drop schema %q: %w", schema, err)
		}
	}
	return nil
}

func dropBaselineShadowMetadata(ctx context.Context, conn *dbschema.DatabaseConnection, tableIdentifier string) error {
	_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableIdentifier)
	if err != nil {
		return fmt.Errorf("baseline shadow check failed: drop metadata table: %w", err)
	}
	return nil
}

func quoteBaselinePostgresIdentifier(value string) string {
	return `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
}
