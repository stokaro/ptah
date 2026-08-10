package integration

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/integrationfixture"
	"go.5x5.cz/ptah/migration/generator"
)

type appliedRoundTripMigration struct {
	Version         string
	PreviousVersion string
}

func testMigrationGeneratorRoundTripFixtures(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	fixtures fs.FS,
	recorder *StepRecorder,
	cleanup databaseCleanupFunc,
) error {
	for _, fixture := range integrationfixture.RoundTrips() {
		if issue := fixture.BlockedByDialect[conn.Info().Dialect]; issue != "" {
			if err := recordSkippedRoundTripFixture(recorder, fixture, issue); err != nil {
				return err
			}
			continue
		}

		if err := recorder.RecordStep(
			"Round-trip fixture "+fixture.Name,
			fixture.Description,
			func() error {
				return runRoundTripFixture(ctx, conn, fixtures, fixture, cleanup)
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func recordSkippedRoundTripFixture(recorder *StepRecorder, fixture integrationfixture.RoundTrip, issue string) error {
	return recorder.RecordStep(
		"Skip blocked round-trip fixture "+fixture.Name,
		fmt.Sprintf("%s is tracked by %s", fixture.Description, issue),
		func() error { return nil },
	)
}

func runRoundTripFixture(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	fixtures fs.FS,
	fixture integrationfixture.RoundTrip,
	cleanup databaseCleanupFunc,
) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	migrationsDir, err := os.MkdirTemp("", "ptah_roundtrip_fixture_*")
	if err != nil {
		return fmt.Errorf("create migrations dir: %w", err)
	}
	defer os.RemoveAll(migrationsDir)

	migrationsFS := os.DirFS(migrationsDir)
	dh := NewDatabaseHelper(conn)

	if err := resetRoundTripFixtureDatabase(ctx, conn, cleanup); err != nil {
		return err
	}

	appliedMigrations := make([]appliedRoundTripMigration, 0, len(fixture.Versions))
	for versionIndex, version := range fixture.Versions {
		applied, err := generateAndApplyRoundTripVersion(ctx, conn, vem, dh, migrationsFS, migrationsDir, fixture, version)
		if err != nil {
			return err
		}
		if applied {
			appliedMigrations = append(appliedMigrations, appliedRoundTripMigration{
				Version:         version,
				PreviousVersion: previousRoundTripVersion(fixture.Versions, versionIndex),
			})
		}
		if err := validateSchemaConsistency(ctx, conn, vem, version); err != nil {
			return fmt.Errorf("%s after %s: %w", fixture.Name, version, err)
		}
	}

	if len(appliedMigrations) > 0 {
		if err := rollbackRoundTripFixtureMigrations(ctx, conn, vem, dh, migrationsFS, fixture, appliedMigrations); err != nil {
			return err
		}
		if err := dh.MigrateUp(ctx, migrationsFS); err != nil {
			return fmt.Errorf("%s re-apply all generated migrations: %w", fixture.Name, err)
		}
		lastVersion := fixture.Versions[len(fixture.Versions)-1]
		if err := validateSchemaConsistency(ctx, conn, vem, lastVersion); err != nil {
			return fmt.Errorf("%s final re-apply validation: %w", fixture.Name, err)
		}
	}

	return resetRoundTripFixtureDatabase(ctx, conn, cleanup)
}

func previousRoundTripVersion(versions []string, versionIndex int) string {
	if versionIndex == 0 {
		return ""
	}
	return versions[versionIndex-1]
}

func rollbackRoundTripFixtureMigrations(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	vem *VersionedEntityManager,
	dh *DatabaseHelper,
	migrationsFS fs.FS,
	fixture integrationfixture.RoundTrip,
	appliedMigrations []appliedRoundTripMigration,
) error {
	for _, applied := range slices.Backward(appliedMigrations) {
		if err := dh.MigrateDown(ctx, migrationsFS); err != nil {
			return fmt.Errorf("%s down from %s: %w", fixture.Name, applied.Version, err)
		}
		if err := validateRoundTripRollbackState(ctx, conn, vem, fixture, applied); err != nil {
			return err
		}
	}
	return nil
}

func validateRoundTripRollbackState(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	vem *VersionedEntityManager,
	fixture integrationfixture.RoundTrip,
	applied appliedRoundTripMigration,
) error {
	if applied.PreviousVersion == "" {
		if err := validateEmptySchema(ctx, conn); err != nil {
			return fmt.Errorf("%s down-to-zero validation after %s: %w", fixture.Name, applied.Version, err)
		}
		return nil
	}

	if err := validateSchemaConsistency(ctx, conn, vem, applied.PreviousVersion); err != nil {
		return fmt.Errorf("%s rollback from %s to %s: %w", fixture.Name, applied.Version, applied.PreviousVersion, err)
	}
	return nil
}

func resetRoundTripFixtureDatabase(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	cleanup databaseCleanupFunc,
) error {
	if err := cleanup(ctx); err != nil {
		return fmt.Errorf("reset round-trip fixture database: %w", err)
	}
	return validateEmptySchema(ctx, conn)
}

func generateAndApplyRoundTripVersion(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	vem *VersionedEntityManager,
	dh *DatabaseHelper,
	migrationsFS fs.FS,
	migrationsDir string,
	fixture integrationfixture.RoundTrip,
	version string,
) (bool, error) {
	if err := vem.LoadEntityVersion(version); err != nil {
		return false, fmt.Errorf("%s load %s: %w", fixture.Name, version, err)
	}

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir: vem.GetEntitiesDir(),
		DBConn:        conn,
		OutputDir:     migrationsDir,
		MigrationName: roundTripMigrationName(fixture.Name, version),
	})
	if err != nil {
		return false, fmt.Errorf("%s generate %s: %w", fixture.Name, version, err)
	}
	if files == nil {
		return false, nil
	}
	if err := dh.MigrateUp(ctx, migrationsFS); err != nil {
		return false, fmt.Errorf("%s apply %s: %w", fixture.Name, version, err)
	}
	return true, nil
}

func roundTripMigrationName(fixtureName, version string) string {
	_, suffix, ok := strings.Cut(version, "-")
	if !ok || suffix == "" {
		suffix = "schema"
	}
	return fixtureName + "_" + strings.ReplaceAll(suffix, "-", "_")
}
