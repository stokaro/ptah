package integration

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"slices"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/generator"
)

type roundTripFixture struct {
	Name             string
	Description      string
	Versions         []string
	BlockedByDialect map[string]string
}

type appliedRoundTripMigration struct {
	Version         string
	PreviousVersion string
}

var roundTripFixtures = []roundTripFixture{
	{
		Name:        "empty_schema",
		Description: "empty entity package stays empty across generated up/down",
		Versions:    []string{"024-roundtrip-empty"},
	},
	{
		Name:        "single_table",
		Description: "single table round-trips from generated migrations",
		Versions:    []string{"025-roundtrip-single-table"},
	},
	{
		Name:        "composite_primary_key",
		Description: "table-level composite primary key survives apply -> introspect -> diff",
		Versions:    []string{"026-roundtrip-composite-pk"},
	},
	{
		Name:        "self_referencing_fk",
		Description: "self-referencing foreign key goes through the generator path",
		Versions:    []string{"027-roundtrip-self-reference"},
	},
	{
		Name:        "parent_child_fk_drop_order",
		Description: "parent/child tables created in one migration roll down to empty through generated down SQL",
		Versions:    []string{"028-roundtrip-parent-child"},
	},
	{
		Name:        "three_level_fk_chain",
		Description: "three-table foreign-key chain is generated, applied, introspected, and rolled back",
		Versions:    []string{"034-roundtrip-fk-chain"},
	},
	{
		Name:        "diamond_fk_graph",
		Description: "diamond-shaped foreign-key graph is generated and verified through the round-trip path",
		Versions:    []string{"035-roundtrip-fk-diamond"},
	},
	{
		Name:        "mutual_fk_cycle",
		Description: "mutual foreign-key cycle is generated, applied, introspected, and rolled back",
		Versions:    []string{"029-roundtrip-mutual-cycle"},
	},
	{
		Name:        "same_name_check_drift",
		Description: "same-name CHECK expression changes must be detected by generated migrations",
		Versions:    []string{"030-roundtrip-check-v1", "031-roundtrip-check-v2"},
	},
	{
		Name:        "same_name_unique_drift",
		Description: "same-name UNIQUE column-set changes must be detected by generated migrations",
		Versions:    []string{"032-roundtrip-unique-v1", "033-roundtrip-unique-v2"},
	},
	{
		Name:        "same_name_check_to_unique_drift",
		Description: "same-name CHECK to UNIQUE type changes must be detected by generated migrations",
		Versions:    []string{"042-roundtrip-check-to-unique-v1", "043-roundtrip-check-to-unique-v2"},
	},
	{
		Name:        "same_name_unique_to_check_drift",
		Description: "same-name UNIQUE to CHECK type changes must be detected by generated migrations",
		Versions:    []string{"044-roundtrip-unique-to-check-v1", "045-roundtrip-unique-to-check-v2"},
	},
	{
		Name:        "composite_primary_key_add_remove",
		Description: "multi-column primary key addition and removal round-trip through generated migrations",
		Versions: []string{
			"036-roundtrip-pk-base",
			"037-roundtrip-pk-composite-added",
			"038-roundtrip-pk-composite-removed",
		},
	},
	{
		Name:        "enum_value_add",
		Description: "enum value addition is generated, applied, introspected, rolled down, and re-applied",
		Versions:    []string{"039-roundtrip-enum-v1", "040-roundtrip-enum-v2-add"},
	},
	{
		Name:        "enum_value_remove",
		Description: "enum value removal is carried as an explicit round-trip fixture",
		Versions:    []string{"040-roundtrip-enum-v2-add", "041-roundtrip-enum-v3-remove"},
	},
	{
		Name:        "foreign_key_added_to_existing_columns",
		Description: "foreign keys added to existing columns, including a self-reference, round-trip through generated migrations",
		Versions:    []string{"046-roundtrip-existing-fk-base", "047-roundtrip-existing-fk-added"},
	},
}

func testMigrationGeneratorRoundTripFixtures(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	fixtures fs.FS,
	recorder *StepRecorder,
) error {
	for _, fixture := range roundTripFixtures {
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
				return runRoundTripFixture(ctx, conn, fixtures, fixture)
			},
		); err != nil {
			return err
		}
	}
	return nil
}

func recordSkippedRoundTripFixture(recorder *StepRecorder, fixture roundTripFixture, issue string) error {
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
	fixture roundTripFixture,
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

	if err := resetRoundTripFixtureDatabase(ctx, conn); err != nil {
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

	return resetRoundTripFixtureDatabase(ctx, conn)
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
	fixture roundTripFixture,
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
	fixture roundTripFixture,
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

func resetRoundTripFixtureDatabase(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	if err := rollbackToEmptyState(ctx, conn); err != nil {
		return err
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
	fixture roundTripFixture,
	version string,
) (bool, error) {
	if err := vem.LoadEntityVersion(version); err != nil {
		return false, fmt.Errorf("%s load %s: %w", fixture.Name, version, err)
	}

	files, err := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
		GoEntitiesDir:  vem.GetEntitiesDir(),
		DBConn:         conn,
		OutputDir:      migrationsDir,
		MigrationName:  roundTripMigrationName(fixture.Name, version),
		CompareOptions: dialectCompareOptions(conn),
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
