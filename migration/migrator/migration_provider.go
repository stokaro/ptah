package migrator

import (
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"sort"
)

// MigrationProvider provides a list of migrations
type MigrationProvider interface {
	// Migrations provides a list of migrations sorted by version in ascending order
	Migrations() []*Migration
}

// RegisteredMigrationProvider is a simple in-memory implementation of MigrationProvider
type RegisteredMigrationProvider struct {
	migrations []*Migration
	sorted     bool
}

// NewRegisteredMigrationProvider creates a new in-memory migration provider with the given migrations.
// The migrations will be sorted by version when accessed through the Migrations() method.
func NewRegisteredMigrationProvider(mirations ...*Migration) *RegisteredMigrationProvider {
	return &RegisteredMigrationProvider{
		migrations: mirations,
	}
}

// Register adds a migration to the provider
func (p *RegisteredMigrationProvider) Register(migration *Migration) {
	p.migrations = append(p.migrations, migration)
	p.sorted = false
}

// Migrations returns the list of migrations sorted by version in ascending order
func (p *RegisteredMigrationProvider) Migrations() []*Migration {
	p.maybeSort()
	return p.migrations
}

// maybeSort sorts the migrations if they haven't been sorted yet
func (p *RegisteredMigrationProvider) maybeSort() {
	if p.sorted {
		return
	}
	sortMigrations(p.migrations)
	p.sorted = true
}

// FSMigrationProvider is a migration provider that loads migrations from a filesystem.
// It scans the filesystem for migration files following the naming convention and
// automatically creates Migration instances from the SQL files.
type FSMigrationProvider struct {
	fsys       fs.FS
	migrations []*Migration
}

// NewFSMigrationProvider creates a new filesystem-based migration provider.
// It scans the provided filesystem for migration files and validates that all migrations
// have both up and down files. Returns an error if the filesystem cannot be scanned
// or if any migrations are incomplete.
func NewFSMigrationProvider(fsys fs.FS) (*FSMigrationProvider, error) {
	p := &FSMigrationProvider{fsys: fsys}
	if err := p.load(); err != nil {
		return nil, err
	}
	return p, nil
}

// Migrations returns the list of migrations loaded from the filesystem, sorted by version in ascending order.
func (p *FSMigrationProvider) Migrations() []*Migration {
	return p.migrations
}

func (p *FSMigrationProvider) load() error {
	migrationsMap := make(map[int]*Migration) // version -> migration

	err := fs.WalkDir(p.fsys, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if d.IsDir() {
			return nil
		}

		// Parse migration filename
		migrationFile, err := ParseMigrationFileName(d.Name())
		if err != nil {
			// Skip files that don't match migration pattern
			return nil
		}

		// Initialize migration if it doesn't exist
		if _, exists := migrationsMap[migrationFile.Version]; !exists {
			migrationsMap[migrationFile.Version] = &Migration{
				Version:     migrationFile.Version,
				Description: migrationFile.Name,
				Up:          NoopMigrationFunc,
				Down:        NoopMigrationFunc,
			}
		}

		// Set the appropriate migration function based on direction
		switch migrationFile.Direction {
		case "up":
			migrationsMap[migrationFile.Version].Up = MigrationFuncFromSQLFilename(path, p.fsys)
		case "down":
			migrationsMap[migrationFile.Version].Down = MigrationFuncFromSQLFilename(path, p.fsys)
		default:
			return fmt.Errorf("invalid migration direction: %s", migrationFile.Direction)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to scan migrations directory: %w", err)
	}

	// Validate that all migrations have both up and down functions
	var incompleteMigrations []int
	for version, migration := range migrationsMap {
		// Check if both up and down are still noop (meaning files weren't found)
		if migration.Up == nil || migration.Down == nil {
			incompleteMigrations = append(incompleteMigrations, version)
		}
	}

	if len(incompleteMigrations) > 0 {
		return fmt.Errorf("incomplete migrations found (missing up or down files): %v", incompleteMigrations)
	}

	p.migrations = slices.Collect(maps.Values(migrationsMap))

	sortMigrations(p.migrations)

	return nil
}

func sortMigrations(migrations []*Migration) {
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
}
