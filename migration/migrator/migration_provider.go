package migrator

import (
	"context"
	"fmt"
	"io/fs"
	"maps"
	"slices"
	"sort"

	"github.com/stokaro/ptah/dbschema"
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
func NewRegisteredMigrationProvider(migrations ...*Migration) *RegisteredMigrationProvider {
	return &RegisteredMigrationProvider{
		migrations: migrations,
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
	fsys        fs.FS
	migrations  []*Migration
	interceptor StatementInterceptor
}

// FSProviderOption configures a FSMigrationProvider before it loads
// migrations.
type FSProviderOption func(*FSMigrationProvider)

// WithStatementInterceptor makes every loaded migration consult the given
// interceptor for each statement (see StatementInterceptor).
func WithStatementInterceptor(interceptor StatementInterceptor) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.interceptor = interceptor
	}
}

// NewFSMigrationProvider creates a new filesystem-based migration provider.
// It scans the provided filesystem for migration files and validates that all migrations
// have both up and down files. Returns an error if the filesystem cannot be scanned
// or if any migrations are incomplete.
func NewFSMigrationProvider(fsys fs.FS, opts ...FSProviderOption) (*FSMigrationProvider, error) {
	p := &FSMigrationProvider{fsys: fsys}
	for _, opt := range opts {
		opt(p)
	}
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
	// Track which files were found for validation
	foundFiles := make(map[int]map[string]bool) // version -> direction -> found

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
			foundFiles[migrationFile.Version] = make(map[string]bool)
		}

		// Track that we found this file
		foundFiles[migrationFile.Version][migrationFile.Direction] = true

		// Set the appropriate migration function based on direction
		migration := migrationsMap[migrationFile.Version]
		switch migrationFile.Direction {
		case "up":
			up, err := migrationFuncFromSQLFilenameWithMetadata(path, p.fsys, p.interceptor)
			if err != nil {
				return fmt.Errorf("failed to load up migration %s: %w", path, err)
			}
			migration.Up = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
				return up.fn(ctx, conn, migration.executionMode())
			}
			migration.UpTimeouts = up.timeouts
			migration.NoTransaction = migration.NoTransaction || up.noTransaction
		case "down":
			down, err := migrationFuncFromSQLFilenameWithMetadata(path, p.fsys, p.interceptor)
			if err != nil {
				return fmt.Errorf("failed to load down migration %s: %w", path, err)
			}
			migration.Down = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
				return down.fn(ctx, conn, migration.executionMode())
			}
			migration.DownTimeouts = down.timeouts
			migration.NoTransaction = migration.NoTransaction || down.noTransaction
		default:
			return fmt.Errorf("invalid migration direction: %s", migrationFile.Direction)
		}

		return nil
	})

	if err != nil {
		return fmt.Errorf("failed to scan migrations directory: %w", err)
	}

	// Validate that all migrations have both up and down files
	var incompleteMigrations []int
	for version := range migrationsMap {
		files := foundFiles[version]
		if !files["up"] || !files["down"] {
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
