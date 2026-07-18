package migrator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path"
	"slices"
	"sort"
	"strings"

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
	fsys              fs.FS
	migrations        []*Migration
	interceptor       StatementInterceptor
	format            MigrationDirFormat
	atlasTemplateData any
}

// FSProviderOption configures a FSMigrationProvider before it loads
// migrations.
type FSProviderOption func(*FSMigrationProvider)

type atlasParts struct {
	migration *Migration
	hasUp     bool
	hasDown   bool
}

// WithStatementInterceptor makes every loaded migration consult the given
// interceptor for each statement (see StatementInterceptor).
func WithStatementInterceptor(interceptor StatementInterceptor) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.interceptor = interceptor
	}
}

// WithMigrationDirFormat selects how filesystem migrations are discovered.
// The default auto mode keeps existing Ptah-pair behavior when Ptah files are
// present and otherwise accepts Atlas single-file migrations.
func WithMigrationDirFormat(format MigrationDirFormat) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.format = format
	}
}

// WithAtlasTemplateData supplies the data object used to render Atlas SQL
// template migrations. When omitted, templates render with AtlasTemplateData{}.
func WithAtlasTemplateData(data any) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.atlasTemplateData = data
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
	files, err := DiscoverMigrationFiles(p.fsys, p.format)
	if err != nil {
		return err
	}
	if len(files) == 0 {
		p.migrations = []*Migration{}
		return nil
	}
	if files[0].Format == MigrationDirFormatAtlas {
		return p.loadAtlas(files)
	}
	return p.loadPtah(files)
}

func (p *FSMigrationProvider) loadPtah(files []MigrationFile) error {
	migrationsMap := make(map[int64]*Migration)
	foundFiles := make(map[int64]map[string]bool)

	for i := range files {
		migrationFile := files[i]
		if _, exists := migrationsMap[migrationFile.Version]; !exists {
			migrationsMap[migrationFile.Version] = &Migration{
				Version:     migrationFile.Version,
				Description: migrationFile.Name,
				Up:          NoopMigrationFunc,
				Down:        NoopMigrationFunc,
			}
			foundFiles[migrationFile.Version] = make(map[string]bool)
		}

		foundFiles[migrationFile.Version][migrationFile.Direction] = true

		migration := migrationsMap[migrationFile.Version]
		switch migrationFile.Direction {
		case "up":
			up, err := migrationFuncFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.interceptor, nil)
			if err != nil {
				return fmt.Errorf("failed to load up migration %s: %w", migrationFile.Path, err)
			}
			migration.Up = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
				return up.fn(ctx, conn, migration.executionMode())
			}
			migration.UpSQL = up.sql
			migration.UpTimeouts = up.timeouts
			migration.NoTransaction = migration.NoTransaction || up.noTransaction
		case "down":
			down, err := migrationFuncFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.interceptor, nil)
			if err != nil {
				return fmt.Errorf("failed to load down migration %s: %w", migrationFile.Path, err)
			}
			migration.Down = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
				return down.fn(ctx, conn, migration.executionMode())
			}
			migration.DownSQL = down.sql
			migration.DownTimeouts = down.timeouts
			migration.NoTransaction = migration.NoTransaction || down.noTransaction
		default:
			return fmt.Errorf("invalid migration direction: %s", migrationFile.Direction)
		}
	}

	// Validate that all migrations have both up and down files
	var incompleteMigrations []int64
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

func (p *FSMigrationProvider) loadAtlas(files []MigrationFile) error {
	hashes, err := readAtlasSumHashes(p.fsys)
	if err != nil {
		return err
	}
	partsByVersion := make(map[int64]*atlasParts)
	for i := range files {
		migrationFile := files[i]
		if migrationFile.Repeatable {
			continue
		}
		parts := partsByVersion[migrationFile.Version]
		if parts == nil {
			parts = &atlasParts{
				migration: &Migration{
					Version:     migrationFile.Version,
					Description: migrationFile.Name,
				},
			}
			partsByVersion[migrationFile.Version] = parts
		}
		if hash := hashes[migrationFile.Path]; hash != "" && migrationFile.Direction == "up" {
			parts.migration.Checksum = hash
		}
		if err := p.loadAtlasFile(parts, migrationFile); err != nil {
			return err
		}
	}

	migrations := make([]*Migration, 0, len(partsByVersion))
	for _, parts := range partsByVersion {
		if !parts.hasUp {
			return fmt.Errorf("Atlas migration version %d has down migration but no up migration", parts.migration.Version)
		}
		if !parts.hasDown {
			parts.migration.downUnavailable = true
			parts.migration.Down = func(_ context.Context, _ *dbschema.DatabaseConnection) error {
				return &AtlasDownNotImplementedError{
					Version:     parts.migration.Version,
					Description: parts.migration.Description,
				}
			}
		}
		migrations = append(migrations, parts.migration)
	}

	p.migrations = migrations
	sortMigrations(p.migrations)
	return nil
}

func (p *FSMigrationProvider) loadAtlasFile(parts *atlasParts, migrationFile MigrationFile) error {
	switch migrationFile.Direction {
	case "up":
		if parts.hasUp {
			return fmt.Errorf("duplicate Atlas up migration for version %d", migrationFile.Version)
		}
		return p.loadAtlasUp(parts, migrationFile)
	case "down":
		if parts.hasDown {
			return fmt.Errorf("duplicate Atlas down migration for version %d", migrationFile.Version)
		}
		return p.loadAtlasDown(parts, migrationFile)
	default:
		return fmt.Errorf("invalid Atlas migration direction: %s", migrationFile.Direction)
	}
}

func (p *FSMigrationProvider) loadAtlasUp(parts *atlasParts, migrationFile MigrationFile) error {
	if isAtlasDirectionalMigrationFile(migrationFile) {
		up, err := migrationFuncFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.interceptor, p.atlasTemplateData)
		if err != nil {
			return fmt.Errorf("failed to load Atlas migration %s: %w", migrationFile.Path, err)
		}
		setAtlasUp(parts, up)
		return nil
	}

	atlasFile, err := atlasSQLMigrationFileFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.interceptor, p.atlasTemplateData)
	if err != nil {
		return fmt.Errorf("failed to load Atlas migration %s: %w", migrationFile.Path, err)
	}
	setAtlasUp(parts, atlasFile.up)
	if atlasFile.hasDown {
		if parts.hasDown {
			return fmt.Errorf("duplicate Atlas down migration for version %d", migrationFile.Version)
		}
		setAtlasDown(parts, atlasFile.down)
	}
	return nil
}

func (p *FSMigrationProvider) loadAtlasDown(parts *atlasParts, migrationFile MigrationFile) error {
	down, err := migrationFuncFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.interceptor, p.atlasTemplateData)
	if err != nil {
		return fmt.Errorf("failed to load Atlas migration %s: %w", migrationFile.Path, err)
	}
	setAtlasDown(parts, down)
	return nil
}

func setAtlasUp(parts *atlasParts, up sqlMigrationFile) {
	migration := parts.migration
	migration.Up = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return up.fn(ctx, conn, migration.executionMode())
	}
	migration.UpSQL = up.sql
	migration.UpTimeouts = up.timeouts
	migration.NoTransaction = migration.NoTransaction || up.noTransaction
	parts.hasUp = true
}

func setAtlasDown(parts *atlasParts, down sqlMigrationFile) {
	migration := parts.migration
	migration.Down = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return down.fn(ctx, conn, migration.executionMode())
	}
	migration.DownSQL = down.sql
	migration.DownTimeouts = down.timeouts
	migration.NoTransaction = migration.NoTransaction || down.noTransaction
	parts.hasDown = true
}

func isAtlasDirectionalMigrationFile(file MigrationFile) bool {
	base := path.Base(file.Path)
	return strings.HasSuffix(base, ".up.sql") || strings.HasSuffix(base, ".down.sql")
}

func readAtlasSumHashes(fsys fs.FS) (map[string]string, error) {
	data, err := fs.ReadFile(fsys, "atlas.sum")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return map[string]string{}, nil
		}
		return nil, fmt.Errorf("failed to read atlas.sum: %w", err)
	}
	hashes := make(map[string]string)
	lines := strings.Split(strings.TrimRight(string(data), "\r\n"), "\n")
	for _, line := range lines[1:] {
		line = strings.TrimRight(line, "\r")
		if strings.TrimSpace(line) == "" {
			continue
		}
		idx := strings.LastIndex(line, " ")
		if idx <= 0 || idx == len(line)-1 {
			return nil, fmt.Errorf("malformed atlas.sum entry line: %q", line)
		}
		hashes[line[:idx]] = line[idx+1:]
	}
	return hashes, nil
}

func sortMigrations(migrations []*Migration) {
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].Version < migrations[j].Version
	})
}
