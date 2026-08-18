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
	"sync"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlashash"
)

// MigrationProvider provides a list of migrations
type MigrationProvider interface {
	// Migrations provides a list of migrations sorted by version in ascending order
	Migrations() []*Migration
}

// RegisteredMigrationProvider is a simple in-memory implementation of MigrationProvider
type RegisteredMigrationProvider struct {
	mu         sync.Mutex
	migrations []*Migration
	sorted     bool
}

// NewRegisteredMigrationProvider creates a new in-memory migration provider with the given migrations.
// The migrations will be sorted by version when accessed through the Migrations() method.
func NewRegisteredMigrationProvider(migrations ...*Migration) *RegisteredMigrationProvider {
	return &RegisteredMigrationProvider{
		migrations: slices.Clone(migrations),
	}
}

// Register adds a migration to the provider
func (p *RegisteredMigrationProvider) Register(migration *Migration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.migrations = append(p.migrations, migration)
	p.sorted = false
}

// Migrations returns the list of migrations sorted by version in ascending order
func (p *RegisteredMigrationProvider) Migrations() []*Migration {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.maybeSortLocked()
	return slices.Clone(p.migrations)
}

// maybeSort sorts the migrations if they haven't been sorted yet
func (p *RegisteredMigrationProvider) maybeSortLocked() {
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
	mu                     sync.Mutex
	fsys                   fs.FS
	migrations             []*Migration
	hooks                  statementExecutionHooks
	format                 MigrationDirFormat
	atlasTemplateData      any
	atlasRevisionVersions  map[int64]string
	atlasRevisionChecksums map[int64]string
	atlasRevisionTypes     map[int64]AtlasRevisionType
	atlasRepeatable        map[int64]bool
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
		p.hooks.interceptor = interceptor
	}
}

// WithStatementValidator makes every loaded migration validate all statements
// before executing the first one.
func WithStatementValidator(validator StatementValidator) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.hooks.validator = validator
	}
}

// WithStatementObserver makes every loaded migration report successfully
// executed statements to the given observer (see StatementObserver).
func WithStatementObserver(observer StatementObserver) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.hooks.observer = observer
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

// WithAtlasRevisionVersions supplies exact Atlas revision-table identities for
// migrations whose numeric [Migration.Version] is only an execution-order key.
// Keys absent from versions keep the revision identity parsed from the file
// name. A present empty value is an exact empty identity, not a fallback.
// A non-nil map also marks recorded identities that no current or historical
// mapping owns as retired source history: they remain readable and contribute
// their exact identity to source ordering without becoming pending migrations.
//
// Most callers do not need this option. It exists for adapters that convert a
// source migration layout into order-preserving numeric Atlas file names while
// retaining that source layout's opaque revision tokens.
func WithAtlasRevisionVersions(versions map[int64]string) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.atlasRevisionVersions = maps.Clone(versions)
	}
}

// WithAtlasRevisionChecksums supplies the h1 hash the SOURCE directory's
// atlas.sum recorded for each converted execution-order key, so a revision row
// carries the checksum the source history uses rather than one recomputed from
// the converted bytes.
//
// Converting a foreign layout rebuilds it as up-only Atlas migrations with no
// integrity file, so without this the migration has no Checksum and
// [migrationRevisionHash] falls back to the hex SHA-256 of the up SQL. A
// history the Atlas community binary wrote stores the atlas.sum h1 instead, and
// the two never compare equal, which is what stopped Ptah continuing such a
// history (stokaro/ptah#1209).
//
// Keys absent from checksums keep the pre-existing behavior, so a directory
// with no atlas.sum and every Ptah-written history are unaffected.
func WithAtlasRevisionChecksums(checksums map[int64]string) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.atlasRevisionChecksums = maps.Clone(checksums)
	}
}

// WithAtlasRevisionTypes supplies Atlas revision-row types for migrations whose
// source format carries semantics lost by filename conversion. Keys absent from
// types retain the ordinary applied type.
func WithAtlasRevisionTypes(types map[int64]AtlasRevisionType) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.atlasRevisionTypes = maps.Clone(types)
	}
}

// WithAtlasRepeatableVersions marks converted execution-order keys whose
// source migrations are Atlas repeatables. A compatibility adapter uses this
// when conversion preserves the SQL body and numeric order but loses the
// source file-name shape that carries repeatability.
func WithAtlasRepeatableVersions(versions []int64) FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.atlasRepeatable = make(map[int64]bool, len(versions))
		for _, version := range versions {
			p.atlasRepeatable[version] = true
		}
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
	p.mu.Lock()
	defer p.mu.Unlock()

	return slices.Clone(p.migrations)
}

// atlasRevisionVersionMap returns every source identity mapping supplied by a
// compatibility adapter, including identities for files a surviving baseline
// has squashed out of the current provider. The migrator needs those extra
// entries to interpret existing history and compute its high-water mark; only
// migrations actually loaded above receive an owned revision identity.
func (p *FSMigrationProvider) atlasRevisionVersionMap() map[int64]string {
	return maps.Clone(p.atlasRevisionVersions)
}

func (p *FSMigrationProvider) hasAtlasRevisionVersionMap() bool {
	return p.atlasRevisionVersions != nil
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
				Version:      migrationFile.Version,
				Description:  migrationFile.Name,
				Up:           NoopMigrationFunc,
				Down:         NoopMigrationFunc,
				IsCheckpoint: migrationFile.IsCheckpoint,
			}
			foundFiles[migrationFile.Version] = make(map[string]bool)
		}

		foundFiles[migrationFile.Version][migrationFile.Direction] = true

		migration := migrationsMap[migrationFile.Version]
		// The up and down halves of one version must agree on whether they are
		// a checkpoint; a mixed pair is a malformed directory.
		if migration.IsCheckpoint != migrationFile.IsCheckpoint {
			return fmt.Errorf("migration version %d mixes checkpoint and non-checkpoint files", migrationFile.Version)
		}
		switch migrationFile.Direction {
		case "up":
			up, err := migrationFuncFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.hooks, nil)
			if err != nil {
				return fmt.Errorf("failed to load up migration %s: %w", migrationFile.Path, err)
			}
			setMigrationUp(migration, up)
		case "down":
			down, err := migrationFuncFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.hooks, nil)
			if err != nil {
				return fmt.Errorf("failed to load down migration %s: %w", migrationFile.Path, err)
			}
			setMigrationDown(migration, down)
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
	maxVersion := atlasMaxNumericVersion(files)
	partsByRevision := make(map[string]*atlasParts)
	for i := range files {
		migrationFile := files[i]
		runtimeVersion := atlasRuntimeVersion(migrationFile, maxVersion)
		revisionVersion := migrationFile.RevisionVersion()
		repeatable := revisionVersion == "R" || strings.HasSuffix(revisionVersion, "R") ||
			p.atlasRepeatable[migrationFile.Version]
		mappedRevisionVersion, mapped := p.atlasRevisionVersions[migrationFile.Version]
		if mapped {
			revisionVersion = mappedRevisionVersion
		}
		parts := partsByRevision[revisionVersion]
		if parts != nil && parts.migration.Version != runtimeVersion &&
			(mapped || parts.migration.atlasRevisionVersionMapped) {
			return fmt.Errorf(
				"atlas revision identity %q maps migration order keys %d and %d",
				revisionVersion, parts.migration.Version, runtimeVersion,
			)
		}
		if parts == nil {
			revisionType := p.atlasRevisionTypes[migrationFile.Version]
			parts = &atlasParts{
				migration: &Migration{
					Version:                    runtimeVersion,
					Description:                migrationFile.Name,
					atlasRevisionVersion:       revisionVersion,
					hasAtlasRevisionVersion:    true,
					atlasRevisionVersionMapped: mapped,
					atlasOrderKey:              migrationFile.Path,
					revisionDescription:        migrationFile.revisionDescription,
					hasRevisionDescription:     true,
					atlasRevisionType:          revisionType,
					atlasRepeatable:            repeatable,
				},
			}
			partsByRevision[revisionVersion] = parts
		}
		p.recordAtlasHashes(parts.migration, migrationFile, hashes[migrationFile.Path], runtimeVersion)
		raw, err := fs.ReadFile(p.fsys, migrationFile.Path)
		if err != nil {
			return fmt.Errorf("failed to read Atlas checksum source %s: %w", migrationFile.Path, err)
		}
		_, hasHashEntry := hashes[migrationFile.Path]
		ignored := atlashash.IsSumIgnored(raw)
		parts.migration.atlasSumContributions = append(
			parts.migration.atlasSumContributions,
			atlasSumContribution{
				name:          migrationFile.Path,
				data:          raw,
				includeData:   !ignored,
				revisionEntry: migrationFile.Direction == "up" && hasHashEntry,
			},
		)
		if err := p.loadAtlasFile(parts, migrationFile); err != nil {
			return err
		}
	}

	migrations := make([]*Migration, 0, len(partsByRevision))
	for _, parts := range partsByRevision {
		if !parts.hasUp {
			return fmt.Errorf("Atlas migration version %s has down migration but no up migration", parts.migration.RevisionVersion())
		}
		if !parts.hasDown {
			parts.migration.downUnavailable = true
			parts.migration.Down = func(_ context.Context, _ *dbschema.DatabaseConnection) error {
				return &AtlasDownNotImplementedError{
					Version:         parts.migration.Version,
					revisionVersion: parts.migration.RevisionVersion(),
					Description:     parts.migration.Description,
				}
			}
		}
		migrations = append(migrations, parts.migration)
	}

	p.migrations = migrations
	sortMigrations(p.migrations)
	return nil
}

func atlasMaxNumericVersion(files []MigrationFile) int64 {
	var maxVersion int64
	for _, file := range files {
		if !file.Repeatable && file.Version > maxVersion {
			maxVersion = file.Version
		}
	}
	return maxVersion
}

func atlasRuntimeVersion(file MigrationFile, maxVersion int64) int64 {
	if !file.Repeatable || file.Version > 0 {
		return file.Version
	}
	return maxVersion + 1
}

// recordAtlasHashes attaches the two checksums a converted migration can carry.
//
// They are different things and are deliberately kept apart. sumHash is what
// THIS directory's own atlas.sum records, and it becomes the migration's
// checksum: it is what Ptah writes into a revision row. The source hash comes
// from the directory a foreign layout was converted FROM, whose atlas.sum the
// conversion drops, and it is only ever an accepted value.
//
// Writing the source hash instead would look tidier and be wrong. An atlas.sum
// h1 chains over every preceding file, so a Ptah history keyed on it would stop
// verifying the moment an unrelated migration was inserted ahead of it -- a
// shape Ptah supports and its mid-sequence-insertion tests cover
// (stokaro/ptah#1209).
func (p *FSMigrationProvider) recordAtlasHashes(
	migration *Migration,
	migrationFile MigrationFile,
	sumHash string,
	runtimeVersion int64,
) {
	if migrationFile.Direction != "up" {
		return
	}
	if sumHash != "" {
		migration.Checksum = sumHash
	}
	if sourceHash := p.atlasRevisionChecksums[runtimeVersion]; sourceHash != "" {
		migration.sourceRevisionHash = sourceHash
	}
}

func (p *FSMigrationProvider) loadAtlasFile(parts *atlasParts, migrationFile MigrationFile) error {
	switch migrationFile.Direction {
	case "up":
		if parts.hasUp {
			return fmt.Errorf("duplicate Atlas up migration for version %s", migrationFile.RevisionVersion())
		}
		return p.loadAtlasUp(parts, migrationFile)
	case "down":
		if parts.hasDown {
			return fmt.Errorf("duplicate Atlas down migration for version %s", migrationFile.RevisionVersion())
		}
		return p.loadAtlasDown(parts, migrationFile)
	default:
		return fmt.Errorf("invalid Atlas migration direction: %s", migrationFile.Direction)
	}
}

func (p *FSMigrationProvider) loadAtlasUp(parts *atlasParts, migrationFile MigrationFile) error {
	sql, err := readSQLMigrationFile(p.fsys, migrationFile.Path, p.atlasTemplateData)
	if err != nil {
		return fmt.Errorf("failed to load Atlas migration %s: %w", migrationFile.Path, err)
	}

	// Atlas marks checkpoints with a first-line `-- atlas:checkpoint` file
	// directive rather than Ptah's `.checkpoint.` file-name marker. Detect it
	// here — before the migration body is wired up — so the migrator applies
	// the same bootstrap-or-skip semantics to externally produced Atlas
	// checkpoint directories.
	isCheckpoint, err := atlasCheckpointFromSQL(migrationFile.Path, sql)
	if err != nil {
		return err
	}
	parts.migration.IsCheckpoint = isCheckpoint

	if isAtlasDirectionalMigrationFile(migrationFile) {
		up, err := migrationFuncFromSQLContentWithMetadata(migrationFile.Path, sql, p.hooks)
		if err != nil {
			return fmt.Errorf("failed to load Atlas migration %s: %w", migrationFile.Path, err)
		}
		setAtlasUp(parts, up)
		return nil
	}

	atlasFile, err := atlasSQLMigrationFileFromSQLContentWithMetadata(migrationFile.Path, sql, p.hooks)
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
	down, err := migrationFuncFromSQLFilenameWithMetadata(migrationFile.Path, p.fsys, p.hooks, p.atlasTemplateData)
	if err != nil {
		return fmt.Errorf("failed to load Atlas migration %s: %w", migrationFile.Path, err)
	}
	setAtlasDown(parts, down)
	return nil
}

func setAtlasUp(parts *atlasParts, up sqlMigrationFile) {
	setMigrationUp(parts.migration, up)
	parts.hasUp = true
}

func setMigrationUp(migration *Migration, up sqlMigrationFile) {
	migration.Up = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		txMode := migration.parsedUpTxModeForDialect(databaseConnectionDialect(conn))
		if txMode.err != nil {
			return txMode.err
		}
		return up.fn(ctx, conn, migrationExecutionModeForFileTxMode(txMode.mode))
	}
	migration.upSQLFunc = up.fn
	migration.upHasStatementInterceptor = up.statementIntercepted
	migration.UpSQL = up.sql
	migration.atlasCheckFiles = up.checkFiles
	migration.UpTimeouts = up.timeouts
	migration.upParsedTimeouts = up.timeouts
	migration.upTimeoutsFromSQL = true
	migration.UpTxMode = up.txMode
	migration.upParsedTxMode = up.txMode
	migration.upTxModeFromSQL = true
	migration.upTxModeSource = up.txModeSource
	migration.upTxModeErr = up.txModeErr
	migration.upSourcePath = up.sourcePath
}

func setAtlasDown(parts *atlasParts, down sqlMigrationFile) {
	setMigrationDown(parts.migration, down)
	parts.hasDown = true
}

func setMigrationDown(migration *Migration, down sqlMigrationFile) {
	migration.Down = func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		txMode := migration.parsedDownTxModeForDialect(databaseConnectionDialect(conn))
		if txMode.err != nil {
			return txMode.err
		}
		return down.fn(ctx, conn, migrationExecutionModeForFileTxMode(txMode.mode))
	}
	migration.downSQLFunc = down.fn
	migration.downHasStatementInterceptor = down.statementIntercepted
	migration.DownSQL = down.sql
	migration.DownTimeouts = down.timeouts
	migration.downParsedTimeouts = down.timeouts
	migration.downTimeoutsFromSQL = true
	migration.DownTxMode = down.txMode
	migration.downParsedTxMode = down.txMode
	migration.downTxModeFromSQL = true
	migration.downTxModeSource = down.txModeSource
	migration.downTxModeErr = down.txModeErr
	migration.downSourcePath = down.sourcePath
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
		if migrations[i].atlasOrderKey != "" || migrations[j].atlasOrderKey != "" {
			return migrations[i].atlasOrderKey < migrations[j].atlasOrderKey
		}
		return migrations[i].Version < migrations[j].Version
	})
}
