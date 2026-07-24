package generator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/dbschematogo"
	"github.com/stokaro/ptah/internal/convert/fromschema"
	"github.com/stokaro/ptah/internal/deporder"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/internal/pathguard"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
	"github.com/stokaro/ptah/migration/safety"
	"github.com/stokaro/ptah/migration/schemadiff"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

// GenerateMigrationOptions contains options for migration generation
type GenerateMigrationOptions struct {
	// GoEntitiesDir is the directory to scan for Go entities
	GoEntitiesDir string
	// GoEntitiesFS is the filesystem to use for reading entities (optional, defaults to os.DirFS)
	GoEntitiesFS fs.FS
	// DatabaseURL is the connection string for the database
	DatabaseURL string
	// DBConn is the database connection (optional, if not provided, a new connection will be created)
	DBConn *dbschema.DatabaseConnection
	// MigrationName is the name for the migration (optional, defaults to "migration")
	MigrationName string
	// OutputDir is the directory where migration files will be saved (always real filesystem)
	OutputDir string
	// AllowedOutputRoot constrains OutputDir when set. Embedders that accept
	// user-supplied output paths should set this to the project/workspace root.
	AllowedOutputRoot string
	// CompareOptions are the options to use when comparing schemas
	CompareOptions *config.CompareOptions
	// Schemas restricts database introspection to the listed schemas when the
	// connected dialect supports schema scoping.
	Schemas []string
	// CheckDestructive refuses to generate destructive up migrations unless
	// AllowDestructive is set.
	CheckDestructive bool
	// AllowDestructive permits destructive up migrations when CheckDestructive is set.
	AllowDestructive bool
	// ReportFormat optionally writes a safety report next to generated files.
	// Supported values: "", "html", "json".
	ReportFormat string
	// ShadowDatabaseURL enables pre-write verification on an ephemeral database.
	// The generator drops all objects in this database, replays existing
	// migrations from OutputDir, applies the candidate migration, re-introspects
	// the result, and aborts if it differs from the Go schema.
	ShadowDatabaseURL string
}

// MigrationFilePair represents one generated up/down migration file pair.
type MigrationFilePair struct {
	UpFile        string // Path to the up migration file
	DownFile      string // Path to the down migration file
	ReportFile    string // Path to the safety report file, when requested
	Version       int64  // Migration version (timestamp)
	NoTransaction bool   // Whether the pair is marked with +ptah no_transaction
}

// MigrationFiles represents the generated migration files.
type MigrationFiles struct {
	UpFile     string              // Path to the first up migration file
	DownFile   string              // Path to the first down migration file
	ReportFile string              // Path to the first safety report file, when requested
	Version    int64               // First migration version (timestamp)
	Files      []MigrationFilePair // All generated migration file pairs, in apply order
}

// EmptyMigrationOptions contains options for skeleton migration creation.
type EmptyMigrationOptions struct {
	// MigrationName is the descriptive migration name used in filenames and headers.
	MigrationName string
	// OutputDir is the directory where migration files will be saved.
	OutputDir string
	// AllowedOutputRoot constrains OutputDir when set.
	AllowedOutputRoot string
	// DirFormat selects the generated migration file layout. Empty generates
	// Ptah paired up/down files.
	DirFormat migrator.MigrationDirFormat
}

// GenerateEmptyMigration creates skeleton migration files for manual SQL
// authoring.
func GenerateEmptyMigration(opts EmptyMigrationOptions) (*MigrationFiles, error) {
	name := strings.TrimSpace(opts.MigrationName)
	if strings.TrimSpace(opts.OutputDir) == "" {
		return nil, fmt.Errorf("output directory is required")
	}
	dirFormat, err := migrator.ParseMigrationDirFormat(string(opts.DirFormat))
	if err != nil {
		return nil, err
	}

	outputDir, err := pathguard.ResolveWithinRoot(opts.OutputDir, opts.AllowedOutputRoot)
	if err != nil {
		return nil, fmt.Errorf("error validating output directory: %w", err)
	}
	if dirFormat == migrator.MigrationDirFormatAtlas {
		return generateEmptyAtlasMigration(name, outputDir)
	}
	if err := validateEmptyMigrationName(name); err != nil {
		return nil, err
	}

	version := migrator.GetNextMigrationVersion()
	version = nextAvailableMigrationVersion(outputDir, version, name)
	generatedAt := time.Now().UTC().Format(time.RFC3339)

	return createMigrationFiles(
		outputDir,
		version,
		name,
		emptyMigrationSQL(name, generatedAt, "UP"),
		emptyMigrationSQL(name, generatedAt, "DOWN"),
	)
}

func generateEmptyAtlasMigration(name, outputDir string) (*MigrationFiles, error) {
	if err := ensureMigrationOutputDir(outputDir); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	version := nextAvailableAtlasMigrationVersion(outputDir, nextAtlasMigrationVersion())
	for {
		filePath := filepath.Join(outputDir, atlasEmptyMigrationFileName(version, name))
		if err := writeNewMigrationFile(filePath, ""); err != nil {
			if errors.Is(err, os.ErrExist) {
				version++
				continue
			}
			return nil, fmt.Errorf("failed to write atlas migration file: %w", err)
		}
		if _, err := migratesum.WriteWithFormat(outputDir, migrator.MigrationDirFormatAtlas); err != nil {
			_ = os.Remove(filePath)
			return nil, fmt.Errorf("failed to write atlas migration checksum: %w", err)
		}
		pair := MigrationFilePair{
			UpFile:  filePath,
			Version: version,
		}
		return migrationFilesFromPairs([]MigrationFilePair{pair}), nil
	}
}

func nextAtlasMigrationVersion() int64 {
	version, err := strconv.ParseInt(time.Now().UTC().Format("20060102150405"), 10, 64)
	if err != nil {
		return migrator.GetNextMigrationVersion()
	}
	return version
}

func nextAvailableAtlasMigrationVersion(outputDir string, version int64) int64 {
	if latest := latestExistingAtlasMigrationVersion(outputDir); latest >= version {
		version = latest + 1
	}
	for fileExists(filepath.Join(outputDir, atlasEmptyMigrationFileName(version, ""))) {
		version++
	}
	return version
}

func latestExistingAtlasMigrationVersion(outputDir string) int64 {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return 0
	}
	var latest int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		migrationFile, err := migrator.ParseAtlasMigrationFileName(entry.Name())
		if err != nil {
			continue
		}
		if migrationFile.Version > latest {
			latest = migrationFile.Version
		}
	}
	return latest
}

func atlasEmptyMigrationFileName(version int64, name string) string {
	name = atlasEmptyMigrationName(name)
	if name == "" {
		return fmt.Sprintf("%d.sql", version)
	}
	return fmt.Sprintf("%d_%s.sql", version, name)
}

func atlasEmptyMigrationName(name string) string {
	name = strings.TrimSpace(name)
	name = strings.ReplaceAll(name, " ", "-")
	var b strings.Builder
	for _, r := range name {
		if isAtlasMigrationNameChar(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

func isAtlasMigrationNameChar(r rune) bool {
	return r == '-' || r == '_' ||
		('0' <= r && r <= '9') ||
		('A' <= r && r <= 'Z') ||
		('a' <= r && r <= 'z')
}

func validateEmptyMigrationName(name string) error {
	if name == "" {
		return fmt.Errorf("migration name is required")
	}

	fileName := migrator.GenerateMigrationFileName(1, name, "up")
	if strings.HasPrefix(fileName, "0000000001_.") {
		return fmt.Errorf("migration name must contain letters, digits, or underscores")
	}

	return nil
}

func emptyMigrationSQL(name, generatedAt, direction string) string {
	return fmt.Sprintf(`-- Migration: %s
-- Generated on: %s
-- Direction: %s

-- Add your migration SQL here.
`, name, generatedAt, direction)
}

// GenerateMigration generates both up and down migration files by comparing
// the desired schema (from Go entities) with the current database state.
//
// The context bounds only the initial database connection attempt performed
// when opts.DBConn is nil (so a stuck host cannot block the call
// indefinitely). The schema-reading and migration-writing work below does not
// yet propagate the context; future work may thread it through there too.
// When opts.DBConn is supplied the context is currently unused.
func GenerateMigration(ctx context.Context, opts GenerateMigrationOptions) (*MigrationFiles, error) {
	opts, err := normalizeGenerateMigrationOptions(opts)
	if err != nil {
		return nil, err
	}

	var entitiesDir string

	if opts.GoEntitiesFS == nil {
		// Default to using the real filesystem
		// We need to set up the filesystem root and relative path correctly
		absPath, err := filepath.Abs(opts.GoEntitiesDir)
		if err != nil {
			return nil, fmt.Errorf("error resolving root directory path: %w", err)
		}

		// Use the parent directory as filesystem root and the basename as the path
		fsRoot := filepath.Dir(absPath)
		entitiesDir = filepath.Base(absPath)
		opts.GoEntitiesFS = os.DirFS(fsRoot)
	} else {
		// For custom filesystems, use the path as-is
		entitiesDir = opts.GoEntitiesDir
	}

	// 1. Parse Go entities to get desired schema
	generated, err := goschema.ParseFS(opts.GoEntitiesFS, entitiesDir)
	if err != nil {
		return nil, fmt.Errorf("error parsing Go entities: %w", err)
	}

	// 2. Connect to database and read current schema
	var conn *dbschema.DatabaseConnection

	if opts.DBConn != nil {
		conn = opts.DBConn
	} else {
		conn, err = dbschema.ConnectToDatabase(ctx, opts.DatabaseURL)
		if err != nil {
			return nil, fmt.Errorf("error connecting to database: %w", err)
		}
		defer dbschema.CloseAndWarn(conn)
	}

	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, opts.Schemas)
	if err != nil {
		return nil, fmt.Errorf("error reading database schema: %w", err)
	}

	// 3. Calculate the diff between desired and current schema.
	// Thread the connection dialect into the compare options so dialect-specific
	// normalization (e.g. MySQL/MariaDB RESTRICT == NO ACTION on foreign keys)
	// is applied; without it MariaDB would loop drop+add on an unchanged FK.
	compareOpts := withDialect(opts.CompareOptions, conn.Info().Dialect)
	diff := schemadiff.CompareWithOptions(generated, dbSchema, compareOpts)

	// Check if there are any changes
	if !diff.HasChanges() {
		// No changes detected - this is a successful no-op operation
		return nil, nil
	}

	// 4. Generate migration version (timestamp)
	version := migrator.GetNextMigrationVersion()
	version = nextAvailableMigrationVersion(opts.OutputDir, version, opts.MigrationName)
	slog.Debug("Generated migration version", "version", version)

	info := conn.Info()
	specs, assessments, err := planGeneratedMigrationSpecs(diff, generated, dbSchema, info, version, opts.MigrationName)
	if err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, nil
	}
	if err := checkDestructiveAllowed(opts, assessments); err != nil {
		return nil, err
	}

	if opts.ShadowDatabaseURL != "" {
		if err := verifyShadowMigration(ctx, shadowMigrationOptions{
			DatabaseURL:   opts.ShadowDatabaseURL,
			MigrationsDir: opts.OutputDir,
			Dialect:       info.Dialect,
			Capabilities:  info.Capabilities,
			Candidates:    shadowCandidatesFromSpecs(specs),
			Generated:     generated,
			CompareOpts:   compareOpts,
			Schemas:       opts.Schemas,
		}); err != nil {
			return nil, err
		}
	}

	// 7. Create migration files
	files, err := createMigrationFilesFromSpecs(opts.OutputDir, opts.ReportFormat, specs)
	if err != nil {
		return nil, fmt.Errorf("error creating migration files: %w", err)
	}

	return files, nil
}

func normalizeGenerateMigrationOptions(opts GenerateMigrationOptions) (GenerateMigrationOptions, error) {
	if opts.MigrationName == "" {
		opts.MigrationName = "migration"
	}
	outputDir, err := pathguard.ResolveWithinRoot(opts.OutputDir, opts.AllowedOutputRoot)
	if err != nil {
		return opts, fmt.Errorf("error validating output directory: %w", err)
	}
	opts.OutputDir = outputDir
	return opts, nil
}

// withDialect returns a copy of opts with the Dialect set, allocating a default
// options value when opts is nil. An explicit Dialect already present on opts is
// preserved. The comparator consults Dialect only for dialect-specific
// referential-action normalization (see config.CompareOptions.Dialect).
func withDialect(opts *config.CompareOptions, dialect string) *config.CompareOptions {
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}
	clone := *opts
	if clone.Dialect == "" {
		clone.Dialect = dialect
	}
	return &clone
}

func checkDestructiveAllowed(opts GenerateMigrationOptions, assessments []safety.StatementAssessment) error {
	if opts.CheckDestructive && safety.HasDestructiveAssessment(assessments) && !opts.AllowDestructive {
		return fmt.Errorf("destructive migration statements require AllowDestructive")
	}
	return nil
}

type generatedMigrationSpec struct {
	Version       int64
	Name          string
	UpSQL         string
	DownSQL       string
	Assessments   []safety.StatementAssessment
	NoTransaction bool
}

func planGeneratedMigrationSpecs(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
	version int64,
	migrationName string,
) ([]generatedMigrationSpec, []safety.StatementAssessment, error) {
	concurrentIndexNames := concurrentIndexNamesForPopulatedTables(diff, generated, dbSchema, info)
	plannerOpts := planner.Options{
		Capabilities:         info.Capabilities,
		ConcurrentIndexNames: concurrentIndexNames,
	}
	upNodes, err := planner.GenerateSchemaDiffASTWithOptions(diff, generated, info.Dialect, plannerOpts)
	if err != nil {
		return nil, nil, fmt.Errorf("error generating up migration plan: %w", err)
	}
	if len(upNodes) == 0 {
		return nil, nil, nil
	}
	requiresNoTransaction := planner.RequiresNoTransaction(info.Dialect, upNodes)
	if !requiresNoTransaction {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:         diff,
			Generated:    generated,
			DBSchema:     dbSchema,
			Dialect:      info.Dialect,
			Capabilities: info.Capabilities,
			Version:      version,
			Name:         migrationName,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return []generatedMigrationSpec{spec}, assessments, nil
	}

	nodeGroups := splitNoTransactionNodes(info.Dialect, upNodes)
	if len(nodeGroups.transactional) == 0 {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:                 diff,
			Generated:            generated,
			DBSchema:             dbSchema,
			Dialect:              info.Dialect,
			Capabilities:         info.Capabilities,
			Version:              version,
			Name:                 migrationName,
			ConcurrentIndexNames: concurrentIndexNames,
			NoTransaction:        true,
		})
		if err != nil || spec.UpSQL == "" {
			return nil, assessments, err
		}
		return []generatedMigrationSpec{spec}, assessments, nil
	}
	if !allNoTransactionNodesAreConcurrentIndexes(nodeGroups.noTransaction) {
		return nil, nil, fmt.Errorf("generated migration mixes transactional statements with non-transactional statements that cannot be split automatically")
	}

	diffGroups := splitConcurrentIndexDiff(diff, concurrentIndexNames)
	specs := make([]generatedMigrationSpec, 0, 2)
	allAssessments := make([]safety.StatementAssessment, 0)
	if diffGroups.transactional.HasChanges() {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:         diffGroups.transactional,
			Generated:    generated,
			DBSchema:     dbSchema,
			Dialect:      info.Dialect,
			Capabilities: info.Capabilities,
			Version:      version,
			Name:         migrationName + "_transactional",
		})
		if err != nil {
			return nil, nil, err
		}
		if spec.UpSQL != "" {
			specs = append(specs, spec)
			allAssessments = append(allAssessments, assessments...)
			version++
		}
	}
	if diffGroups.noTransaction.HasChanges() {
		spec, assessments, err := buildGeneratedMigrationSpec(generatedMigrationSpecOptions{
			Diff:                 diffGroups.noTransaction,
			Generated:            generated,
			DBSchema:             dbSchema,
			Dialect:              info.Dialect,
			Capabilities:         info.Capabilities,
			Version:              version,
			Name:                 migrationName + "_concurrent_indexes",
			ConcurrentIndexNames: concurrentIndexNames,
			NoTransaction:        true,
		})
		if err != nil {
			return nil, nil, err
		}
		if spec.UpSQL != "" {
			specs = append(specs, spec)
			allAssessments = append(allAssessments, assessments...)
		}
	}
	return specs, allAssessments, nil
}

type generatedMigrationSpecOptions struct {
	Diff                 *types.SchemaDiff
	Generated            *goschema.Database
	DBSchema             *dbschematypes.DBSchema
	Dialect              string
	Capabilities         capability.Capabilities
	Version              int64
	Name                 string
	ConcurrentIndexNames []string
	NoTransaction        bool
}

func buildGeneratedMigrationSpec(opts generatedMigrationSpecOptions) (generatedMigrationSpec, []safety.StatementAssessment, error) {
	plannerOpts := planner.Options{
		Capabilities:         opts.Capabilities,
		ConcurrentIndexNames: opts.ConcurrentIndexNames,
	}
	upNodes, err := planner.GenerateSchemaDiffASTWithOptions(opts.Diff, opts.Generated, opts.Dialect, plannerOpts)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating up migration plan: %w", err)
	}
	assessments, err := safety.AssessRenderedWithCapabilities(upNodes, opts.Dialect, opts.Capabilities)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error assessing migration safety: %w", err)
	}
	directiveOpts := generatedDirectiveOptions{skipTimeouts: opts.NoTransaction}
	upSQL, err := renderGeneratedMigrationSQL(upNodes, opts.Dialect, opts.Capabilities, "UP", directiveOpts)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating up migration SQL: %w", err)
	}
	if upSQL == "" {
		return generatedMigrationSpec{}, assessments, nil
	}
	if opts.NoTransaction {
		upSQL = withNoTransactionDirective(upSQL)
	}

	downSQL, err := generateDownMigrationSQLWithOptions(opts.Diff, opts.Generated, opts.DBSchema, opts.Dialect, directiveOpts, opts.Capabilities)
	if err != nil {
		return generatedMigrationSpec{}, nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if opts.NoTransaction {
		downSQL = withNoTransactionDirective(downSQL)
	}

	return generatedMigrationSpec{
		Version:       opts.Version,
		Name:          opts.Name,
		UpSQL:         upSQL,
		DownSQL:       downSQL,
		Assessments:   assessments,
		NoTransaction: opts.NoTransaction,
	}, assessments, nil
}

func renderGeneratedMigrationSQL(
	nodes []ast.Node,
	dialect string,
	caps capability.Capabilities,
	direction string,
	directiveOpts generatedDirectiveOptions,
) (string, error) {
	rawSQL, err := renderer.RenderSQLWithCapabilities(dialect, caps, nodes...)
	if err != nil {
		return "", err
	}
	statements := sqlutil.SplitSQLStatements(rawSQL)
	if len(statements) == 0 || !hasActualSQLStatements(statements) {
		return "", nil
	}
	header := fmt.Sprintf("-- Migration generated from schema differences\n-- Generated on: %s\n-- Direction: %s\n\n",
		time.Now().Format(time.RFC3339), direction)
	return withGeneratedTimeoutDirectivesForOptions(header+strings.Join(statements, ";\n")+";", dialect, directiveOpts), nil
}

type splitMigrationNodes struct {
	transactional []ast.Node
	noTransaction []ast.Node
}

func splitNoTransactionNodes(dialect string, nodes []ast.Node) splitMigrationNodes {
	txNodes := make([]ast.Node, 0, len(nodes))
	noTxNodes := make([]ast.Node, 0)
	for _, node := range nodes {
		if planner.NodeRequiresNoTransaction(dialect, node) {
			noTxNodes = append(noTxNodes, node)
			continue
		}
		txNodes = append(txNodes, node)
	}
	return splitMigrationNodes{transactional: txNodes, noTransaction: noTxNodes}
}

func allNoTransactionNodesAreConcurrentIndexes(nodes []ast.Node) bool {
	for _, node := range nodes {
		index, ok := node.(*ast.IndexNode)
		if !ok || !index.Concurrently {
			return false
		}
	}
	return true
}

func concurrentIndexNamesForPopulatedTables(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	info dbschematypes.DBInfo,
) []string {
	if !platform.IsPostgresFamily(info.Dialect) || !info.Capabilities.Has(capability.CreateIndexConcurrently) {
		return nil
	}
	added := stringSet(diff.IndexesAdded)
	populatedTables := populatedTableSet(dbSchema)
	structToTable := generatedStructTableMap(generated)
	var names []string
	for _, index := range generated.Indexes {
		if _, ok := added[index.Name]; !ok {
			continue
		}
		tableName := resolveGeneratedIndexTable(index, structToTable)
		if _, ok := populatedTables[tableName]; ok {
			names = append(names, index.Name)
		}
	}
	slices.Sort(names)
	return names
}

func populatedTableSet(dbSchema *dbschematypes.DBSchema) map[string]struct{} {
	out := make(map[string]struct{})
	if dbSchema == nil {
		return out
	}
	for _, table := range dbSchema.Tables {
		if table.EstimatedRows <= 0 {
			continue
		}
		out[table.QualifiedName()] = struct{}{}
		if table.Schema != "" {
			out[table.Name] = struct{}{}
		}
	}
	return out
}

func generatedStructTableMap(generated *goschema.Database) map[string]string {
	out := make(map[string]string, len(generated.Tables))
	for _, table := range generated.Tables {
		out[table.StructName] = table.QualifiedName()
	}
	return out
}

func resolveGeneratedIndexTable(index goschema.Index, structToTable map[string]string) string {
	if strings.TrimSpace(index.TableName) != "" {
		return index.TableName
	}
	return structToTable[index.StructName]
}

type splitSchemaDiffs struct {
	transactional *types.SchemaDiff
	noTransaction *types.SchemaDiff
}

func splitConcurrentIndexDiff(diff *types.SchemaDiff, concurrentIndexNames []string) splitSchemaDiffs {
	concurrent := stringSet(concurrentIndexNames)
	txDiff := cloneSchemaDiff(diff)
	noTxDiff := emptySchemaDiff()
	txDiff.IndexesAdded = txDiff.IndexesAdded[:0]
	for _, indexName := range diff.IndexesAdded {
		if _, ok := concurrent[indexName]; ok {
			noTxDiff.IndexesAdded = append(noTxDiff.IndexesAdded, indexName)
			continue
		}
		txDiff.IndexesAdded = append(txDiff.IndexesAdded, indexName)
	}
	return splitSchemaDiffs{transactional: txDiff, noTransaction: noTxDiff}
}

func stringSet(values []string) map[string]struct{} {
	out := make(map[string]struct{}, len(values))
	for _, value := range values {
		out[value] = struct{}{}
	}
	return out
}

func emptySchemaDiff() *types.SchemaDiff {
	return &types.SchemaDiff{}
}

func cloneSchemaDiff(diff *types.SchemaDiff) *types.SchemaDiff {
	clone := *diff
	clone.TablesAdded = slices.Clone(diff.TablesAdded)
	clone.TablesRemoved = slices.Clone(diff.TablesRemoved)
	clone.TablesModified = slices.Clone(diff.TablesModified)
	clone.EnumsAdded = slices.Clone(diff.EnumsAdded)
	clone.EnumsRemoved = slices.Clone(diff.EnumsRemoved)
	clone.EnumsModified = slices.Clone(diff.EnumsModified)
	clone.IndexesAdded = slices.Clone(diff.IndexesAdded)
	clone.IndexesRemoved = slices.Clone(diff.IndexesRemoved)
	clone.IndexesRemovedWithTables = slices.Clone(diff.IndexesRemovedWithTables)
	clone.ExtensionsAdded = slices.Clone(diff.ExtensionsAdded)
	clone.ExtensionsRemoved = slices.Clone(diff.ExtensionsRemoved)
	clone.FunctionsAdded = slices.Clone(diff.FunctionsAdded)
	clone.FunctionsRemoved = slices.Clone(diff.FunctionsRemoved)
	clone.FunctionsModified = slices.Clone(diff.FunctionsModified)
	clone.SequencesAdded = slices.Clone(diff.SequencesAdded)
	clone.SequencesRemoved = slices.Clone(diff.SequencesRemoved)
	clone.SequencesModified = slices.Clone(diff.SequencesModified)
	clone.ViewsAdded = slices.Clone(diff.ViewsAdded)
	clone.ViewsRemoved = slices.Clone(diff.ViewsRemoved)
	clone.ViewsModified = slices.Clone(diff.ViewsModified)
	clone.MaterializedViewsAdded = slices.Clone(diff.MaterializedViewsAdded)
	clone.MaterializedViewsRemoved = slices.Clone(diff.MaterializedViewsRemoved)
	clone.MaterializedViewsModified = slices.Clone(diff.MaterializedViewsModified)
	clone.TriggersAdded = slices.Clone(diff.TriggersAdded)
	clone.TriggersRemoved = slices.Clone(diff.TriggersRemoved)
	clone.TriggersModified = slices.Clone(diff.TriggersModified)
	clone.RLSPoliciesAdded = slices.Clone(diff.RLSPoliciesAdded)
	clone.RLSPoliciesRemoved = slices.Clone(diff.RLSPoliciesRemoved)
	clone.RLSPoliciesModified = slices.Clone(diff.RLSPoliciesModified)
	clone.RLSEnabledTablesAdded = slices.Clone(diff.RLSEnabledTablesAdded)
	clone.RLSEnabledTablesRemoved = slices.Clone(diff.RLSEnabledTablesRemoved)
	clone.RolesAdded = slices.Clone(diff.RolesAdded)
	clone.RolesRemoved = slices.Clone(diff.RolesRemoved)
	clone.RolesModified = slices.Clone(diff.RolesModified)
	clone.GrantsAdded = slices.Clone(diff.GrantsAdded)
	clone.GrantsRemoved = slices.Clone(diff.GrantsRemoved)
	clone.GrantOptionsAdded = slices.Clone(diff.GrantOptionsAdded)
	clone.GrantOptionsRevoked = slices.Clone(diff.GrantOptionsRevoked)
	clone.ConstraintsAdded = slices.Clone(diff.ConstraintsAdded)
	clone.ConstraintsAddedWithTables = slices.Clone(diff.ConstraintsAddedWithTables)
	clone.ConstraintsRemoved = slices.Clone(diff.ConstraintsRemoved)
	clone.ConstraintsRemovedWithTables = slices.Clone(diff.ConstraintsRemovedWithTables)
	return &clone
}

func createSafetyReportFile(upFile, format string, assessments []safety.StatementAssessment) (string, error) {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "html":
		reportFile := strings.TrimSuffix(upFile, ".up.sql") + ".safety.html"
		return writeSafetyReportFile(reportFile, func(file *os.File) error {
			return safety.RenderHTML(file, assessments)
		})
	case "json":
		reportFile := strings.TrimSuffix(upFile, ".up.sql") + ".safety.json"
		return writeSafetyReportFile(reportFile, func(file *os.File) error {
			return safety.RenderJSON(file, assessments)
		})
	default:
		return "", fmt.Errorf("unsupported safety report format %q", format)
	}
}

func writeSafetyReportFile(reportFile string, render func(*os.File) error) (string, error) {
	file, err := os.Create(reportFile)
	if err != nil {
		return "", err
	}
	defer func() {
		if closeErr := file.Close(); closeErr != nil {
			slog.Warn("failed to close safety report", "path", reportFile, "error", closeErr)
		}
	}()
	if err := render(file); err != nil {
		return "", err
	}
	return reportFile, nil
}

// hasActualSQLStatements checks if the statements contain actual SQL operations (not just comments)
func hasActualSQLStatements(statements []string) bool {
	for _, stmt := range statements {
		// Strip comments and check if there's any actual SQL content
		stripped := strings.TrimSpace(sqlutil.StripComments(stmt))
		if stripped != "" {
			return true
		}
	}
	return false
}

// generateUpMigrationSQL generates the SQL for the up migration.
func generateUpMigrationSQL(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateUpMigrationSQLWithOptions(diff, generated, dialect, generatedDirectiveOptions{}, capsOverride...)
}

type generatedDirectiveOptions struct {
	skipTimeouts bool
}

func generateUpMigrationSQLWithOptions(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	caps := capability.ForDialect(dialect)
	if len(capsOverride) > 0 {
		caps = capsOverride[0]
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(diff, generated, dialect, caps)
	if err != nil {
		return "", fmt.Errorf("error generating up migration SQL: %w", err)
	}

	if len(statements) == 0 || !hasActualSQLStatements(statements) {
		// No actual SQL statements generated - this is a successful no-op operation
		return "", nil
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration generated from schema differences\n-- Generated on: %s\n-- Direction: UP\n\n",
		time.Now().Format(time.RFC3339))

	return withGeneratedTimeoutDirectivesForOptions(header+strings.Join(statements, ";\n")+";", dialect, directiveOpts), nil
}

// generateDownMigrationSQL generates the SQL for the down migration by reversing the diff.
func generateDownMigrationSQL(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateDownMigrationSQLWithOptions(diff, generated, dbSchema, dialect, generatedDirectiveOptions{}, capsOverride...)
}

func generateDownMigrationSQLWithOptions(
	diff *types.SchemaDiff,
	generated *goschema.Database,
	dbSchema *dbschematypes.DBSchema,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	// For down migrations, we need to use the current database schema as the "generated" schema
	// since we're reverting back to the current state
	dbAsGoSchema := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	// Create a reverse diff to generate down migration. We pass the original
	// generated schema to resolve table names for RLS policies, and the
	// introspected database schema so the reversed constraint additions can
	// rebuild the full prior body from the pre-change DB state — that is exactly
	// the definition the down must restore.
	reverseDiff := reverseSchemaDiffWithSchema(diff, generated, dbSchema)
	addMySQLFamilyForeignKeyBackingIndexRemovals(reverseDiff, diff, dbSchema, dialect)

	caps := capability.ForDialect(dialect)
	if len(capsOverride) > 0 {
		caps = capsOverride[0]
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(reverseDiff, dbAsGoSchema, dialect, caps)
	if err != nil {
		return "", fmt.Errorf("error generating down migration SQL: %w", err)
	}

	if len(statements) == 0 {
		// If no statements generated, create a simple comment
		header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n-- No rollback operations needed\n",
			time.Now().Format(time.RFC3339))
		return header, nil
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n",
		time.Now().Format(time.RFC3339))

	return withGeneratedTimeoutDirectivesForOptions(header+strings.Join(statements, ";\n")+";", dialect, directiveOpts), nil
}

func withGeneratedTimeoutDirectivesForOptions(sql, dialect string, opts generatedDirectiveOptions) string {
	if opts.skipTimeouts {
		return sql
	}
	return withGeneratedTimeoutDirectives(sql, dialect)
}

func withGeneratedTimeoutDirectives(sql, dialect string) string {
	if !containsAlterTable(sql) || !supportsGeneratedTimeoutDirectives(dialect) {
		return sql
	}

	directives := "-- +ptah lock_timeout=3s\n-- +ptah statement_timeout=30s\n"
	separator := "\n\n"
	if before, after, ok := strings.Cut(sql, separator); ok {
		return before + "\n" + directives + "\n" + after
	}
	return directives + sql
}

func containsAlterTable(sql string) bool {
	stripped := sqlutil.StripComments(sql)
	return strings.Contains(strings.ToUpper(stripped), "ALTER TABLE")
}

func supportsGeneratedTimeoutDirectives(dialect string) bool {
	normalized := platform.NormalizeDialect(dialect)
	return slices.Contains([]string{platform.Postgres, platform.MySQL, platform.MariaDB}, normalized)
}

func addMySQLFamilyForeignKeyBackingIndexRemovals(
	reverseDiff *types.SchemaDiff,
	upDiff *types.SchemaDiff,
	dbSchema *dbschematypes.DBSchema,
	dialect string,
) {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
	default:
		return
	}

	priorIndexes := dbIndexByTableName(dbSchema)
	removedNames := stringSet(reverseDiff.IndexesRemoved)
	removedWithTables := indexRemovalSet(reverseDiff.IndexesRemovedWithTables)
	for _, add := range upDiff.ConstraintsAddedWithTables {
		if add.TableName == "" || add.Name == "" || !strings.EqualFold(add.Type, "FOREIGN KEY") {
			continue
		}
		if priorIndexes[indexKey(add.TableName, add.Name)] {
			continue
		}
		if _, ok := removedNames[add.Name]; !ok {
			reverseDiff.IndexesRemoved = append(reverseDiff.IndexesRemoved, add.Name)
			removedNames[add.Name] = struct{}{}
		}
		key := indexKey(add.TableName, add.Name)
		if removedWithTables[key] {
			continue
		}
		reverseDiff.IndexesRemovedWithTables = append(reverseDiff.IndexesRemovedWithTables, types.IndexRemovalInfo{
			Name:      add.Name,
			TableName: add.TableName,
		})
		removedWithTables[key] = true
	}

	slices.Sort(reverseDiff.IndexesRemoved)
	slices.SortFunc(reverseDiff.IndexesRemovedWithTables, func(a, b types.IndexRemovalInfo) int {
		if byTable := strings.Compare(a.TableName, b.TableName); byTable != 0 {
			return byTable
		}
		return strings.Compare(a.Name, b.Name)
	})
}

func dbIndexByTableName(dbSchema *dbschematypes.DBSchema) map[string]bool {
	out := make(map[string]bool)
	if dbSchema == nil {
		return out
	}
	for _, index := range dbSchema.Indexes {
		out[indexKey(index.TableName, index.Name)] = true
		if index.Schema != "" {
			out[indexKey(dbschematypes.QualifyTableName(index.Schema, index.TableName), index.Name)] = true
		}
	}
	return out
}

func indexRemovalSet(indexes []types.IndexRemovalInfo) map[string]bool {
	out := make(map[string]bool, len(indexes))
	for _, index := range indexes {
		out[indexKey(index.TableName, index.Name)] = true
	}
	return out
}

func indexKey(tableName, indexName string) string {
	return tableName + "." + indexName
}

// reverseSchemaDiff creates a reverse diff for generating down migrations
//
// Deprecated: Use reverseSchemaDiffWithSchema for proper RLS policy table name resolution
func reverseSchemaDiff(diff *types.SchemaDiff) *types.SchemaDiff {
	return reverseSchemaDiffWithSchema(diff, nil, nil)
}

// reverseSchemaDiffWithSchema creates a reverse diff for generating down migrations with schema context.
//
// schema is the generated (target) Go schema, used to resolve table names for
// RLS policies. dbSchema is the introspected (pre-change) database schema, used
// to rebuild prior FK/PK/CHECK/UNIQUE definitions for reversed constraint
// additions; it may be nil when callers only have the generated schema (the
// reversed additions then fall back to the name-only path).
func reverseSchemaDiffWithSchema(diff *types.SchemaDiff, schema *goschema.Database, dbSchema *dbschematypes.DBSchema) *types.SchemaDiff {
	return &types.SchemaDiff{
		// Reverse table operations
		TablesAdded:    diff.TablesRemoved,                                // Tables to remove become tables to add
		TablesRemoved:  deporder.TableDropOrder(diff.TablesAdded, schema), // Tables to add become tables to remove
		TablesModified: reverseTableDiffs(diff.TablesModified),

		// Reverse enum operations
		EnumsAdded:    diff.EnumsRemoved, // Enums to remove become enums to add
		EnumsRemoved:  diff.EnumsAdded,   // Enums to add become enums to remove
		EnumsModified: reverseEnumDiffs(diff.EnumsModified),

		// Reverse index operations
		IndexesAdded:   diff.IndexesRemoved, // Indexes to remove become indexes to add
		IndexesRemoved: diff.IndexesAdded,   // Indexes to add become indexes to remove

		// Reverse extension operations
		ExtensionsAdded:   diff.ExtensionsRemoved, // Extensions to remove become extensions to add
		ExtensionsRemoved: diff.ExtensionsAdded,   // Extensions to add become extensions to remove

		// Reverse function operations
		FunctionsAdded:    diff.FunctionsRemoved, // Functions to remove become functions to add
		FunctionsRemoved:  diff.FunctionsAdded,   // Functions to add become functions to remove
		FunctionsModified: reverseFunctionDiffs(diff.FunctionsModified),

		// Reverse sequence operations
		SequencesAdded:    diff.SequencesRemoved, // Sequences to remove become sequences to add
		SequencesRemoved:  diff.SequencesAdded,   // Sequences to add become sequences to remove
		SequencesModified: reverseSequenceDiffs(diff.SequencesModified),

		// Reverse RLS policy operations
		RLSPoliciesAdded:    convertRLSPolicyRefsToNames(diff.RLSPoliciesRemoved),                 // Policies to remove become policies to add (convert RLSPolicyRef to string)
		RLSPoliciesRemoved:  convertRLSPolicyNamesToRefsWithSchema(diff.RLSPoliciesAdded, schema), // Policies to add become policies to remove (convert string to RLSPolicyRef with table resolution)
		RLSPoliciesModified: reverseRLSPolicyDiffs(diff.RLSPoliciesModified),

		// Reverse RLS table enablement operations
		RLSEnabledTablesAdded:   diff.RLSEnabledTablesRemoved, // Tables to disable RLS become tables to enable RLS
		RLSEnabledTablesRemoved: diff.RLSEnabledTablesAdded,   // Tables to enable RLS become tables to disable RLS

		// Reverse role operations
		RolesAdded:          diff.RolesRemoved, // Roles to remove become roles to add
		RolesRemoved:        diff.RolesAdded,   // Roles to add become roles to remove
		RolesModified:       reverseRoleDiffs(diff.RolesModified),
		GrantsAdded:         diff.GrantsRemoved,       // Grants to remove become grants to add
		GrantsRemoved:       diff.GrantsAdded,         // Grants to add become grants to revoke
		GrantOptionsAdded:   diff.GrantOptionsRevoked, // Revoked grant options become grant-option additions
		GrantOptionsRevoked: diff.GrantOptionsAdded,   // Grant-option additions become grant-option revocations

		// Reverse constraint operations. A modified constraint is expressed by
		// the comparator as remove + add of the SAME name (e.g. an on_delete
		// change on a field-level FK, issue #189). Swapping the two slices makes
		// the down migration drop the new definition and re-add the old one.
		// reverseConstraintAdditions restores the prior table-qualified body
		// from the introspected schema for the constraint types whose down
		// add-path needs more than a name.
		//
		// ConstraintsAddedWithTables carries the table-qualified prior body so
		// the down add-path can fan a shared constraint name out to every real
		// host table. Without it the down add-path falls back to name-only
		// resolution, which can emit one ADD for a single host while per-host
		// DROP also resolves only one host; the 2nd host's re-add then collides
		// with its still-present old constraint (Postgres 42710, MySQL 1826)
		// and the rollback aborts half-applied.
		ConstraintsAdded:             diff.ConstraintsRemoved,
		ConstraintsRemoved:           diff.ConstraintsAdded,
		ConstraintsRemovedWithTables: reverseConstraintRemovals(diff, schema),
		ConstraintsAddedWithTables:   reverseConstraintAdditions(diff, dbSchema),
	}
}

func generatedTableByStructName(tables []goschema.Table, structName string) *goschema.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func generatedTableReference(tables []goschema.Table, structName, tableName string) *goschema.Table {
	tableName = strings.TrimSpace(tableName)
	for i := range tables {
		table := &tables[i]
		if tableName == "" && table.StructName == structName {
			return table
		}
		if tableName != "" && table.StructName == structName && (table.Name == tableName || table.QualifiedName() == tableName) {
			return table
		}
	}
	if tableName == "" {
		return nil
	}
	if strings.Contains(tableName, ".") {
		for i := range tables {
			if tables[i].QualifiedName() == tableName {
				return &tables[i]
			}
		}
		return nil
	}

	var match *goschema.Table
	for i := range tables {
		if tables[i].Name != tableName {
			continue
		}
		if match != nil {
			return nil
		}
		match = &tables[i]
	}
	return match
}

// reverseConstraintAdditions builds the table-qualified additions for the down
// migration. In the down direction the constraints to add back are the ones the
// up migration REMOVED (diff.ConstraintsRemovedWithTables) — restoring their
// prior definition. The prior body is read from the introspected (pre-change)
// database schema, which is the authoritative source for what the down must
// restore.
//
// Carrying the full per-host body here lets both dialect planners' add-paths
// (which already prefer ConstraintsAddedWithTables) emit one correct ALTER TABLE
// per real host table. This is what makes the down of a multi-host mixin FK
// modify apply cleanly: a name-only down re-adds only one host (and drops only
// one host), so the others collide on re-add (issue #197 DOWN path). When
// dbSchema is nil, the names still flow through ConstraintsAdded and the
// planners fall back to the name-only field scan.
func reverseConstraintAdditions(diff *types.SchemaDiff, dbSchema *dbschematypes.DBSchema) []types.ConstraintAdditionInfo {
	if dbSchema == nil || len(diff.ConstraintsRemovedWithTables) == 0 {
		return nil
	}

	// Index the introspected constraints by (table, name) so each reversed
	// addition restores the body from the exact host it was removed from. A
	// mixin-shared FK name legitimately repeats across host tables, so a
	// name-only key would collapse them onto one host.
	dbConstraintByTableName := make(map[string]dbschematypes.DBConstraint)
	for _, c := range dbSchema.Constraints {
		if c.Type != "FOREIGN KEY" && c.Type != "PRIMARY KEY" && c.Type != "CHECK" && c.Type != "UNIQUE" {
			continue
		}
		dbConstraintByTableName[c.QualifiedTableName()+"."+c.Name] = c
	}

	var infos []types.ConstraintAdditionInfo
	for _, removed := range diff.ConstraintsRemovedWithTables {
		if removed.TableName == "" {
			continue
		}
		dbConstraint, ok := dbConstraintByTableName[removed.TableName+"."+removed.Name]
		if !ok {
			// No introspected body to restore (e.g. the constraint was a
			// pure-removal not present pre-change, or a type this helper does not
			// reconstruct). The name still rides in ConstraintsAdded for the
			// name-only fallback.
			continue
		}
		switch removed.Type {
		case "FOREIGN KEY":
			infos = append(infos, foreignKeyAdditionFromDBConstraint(removed.Name, removed.TableName, dbConstraint))
		case "PRIMARY KEY":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, types.ConstraintAdditionInfo{
					Name:      removed.Name,
					TableName: removed.TableName,
					Type:      "PRIMARY KEY",
					Columns:   append([]string(nil), columns...),
				})
			}
		case "CHECK":
			if dbConstraint.CheckClause != nil && *dbConstraint.CheckClause != "" {
				infos = append(infos, types.ConstraintAdditionInfo{
					Name:            removed.Name,
					TableName:       removed.TableName,
					Type:            "CHECK",
					CheckExpression: *dbConstraint.CheckClause,
				})
			}
		case "UNIQUE":
			if columns := dbConstraint.ColumnNamesOrDefault(); len(columns) > 0 {
				infos = append(infos, types.ConstraintAdditionInfo{
					Name:           removed.Name,
					TableName:      removed.TableName,
					Type:           "UNIQUE",
					Columns:        append([]string(nil), columns...),
					IncludeColumns: append([]string(nil), dbConstraint.IncludeColumns...),
					NullsDistinct:  cloneBoolPtr(dbConstraint.NullsDistinct),
				})
			}
		}
	}
	return infos
}

func cloneBoolPtr(value *bool) *bool {
	if value == nil {
		return nil
	}
	cloned := *value
	return &cloned
}

// foreignKeyAdditionFromDBConstraint builds a ConstraintAdditionInfo carrying the
// full FK body from an introspected database FOREIGN KEY constraint. The
// referential actions come straight from the pre-change DB, so the down
// migration restores exactly the prior ON DELETE / ON UPDATE behavior.
func foreignKeyAdditionFromDBConstraint(name, table string, dbFK dbschematypes.DBConstraint) types.ConstraintAdditionInfo {
	info := types.ConstraintAdditionInfo{
		Name:      name,
		TableName: table,
		Type:      "FOREIGN KEY",
		OnDelete:  derefString(dbFK.DeleteRule),
		OnUpdate:  derefString(dbFK.UpdateRule),
	}
	if columns := dbFK.ColumnNamesOrDefault(); len(columns) > 0 {
		info.Columns = uniqueStringsPreserveOrder(columns)
	}
	if dbFK.ForeignTable != nil {
		info.ForeignTable = *dbFK.ForeignTable
	}
	if foreignColumns := dbFK.ForeignColumnsOrDefault(); len(foreignColumns) > 0 {
		foreignColumns = uniqueStringsPreserveOrder(foreignColumns)
		info.ForeignColumn = foreignColumns[0]
		info.ForeignColumns = foreignColumns
	}
	return info
}

func uniqueStringsPreserveOrder(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}

// derefString returns the pointed-to string or "" when nil.
func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// reverseConstraintRemovals builds the table-qualified removal info for the
// down migration. In the down direction the constraints to remove are the ones
// the up migration ADDED (diff.ConstraintsAdded); their owning table and type
// are resolved from the generated schema, which is the source the up side
// synthesized them from. This lets dialect planners that need the table and a
// type-specific drop syntax (MySQL/MariaDB DROP FOREIGN KEY) emit a real drop in
// the down migration. When the schema is unavailable, the names still flow
// through ConstraintsRemoved; only the richer per-table info is omitted.
func reverseConstraintRemovals(diff *types.SchemaDiff, schema *goschema.Database) []types.ConstraintRemovalInfo {
	if schema == nil {
		return nil
	}

	// Index explicit table-level constraints by name.
	tableConstraints := make(map[string]goschema.Constraint, len(schema.Constraints))
	for _, c := range schema.Constraints {
		tableConstraints[c.Name] = c
	}

	// Prefer the table-qualified additions the comparator recorded. A
	// field-level FK contributed by an embedded inline-relation mixin shares one
	// name across every host table, so resolving the table from a field's Go
	// struct name collapses every host onto the same (often non-table) name —
	// the down migration would then drop the constraint from the wrong table or
	// from a struct name that does not exist (issue #197). ConstraintsAddedWithTables
	// carries the concrete table for each addition, so the down side drops the
	// FK from exactly the table the up side added it to. Names present here are
	// recorded so the field-scan fallback below does not double-emit them.
	var infos []types.ConstraintRemovalInfo
	seen := make(map[string]struct{})
	handled := make(map[string]struct{})
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			continue
		}
		infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{
			Name:      add.Name,
			TableName: add.TableName,
			Type:      add.Type,
		})
		handled[add.Name] = struct{}{}
	}
	infos = appendAddedTableForeignKeyRemovals(infos, seen, diff.TablesAdded, schema)

	// Index field-level constraint names to their owning table for the names
	// that did not arrive with table-qualified info.
	structToTable := make(map[string]string, len(schema.Tables))
	for _, t := range schema.Tables {
		structToTable[t.StructName] = t.Name
	}
	fkTables := make(map[string]string, len(schema.Fields))
	checkTables := make(map[string]string, len(schema.Fields))
	for _, f := range schema.Fields {
		tableName := structToTable[f.StructName]
		if tableName == "" {
			tableName = f.StructName
		}

		if f.Foreign != "" {
			name := f.ForeignKeyName
			if name == "" {
				name = fromschema.GenerateForeignKeyName(tableName, f.Name)
			}
			fkTables[name] = tableName
		}

		if f.Check != "" {
			name := f.CheckName
			if name == "" {
				name = tableName + "_" + f.Name + "_check"
			}
			checkTables[name] = tableName
		}
	}

	for _, name := range diff.ConstraintsAdded {
		if _, done := handled[name]; done {
			continue
		}
		switch {
		case tableConstraints[name].Name != "":
			c := tableConstraints[name]
			infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{Name: name, TableName: c.Table, Type: c.Type})
		case fkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{Name: name, TableName: fkTables[name], Type: "FOREIGN KEY"})
		case checkTables[name] != "":
			infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{Name: name, TableName: checkTables[name], Type: "CHECK"})
		}
	}
	return infos
}

func appendAddedTableForeignKeyRemovals(
	infos []types.ConstraintRemovalInfo,
	seen map[string]struct{},
	tableNames []string,
	schema *goschema.Database,
) []types.ConstraintRemovalInfo {
	addedTables := make(map[string]struct{}, len(tableNames))
	for _, tableName := range tableNames {
		addedTables[tableName] = struct{}{}
	}
	if len(addedTables) == 0 {
		return infos
	}

	for _, field := range schema.Fields {
		if field.Foreign == "" {
			continue
		}
		table := generatedTableByStructName(schema.Tables, field.StructName)
		if table == nil || !generatedTableInSet(*table, addedTables) {
			continue
		}
		tableName := table.QualifiedName()
		name := field.ForeignKeyName
		if name == "" {
			name = fromschema.GenerateForeignKeyName(table.Name, field.Name)
		}
		infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{
			Name:      name,
			TableName: tableName,
			Type:      "FOREIGN KEY",
		})
	}

	for _, constraint := range schema.Constraints {
		if !strings.EqualFold(constraint.Type, "FOREIGN KEY") {
			continue
		}
		table := generatedTableReference(schema.Tables, constraint.StructName, constraint.Table)
		if table == nil || !generatedTableInSet(*table, addedTables) {
			continue
		}
		tableName := table.QualifiedName()
		if constraint.Table != "" {
			tableName = constraint.Table
		}
		name := constraint.Name
		if name == "" {
			name = defaultForeignKeyConstraintName(table.Name, constraint.Columns)
		}
		infos = appendConstraintRemovalInfo(infos, seen, types.ConstraintRemovalInfo{
			Name:      name,
			TableName: tableName,
			Type:      "FOREIGN KEY",
		})
	}

	return infos
}

func generatedTableInSet(table goschema.Table, tableNames map[string]struct{}) bool {
	_, byName := tableNames[table.Name]
	_, byQualifiedName := tableNames[table.QualifiedName()]
	return byName || byQualifiedName
}

func appendConstraintRemovalInfo(
	infos []types.ConstraintRemovalInfo,
	seen map[string]struct{},
	info types.ConstraintRemovalInfo,
) []types.ConstraintRemovalInfo {
	if info.Name == "" || info.TableName == "" {
		return infos
	}
	key := info.TableName + "." + info.Name
	if _, ok := seen[key]; ok {
		return infos
	}
	seen[key] = struct{}{}
	return append(infos, info)
}

func defaultForeignKeyConstraintName(tableName string, columns []string) string {
	columnName := strings.Join(columns, "_")
	if columnName == "" {
		columnName = "foreign_key"
	}
	return fromschema.GenerateForeignKeyName(tableName, columnName)
}

// reverseTableDiffs reverses table modifications for down migrations
func reverseTableDiffs(tableDiffs []types.TableDiff) []types.TableDiff {
	reversed := make([]types.TableDiff, len(tableDiffs))
	for i, tableDiff := range tableDiffs {
		reversed[i] = types.TableDiff{
			TableName:       tableDiff.TableName,
			ColumnsAdded:    tableDiff.ColumnsRemoved, // Columns to remove become columns to add
			ColumnsRemoved:  tableDiff.ColumnsAdded,   // Columns to add become columns to remove
			ColumnsModified: reverseColumnDiffs(tableDiff.ColumnsModified),
		}
	}
	return reversed
}

// reverseColumnDiffs reverses column modifications for down migrations
func reverseColumnDiffs(columnDiffs []types.ColumnDiff) []types.ColumnDiff {
	reversed := make([]types.ColumnDiff, len(columnDiffs))
	for i, columnDiff := range columnDiffs {
		// For column changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range columnDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = types.ColumnDiff{
			ColumnName: columnDiff.ColumnName,
			Changes:    reversedChanges,
		}
	}
	return reversed
}

// reverseEnumDiffs reverses enum modifications for down migrations
func reverseEnumDiffs(enumDiffs []types.EnumDiff) []types.EnumDiff {
	reversed := make([]types.EnumDiff, len(enumDiffs))
	for i, enumDiff := range enumDiffs {
		reversed[i] = types.EnumDiff{
			EnumName:      enumDiff.EnumName,
			ValuesAdded:   enumDiff.ValuesRemoved, // Values to remove become values to add
			ValuesRemoved: enumDiff.ValuesAdded,   // Values to add become values to remove
		}
	}
	return reversed
}

// reverseFunctionDiffs reverses function modifications for down migrations
func reverseFunctionDiffs(functionDiffs []types.FunctionDiff) []types.FunctionDiff {
	reversed := make([]types.FunctionDiff, len(functionDiffs))
	for i, functionDiff := range functionDiffs {
		// For function changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range functionDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = types.FunctionDiff{
			FunctionName: functionDiff.FunctionName,
			Changes:      reversedChanges,
		}
	}
	return reversed
}

// reverseSequenceDiffs reverses sequence modifications for down migrations.
func reverseSequenceDiffs(sequenceDiffs []types.SequenceDiff) []types.SequenceDiff {
	reversed := make([]types.SequenceDiff, len(sequenceDiffs))
	for i, sequenceDiff := range sequenceDiffs {
		reversedChanges := make(map[string]string)
		for key, change := range sequenceDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				reversedChanges[key] = change
			}
		}

		reversed[i] = types.SequenceDiff{
			SequenceName: sequenceDiff.SequenceName,
			Changes:      reversedChanges,
		}
	}
	return reversed
}

// convertRLSPolicyRefsToNames converts RLSPolicyRef slice to policy names for down migrations
func convertRLSPolicyRefsToNames(policyRefs []types.RLSPolicyRef) []string {
	names := make([]string, len(policyRefs))
	for i, policyRef := range policyRefs {
		names[i] = policyRef.PolicyName
	}
	return names
}

// convertRLSPolicyNamesToRefs converts policy names to RLSPolicyRef for down migrations
// This is needed because RLSPoliciesAdded contains policy names (strings) but
// RLSPoliciesRemoved needs RLSPolicyRef (with both policy name and table name)
//
// Deprecated: Use convertRLSPolicyNamesToRefsWithSchema for proper table name resolution
func convertRLSPolicyNamesToRefs(policyNames []string) []types.RLSPolicyRef {
	return convertRLSPolicyNamesToRefsWithSchema(policyNames, nil)
}

// convertRLSPolicyNamesToRefsWithSchema converts policy names to RLSPolicyRef for down migrations
// with proper table name resolution using the provided schema context
func convertRLSPolicyNamesToRefsWithSchema(policyNames []string, schema *goschema.Database) []types.RLSPolicyRef {
	refs := make([]types.RLSPolicyRef, len(policyNames))

	// Create a lookup map for policy name to table name if schema is provided
	policyToTable := make(map[string]string)
	if schema != nil {
		for _, policy := range schema.RLSPolicies {
			policyToTable[policy.Name] = policy.Table
		}
	}

	for i, policyName := range policyNames {
		tableName := ""
		if schema != nil {
			if table, found := policyToTable[policyName]; found {
				tableName = table
			}
		}

		refs[i] = types.RLSPolicyRef{
			PolicyName: policyName,
			TableName:  tableName,
		}
	}
	return refs
}

// reverseRLSPolicyDiffs reverses RLS policy modifications for down migrations
func reverseRLSPolicyDiffs(policyDiffs []types.RLSPolicyDiff) []types.RLSPolicyDiff {
	reversed := make([]types.RLSPolicyDiff, len(policyDiffs))
	for i, policyDiff := range policyDiffs {
		// For policy changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range policyDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = types.RLSPolicyDiff{
			PolicyName: policyDiff.PolicyName,
			TableName:  policyDiff.TableName,
			Changes:    reversedChanges,
		}
	}
	return reversed
}

// reverseRoleDiffs reverses role modifications for down migrations
func reverseRoleDiffs(roleDiffs []types.RoleDiff) []types.RoleDiff {
	reversed := make([]types.RoleDiff, len(roleDiffs))
	for i, roleDiff := range roleDiffs {
		// For role changes, we need to reverse the direction of changes
		reversedChanges := make(map[string]string)
		for key, change := range roleDiff.Changes {
			// Split "old -> new" and reverse to "new -> old"
			parts := strings.Split(change, " -> ")
			if len(parts) == 2 {
				reversedChanges[key] = parts[1] + " -> " + parts[0]
			} else {
				// If format is unexpected, keep as is
				reversedChanges[key] = change
			}
		}

		reversed[i] = types.RoleDiff{
			RoleName: roleDiff.RoleName,
			Changes:  reversedChanges,
		}
	}
	return reversed
}

func nextAvailableMigrationVersion(outputDir string, version int64, migrationName string) int64 {
	if latest := latestExistingMigrationVersion(outputDir); latest >= version {
		version = latest + 1
	}
	for {
		upFilePath := filepath.Join(outputDir, migrator.GenerateMigrationFileName(version, migrationName, "up"))
		downFilePath := filepath.Join(outputDir, migrator.GenerateMigrationFileName(version, migrationName, "down"))
		if !fileExists(upFilePath) && !fileExists(downFilePath) {
			return version
		}
		version++
	}
}

func latestExistingMigrationVersion(outputDir string) int64 {
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		return 0
	}
	var latest int64
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		migrationFile, err := migrator.ParseMigrationFileName(entry.Name())
		if err != nil {
			continue
		}
		if migrationFile.Version > latest {
			latest = migrationFile.Version
		}
	}
	return latest
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func withNoTransactionDirective(sql string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}
	if directive, ok := migrator.ParseFileDirectives(sql)[migrator.DirectiveNoTransaction]; ok && directive == "true" {
		return sql
	}
	return "-- +ptah " + migrator.DirectiveNoTransaction + "\n" + sql
}

// createMigrationFiles creates the up and down migration files
func createMigrationFiles(outputDir string, version int64, migrationName, upSQL, downSQL string) (*MigrationFiles, error) {
	if err := ensureMigrationOutputDir(outputDir); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	for {
		upFileName := migrator.GenerateMigrationFileName(version, migrationName, "up")
		downFileName := migrator.GenerateMigrationFileName(version, migrationName, "down")

		upFilePath := filepath.Join(outputDir, upFileName)
		downFilePath := filepath.Join(outputDir, downFileName)

		if err := writeNewMigrationFile(upFilePath, upSQL); err != nil {
			if errors.Is(err, os.ErrExist) {
				version++
				continue
			}
			return nil, fmt.Errorf("failed to write up migration file: %w", err)
		}

		if err := writeNewMigrationFile(downFilePath, downSQL); err != nil {
			_ = os.Remove(upFilePath)
			if errors.Is(err, os.ErrExist) {
				version++
				continue
			}
			return nil, fmt.Errorf("failed to write down migration file: %w", err)
		}

		pair := MigrationFilePair{
			UpFile:   upFilePath,
			DownFile: downFilePath,
			Version:  version,
		}
		return migrationFilesFromPairs([]MigrationFilePair{pair}), nil
	}
}

func createMigrationFilesFromSpecs(outputDir, reportFormat string, specs []generatedMigrationSpec) (*MigrationFiles, error) {
	pairs := make([]MigrationFilePair, 0, len(specs))
	cleanup := func() {
		for _, pair := range pairs {
			_ = os.Remove(pair.UpFile)
			_ = os.Remove(pair.DownFile)
			if pair.ReportFile != "" {
				_ = os.Remove(pair.ReportFile)
			}
		}
	}
	for _, spec := range specs {
		files, err := createMigrationFiles(outputDir, spec.Version, spec.Name, spec.UpSQL, spec.DownSQL)
		if err != nil {
			cleanup()
			return nil, err
		}
		pair := files.Files[0]
		pair.NoTransaction = spec.NoTransaction
		if reportFormat != "" {
			reportFile, err := createSafetyReportFile(pair.UpFile, reportFormat, spec.Assessments)
			if err != nil {
				pairs = append(pairs, pair)
				cleanup()
				return nil, fmt.Errorf("error creating safety report: %w", err)
			}
			pair.ReportFile = reportFile
		}
		pairs = append(pairs, pair)
	}
	return migrationFilesFromPairs(pairs), nil
}

func shadowCandidatesFromSpecs(specs []generatedMigrationSpec) []shadowCandidate {
	candidates := make([]shadowCandidate, 0, len(specs))
	for _, spec := range specs {
		candidates = append(candidates, shadowCandidate{
			Version: spec.Version,
			Name:    spec.Name,
			UpSQL:   spec.UpSQL,
			DownSQL: spec.DownSQL,
		})
	}
	return candidates
}

func migrationFilesFromPairs(pairs []MigrationFilePair) *MigrationFiles {
	if len(pairs) == 0 {
		return nil
	}
	first := pairs[0]
	return &MigrationFiles{
		UpFile:     first.UpFile,
		DownFile:   first.DownFile,
		ReportFile: first.ReportFile,
		Version:    first.Version,
		Files:      pairs,
	}
}

func ensureMigrationOutputDir(outputDir string) error {
	info, err := os.Stat(outputDir)
	if err == nil {
		if !info.IsDir() {
			return fmt.Errorf("%q exists and is not a directory", outputDir)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	parent := filepath.Dir(outputDir)
	if parentInfo, statErr := os.Stat(parent); statErr != nil {
		return fmt.Errorf("parent directory %q is not available: %w", parent, statErr)
	} else if !parentInfo.IsDir() {
		return fmt.Errorf("parent path %q is not a directory", parent)
	}
	return os.Mkdir(outputDir, 0755)
}

func writeNewMigrationFile(path, content string) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	removeOnError := true
	defer func() {
		if removeOnError {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.WriteString(content); err != nil {
		_ = file.Close()
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	removeOnError = false
	return nil
}
