package generator

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/convert/dbschematogo"
	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/planner"
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
	// CompareOptions are the options to use when comparing schemas
	CompareOptions *config.CompareOptions
}

// MigrationFiles represents the generated migration files
type MigrationFiles struct {
	UpFile   string // Path to the up migration file
	DownFile string // Path to the down migration file
	Version  int    // Migration version (timestamp)
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
	// Set default migration name if not provided
	if opts.MigrationName == "" {
		opts.MigrationName = "migration"
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

	dbSchema, err := conn.Reader().ReadSchema()
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
	slog.Debug("Generated migration version", "version", version)

	// 5. Generate up migration SQL
	upSQL, err := generateUpMigrationSQL(diff, generated, conn.Info().Dialect)
	if err != nil {
		return nil, fmt.Errorf("error generating up migration SQL: %w", err)
	}

	// Check if no actual migration is needed (empty upSQL indicates no changes)
	if upSQL == "" {
		// No migration needed - this is a successful no-op operation
		return nil, nil
	}

	// 6. Generate down migration SQL
	downSQL, err := generateDownMigrationSQL(diff, generated, dbSchema, conn.Info().Dialect)
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}

	// 7. Create migration files
	files, err := createMigrationFiles(opts.OutputDir, version, opts.MigrationName, upSQL, downSQL)
	if err != nil {
		return nil, fmt.Errorf("error creating migration files: %w", err)
	}

	return files, nil
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

// generateUpMigrationSQL generates the SQL for the up migration
func generateUpMigrationSQL(diff *types.SchemaDiff, generated *goschema.Database, dialect string) (string, error) {
	statements := planner.GenerateSchemaDiffSQLStatements(diff, generated, dialect)

	if len(statements) == 0 || !hasActualSQLStatements(statements) {
		// No actual SQL statements generated - this is a successful no-op operation
		return "", nil
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration generated from schema differences\n-- Generated on: %s\n-- Direction: UP\n\n",
		time.Now().Format(time.RFC3339))

	return header + strings.Join(statements, ";\n") + ";", nil
}

// generateDownMigrationSQL generates the SQL for the down migration by reversing the diff
func generateDownMigrationSQL(diff *types.SchemaDiff, generated *goschema.Database, dbSchema *dbschematypes.DBSchema, dialect string) (string, error) {
	// For down migrations, we need to use the current database schema as the "generated" schema
	// since we're reverting back to the current state
	dbAsGoSchema := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema)

	// Create a reverse diff to generate down migration. We pass the original
	// generated schema to resolve table names for RLS policies, and the
	// introspected database schema so the reversed constraint additions can
	// rebuild the FULL prior FK body (columns, target, on_delete/on_update) from
	// the pre-change DB state — that is exactly the action the down must restore.
	reverseDiff := reverseSchemaDiffWithSchema(diff, generated, dbSchema)

	statements := planner.GenerateSchemaDiffSQLStatements(reverseDiff, dbAsGoSchema, dialect)

	if len(statements) == 0 {
		// If no statements generated, create a simple comment
		header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n-- No rollback operations needed\n",
			time.Now().Format(time.RFC3339))
		return header, nil
	}

	// Add header comment
	header := fmt.Sprintf("-- Migration rollback\n-- Generated on: %s\n-- Direction: DOWN\n\n",
		time.Now().Format(time.RFC3339))

	return header + strings.Join(statements, ";\n") + ";", nil
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
// to rebuild the prior FK definition for the reversed constraint additions; it
// may be nil for legacy callers that only have the generated schema (the
// reversed additions then fall back to the name-only path).
func reverseSchemaDiffWithSchema(diff *types.SchemaDiff, schema *goschema.Database, dbSchema *dbschematypes.DBSchema) *types.SchemaDiff {
	return &types.SchemaDiff{
		// Reverse table operations
		TablesAdded:    diff.TablesRemoved, // Tables to remove become tables to add
		TablesRemoved:  diff.TablesAdded,   // Tables to add become tables to remove
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

		// Reverse RLS policy operations
		RLSPoliciesAdded:    convertRLSPolicyRefsToNames(diff.RLSPoliciesRemoved),                 // Policies to remove become policies to add (convert RLSPolicyRef to string)
		RLSPoliciesRemoved:  convertRLSPolicyNamesToRefsWithSchema(diff.RLSPoliciesAdded, schema), // Policies to add become policies to remove (convert string to RLSPolicyRef with table resolution)
		RLSPoliciesModified: reverseRLSPolicyDiffs(diff.RLSPoliciesModified),

		// Reverse RLS table enablement operations
		RLSEnabledTablesAdded:   diff.RLSEnabledTablesRemoved, // Tables to disable RLS become tables to enable RLS
		RLSEnabledTablesRemoved: diff.RLSEnabledTablesAdded,   // Tables to enable RLS become tables to disable RLS

		// Reverse role operations
		RolesAdded:    diff.RolesRemoved, // Roles to remove become roles to add
		RolesRemoved:  diff.RolesAdded,   // Roles to add become roles to remove
		RolesModified: reverseRoleDiffs(diff.RolesModified),

		// Reverse constraint operations. A modified constraint is expressed by
		// the comparator as remove + add of the SAME name (e.g. an on_delete
		// change on a field-level FK, issue #189). Swapping the two slices makes
		// the down migration drop the new definition and re-add the old one — the
		// down planner resolves the old definition from the introspected schema
		// (see dbschematogo.ConvertDBSchemaToGoSchema, which now carries the
		// FK action), so the prior action is faithfully restored.
		//
		// ConstraintsAddedWithTables carries the table-qualified prior FK body so
		// the down add-path can fan a mixin-shared FK name out to every host
		// table. Without it the down add-path falls back to the name-only field
		// scan, which emits one ADD for a single host while the per-host DROP also
		// resolves only one host — so the 2nd host's re-add collides with its
		// still-present old constraint (Postgres 42710, MySQL 1826) and the
		// rollback aborts half-applied. This is the DOWN mirror of the UP
		// multi-host fix (issue #197).
		ConstraintsAdded:             diff.ConstraintsRemoved,
		ConstraintsRemoved:           diff.ConstraintsAdded,
		ConstraintsRemovedWithTables: reverseConstraintRemovals(diff, schema),
		ConstraintsAddedWithTables:   reverseConstraintAdditions(diff, dbSchema),
	}
}

// reverseConstraintAdditions builds the table-qualified additions for the down
// migration. In the down direction the constraints to add back are the ones the
// up migration REMOVED (diff.ConstraintsRemovedWithTables) — restoring their
// prior definition. The prior FK body (columns, foreign table/column,
// on_delete/on_update) is read from the introspected (pre-change) database
// schema, which is the authoritative source for what the down must restore.
//
// Carrying the full per-host body here lets both dialect planners' add-paths
// (which already prefer ConstraintsAddedWithTables) emit one correct ALTER TABLE
// per real host table. This is what makes the down of a multi-host mixin FK
// modify apply cleanly: a name-only down re-adds only one host (and drops only
// one host), so the others collide on re-add (issue #197 DOWN path). When
// dbSchema is nil (legacy callers) the names still flow through ConstraintsAdded
// and the planners fall back to the name-only field scan.
func reverseConstraintAdditions(diff *types.SchemaDiff, dbSchema *dbschematypes.DBSchema) []types.ConstraintAdditionInfo {
	if dbSchema == nil || len(diff.ConstraintsRemovedWithTables) == 0 {
		return nil
	}

	// Index the introspected FOREIGN KEY constraints by (table, name) so each
	// reversed addition restores the body from the exact host it was removed
	// from. A mixin-shared FK name legitimately repeats across host tables, so a
	// name-only key would collapse them onto one host.
	dbFKByTableName := make(map[string]dbschematypes.DBConstraint)
	for _, c := range dbSchema.Constraints {
		if c.Type != "FOREIGN KEY" {
			continue
		}
		dbFKByTableName[c.TableName+"."+c.Name] = c
	}

	var infos []types.ConstraintAdditionInfo
	for _, removed := range diff.ConstraintsRemovedWithTables {
		if removed.Type != "FOREIGN KEY" || removed.TableName == "" {
			continue
		}
		dbFK, ok := dbFKByTableName[removed.TableName+"."+removed.Name]
		if !ok {
			// No introspected body to restore (e.g. the constraint was a
			// pure-removal not present pre-change, or a non-FK). The name still
			// rides in ConstraintsAdded for the name-only fallback.
			continue
		}
		infos = append(infos, foreignKeyAdditionFromDBConstraint(removed.Name, removed.TableName, dbFK))
	}
	return infos
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
	if dbFK.ColumnName != "" {
		info.Columns = []string{dbFK.ColumnName}
	}
	if dbFK.ForeignTable != nil {
		info.ForeignTable = *dbFK.ForeignTable
	}
	if dbFK.ForeignColumn != nil {
		info.ForeignColumn = *dbFK.ForeignColumn
	}
	return info
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
// the down migration. When the schema is unavailable (legacy callers) the names
// still flow through ConstraintsRemoved; only the richer per-table info is
// omitted.
func reverseConstraintRemovals(diff *types.SchemaDiff, schema *goschema.Database) []types.ConstraintRemovalInfo {
	if schema == nil || len(diff.ConstraintsAdded) == 0 {
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
	// recorded so the legacy field-scan fallback below does not double-emit them.
	var infos []types.ConstraintRemovalInfo
	handled := make(map[string]struct{})
	for _, add := range diff.ConstraintsAddedWithTables {
		if add.TableName == "" {
			continue
		}
		infos = append(infos, types.ConstraintRemovalInfo{Name: add.Name, TableName: add.TableName, Type: add.Type})
		handled[add.Name] = struct{}{}
	}

	// Index field-level constraint names to their owning table for the names
	// that did not arrive with table-qualified info (legacy callers / explicit
	// table-level constraints).
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
			infos = append(infos, types.ConstraintRemovalInfo{Name: name, TableName: c.Table, Type: c.Type})
		case fkTables[name] != "":
			infos = append(infos, types.ConstraintRemovalInfo{Name: name, TableName: fkTables[name], Type: "FOREIGN KEY"})
		case checkTables[name] != "":
			infos = append(infos, types.ConstraintRemovalInfo{Name: name, TableName: checkTables[name], Type: "CHECK"})
		}
	}
	return infos
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

// createMigrationFiles creates the up and down migration files
func createMigrationFiles(outputDir string, version int, migrationName, upSQL, downSQL string) (*MigrationFiles, error) {
	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Generate file names using the existing utility
	upFileName := migrator.GenerateMigrationFileName(version, migrationName, "up")
	downFileName := migrator.GenerateMigrationFileName(version, migrationName, "down")

	upFilePath := filepath.Join(outputDir, upFileName)
	downFilePath := filepath.Join(outputDir, downFileName)

	for {
		info, err := os.Stat(upFilePath)
		if err != nil || info.Size() == 0 {
			break
		}

		version++
		upFileName = migrator.GenerateMigrationFileName(version, migrationName, "up")
		downFileName = migrator.GenerateMigrationFileName(version, migrationName, "down")
		upFilePath = filepath.Join(outputDir, upFileName)
		downFilePath = filepath.Join(outputDir, downFileName)
	}

	// Write up migration file
	if err := os.WriteFile(upFilePath, []byte(upSQL), 0644); err != nil { //nolint:gosec // 0644 is fine
		return nil, fmt.Errorf("failed to write up migration file: %w", err)
	}

	// Write down migration file
	if err := os.WriteFile(downFilePath, []byte(downSQL), 0644); err != nil { //nolint:gosec // 0644 is fine
		return nil, fmt.Errorf("failed to write down migration file: %w", err)
	}

	return &MigrationFiles{
		UpFile:   upFilePath,
		DownFile: downFilePath,
		Version:  version,
	}, nil
}
