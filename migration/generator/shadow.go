package generator

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"os"
	"regexp"
	"sort"
	"strings"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/migrator"
	"github.com/stokaro/ptah/migration/schemadiff"
	"github.com/stokaro/ptah/migration/schemadiff/types"
)

var missingColumnErrorRe = regexp.MustCompile(`column "([^"]+)" of relation "([^"]+)" does not exist`)

// ShadowVerificationResult is a structured shadow-database verification
// outcome. Errors preserve the historical text form through
// ShadowVerificationError.Error while exposing mismatches to callers that need
// machine-readable diagnostics.
type ShadowVerificationResult struct {
	Stage      string           `json:"stage"`
	Success    bool             `json:"success"`
	Mismatches []ShadowMismatch `json:"mismatches,omitempty"`
}

// ShadowMismatch describes one schema mismatch found during shadow
// verification.
type ShadowMismatch struct {
	Kind       string            `json:"kind"`
	Object     string            `json:"object,omitempty"`
	Table      string            `json:"table,omitempty"`
	Column     string            `json:"column,omitempty"`
	Constraint string            `json:"constraint,omitempty"`
	Changes    map[string]string `json:"changes,omitempty"`
	Message    string            `json:"message"`
}

// ShadowVerificationError wraps a structured shadow verification result.
type ShadowVerificationError struct {
	Result ShadowVerificationResult `json:"result"`
}

func (e *ShadowVerificationError) Error() string {
	if len(e.Result.Mismatches) > 0 {
		return "shadow check failed: " + e.Result.Mismatches[0].Message
	}
	return "shadow check failed: schema differs"
}

type shadowMigrationOptions struct {
	DatabaseURL   string
	MigrationsDir string
	Dialect       string
	Capabilities  capability.Capabilities
	Candidates    []shadowCandidate
	Generated     *goschema.Database
	CompareOpts   *config.CompareOptions
	Schemas       []string
}

type shadowCandidate struct {
	Version int64
	Name    string
	UpSQL   string
	DownSQL string
}

func verifyShadowMigration(ctx context.Context, opts shadowMigrationOptions) error {
	conn, err := dbschema.ConnectToDatabase(ctx, opts.DatabaseURL)
	if err != nil {
		return fmt.Errorf("shadow check failed: connect to shadow database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	if !sameDialect(opts.Dialect, conn.Info().Dialect) {
		return fmt.Errorf("shadow check failed: shadow database dialect %q does not match target dialect %q", conn.Info().Dialect, opts.Dialect)
	}
	if opts.Capabilities != nil && !maps.Equal(opts.Capabilities, conn.Info().Capabilities) {
		return fmt.Errorf("shadow check failed: shadow database capabilities do not match target %s capabilities", opts.Dialect)
	}

	if err := conn.Writer().DropAllTables(); err != nil {
		return fmt.Errorf("shadow check failed: drop all objects: %w", err)
	}
	replayCtx := context.Background()

	prior, err := loadPriorMigrations(opts.MigrationsDir)
	if err != nil {
		return fmt.Errorf("shadow check failed: load prior migrations: %w", err)
	}

	migrations := make([]*migrator.Migration, 0, len(prior)+len(opts.Candidates))
	migrations = append(migrations, prior...)
	for _, candidate := range opts.Candidates {
		migrations = append(migrations,
			migrator.CreateMigrationFromSQL(candidate.Version, candidate.Name, candidate.UpSQL, candidate.DownSQL),
		)
	}

	mig := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider(migrations...))
	if err := mig.MigrateUp(replayCtx); err != nil {
		if description := describeReplayError(err); description != "" {
			return fmt.Errorf("shadow check failed: %s", description)
		}
		return fmt.Errorf("shadow check failed: replay migrations: %w", err)
	}
	if err := assertShadowSchemaMatches(conn, opts); err != nil {
		return err
	}

	previousVersion := latestMigrationVersion(prior)
	if err := mig.MigrateDownTo(replayCtx, previousVersion); err != nil {
		return fmt.Errorf("shadow check failed: round-trip down: %w", err)
	}
	if err := mig.MigrateTo(replayCtx, latestMigrationVersion(migrations)); err != nil {
		return fmt.Errorf("shadow check failed: round-trip up: %w", err)
	}
	return assertShadowSchemaMatches(conn, opts)
}

func describeReplayError(err error) string {
	match := missingColumnErrorRe.FindStringSubmatch(err.Error())
	if match == nil {
		return ""
	}
	return fmt.Sprintf("missing column %s.%s", match[2], match[1])
}

func sameDialect(left, right string) bool {
	return platform.NormalizeDialect(left) == platform.NormalizeDialect(right)
}

func loadPriorMigrations(dir string, opts ...migrator.FSProviderOption) ([]*migrator.Migration, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, nil
	}
	if _, err := os.Stat(dir); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}

	provider, err := migrator.NewFSMigrationProvider(os.DirFS(dir), opts...)
	if err != nil {
		return nil, err
	}
	migrations := provider.Migrations()
	out := make([]*migrator.Migration, len(migrations))
	copy(out, migrations)
	return out, nil
}

func latestMigrationVersion(migrations []*migrator.Migration) int64 {
	var latest int64
	for _, migration := range migrations {
		if migration.Version > latest {
			latest = migration.Version
		}
	}
	return latest
}

func assertShadowSchemaMatches(conn *dbschema.DatabaseConnection, opts shadowMigrationOptions) error {
	dbSchema, err := dbschema.ReadSchemaWithSchemas(conn, opts.Schemas)
	if err != nil {
		return fmt.Errorf("shadow check failed: re-introspect shadow database: %w", err)
	}

	diff := schemadiff.CompareWithOptions(opts.Generated, dbSchema, opts.CompareOpts)
	if !diff.HasChanges() {
		return nil
	}
	return &ShadowVerificationError{Result: ShadowVerificationResult{
		Stage:      "schema-match",
		Success:    false,
		Mismatches: collectShadowMismatches(diff),
	}}
}

func describeShadowDiff(diff *types.SchemaDiff) string {
	mismatches := collectShadowMismatches(diff)
	if len(mismatches) > 0 {
		return mismatches[0].Message
	}
	return "schema differs"
}

func collectShadowMismatches(diff *types.SchemaDiff) []ShadowMismatch {
	if diff == nil {
		return []ShadowMismatch{{Kind: "schema", Message: "schema differs"}}
	}

	for _, tableName := range sortedStrings(diff.TablesAdded) {
		return []ShadowMismatch{{Kind: "missing_table", Table: tableName, Object: tableName, Message: "missing table " + tableName}}
	}
	for _, table := range sortedTableDiffs(diff.TablesModified) {
		for _, columnName := range sortedStrings(table.ColumnsAdded) {
			message := fmt.Sprintf("missing column %s.%s", table.TableName, columnName)
			return []ShadowMismatch{{Kind: "missing_column", Table: table.TableName, Column: columnName, Object: table.TableName + "." + columnName, Message: message}}
		}
		for _, constraintName := range sortedStrings(table.ConstraintsAdded) {
			message := fmt.Sprintf("missing constraint %s.%s", table.TableName, constraintName)
			return []ShadowMismatch{{Kind: "missing_constraint", Table: table.TableName, Constraint: constraintName, Object: table.TableName + "." + constraintName, Message: message}}
		}
		for _, column := range sortedColumnDiffs(table.ColumnsModified) {
			message := fmt.Sprintf("column mismatch %s.%s: %s", table.TableName, column.ColumnName, describeChanges(column.Changes))
			return []ShadowMismatch{{Kind: "column_mismatch", Table: table.TableName, Column: column.ColumnName, Object: table.TableName + "." + column.ColumnName, Changes: column.Changes, Message: message}}
		}
		for _, columnName := range sortedStrings(table.ColumnsRemoved) {
			message := fmt.Sprintf("extra column %s.%s", table.TableName, columnName)
			return []ShadowMismatch{{Kind: "extra_column", Table: table.TableName, Column: columnName, Object: table.TableName + "." + columnName, Message: message}}
		}
		for _, constraintName := range sortedStrings(table.ConstraintsRemoved) {
			message := fmt.Sprintf("extra constraint %s.%s", table.TableName, constraintName)
			return []ShadowMismatch{{Kind: "extra_constraint", Table: table.TableName, Constraint: constraintName, Object: table.TableName + "." + constraintName, Message: message}}
		}
	}
	for _, enumName := range sortedStrings(diff.EnumsAdded) {
		return []ShadowMismatch{{Kind: "missing_enum", Object: enumName, Message: "missing enum " + enumName}}
	}
	for _, enum := range sortedEnumDiffs(diff.EnumsModified) {
		for _, value := range sortedStrings(enum.ValuesAdded) {
			message := fmt.Sprintf("missing enum value %s.%s", enum.EnumName, value)
			return []ShadowMismatch{{Kind: "missing_enum_value", Object: enum.EnumName + "." + value, Message: message}}
		}
		for _, value := range sortedStrings(enum.ValuesRemoved) {
			message := fmt.Sprintf("extra enum value %s.%s", enum.EnumName, value)
			return []ShadowMismatch{{Kind: "extra_enum_value", Object: enum.EnumName + "." + value, Message: message}}
		}
	}
	for _, indexName := range sortedStrings(diff.IndexesAdded) {
		return []ShadowMismatch{{Kind: "missing_index", Object: indexName, Message: "missing index " + indexName}}
	}
	for _, extensionName := range sortedStrings(diff.ExtensionsAdded) {
		return []ShadowMismatch{{Kind: "missing_extension", Object: extensionName, Message: "missing extension " + extensionName}}
	}
	for _, functionName := range sortedStrings(diff.FunctionsAdded) {
		return []ShadowMismatch{{Kind: "missing_function", Object: functionName, Message: "missing function " + functionName}}
	}
	for _, policyName := range sortedStrings(diff.RLSPoliciesAdded) {
		return []ShadowMismatch{{Kind: "missing_rls_policy", Object: policyName, Message: "missing RLS policy " + policyName}}
	}
	for _, tableName := range sortedStrings(diff.RLSEnabledTablesAdded) {
		return []ShadowMismatch{{Kind: "missing_rls_enablement", Table: tableName, Object: tableName, Message: "missing RLS enablement " + tableName}}
	}
	for _, roleName := range sortedStrings(diff.RolesAdded) {
		return []ShadowMismatch{{Kind: "missing_role", Object: roleName, Message: "missing role " + roleName}}
	}
	for _, constraintName := range sortedStrings(diff.ConstraintsAdded) {
		return []ShadowMismatch{{Kind: "missing_constraint", Constraint: constraintName, Object: constraintName, Message: "missing constraint " + constraintName}}
	}

	return []ShadowMismatch{{Kind: "schema", Message: "schema differs"}}
}

func sortedStrings(values []string) []string {
	out := append([]string(nil), values...)
	sort.Strings(out)
	return out
}

func sortedTableDiffs(values []types.TableDiff) []types.TableDiff {
	out := append([]types.TableDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].TableName < out[j].TableName
	})
	return out
}

func sortedColumnDiffs(values []types.ColumnDiff) []types.ColumnDiff {
	out := append([]types.ColumnDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].ColumnName < out[j].ColumnName
	})
	return out
}

func sortedEnumDiffs(values []types.EnumDiff) []types.EnumDiff {
	out := append([]types.EnumDiff(nil), values...)
	sort.Slice(out, func(i, j int) bool {
		return out[i].EnumName < out[j].EnumName
	})
	return out
}

func describeChanges(changes map[string]string) string {
	if len(changes) == 0 {
		return "unknown change"
	}

	keys := make([]string, 0, len(changes))
	for key := range changes {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+" "+changes[key])
	}
	return strings.Join(parts, ", ")
}
