package generator

// SQL and the artifacts around it: the up and down bodies, the directives that
// go above them, and the safety report beside them.

import (
	"bytes"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/core/sqlutil"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/migration/migrationfile"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/safety"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

func renderSafetyReport(
	upFile, format string,
	assessments []safety.StatementAssessment,
) (string, []byte, error) {
	var contents bytes.Buffer
	var reportFile string
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "html":
		reportFile = strings.TrimSuffix(upFile, ".up.sql") + ".safety.html"
		if err := safety.RenderHTML(&contents, assessments); err != nil {
			return "", nil, err
		}
	case "json":
		reportFile = strings.TrimSuffix(upFile, ".up.sql") + ".safety.json"
		if err := safety.RenderJSON(&contents, assessments); err != nil {
			return "", nil, err
		}
	default:
		return "", nil, fmt.Errorf("unsupported safety report format %q", format)
	}
	return reportFile, contents.Bytes(), nil
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
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateUpMigrationSQLWithOptions(diff, desired, dialect, generatedDirectiveOptions{}, capsOverride...)
}

type generatedDirectiveOptions struct {
	skipTimeouts bool
}

func generateUpMigrationSQLWithOptions(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	caps := capability.ForDialect(dialect)
	if len(capsOverride) > 0 {
		caps = capsOverride[0]
	}
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
		diff, dialect,
		planner.Options{Capabilities: caps},
	)

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
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateDownMigrationSQLWithOptions(diff, desired, dbSchema, dialect, generatedDirectiveOptions{}, capsOverride...)
}

func generateDownMigrationSQLWithOptions(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	directiveOpts generatedDirectiveOptions,
	capsOverride ...capability.Capabilities,
) (string, error) {
	opts := downMigrationOptions{directives: directiveOpts}
	if len(capsOverride) > 0 {
		opts.capabilities = capsOverride[0]
	}
	return generateDownMigrationSQLQualified(diff, desired, dbSchema, dialect, opts)
}

// downMigrationOptions carries the down-direction planning inputs that vary per
// caller. A nil capabilities set means "the dialect default preset".
type downMigrationOptions struct {
	directives   generatedDirectiveOptions
	qualifier    atlasmigrate.Qualifier
	capabilities capability.Capabilities
	// concurrentIndexRefs and concurrentIndexDropRefs are expressed in DOWN
	// direction terms: they name indexes the down file builds and drops, which
	// are the mirror image of the up file's own two sets.
	concurrentIndexRefs     []difftypes.IndexRef
	concurrentIndexDropRefs []difftypes.IndexRef
}

func generateDownMigrationSQLQualified(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	opts downMigrationOptions,
) (string, error) {
	directiveOpts := opts.directives
	// For down migrations, we need to use the current database schema as the "generated" schema
	// since we're reverting back to the current state
	dbAsGoSchema := dbschematogo.ConvertDBSchemaToGoSchema(dbSchema, dialect)

	// Create a reverse diff to generate down migration. We pass the original
	// generated schema to resolve table names for RLS policies, and the
	// introspected database schema so the reversed constraint additions can
	// rebuild the full prior body from the pre-change DB state — that is exactly
	// the definition the down must restore.
	reverseDiff := reverseSchemaDiffWithSchemaForDialect(diff, desired, dbSchema, dialect)
	if normalized := platform.NormalizeDialect(dialect); normalized == platform.MySQL || normalized == platform.MariaDB {
		forwardNodes, err := planner.GenerateSchemaDiffASTWithOptions(
			diff, dialect, planner.Options{Capabilities: opts.capabilities},
		)
		if err != nil {
			return "", fmt.Errorf("error planning forward migration: %w", err)
		}
		if err := addMySQLFamilyForeignKeyBackingIndexRemovals(
			reverseDiff,
			diff,
			dbSchema,
			dialect,
			forwardNodes,
		); err != nil {
			return "", err
		}
	}

	plannerOpts := planner.Options{
		Capabilities:            opts.capabilities,
		ConcurrentIndexRefs:     opts.concurrentIndexRefs,
		ConcurrentIndexDropRefs: opts.concurrentIndexDropRefs,
	}
	statements, err := planDownMigrationStatements(reverseDiff, dbAsGoSchema, dialect, plannerOpts, opts.qualifier)
	if err != nil {
		return "", err
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

// planDownMigrationStatements renders the reversed diff into ordered down
// statements. Without a qualifier it is the historical direct-render path;
// with one, the plan is generated as AST first so the qualifier rewrite runs
// before rendering, mirroring the up direction.
func planDownMigrationStatements(
	reverseDiff *difftypes.SchemaDiff,
	dbAsGoSchema *schemamodel.Database,
	dialect string,
	plannerOpts planner.Options,
	qualifier atlasmigrate.Qualifier,
) ([]string, error) {
	// The rollback's target is the schema the database currently holds. The
	// forward direction's target is validated by the comparison that produced
	// the forward diff; a reversal has no comparison of its own, so without
	// this the assertion would be made for one direction only
	// (stokaro/ptah#2315).
	if err := validateRollbackTarget(
		dbAsGoSchema, reverseDiff, dialect, plannerOpts.CapabilitiesFor(dialect),
	); err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if qualifier.IsZero() {
		statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(reverseDiff, dialect, plannerOpts)
		if err != nil {
			return nil, fmt.Errorf("error generating down migration SQL: %w", err)
		}
		return statements, nil
	}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(reverseDiff, dialect, plannerOpts)
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if err := qualifier.ApplyToPlan(dialect, dbAsGoSchema, nodes); err != nil {
		return nil, err
	}
	output, err := renderer.RenderSQLWithCapabilities(dialect, plannerOpts.CapabilitiesFor(dialect), nodes...)
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	return sqlutil.SplitSQLStatementsForDialect(output, dialect), nil
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

func withNoTransactionDirective(sql string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}
	if directive, ok := migrationfile.ParseDirectives(sql)[migrationfile.DirectiveNoTransaction]; ok && directive == "true" {
		return sql
	}
	return "-- +ptah " + migrationfile.DirectiveNoTransaction + "\n" + sql
}

func renderMigrationArtifacts(
	outputDir, reportFormat string,
	specs []generatedMigrationSpec,
) ([]atlasmigrate.PublicationArtifact, []MigrationFilePair, error) {
	artifacts := make([]atlasmigrate.PublicationArtifact, 0, len(specs)*3)
	pairs := make([]MigrationFilePair, 0, len(specs))
	for _, spec := range specs {
		upName := migrationfile.FileName(spec.Version, spec.Name, "up")
		downName := migrationfile.FileName(spec.Version, spec.Name, "down")
		pair := MigrationFilePair{
			UpFile:        filepath.Join(outputDir, upName),
			DownFile:      filepath.Join(outputDir, downName),
			Version:       spec.Version,
			NoTransaction: spec.NoTransaction,
		}
		artifacts = append(
			artifacts,
			atlasmigrate.PublicationArtifact{Name: upName, Contents: []byte(spec.UpSQL)},
			atlasmigrate.PublicationArtifact{Name: downName, Contents: []byte(spec.DownSQL)},
		)
		if reportFormat != "" {
			reportName, reportContents, err := renderSafetyReport(
				upName,
				reportFormat,
				spec.Assessments,
			)
			if err != nil {
				return nil, nil, fmt.Errorf("error creating safety report: %w", err)
			}
			pair.ReportFile = filepath.Join(outputDir, reportName)
			artifacts = append(artifacts, atlasmigrate.PublicationArtifact{
				Name:     reportName,
				Contents: reportContents,
			})
		}
		pairs = append(pairs, pair)
	}
	return artifacts, pairs, nil
}
