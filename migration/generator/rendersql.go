package generator

// SQL and the artifacts around it: the up and down bodies, the directives that
// go above them, and the safety report beside them.

import (
	"bytes"
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/sqlscript"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/planner"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
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
	return slices.ContainsFunc(statements, func(statement string) bool {
		return !sqlscript.CommentOnly(statement)
	})
}

// joinScriptStatements writes statements as the body of a migration file, one
// after another, each executable statement ended with a semicolon. A statement
// that is only comments -- a planner's note with nothing to run after it --
// gets none; see [sqlscript].
func joinScriptStatements(statements []string) string {
	var script strings.Builder
	for i, statement := range statements {
		if i > 0 {
			script.WriteString("\n")
		}
		script.WriteString(statement)
		script.WriteString(sqlscript.Terminator(statement))
	}
	return script.String()
}

// generateUpMigrationSQL generates the SQL for the up migration.
func generateUpMigrationSQL(
	ctx context.Context, runtime Runtime,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateUpMigrationSQLWithOptions(
		ctx, runtime,
		diff, desired, dialect, generatedDirectiveOptions{}, capsOverride...,
	)
}

type generatedDirectiveOptions struct {
	// skipTimeouts leaves the generated timeouts off a file that runs outside a
	// transaction. Such a file holds a concurrent index build or a constraint
	// validation, which runs as long as the table is large under a lock that
	// blocks no reader or writer: the generated 30s statement timeout would
	// cancel it on a large table, and the 3s lock timeout stops a concurrent
	// build that waits on a write transaction, leaving an invalid index. A run
	// that wants the bound sets it with --lock-timeout.
	skipTimeouts bool
}

func generateUpMigrationSQLWithOptions(
	ctx context.Context, runtime Runtime,
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
		ctx, runtime,
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

	return withGeneratedTimeoutDirectivesForOptions(header+joinScriptStatements(statements), dialect, directiveOpts), nil
}

// generateDownMigrationSQL generates the SQL for the down migration by reversing the diff.
func generateDownMigrationSQL(
	ctx context.Context,
	runtime Runtime,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	capsOverride ...capability.Capabilities,
) (string, error) {
	return generateDownMigrationSQLWithOptions(ctx, runtime, diff, desired, dbSchema, dialect, generatedDirectiveOptions{}, capsOverride...)
}

func generateDownMigrationSQLWithOptions(
	ctx context.Context,
	runtime Runtime,
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
	return generateDownMigrationSQLQualified(ctx, runtime, diff, desired, dbSchema, dialect, opts)
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
	ctx context.Context,
	runtime Runtime,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	dbSchema *catalog.Database,
	dialect string,
	opts downMigrationOptions,
) (string, error) {
	if normalized := platform.NormalizeDialect(dialect); normalized != "" {
		dialect = normalized
	}
	directiveOpts := opts.directives
	// For down migrations, we need to use the current database schema as the "generated" schema
	// since we're reverting back to the current state
	current, err := restoreTableSource(diff, dbSchema, dialect)
	if err != nil {
		return "", err
	}
	dbSchema = current
	dbAsGoSchema, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, dbSchema, dialect, runtime)
	if err != nil {
		return "", err
	}

	// Create a reverse diff to generate down migration. We pass the original
	// generated schema to resolve table names for RLS policies, and the
	// introspected database schema so the reversed constraint additions can
	// rebuild the full prior body from the pre-change DB state — that is exactly
	// the definition the down must restore.
	reverseDiff := reverseSchemaDiffWithPrior(diff, desired, dbSchema, dbAsGoSchema, dialect)
	caps := opts.capabilities
	if caps == nil {
		caps = capability.ForDialect(dialect)
	}
	recovery, err := reverseFeatureChanges(ctx, diff, reverseDiff, dialect, caps, runtime)
	if err != nil {
		return "", err
	}
	if normalized := platform.NormalizeDialect(dialect); normalized == platform.MySQL || normalized == platform.MariaDB {
		forwardNodes, err := planner.GenerateSchemaDiffASTWithOptions(
			ctx, runtime,
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
	statements, err := planDownMigrationStatements(
		ctx, runtime,
		reverseDiff, dbAsGoSchema, dialect, plannerOpts, opts.qualifier,
	)
	if err != nil {
		return "", err
	}
	notes, err := renderer.Render(ctx, runtime, renderer.Request{Target: dialect, Capabilities: caps, Nodes: recoveryNotes(recovery)})
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

	return withGeneratedTimeoutDirectivesForOptions(header+notes.SQL()+joinScriptStatements(statements), dialect, directiveOpts), nil
}

// planDownMigrationStatements renders the reversed diff into ordered down
// statements. Without a qualifier it is the historical direct-render path;
// with one, the plan is generated as AST first so the qualifier rewrite runs
// before rendering, mirroring the up direction.
func planDownMigrationStatements(
	ctx context.Context, runtime Runtime,
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
		ctx, runtime, dbAsGoSchema, reverseDiff, dialect, plannerOpts.CapabilitiesFor(dialect),
	); err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if qualifier.IsZero() {
		statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(
			ctx, runtime,
			reverseDiff, dialect, plannerOpts,
		)
		if err != nil {
			return nil, fmt.Errorf("error generating down migration SQL: %w", err)
		}
		return statements, nil
	}
	nodes, err := planner.GenerateSchemaDiffASTWithOptions(
		ctx, runtime,
		reverseDiff, dialect, plannerOpts,
	)
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	if err := qualifier.ApplyToPlan(dialect, dbAsGoSchema, nodes); err != nil {
		return nil, err
	}
	output, err := renderer.Render(ctx, runtime, renderer.Request{Target: dialect, Capabilities: plannerOpts.CapabilitiesFor(dialect), Nodes: nodes})
	if err != nil {
		return nil, fmt.Errorf("error generating down migration SQL: %w", err)
	}
	return sqlutil.SplitSQLStatementsForDialect(output.SQL(), dialect), nil
}

func withGeneratedTimeoutDirectivesForOptions(sql, dialect string, opts generatedDirectiveOptions) string {
	if opts.skipTimeouts {
		return sql
	}
	return withGeneratedTimeoutDirectives(sql, dialect)
}

func withGeneratedTimeoutDirectives(sql, dialect string) string {
	if !waitsForTableLock(sql) || !supportsGeneratedTimeoutDirectives(dialect) {
		return sql
	}

	directives := "-- +ptah lock_timeout=3s\n-- +ptah statement_timeout=30s\n"
	separator := "\n\n"
	if before, after, ok := strings.Cut(sql, separator); ok {
		return before + "\n" + directives + "\n" + after
	}
	return directives + sql
}

// tableLockWaiter matches a statement that waits for a lock on a table that
// already exists: ALTER TABLE, and the LOCK TABLES, CREATE TRIGGER and DROP
// TRIGGER of a trigger change. On MySQL and MariaDB each waits for the table's
// metadata lock, and every later write to the table queues behind it, so the
// generated lock timeout is what bounds the wait (stokaro/ptah#4012).
var tableLockWaiter = regexp.MustCompile(
	`(?im)^\s*(ALTER\s+TABLE|LOCK\s+TABLES?|DROP\s+TRIGGER|CREATE\s+(OR\s+REPLACE\s+)?(DEFINER\s*=\s*\S+\s+)?TRIGGER)\b`,
)

func waitsForTableLock(sql string) bool {
	return tableLockWaiter.MatchString(sqlutil.StripComments(sql))
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
