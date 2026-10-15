package generator

// SQL and the artifacts around it: the up and down bodies, the directives that
// go above them, and the safety report beside them.

import (
	"bytes"
	"fmt"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/sqlutil"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/internal/sqlscript"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/safety"
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

func withGeneratedTimeoutDirectivesForOptions(sql, dialect string, opts generatedDirectiveOptions) string {
	if opts.skipTimeouts {
		return sql
	}
	return withGeneratedTimeoutDirectives(sql, dialect)
}

// withGeneratedTimeoutDirectives states the GeneratedTimeouts of the
// dialect's descriptor ([ptah.run/core/platform/descriptor.Dialect]) at the
// head of sql when one of its statements waits for a lock on a table. A
// dialect that states none, and a file whose statements wait for no table
// lock, are left as they are.
func withGeneratedTimeoutDirectives(sql, dialect string) string {
	if !waitsForTableLock(sql) {
		return sql
	}
	described, _ := platform.Lookup(dialect)
	var directives string
	if timeout := described.GeneratedTimeouts.Lock; timeout != "" {
		directives += "-- +ptah lock_timeout=" + timeout + "\n"
	}
	if timeout := described.GeneratedTimeouts.Statement; timeout != "" {
		directives += "-- +ptah statement_timeout=" + timeout + "\n"
	}
	if directives == "" {
		return sql
	}
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
