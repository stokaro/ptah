package project

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"strings"

	"go.5x5.cz/ptah/cmd/internal/migrateflags"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/adoptpreflight"
	"go.5x5.cz/ptah/migration/migrationfile"
	"go.5x5.cz/ptah/migration/migrator"
)

// databaseAdoptionAdvisory is what the report says about the database when
// nobody asked it to look.
//
// It is a line rather than a silence because a project-file analysis that
// printed a clean verdict and said nothing about persisted state would read as
// "adoption is done", and the database half is where the outcomes #1215
// forbids actually happen -- re-running applied SQL, marking SQL applied that
// did not run. It is deliberately NOT counted as something needing adoption:
// the file analysis is a complete answer to the question it was asked, and a
// run that was never given a database has nothing to report a gap about.
const databaseAdoptionAdvisory = "not inspected; --preflight reads the revision history this project's " +
	"database holds, which is what has to be decided before switching writers"

// runDatabasePreflight inspects the persisted revision state.
//
// It reads the project's own database URL rather than taking one: a preflight
// against a database the project does not point at would answer about the
// wrong history, and the project file is where the answer to "which database"
// already lives.
func runDatabasePreflight(ctx context.Context, config projectconfig.Config) (*adoptpreflight.Report, error) {
	databaseURL := strings.TrimSpace(config.DatabaseURL)
	if databaseURL == "" {
		return nil, fmt.Errorf(
			"--preflight needs the database this project targets, and its configuration names none")
	}

	dirFormat, err := migrationfile.ParseDirFormat(config.Migration.Format)
	if err != nil {
		return nil, err
	}

	// A project naming no migration directory is not refused. The preflight
	// answers what it can from the rows alone and reports the rest as
	// undecided, which is a truer answer than a refusal to look at the
	// database at all. A directory that IS named and cannot be read is an
	// error, because that is a configuration the operator can fix and a
	// silently empty one would make every file-comparing dimension pass.
	migrationsFS, dirFormat, err := resolveMigrations(ctx, config.Migration.Dir, dirFormat)
	if err != nil {
		return nil, err
	}

	// Empty means the project declares no format, and that is load-bearing:
	// the preflight tells the two layouts apart by their default table names,
	// and only a project that declares nothing leaves that evidence intact.
	// ParseRevisionTableFormat answers ptah for an empty value, so the raw
	// string is what decides whether anything was declared.
	var revisionFormat migrator.RevisionTableFormat
	if declared := strings.TrimSpace(config.Migration.RevisionFormat); declared != "" {
		revisionFormat, err = migrateflags.ParseRevisionTableFormat(declared)
		if err != nil {
			return nil, err
		}
	}

	conn, err := dbschema.ConnectToDatabase(ctx, databaseURL)
	if err != nil {
		return nil, fmt.Errorf("error connecting to database: %w", err)
	}
	defer dbschema.CloseAndWarn(conn)

	report, err := adoptpreflight.Analyze(ctx, adoptpreflight.Options{
		Conn:            conn,
		DatabaseURL:     databaseURL,
		MigrationsFS:    migrationsFS,
		DirFormat:       dirFormat,
		RevisionsSchema: config.Migration.RevisionsSchema,
		RevisionsTable:  config.Migration.RevisionsTable,
		RevisionFormat:  revisionFormat,
	})
	if err != nil {
		return nil, err
	}
	return &report, nil
}

// resolveMigrations reads the migration directory, and answers nil for a
// project that names none. The resolved format is returned with it because a
// directory can settle a format the project left on auto.
func resolveMigrations(
	ctx context.Context,
	dir string,
	dirFormat migrationfile.DirFormat,
) (fs.FS, migrationfile.DirFormat, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, dirFormat, nil
	}
	source, err := migrationsource.Resolve(ctx, dir, migrationsource.Options{DirFormat: dirFormat})
	if err != nil {
		return nil, dirFormat, fmt.Errorf("failed to read the migration directory: %w", err)
	}
	return source.FileSystem, source.DirFormat, nil
}

// writeDatabaseAdoption prints the database section of the report.
func writeDatabaseAdoption(out io.Writer, report *adoptpreflight.Report) {
	fmt.Fprintln(out, "Database adoption:")
	if report == nil {
		fmt.Fprintf(out, "  %s %s\n\n", severityMarker(""), databaseAdoptionAdvisory)
		return
	}
	for _, finding := range report.Findings {
		fmt.Fprintf(out, "  %s %s\n", severityMarker(finding.Severity), finding.Summary)
		if finding.Detail != "" {
			fmt.Fprintf(out, "    %s\n", finding.Detail)
		}
	}
	fmt.Fprintln(out)
}

// severityMarker is the glyph #1215's example report uses for each outcome.
func severityMarker(severity adoptpreflight.Severity) string {
	switch severity {
	case adoptpreflight.SeverityOK:
		return "✓"
	case adoptpreflight.SeverityAction:
		return "~"
	case adoptpreflight.SeverityRefuse:
		return "!"
	}
	return "-"
}
