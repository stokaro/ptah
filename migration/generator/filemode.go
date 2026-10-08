package generator

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasmigrate"
	"ptah.run/migration/migrator"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

// planMigrationFiles plans the migration files for diff and marks those the
// migrator would refuse to run in transaction mode file.
func planMigrationFiles(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current *catalog.Database,
	version int64,
	opts GenerateMigrationOptions,
	qualifier atlasmigrate.Qualifier,
) ([]generatedMigrationSpec, []safety.StatementAssessment, error) {
	specs, assessments, err := planGeneratedMigrationSpecs(
		ctx, opts.Runtime, diff, desired, current, conn.Info(), version, opts.MigrationName, opts.DiffPolicy, qualifier,
	)
	if err != nil || len(specs) == 0 {
		return specs, assessments, err
	}
	specs, err = markFileModeRefusals(ctx, conn, specs)
	return specs, assessments, err
}

// markFileModeRefusals marks no_transaction each migration file the migrator
// would refuse to run in transaction mode file, the default of
// `migrations up`, so that every file a plan writes is one the migrator runs
// (stokaro/ptah#4013).
//
// The migrator answers, through [migrator.FileModeRefusal], against the
// database the plan was compared with. A second list of what transaction mode
// file refuses would agree with the migrator only until one of them changed.
// Only MySQL and MariaDB refuse anything, and there DDL commits on its own, so
// running a file outside a transaction gives up nothing a transaction kept.
//
// The down file runs against the database the up file left, whose catalog the
// connection cannot show yet: a trigger the up file creates makes every later
// statement on its table refused. So a refused up file marks its down file as
// well. The timeouts stay: unlike a concurrent index build, nothing in such a
// file runs long, and lock_timeout is what bounds its wait for a busy table.
func markFileModeRefusals(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	specs []generatedMigrationSpec,
) ([]generatedMigrationSpec, error) {
	for i := range specs {
		upRefused, err := fileModeRefuses(ctx, conn, specs[i].UpSQL)
		if err != nil {
			return nil, err
		}
		downRefused := upRefused
		if !downRefused {
			downRefused, err = fileModeRefuses(ctx, conn, specs[i].DownSQL)
			if err != nil {
				return nil, err
			}
		}
		if upRefused {
			specs[i].UpSQL = withNoTransactionDirective(specs[i].UpSQL)
		}
		if downRefused {
			specs[i].DownSQL = withNoTransactionDirective(specs[i].DownSQL)
		}
		specs[i].NoTransaction = specs[i].NoTransaction || upRefused || downRefused
	}
	return specs, nil
}

// fileModeRefuses reports whether the migrator would refuse body in
// transaction mode file. A canceled context is an error rather than a refusal:
// the question went unanswered.
func fileModeRefuses(ctx context.Context, conn *dbschema.DatabaseConnection, body string) (bool, error) {
	if body == "" {
		return false, nil
	}
	refusal := migrator.FileModeRefusal(ctx, conn, body)
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return refusal != nil, nil
}
