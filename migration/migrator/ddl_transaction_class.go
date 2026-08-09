package migrator

import (
	"context"

	"go.5x5.cz/ptah/internal/ddltx"
)

// This file holds everything the migrator does with a target's DDL transaction
// contract beyond deciding which SQL to emit: the marker that says a failure
// happened after the body finished, and the progress that marker licenses.
// internal/ddltx owns the classification itself.

type migrationBodyCompletedContextKey struct{}

// withCompletedMigrationBody declares that every statement of the migration
// body executed successfully and that whatever failed afterwards was
// bookkeeping, not the migration.
//
// The failure the migrator records carries no statement index in that case,
// because no statement failed, so the progress derived from it is zero. Zero is
// the right answer where the body rolls back with the bookkeeping and the wrong
// one where the body is already durable: a later `--allow-dirty` retry resumes
// at applied+1 and replays a statement the database has already run. On
// ClickHouse that means re-issuing the body's CREATE TABLE and failing with
// "table already exists", which leaves the revision dirty for good and puts
// automatic recovery out of reach.
//
// Like [withMigrationResume], this travels in the context rather than as a
// parameter: the sites that know the body finished reach the revision writer
// through failMigrationWithDirtyState, whose signature is shared with a dozen
// failure sites that know nothing of the kind.
func withCompletedMigrationBody(ctx context.Context) context.Context {
	return context.WithValue(ctx, migrationBodyCompletedContextKey{}, true)
}

// migrationBodyCompleted reports whether [withCompletedMigrationBody] marked
// this failure as one the migration body survived intact.
func migrationBodyCompleted(ctx context.Context) bool {
	completed, _ := ctx.Value(migrationBodyCompletedContextKey{}).(bool)
	return completed
}

// completedBodyApplied returns the progress a failure that happened after the
// body finished should record, or zero when the class cannot claim any.
//
// The claim is limited to targets where every statement is independently
// durable. The MySQL family is excluded even though its DDL is durable too,
// because only some of its statements are: the revision-row witness installed
// by [Migrator.withTransactionalProgressRecorder] already reported which, and
// overwriting that with the whole body would record rolled-back DML as applied.
// See [ddltx.AllStatementsDurable].
func completedBodyApplied(ctx context.Context, dialect string, total int) int {
	if !migrationBodyCompleted(ctx) || !ddltx.AllStatementsDurable(ddltx.ClassOf(dialect)) {
		return 0
	}
	return total
}
