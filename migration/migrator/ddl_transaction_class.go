package migrator

import (
	"context"

	"go.5x5.cz/ptah/internal/ddltx"
)

// withCompletedBodyProgress records that every statement of the migration body
// executed successfully and that whatever failed afterwards was bookkeeping,
// not the migration.
//
// The failure the migrator is about to record carries no statement index,
// because no statement failed, so the progress derived from it is zero. Zero is
// the right answer where the body rolls back with the bookkeeping and the wrong
// one where the body is already durable: a later `--allow-dirty` retry resumes
// at applied+1 and replays a statement the database has already run. On
// ClickHouse that meant re-issuing the body's CREATE TABLE and failing with
// "table already exists", which left the revision dirty for good and put
// automatic recovery out of reach.
//
// The claim is limited to [ddltx.AllStatementsDurable] targets. The MySQL
// family is excluded even though its DDL is durable too, because only some of
// its statements are: the revision-row witness installed by
// [Migrator.withTransactionalProgressRecorder] already reported which, and
// claiming the whole body would record rolled-back DML as applied.
//
// It travels as the committed-statement floor [withMigrationResume] already
// carries, because that is the same quantity under a different name -- "these
// statements are committed, do not run them again" -- and
// failMigrationRevisionWithMode already raises the recorded progress to it. The
// floor is normally installed from an earlier attempt's progress; here this
// attempt is the one that committed them.
func withCompletedBodyProgress(ctx context.Context, dialect string, total int) context.Context {
	if !ddltx.AllStatementsDurable(ddltx.ClassOf(dialect)) {
		return ctx
	}
	return withMigrationResume(ctx, total+1)
}
