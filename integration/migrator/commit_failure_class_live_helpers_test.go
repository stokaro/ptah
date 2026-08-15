//go:build integration

package migrator_test

import (
	"fmt"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
)

// installPostgresDeferredCommitFault installs a constraint trigger deferred to
// the end of the transaction. The completion UPDATE succeeds, and the trigger
// it queued raises when the migrator commits.
func installPostgresDeferredCommitFault(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
) {
	c.Helper()
	execRevisionCompletionSQL(c.TB, conn, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger AS $fault$
BEGIN
	IF NEW.state = 'applied' THEN
		RAISE EXCEPTION 'reject applied revision at commit';
	END IF;
	RETURN NEW;
END;
$fault$ LANGUAGE plpgsql`, names.fault))
	execRevisionCompletionSQL(c.TB, conn, fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    version BIGINT PRIMARY KEY,
    description TEXT NOT NULL,
    applied_at TIMESTAMP NOT NULL,
    state VARCHAR(32) NOT NULL DEFAULT 'applied',
    applied INTEGER NOT NULL DEFAULT 1,
    total INTEGER NOT NULL DEFAULT 1,
    error TEXT NULL,
    error_stmt TEXT NULL,
    execution_time_ms BIGINT NOT NULL DEFAULT 0,
    checksum VARCHAR(64) NOT NULL DEFAULT ''
)`, names.revisions))
	execRevisionCompletionSQL(c.TB, conn, fmt.Sprintf(
		`CREATE CONSTRAINT TRIGGER %s AFTER UPDATE ON %s
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION %s()`,
		names.fault,
		names.revisions,
		names.fault,
	))
}

func removePostgresDeferredCommitFault(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	names revisionCompletionNames,
) {
	c.Helper()
	execRevisionCompletionSQL(c.TB, conn, fmt.Sprintf("DROP TRIGGER %s ON %s", names.fault, names.revisions))
}
