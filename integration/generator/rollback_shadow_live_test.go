//go:build integration

package generator_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/migration/shadow"
)

func TestVerifyRollback_FailurePathRejectsLiveDialectMismatch(t *testing.T) {
	c := qt.New(t)
	targetConn := openRollbackTarget(c, requireRollbackPostgresURL(c))

	err := shadow.VerifyRollback(c.Context(), shadow.RollbackVerifyOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: "sqlite://" + filepath.Join(t.TempDir(), "shadow.db"),
		FS:                os.DirFS(t.TempDir()),
	})

	c.Assert(
		err,
		qt.ErrorMatches,
		`rollback verification failed: shadow database dialect "sqlite" does not match target dialect "postgres"`,
	)
}

func TestVerifyRollback_FailurePathRejectsDriverOverrideAliasLive(t *testing.T) {
	c := qt.New(t)
	targetURL := rollbackPostgresDatabaseURL(
		c,
		requireRollbackPostgresURL(c),
		"postgres",
	)
	shadowURL := rollbackPostgresDriverOverrideURL(c, targetURL)
	targetConn := openRollbackTarget(c, targetURL)
	fastPathSame, err := atlasurl.SameDatabaseEndpoint(targetURL, shadowURL)
	c.Assert(err, qt.IsNil)
	c.Assert(fastPathSame, qt.IsTrue)

	err = shadow.VerifyRollback(c.Context(), shadow.RollbackVerifyOptions{
		TargetConnection:  targetConn,
		ShadowDatabaseURL: shadowURL,
		FS:                os.DirFS(t.TempDir()),
	})

	c.Assert(
		err,
		qt.ErrorMatches,
		`rollback verification failed: shadow database must be distinct from target database`,
	)
}
