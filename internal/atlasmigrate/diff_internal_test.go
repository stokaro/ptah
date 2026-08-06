package atlasmigrate

// White-box testing required: filesystem publication races and post-replay
// fault injection cannot be staged deterministically through the exported
// GenerateDiff API.

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/atlasurl"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationreplay"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestWriteMigrationFilesAt_CollisionRejectsStalePlan(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	// A non-cooperating writer claimed version 2 after this run allocated 1..2.
	// Publishing the stale plan must fail instead of silently moving it above
	// a migration that was never replayed.
	c.Assert(os.WriteFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"), []byte("taken"), 0o600), qt.IsNil)
	contents := []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: "_concurrent_indexes", SQL: "SELECT 2;", NoTransaction: true},
	}

	batch, err := stageMigrationBatchAt(pub, "add_email", 1, contents)
	c.Assert(err, qt.IsNil)
	published, err := publishMigrationBatch(pub, batch)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during publication: .*2_add_email_concurrent_indexes\.sql already exists`)
	c.Assert(published, qt.Equals, 1)
	c.Assert(rollBackUnjournaledBatch(pub, batch, published), qt.IsNil)
	// The colliding file kept its content and no version-1 leftover remains
	// from the aborted attempt.
	taken, readErr := os.ReadFile(filepath.Join(dir, "2_add_email_concurrent_indexes.sql"))
	c.Assert(readErr, qt.IsNil)
	c.Assert(string(taken), qt.Equals, "taken")
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteMigrationFiles_FailedWriteRollsBackEarlierFiles(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	// The second file's name exceeds the filesystem's name limit, so its
	// create fails with a non-collision error after the first file was
	// already written; the batch rolls back completely.
	oversizedSuffix := "_" + strings.Repeat("x", 512)

	batch, err := stageMigrationBatch(pub, "add_email", []MigrationFileContent{
		{NameSuffix: "_transactional", SQL: "SELECT 1;"},
		{NameSuffix: oversizedSuffix, SQL: "SELECT 2;", NoTransaction: true},
	})
	c.Assert(err, qt.IsNil)
	published, err := publishMigrationBatch(pub, batch)

	c.Assert(err, qt.ErrorMatches, `publish migration file: .*`)
	c.Assert(published, qt.Equals, 1)
	c.Assert(rollBackUnjournaledBatch(pub, batch, published), qt.IsNil)
	_, leftoverErr := os.Stat(filepath.Join(dir, "1_add_email_transactional.sql"))
	c.Assert(leftoverErr, qt.ErrorIs, os.ErrNotExist)
}

func TestPublishMigrationBatch_ExclusiveCopyMode(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	batch, err := stageMigrationBatchAt(
		pub,
		"copy",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeCopy

	published, err := publishMigrationBatch(pub, batch)

	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	stagedInfo, err := os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.IsNil)
	finalInfo, err := os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.IsNil)
	c.Assert(os.SameFile(stagedInfo, finalInfo), qt.IsFalse)
	stagedContents, err := os.ReadFile(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.IsNil)
	finalContents, err := os.ReadFile(batch.paths(pub)[0])
	c.Assert(err, qt.IsNil)
	c.Assert(finalContents, qt.DeepEquals, stagedContents)
	c.Assert(rollBackUnjournaledBatch(pub, batch, published), qt.IsNil)
}

func TestWritePublicationJournal_FallsBackWithoutHardLinks(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeAtlasSum,
		Entries: []publicationEntry{{
			Staged: ".ptah-migrate-diff-staged.tmp",
			Final:  "1_copy.sql",
			Mode:   string(publicationModeCopy),
			Digest: contentDigest([]byte("sum")),
		}},
		Sum: []byte("sum"),
	}

	err := writePublicationJournalWithLink(
		pub,
		journal,
		func(string, string) error {
			return syscall.ENOTSUP
		},
	)

	c.Assert(err, qt.IsNil)
	journalPath := pub.journalPath()
	got, err := readPublicationJournal(pub)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, journal)
	backups, err := filepath.Glob(journalPath + ".*.tmp")
	c.Assert(err, qt.IsNil)
	c.Assert(backups, qt.HasLen, 1)
	c.Assert(os.WriteFile(journalPath, []byte("{"), 0o600), qt.IsNil)
	got, err = readPublicationJournal(pub)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, journal)
	c.Assert(removePublicationJournal(pub), qt.IsNil)
}

func TestPublishArtifactsLocked_PublishesCompleteBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)

	paths, err := PublishArtifactsLocked(
		t.Context(),
		dir,
		[]PublicationArtifact{
			{Name: "1_change.up.sql", Contents: []byte("SELECT 1;\n")},
			{Name: "1_change.down.sql", Contents: []byte("SELECT 2;\n")},
			{Name: "1_change.safety.json", Contents: []byte("{}\n")},
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(paths, qt.DeepEquals, []string{
		filepath.Join(dir, "1_change.up.sql"),
		filepath.Join(dir, "1_change.down.sql"),
		filepath.Join(dir, "1_change.safety.json"),
	})
	up, err := os.ReadFile(paths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "SELECT 1;\n")
	down, err := os.ReadFile(paths[1])
	c.Assert(err, qt.IsNil)
	c.Assert(string(down), qt.Equals, "SELECT 2;\n")
	report, err := os.ReadFile(paths[2])
	c.Assert(err, qt.IsNil)
	c.Assert(string(report), qt.Equals, "{}\n")
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalPath + publicationCommitMarkerSuffix)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestPublishArtifactsLocked_CollisionRollsBackWholeBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	existingPath := filepath.Join(dir, "1_change.down.sql")
	c.Assert(os.WriteFile(existingPath, []byte("existing\n"), 0o600), qt.IsNil)

	paths, err := PublishArtifactsLocked(
		t.Context(),
		dir,
		[]PublicationArtifact{
			{Name: "1_change.up.sql", Contents: []byte("SELECT 1;\n")},
			{Name: "1_change.down.sql", Contents: []byte("SELECT 2;\n")},
		},
	)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during publication: .* already exists`)
	c.Assert(paths, qt.IsNil)
	_, err = os.Stat(filepath.Join(dir, "1_change.up.sql"))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	existing, err := os.ReadFile(existingPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(existing), qt.Equals, "existing\n")
	staged, err := filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	c.Assert(staged, qt.HasLen, 0)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestStageMigrationBatch_FallsBackToExclusiveCopy(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)

	batch, err := stageMigrationBatchAtWithModeDetector(
		pub,
		"copy",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		func(root *pathguard.OpenedDirectory, stagedName string) (publicationMode, error) {
			return detectPublicationModeWithLink(
				root,
				stagedName,
				func(string, string) error {
					return syscall.ENOTSUP
				},
			)
		},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(batch.mode, qt.Equals, publicationModeCopy)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	stagedInfo, err := os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.IsNil)
	finalInfo, err := os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.IsNil)
	c.Assert(os.SameFile(stagedInfo, finalInfo), qt.IsFalse)
	c.Assert(rollBackUnjournaledBatch(pub, batch, published), qt.IsNil)
}

func TestWriteDiffArtifacts_SumPublishFailureRollsBackMigrations(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	c.Assert(os.Mkdir(filepath.Join(dir, "atlas.sum"), 0o700), qt.IsNil)
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)

	result, err := writeDiffArtifacts(
		t.Context(),
		pub,
		"add_email",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
		nil,
	)

	c.Assert(err, qt.ErrorMatches, `write atlas\.sum: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	entries, readErr := os.ReadDir(dir)
	c.Assert(readErr, qt.IsNil)
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].Name(), qt.Equals, "atlas.sum")
	c.Assert(entries[0].IsDir(), qt.IsTrue)
}

func TestRecoverPendingPublication_RollsBackInterruptedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	initialPath := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(initialPath, []byte("SELECT 1;"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	batch, err := stageMigrationBatchAt(
		pub,
		"interrupted",
		2,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		pub,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	_, err = os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_RollsBackInterruptedCopyBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	initialPath := filepath.Join(dir, "1_initial.sql")
	c.Assert(os.WriteFile(initialPath, []byte("SELECT 1;"), 0o600), qt.IsNil)
	_, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	batch, err := stageMigrationBatchAt(
		pub,
		"interrupted_copy",
		2,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeCopy
	_, _ = beginTestPublication(
		c,
		pub,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 2;"}},
	)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	_, err = os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(
		os.DirFS(dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_RollsBackInterruptedMoveBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	batch, err := stageMigrationBatchAt(
		pub,
		"interrupted_move",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeWriteThroughMove
	_, _ = beginTestPublication(
		c,
		pub,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	_, err = os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	_, err = os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestRecoverPendingPublication_CleansQuarantineOnlyStateForEveryMode(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		mode publicationMode
	}{
		{name: "hard link", mode: publicationModeHardLink},
		{name: "copy", mode: publicationModeCopy},
		{name: "write-through move", mode: publicationModeWriteThroughMove},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dir := c.TempDir()
			pub := openTestPublicationDir(c, dir)
			batch, err := stageMigrationBatchAt(
				pub,
				"interrupted",
				1,
				[]MigrationFileContent{{SQL: "SELECT 1;"}},
			)
			c.Assert(err, qt.IsNil)
			batch.mode = test.mode
			_, _ = beginTestPublication(
				c,
				pub,
				batch,
				[]MigrationFileContent{{SQL: "SELECT 1;"}},
			)
			published, err := publishMigrationBatch(pub, batch)
			c.Assert(err, qt.IsNil)
			c.Assert(published, qt.Equals, 1)
			quarantinePath := batch.stagedPaths(pub)[0] + rollbackQuarantineSuffix
			c.Assert(os.Rename(batch.paths(pub)[0], quarantinePath), qt.IsNil)
			c.Assert(removeNames(pub.dir, batch.stagedNames), qt.IsNil)

			c.Assert(recoverPendingPublication(pub), qt.IsNil)

			_, err = os.Stat(batch.paths(pub)[0])
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			_, err = os.Stat(batch.stagedPaths(pub)[0])
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			_, err = os.Stat(quarantinePath)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
			journalPath := pub.journalPath()
			_, err = os.Stat(journalPath)
			c.Assert(err, qt.ErrorIs, os.ErrNotExist)
		})
	}
}

func TestRemovePublicationJournal_RetireFailurePreservesCommitMarker(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	journalPath := pub.journalPath()
	markerPath := journalPath + publicationCommitMarkerSuffix
	c.Assert(os.WriteFile(journalPath, []byte("{}"), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(markerPath, []byte("committed"), 0o600), qt.IsNil)
	retireErr := errors.New("injected journal retirement failure")

	err := removePublicationJournalWithRetirer(
		pub,
		func(string, string) error {
			return retireErr
		},
	)

	c.Assert(err, qt.ErrorIs, retireErr)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)
	_, err = os.Stat(markerPath)
	c.Assert(err, qt.IsNil)
}

func TestRecoverPendingPublication_FinalizesMarkerCommittedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	batch, err := stageArtifactBatch(
		pub,
		[]PublicationArtifact{
			{Name: "1_change.up.sql", Contents: []byte("SELECT 1;\n")},
			{Name: "1_change.safety.json", Contents: []byte("{}\n")},
		},
	)
	c.Assert(err, qt.IsNil)
	journal, err := beginMarkerPublication(pub, batch)
	c.Assert(err, qt.IsNil)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 2)
	c.Assert(writePublicationCommitMarker(pub, journal), qt.IsNil)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	up, err := os.ReadFile(batch.paths(pub)[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(up), qt.Equals, "SELECT 1;\n")
	report, err := os.ReadFile(batch.paths(pub)[1])
	c.Assert(err, qt.IsNil)
	c.Assert(string(report), qt.Equals, "{}\n")
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalPath + publicationCommitMarkerSuffix)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalPath + publicationCleanupSuffix)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestRecoverPendingPublication_FinalizesCommittedBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	batch, err := stageMigrationBatchAt(
		pub,
		"committed",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	journal, sum := beginTestPublication(
		c,
		pub,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	_, err = writeDirSum(pub, sum)
	c.Assert(err, qt.IsNil)
	committed, err := publicationCommitted(pub, journal)
	c.Assert(err, qt.IsNil)
	c.Assert(committed, qt.IsTrue)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	contents, err := os.ReadFile(batch.paths(pub)[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 1;")
	_, err = os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	result, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	c.Assert(result.OK(), qt.IsTrue)
}

func TestRecoverPendingPublication_RejectsForeignCollision(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	batch, err := stageMigrationBatchAt(
		pub,
		"collision",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		pub,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(os.WriteFile(batch.paths(pub)[0], []byte("foreign"), 0o600), qt.IsNil)

	err = recoverPendingPublication(pub)

	c.Assert(err, qt.ErrorMatches, `cannot safely recover migration publication: .* content changed; preserved at .*`)
	_, err = os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(batch.stagedPaths(pub)[0] + rollbackQuarantineSuffix)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "foreign")
	_, err = os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.IsNil)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)
}

func TestAbortPendingPublication_RejectsForeignReplacement(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	batch, err := stageMigrationBatchAt(
		pub,
		"collision",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	_, _ = beginTestPublication(
		c,
		pub,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	c.Assert(os.Remove(batch.paths(pub)[0]), qt.IsNil)
	c.Assert(os.WriteFile(batch.paths(pub)[0], []byte("foreign"), 0o600), qt.IsNil)

	err = abortPendingPublication(pub, batch, published)

	c.Assert(err, qt.ErrorMatches, `roll back published migration files: cannot safely recover migration publication: .* content changed; preserved at .*`)
	_, err = os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(batch.stagedPaths(pub)[0] + rollbackQuarantineSuffix)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "foreign")
	_, err = os.Stat(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.IsNil)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)
}

func TestAbortPendingPublication_RejectsInPlaceHardLinkMutation(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	batch, err := stageMigrationBatchAt(
		pub,
		"mutated",
		1,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	c.Assert(err, qt.IsNil)
	batch.mode = publicationModeHardLink
	_, _ = beginTestPublication(
		c,
		pub,
		batch,
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
	)
	published, err := publishMigrationBatch(pub, batch)
	c.Assert(err, qt.IsNil)
	c.Assert(published, qt.Equals, 1)
	c.Assert(os.WriteFile(batch.paths(pub)[0], []byte("foreign"), 0o600), qt.IsNil)

	err = abortPendingPublication(pub, batch, published)

	c.Assert(err, qt.ErrorMatches, `roll back published migration files: cannot safely recover migration publication: staging file content changed; preserved .*`)
	_, err = os.Stat(batch.paths(pub)[0])
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	contents, err := os.ReadFile(batch.stagedPaths(pub)[0] + rollbackQuarantineSuffix)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "foreign")
	stagedContents, err := os.ReadFile(batch.stagedPaths(pub)[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(stagedContents), qt.Equals, "foreign")
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)
}

func TestWriteDiffArtifacts_CommitUncertainRetainsRecoverableBatch(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)

	result, err := writeDiffArtifactsWithSumWriter(
		t.Context(),
		pub,
		"uncertain",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
		nil,
		func(pub *publicationDir, sum *migratesum.SumFile) (string, error) {
			path, writeErr := writeDirSum(pub, sum)
			c.Assert(writeErr, qt.IsNil)
			return path, &migratesum.CommitUncertainError{
				Err: errors.New("injected directory sync failure"),
			}
		},
	)

	c.Assert(err, qt.ErrorMatches, `write atlas\.sum; migration publication journal retained for recovery: injected directory sync failure`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	sqlFiles, err := filepath.Glob(filepath.Join(dir, "*_uncertain.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	stagedFiles, err := filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	c.Assert(stagedFiles, qt.HasLen, 1)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.IsNil)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)
	sqlFiles, err = filepath.Glob(filepath.Join(dir, "*_uncertain.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	stagedFiles, err = filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	c.Assert(err, qt.IsNil)
	c.Assert(stagedFiles, qt.HasLen, 0)
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	verifyResult, err := migratesum.VerifyWithFormat(
		os.DirFS(dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(verifyResult.OK(), qt.IsTrue)
}

func TestWriteDiffArtifacts_RejectsUnreplayedConcurrentMigration(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	baseSnapshot, err := migrationsnapshot.CaptureStable(os.DirFS(dir))
	c.Assert(err, qt.IsNil)
	foreignPath := filepath.Join(dir, "99_foreign.sql")
	c.Assert(os.WriteFile(foreignPath, []byte("SELECT 99;"), 0o600), qt.IsNil)

	result, err := writeDiffArtifacts(
		t.Context(),
		pub,
		"planned",
		[]MigrationFileContent{{SQL: "SELECT 1;"}},
		baseSnapshot,
		nil,
	)

	c.Assert(err, qt.ErrorMatches, `migration directory changed during migrate diff planning`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	contents, err := os.ReadFile(foreignPath)
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 99;")
	plannedFiles, err := filepath.Glob(filepath.Join(dir, "*_planned.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(plannedFiles, qt.HasLen, 0)
	journalPath := pub.journalPath()
	_, err = os.Stat(journalPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func openTestPublicationDir(c *qt.C, dir string) *publicationDir {
	c.Helper()
	pub, err := openPublicationDir(dir)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(pub.close(), qt.IsNil)
	})
	return pub
}

func beginTestPublication(
	c *qt.C,
	pub *publicationDir,
	batch migrationBatch,
	contents []MigrationFileContent,
) (publicationJournal, *migratesum.SumFile) {
	c.Helper()
	baseSnapshot, err := migrationsnapshot.CaptureStable(pub.fsys())
	c.Assert(err, qt.IsNil)
	_, sum, err := preparePublicationSnapshot(baseSnapshot, batch, contents)
	c.Assert(err, qt.IsNil)
	journal, err := beginPublication(pub, batch, sum.Bytes())
	c.Assert(err, qt.IsNil)
	return journal, sum
}

func TestRecoverPendingPublication_RemovesOrphanTemps(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)
	stagedName, err := stageMigrationFile(pub, "SELECT 1;")
	c.Assert(err, qt.IsNil)
	journalPath := pub.journalPath()
	journalTemp, err := os.CreateTemp(
		filepath.Dir(journalPath),
		filepath.Base(journalPath)+".*.tmp",
	)
	c.Assert(err, qt.IsNil)
	journalTempPath := journalTemp.Name()
	c.Assert(journalTemp.Close(), qt.IsNil)

	c.Assert(recoverPendingPublication(pub), qt.IsNil)

	_, err = os.Stat(pub.path(stagedName))
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
	_, err = os.Stat(journalTempPath)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func TestWriteMigrationFiles_EmptySQLIsRejected(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	pub := openTestPublicationDir(c, dir)

	batch, err := stageMigrationBatch(pub, "noop", []MigrationFileContent{{SQL: "   \n"}})

	c.Assert(err, qt.ErrorMatches, `migration SQL is empty`)
	c.Assert(batch.names, qt.HasLen, 0)
}

func publishMigrationBatch(pub *publicationDir, batch migrationBatch) (int, error) {
	return publishMigrationBatchContext(context.Background(), pub, batch)
}

func rollBackUnjournaledBatch(
	pub *publicationDir,
	batch migrationBatch,
	published int,
) error {
	if err := removeNames(pub.dir, batch.names[:published]); err != nil {
		return fmt.Errorf("roll back published migration files: %w", err)
	}
	if err := removeNames(pub.dir, batch.stagedNames); err != nil {
		return fmt.Errorf("remove rolled back migration staging files: %w", err)
	}
	if err := pub.dir.Sync(); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return nil
}

func TestGenerateDiff_PostReplayReadFailureCleansAndReleasesLock(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	readErr := errors.New("injected schema read failure")

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return nil, readErr
		},
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorIs, readErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_FinalCleanupFailureIsNotRetried(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	cleanupErr := errors.New("injected final cleanup failure")
	cleanupCalls := 0

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: readScopedDevSchema,
		withReplayedSnapshot: func(
			ctx context.Context,
			conn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return errors.Join(
				migrationreplay.WithReplayedSnapshotLocked(ctx, conn, snapshot, format, consume),
				cleanupErr,
			)
		},
	})

	c.Assert(err, qt.ErrorIs, cleanupErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_JoinsPostReplayFailureAndCleanupFailure(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	readErr := errors.New("injected schema read failure")
	cleanupErr := errors.New("injected cleanup failure")
	cleanupCalls := 0

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema: func(
			*dbschema.DatabaseConnection,
			[]string,
			string,
		) (*dbschematypes.DBSchema, error) {
			return nil, readErr
		},
		withReplayedSnapshot: func(
			ctx context.Context,
			conn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return errors.Join(
				migrationreplay.WithReplayedSnapshotLocked(ctx, conn, snapshot, format, consume),
				cleanupErr,
			)
		},
	})

	c.Assert(err, qt.ErrorIs, readErr)
	c.Assert(err, qt.ErrorIs, cleanupErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_CancellationDuringCleanupPreventsArtifacts(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	ctx, cancel := context.WithCancel(t.Context())
	cleanupCalls := 0

	result, err := generateDiff(ctx, conn, opts, diffRuntime{
		readDevSchema: readScopedDevSchema,
		withReplayedSnapshot: func(
			replayCtx context.Context,
			replayConn *dbschema.DatabaseConnection,
			snapshot fs.FS,
			format migrator.MigrationDirFormat,
			consume func(*dbschema.DatabaseConnection) error,
		) error {
			cleanupCalls++
			return migrationreplay.WithReplayedSnapshotLocked(
				replayCtx,
				replayConn,
				snapshot,
				format,
				func(conn *dbschema.DatabaseConnection) error {
					consumeErr := consume(conn)
					cancel()
					return errors.Join(consumeErr, replayCtx.Err())
				},
			)
		},
	})

	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(cleanupCalls, qt.Equals, 1)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestGenerateDiff_PreparePublicationFailurePreservesExistingArtifacts(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	previousSum, err := os.ReadFile(
		filepath.Join(opts.Dir, migratesum.AtlasFileName),
	)
	c.Assert(err, qt.IsNil)
	prepareErr := errors.New("injected preparation failure")
	opts.PreparePublication = func(stagedPaths []string) error {
		writeErr := os.WriteFile(
			stagedPaths[0],
			[]byte("SELECT 99;\n"),
			0o600,
		)
		return errors.Join(writeErr, prepareErr)
	}

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorIs, prepareErr)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	sqlFiles, err := filepath.Glob(filepath.Join(opts.Dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 1)
	restoredSum, err := os.ReadFile(
		filepath.Join(opts.Dir, migratesum.AtlasFileName),
	)
	c.Assert(err, qt.IsNil)
	c.Assert(restoredSum, qt.DeepEquals, previousSum)
	verification, err := migratesum.VerifyWithFormat(
		os.DirFS(opts.Dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(verification.OK(), qt.IsTrue)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryLockReleased(c, opts.Dir)
}

func TestGenerateDiff_PreparedContentsArePublishedWithMatchingChecksum(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	opts.PreparePublication = func(stagedPaths []string) error {
		return os.WriteFile(
			stagedPaths[0],
			[]byte("SELECT 99;\n"),
			0o600,
		)
	}

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.MigrationPaths, qt.HasLen, 1)
	sqlFiles, err := filepath.Glob(filepath.Join(opts.Dir, "*.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(sqlFiles, qt.HasLen, 2)
	contents, err := os.ReadFile(result.MigrationPaths[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(contents), qt.Equals, "SELECT 99;\n")
	verification, err := migratesum.VerifyWithFormat(
		os.DirFS(opts.Dir),
		migrator.MigrationDirFormatAtlas,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(verification.OK(), qt.IsTrue)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryLockReleased(c, opts.Dir)
}

func TestGenerateDiff_PreparePublicationRunsUnderDirectoryLock(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	var competingLock *dirLock
	var competingErr error
	opts.LockTimeout = time.Millisecond
	opts.PreparePublication = func([]string) error {
		competingLock, competingErr = acquireDirLock(
			t.Context(),
			opts.Dir,
			opts.LockTimeout,
		)
		return competingErr
	}

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorMatches, `.*migration directory lock timeout after 1ms: .*`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	c.Assert(competingLock, qt.IsNil)
	c.Assert(competingErr, qt.IsNotNil)
	assertSQLiteCleanupObjectCount(c, conn, 0)
	assertDiffDirectoryReleased(c, opts.Dir)
}

func TestCanonicalMigrationDirResolvesSymlinkAlias(t *testing.T) {
	c := qt.New(t)
	root := c.TempDir()
	realDir := filepath.Join(root, "real", "migrations")
	c.Assert(os.MkdirAll(realDir, 0o755), qt.IsNil)
	aliasDir := filepath.Join(root, "alias")
	c.Assert(os.Symlink(realDir, aliasDir), qt.IsNil)

	canonicalReal, err := canonicalMigrationDir(realDir)
	c.Assert(err, qt.IsNil)
	canonicalAlias, err := canonicalMigrationDir(aliasDir)
	c.Assert(err, qt.IsNil)

	c.Assert(canonicalAlias, qt.Equals, canonicalReal)
	c.Assert(migrationDirLockPath(canonicalAlias), qt.Equals, migrationDirLockPath(canonicalReal))
}

func TestTryAcquireDirLock_ReclaimsStaleFile(t *testing.T) {
	c := qt.New(t)
	lockPath := filepath.Join(c.TempDir(), ".migrations"+lockFileName)
	c.Assert(os.WriteFile(lockPath, []byte("stale"), 0o600), qt.IsNil)

	lock, err := tryAcquireDirLock(lockPath)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.release(), qt.IsNil)

	info, err := os.Stat(lockPath)
	c.Assert(err, qt.IsNil)
	c.Assert(info.Mode().IsRegular(), qt.IsTrue)
	lock, err = tryAcquireDirLock(lockPath)
	c.Assert(err, qt.IsNil)
	c.Assert(lock.release(), qt.IsNil)
}

func TestGenerateDiff_SerializesSQLiteDevDatabaseAcrossDirectories(t *testing.T) {
	c := qt.New(t)
	conn, opts := prepareGenerateDiffFaultTest(c)
	opts.Dir = filepath.Join(c.TempDir(), "other-migrations")
	opts.LockTimeout = time.Millisecond
	devLock, err := acquireDevDatabaseLock(t.Context(), conn, 0)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(devLock.release(), qt.IsNil)
	})

	result, err := generateDiff(t.Context(), conn, opts, diffRuntime{
		readDevSchema:        readScopedDevSchema,
		withReplayedSnapshot: migrationreplay.WithReplayedSnapshotLocked,
	})

	c.Assert(err, qt.ErrorMatches, `acquire migrate diff dev database lock: acquire sqlite dev database realm lock: lock timeout after 1ms`)
	c.Assert(result.MigrationPaths, qt.HasLen, 0)
	_, err = os.Stat(opts.Dir)
	c.Assert(err, qt.ErrorIs, os.ErrNotExist)
}

func prepareGenerateDiffFaultTest(c *qt.C) (*dbschema.DatabaseConnection, DiffOptions) {
	c.Helper()
	dir := c.TempDir()
	migrationsDir := filepath.Join(dir, "migrations")
	c.Assert(os.MkdirAll(migrationsDir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "1_init.sql"),
		[]byte(`
CREATE TABLE replayed_users (id INTEGER PRIMARY KEY);
CREATE VIEW replayed_user_ids AS SELECT id FROM replayed_users;
`),
		0o600,
	), qt.IsNil)
	_, err := migratesum.WriteWithFormat(migrationsDir, migrator.MigrationDirFormatAtlas)
	c.Assert(err, qt.IsNil)
	schemaPath := filepath.Join(dir, "schema.sql")
	c.Assert(os.WriteFile(
		schemaPath,
		[]byte("CREATE TABLE desired_users (id INTEGER PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	desired, err := atlassource.ClassifySet(
		"--to",
		[]string{"file://" + schemaPath},
		atlassource.ProjectEnv{},
	)
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(
		c.Context(),
		atlasurl.SQLiteURLFromPath(filepath.Join(dir, "dev.db")),
	)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	return conn, DiffOptions{
		Dir:     migrationsDir,
		Desired: desired,
		Name:    "fault_injection",
	}
}

func assertSQLiteCleanupObjectCount(
	c *qt.C,
	conn *dbschema.DatabaseConnection,
	want int,
) {
	c.Helper()
	var count int
	err := conn.QueryRowContext(c.Context(), `
		SELECT COUNT(*)
		FROM pragma_table_list
		WHERE schema = ?
		  AND type IN ('table', 'view', 'virtual')
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
	`, "main").Scan(&count)
	c.Assert(err, qt.IsNil)
	c.Assert(count, qt.Equals, want)
}

func assertDiffDirectoryReleased(c *qt.C, dir string) {
	c.Helper()
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)
	assertDiffDirectoryLockReleased(c, dir)
}

func assertDiffDirectoryLockReleased(c *qt.C, dir string) {
	c.Helper()
	lock, err := tryAcquireDirLock(migrationDirLockPath(dir))
	c.Assert(err, qt.IsNil)
	c.Assert(lock.release(), qt.IsNil)
}

// TestDiffOptionsVerifyDirRoutesThroughTheCallersPredicate pins the seam
// stokaro/ptah#1086 added.
//
// `migrate diff` re-checks the directory's integrity file once the migration
// directory lock is held, and until #1086 that recheck was a private verifier
// with rules of its own: it accepted a directory carrying no atlas.sum at all,
// and reported a stale one in wording no other verb uses. The compatibility
// surface now supplies the same predicate its preflight refused with, so a
// directory edited between the two is refused with the community binary's bytes
// instead of a second verifier's.
//
// Reverting the routing -- calling verifyDirSum unconditionally -- prints
// `got nil error but want non-nil` on the "caller's predicate refuses" row,
// because the default accepts exactly what the caller rejects. The nil row is
// the control: it must keep answering nil, so the seam cannot be "always
// refuse".
func TestDiffOptionsVerifyDirRoutesThroughTheCallersPredicate(t *testing.T) {
	errCallerRefused := errors.New("caller refused")

	tests := []struct {
		name string
		// opts carries the hook under test.
		opts DiffOptions
		// check states what the returned error must be.
		check func(c *qt.C, err error)
	}{
		{
			name: "nil hook keeps the permissive default",
			opts: DiffOptions{},
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.IsNil)
			},
		},
		{
			name: "caller's predicate refuses",
			opts: DiffOptions{VerifyDir: func(fs.FS) error { return errCallerRefused }},
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.ErrorIs, errCallerRefused)
			},
		},
		{
			name: "caller's predicate accepts",
			opts: DiffOptions{VerifyDir: func(fs.FS) error { return nil }},
			check: func(c *qt.C, err error) {
				c.Assert(err, qt.IsNil)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			// A directory holding one migration and no atlas.sum: the state the
			// default accepts and the compatibility gate refuses.
			dir := c.TempDir()
			c.Assert(os.WriteFile(
				filepath.Join(dir, "1_init.sql"),
				[]byte("CREATE TABLE users (id INTEGER PRIMARY KEY);\n"),
				0o600,
			), qt.IsNil)
			snapshot, captureErr := migrationsnapshot.CaptureStable(os.DirFS(dir))
			c.Assert(captureErr, qt.IsNil)

			tt.check(c, tt.opts.verifyDir(snapshot))
		})
	}
}
