package atlasmigrate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/stokaro/ptah/internal/fsdurable"
	"github.com/stokaro/ptah/internal/fsnapshot"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	publicationJournalVersion     = 5
	publicationJournalSuffix      = ".ptah-migrate-diff.pending"
	publicationCommitMarkerSuffix = ".committed"
	publicationCleanupSuffix      = ".cleanup"
	stagedMigrationPattern        = ".ptah-migrate-diff-*.tmp"

	publicationCommitModeAtlasSum = "atlas-sum"
	publicationCommitModeMarker   = "journal-marker"
)

type publicationMode string

const (
	publicationModeHardLink         publicationMode = "hard-link"
	publicationModeCopy             publicationMode = "exclusive-copy"
	publicationModeWriteThroughMove publicationMode = "write-through-move"
)

type publicationEntry struct {
	Staged string `json:"staged"`
	Final  string `json:"final"`
	Mode   string `json:"mode"`
	Digest string `json:"digest"`
}

type publicationJournal struct {
	Version    int                `json:"version"`
	CommitMode string             `json:"commit_mode"`
	Entries    []publicationEntry `json:"entries"`
	Sum        []byte             `json:"sum,omitempty"`
}

// PublicationArtifact is one immutable file in a journaled publication batch.
type PublicationArtifact struct {
	Name     string
	Contents []byte
}

type migrationBatch struct {
	paths       []string
	stagedPaths []string
	digests     []string
	mode        publicationMode
}

// writeDiffArtifacts durably publishes one batch of migration files and then
// atlas.sum. A journal next to the migration directory makes an interrupted
// publication recoverable by the next lock holder.
func writeDiffArtifacts(
	ctx context.Context,
	dir, name string,
	contents []MigrationFileContent,
	baseSnapshot fsnapshot.Snapshot,
	prepare func([]string) error,
) (DiffResult, error) {
	return writeDiffArtifactsWithSumWriter(
		ctx,
		dir,
		name,
		contents,
		baseSnapshot,
		prepare,
		writeDirSum,
	)
}

func writeDiffArtifactsWithSumWriter(
	ctx context.Context,
	dir, name string,
	contents []MigrationFileContent,
	baseSnapshot fsnapshot.Snapshot,
	prepare func([]string) error,
	writeSum func(string, *migratesum.SumFile) (string, error),
) (DiffResult, error) {
	if err := ctx.Err(); err != nil {
		return DiffResult{}, err
	}
	if err := recoverPendingPublication(dir); err != nil {
		return DiffResult{}, fmt.Errorf("recover previous migration artifact publication: %w", err)
	}
	version, err := nextMigrationVersionFS(baseSnapshot)
	if err != nil {
		return DiffResult{}, err
	}
	batch, err := stageMigrationBatchAt(dir, name, version, contents)
	if err != nil {
		return DiffResult{}, err
	}
	if err := prepareStagedMigrationBatch(ctx, batch, prepare); err != nil {
		return DiffResult{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	stagedContents, err := readStagedMigrationContents(&batch)
	if err != nil {
		return DiffResult{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	publishedSnapshot, sum, err := preparePublicationSnapshot(
		baseSnapshot,
		batch,
		stagedContents,
	)
	if err != nil {
		return DiffResult{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	journal, err := beginPublication(dir, batch, sum.Bytes())
	if err != nil {
		return DiffResult{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	published, err := publishMigrationBatchContext(ctx, dir, batch)
	if err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	if err := verifyMigrationDirUnchanged(dir, publishedSnapshot); err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	if err := ctx.Err(); err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	sumPath, err := writeSum(dir, sum)
	if err != nil {
		if migratesum.IsCommitUncertain(err) {
			return DiffResult{}, fmt.Errorf(
				"write atlas.sum; migration publication journal retained for recovery: %w",
				err,
			)
		}
		return DiffResult{}, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	result := DiffResult{MigrationPaths: batch.paths, SumPath: sumPath}
	journalPath, err := publicationJournalPath(dir)
	if err != nil {
		return result, err
	}
	committed, err := publicationCommitted(dir, journal)
	if err != nil {
		return result, fmt.Errorf("verify migration publication commit marker: %w", err)
	}
	if !committed {
		return result, errors.New("atlas.sum does not match the journaled migration publication")
	}
	if err := finalizeCommittedPublication(dir, journalPath, journal); err != nil {
		return result, fmt.Errorf("finalize migration artifact publication: %w", err)
	}
	return result, nil
}

// PublishArtifactsLocked durably publishes all artifacts as one batch. The
// caller must hold the migration-directory lock for dir.
func PublishArtifactsLocked(
	ctx context.Context,
	dir string,
	artifacts []PublicationArtifact,
) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := recoverPendingPublication(dir); err != nil {
		return nil, fmt.Errorf("recover previous artifact publication: %w", err)
	}
	batch, err := stageArtifactBatch(dir, artifacts)
	if err != nil {
		return nil, err
	}
	journal, err := beginMarkerPublication(dir, batch)
	if err != nil {
		return nil, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	published, err := publishMigrationBatchContext(ctx, dir, batch)
	if err != nil {
		return nil, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	if err := writePublicationCommitMarker(dir, journal); err != nil {
		committed, commitErr := publicationCommitted(dir, journal)
		if committed && commitErr == nil {
			return nil, fmt.Errorf(
				"write artifact publication commit marker; journal retained for recovery: %w",
				err,
			)
		}
		return nil, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	journalPath, err := publicationJournalPath(dir)
	if err != nil {
		return nil, err
	}
	committed, err := publicationCommitted(dir, journal)
	if err != nil {
		return nil, fmt.Errorf("verify artifact publication commit marker: %w", err)
	}
	if !committed {
		return nil, errors.New("artifact publication commit marker does not match its journal")
	}
	if err := finalizeCommittedPublication(dir, journalPath, journal); err != nil {
		return slices.Clone(batch.paths), fmt.Errorf("finalize artifact publication: %w", err)
	}
	return slices.Clone(batch.paths), nil
}

func prepareStagedMigrationBatch(
	ctx context.Context,
	batch migrationBatch,
	prepare func([]string) error,
) error {
	if prepare != nil {
		if err := prepare(slices.Clone(batch.stagedPaths)); err != nil {
			return fmt.Errorf("prepare migration files for publication: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, path := range batch.stagedPaths {
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("inspect staged migration file after preparation: %w", err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("staged migration file is not a regular file: %s", path)
		}
		file, err := os.OpenFile(path, os.O_RDWR, 0)
		if err != nil {
			return fmt.Errorf("open staged migration file after preparation: %w", err)
		}
		openedInfo, err := file.Stat()
		if err != nil {
			return errors.Join(
				fmt.Errorf("inspect opened staged migration file: %w", err),
				file.Close(),
			)
		}
		if !os.SameFile(info, openedInfo) {
			return errors.Join(
				fmt.Errorf("staged migration file changed while being opened: %s", path),
				file.Close(),
			)
		}
		if err := errors.Join(file.Sync(), file.Close()); err != nil {
			return fmt.Errorf("sync staged migration file after preparation: %w", err)
		}
	}
	return nil
}

func readStagedMigrationContents(batch *migrationBatch) ([]MigrationFileContent, error) {
	contents := make([]MigrationFileContent, len(batch.stagedPaths))
	batch.digests = make([]string, len(batch.stagedPaths))
	for i, path := range batch.stagedPaths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read staged migration file after preparation: %w", err)
		}
		contents[i] = MigrationFileContent{SQL: string(data)}
		batch.digests[i] = contentDigest(data)
	}
	return contents, nil
}

func contentDigest(contents []byte) string {
	sum := sha256.Sum256(contents)
	return hex.EncodeToString(sum[:])
}

func preparePublicationSnapshot(
	baseSnapshot fsnapshot.Snapshot,
	batch migrationBatch,
	contents []MigrationFileContent,
) (fsnapshot.Snapshot, *migratesum.SumFile, error) {
	files := make(map[string][]byte, len(batch.paths))
	for i, path := range batch.paths {
		files[filepath.Base(path)] = []byte(contents[i].SQL)
	}
	publishedSnapshot, err := baseSnapshot.WithFiles(files)
	if err != nil {
		return fsnapshot.Snapshot{}, nil, fmt.Errorf("build published migration snapshot: %w", err)
	}
	sum, err := migratesum.ComputeWithFormat(
		publishedSnapshot,
		migrator.MigrationDirFormatAtlas,
	)
	if err != nil {
		return fsnapshot.Snapshot{}, nil, fmt.Errorf("compute published migration checksum: %w", err)
	}
	return publishedSnapshot, sum, nil
}

func beginPublication(
	dir string,
	batch migrationBatch,
	sum []byte,
) (publicationJournal, error) {
	// Persist the staging directory entries before making the journal durable.
	// Once the journal exists, recovery relies on the staging links to prove
	// ownership of any published final paths.
	if err := fsdurable.SyncDir(dir); err != nil {
		return publicationJournal{}, fmt.Errorf("sync staged migration files: %w", err)
	}

	entries := make([]publicationEntry, len(batch.paths))
	for i := range batch.paths {
		entries[i] = publicationEntry{
			Staged: filepath.Base(batch.stagedPaths[i]),
			Final:  filepath.Base(batch.paths[i]),
			Mode:   string(batch.mode),
			Digest: batch.digests[i],
		}
	}
	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeAtlasSum,
		Entries:    entries,
		Sum:        slices.Clone(sum),
	}
	if err := writePublicationJournal(dir, journal); err != nil {
		return publicationJournal{}, fmt.Errorf("write migration publication journal: %w", err)
	}
	return journal, nil
}

func beginMarkerPublication(
	dir string,
	batch migrationBatch,
) (publicationJournal, error) {
	if err := fsdurable.SyncDir(dir); err != nil {
		return publicationJournal{}, fmt.Errorf("sync staged artifact files: %w", err)
	}
	entries := make([]publicationEntry, len(batch.paths))
	for i := range batch.paths {
		entries[i] = publicationEntry{
			Staged: filepath.Base(batch.stagedPaths[i]),
			Final:  filepath.Base(batch.paths[i]),
			Mode:   string(batch.mode),
			Digest: batch.digests[i],
		}
	}
	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeMarker,
		Entries:    entries,
	}
	if err := writePublicationJournal(dir, journal); err != nil {
		return publicationJournal{}, fmt.Errorf("write artifact publication journal: %w", err)
	}
	return journal, nil
}

func publishMigrationBatchContext(
	ctx context.Context,
	dir string,
	batch migrationBatch,
) (int, error) {
	for i, stagedPath := range batch.stagedPaths {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		err := publishStagedFile(stagedPath, batch.paths[i], batch.mode)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return i, fmt.Errorf(
					"migration directory changed during publication: %s already exists",
					batch.paths[i],
				)
			}
			return i, fmt.Errorf("publish migration file: %w", err)
		}
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return len(batch.paths), fmt.Errorf("sync published migration files: %w", err)
	}
	return len(batch.paths), nil
}

func publishStagedFile(
	stagedPath, finalPath string,
	mode publicationMode,
) error {
	switch mode {
	case publicationModeHardLink:
		return os.Link(stagedPath, finalPath)
	case publicationModeCopy:
		return copyFileExclusive(stagedPath, finalPath)
	case publicationModeWriteThroughMove:
		return fsdurable.MoveFileNoReplace(stagedPath, finalPath)
	default:
		return fmt.Errorf("unsupported migration publication mode %q", mode)
	}
}

func copyFileExclusive(sourcePath, destinationPath string) (resultErr error) {
	contents, err := os.ReadFile(sourcePath)
	if err != nil {
		return err
	}
	destination, err := os.OpenFile(
		destinationPath,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		0o644,
	)
	if err != nil {
		return err
	}
	complete := false
	defer func() {
		if !complete {
			resultErr = errors.Join(
				resultErr,
				destination.Close(),
				removeFiles([]string{destinationPath}),
			)
		}
	}()
	if _, err := destination.Write(contents); err != nil {
		return err
	}
	if err := destination.Sync(); err != nil {
		return err
	}
	if err := destination.Close(); err != nil {
		return err
	}
	complete = true
	return nil
}

func abortPendingPublication(dir string, batch migrationBatch, published int) error {
	for i := range published {
		if err := rollBackPublicationEntry(
			batch.stagedPaths[i],
			batch.paths[i],
			batch.digests[i],
		); err != nil {
			return fmt.Errorf("roll back published migration files: %w", err)
		}
	}
	if err := removeFiles(batch.stagedPaths[published:]); err != nil {
		return fmt.Errorf("remove rolled back migration staging files: %w", err)
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	journalPath, err := publicationJournalPath(dir)
	if err != nil {
		return err
	}
	return removePublicationJournal(journalPath)
}

func recoverPendingPublication(dir string) error {
	journalPath, err := publicationJournalPath(dir)
	if err != nil {
		return err
	}
	journal, err := readPublicationJournal(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return removeOrphanPublicationTemps(dir, journalPath)
	}
	if err != nil {
		return err
	}
	committed, err := publicationCommitted(dir, journal)
	if err != nil {
		return err
	}
	if committed {
		return finalizeCommittedPublication(dir, journalPath, journal)
	}
	return rollBackPendingPublication(dir, journalPath, journal)
}

// RecoverPendingPublicationLocked resolves an interrupted artifact publication.
// The caller must hold the migration-directory lock for dir.
func RecoverPendingPublicationLocked(dir string) error {
	return recoverPendingPublication(dir)
}

// RecoverPendingPublication resolves an interrupted artifact publication,
// acquiring the migration-directory lock unless ctx proves that the caller
// already holds it.
func RecoverPendingPublication(ctx context.Context, dir string) error {
	held, err := migrationDirectoryLockHeld(ctx, dir)
	if err != nil {
		return fmt.Errorf("resolve migration directory lock path: %w", err)
	}
	if held {
		return recoverPendingPublication(dir)
	}
	return WithMigrationDirectoryLock(ctx, dir, 0, func(context.Context) error {
		return recoverPendingPublication(dir)
	})
}

func publicationCommitted(dir string, journal publicationJournal) (bool, error) {
	switch journal.CommitMode {
	case publicationCommitModeAtlasSum:
		contents, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return bytes.Equal(contents, journal.Sum), nil
	case publicationCommitModeMarker:
		journalPath, err := publicationJournalPath(dir)
		if err != nil {
			return false, err
		}
		contents, err := os.ReadFile(publicationCommitMarkerPath(journalPath))
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		expected, err := publicationJournalDigest(journal)
		if err != nil {
			return false, err
		}
		return bytes.Equal(contents, expected), nil
	default:
		return false, fmt.Errorf(
			"unsupported publication commit mode %q",
			journal.CommitMode,
		)
	}
}

func finalizeCommittedPublication(
	dir, journalPath string,
	journal publicationJournal,
) error {
	stagedPaths := publicationStagedPaths(dir, journal)
	if err := removeFiles(stagedPaths); err != nil {
		return fmt.Errorf("remove committed migration staging files: %w", err)
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync committed migration directory: %w", err)
	}
	return removePublicationJournal(journalPath)
}

func rollBackPendingPublication(
	dir, journalPath string,
	journal publicationJournal,
) error {
	for _, entry := range journal.Entries {
		stagedPath := filepath.Join(dir, entry.Staged)
		finalPath := filepath.Join(dir, entry.Final)
		if err := rollBackPublicationEntry(
			stagedPath,
			finalPath,
			entry.Digest,
		); err != nil {
			return err
		}
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return removePublicationJournal(journalPath)
}

func rollBackPublicationEntry(
	stagedPath, finalPath, expectedDigest string,
) error {
	quarantineDir := stagedPath + ".rollback"
	quarantinePath := filepath.Join(quarantineDir, "published")

	if err := quarantinePublishedMigration(finalPath, quarantineDir, quarantinePath); err != nil {
		return err
	}
	stagedDigest, stagedErr := fileDigest(stagedPath)
	quarantinedDigest, quarantineErr := fileDigest(quarantinePath)
	return reconcileRollbackFiles(rollbackFileState{
		stagedPath:        stagedPath,
		finalPath:         finalPath,
		quarantineDir:     quarantineDir,
		quarantinePath:    quarantinePath,
		expectedDigest:    expectedDigest,
		stagedDigest:      stagedDigest,
		stagedErr:         stagedErr,
		quarantinedDigest: quarantinedDigest,
		quarantineErr:     quarantineErr,
	})
}

func quarantinePublishedMigration(finalPath, quarantineDir, quarantinePath string) error {
	if err := os.Mkdir(quarantineDir, 0700); err != nil && !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("create migration rollback quarantine: %w", err)
	}
	if err := fsdurable.SyncDir(filepath.Dir(quarantineDir)); err != nil {
		return fmt.Errorf("sync migration rollback quarantine directory: %w", err)
	}
	if _, err := os.Stat(quarantinePath); errors.Is(err, os.ErrNotExist) {
		if moveErr := fsdurable.MoveFileNoReplace(finalPath, quarantinePath); moveErr != nil {
			if !errors.Is(moveErr, os.ErrNotExist) {
				return fmt.Errorf("quarantine published migration file: %w", moveErr)
			}
		} else if err := syncQuarantineMove(finalPath, quarantineDir); err != nil {
			return err
		}
	} else if err != nil {
		return fmt.Errorf("inspect quarantined migration file: %w", err)
	}
	return nil
}

type rollbackFileState struct {
	stagedPath        string
	finalPath         string
	quarantineDir     string
	quarantinePath    string
	expectedDigest    string
	stagedDigest      string
	stagedErr         error
	quarantinedDigest string
	quarantineErr     error
}

func reconcileRollbackFiles(state rollbackFileState) error {
	switch {
	case errors.Is(state.stagedErr, os.ErrNotExist) && errors.Is(state.quarantineErr, os.ErrNotExist):
		return removeEmptyQuarantineDir(state.quarantineDir)
	case state.stagedErr == nil && errors.Is(state.quarantineErr, os.ErrNotExist):
		if state.stagedDigest != state.expectedDigest {
			return fmt.Errorf(
				"cannot safely recover migration publication: staging file content changed; preserved %s",
				state.stagedPath,
			)
		}
		return removeRollbackFiles(state.quarantineDir, state.stagedPath)
	case errors.Is(state.stagedErr, os.ErrNotExist) &&
		state.quarantineErr == nil:
		if state.quarantinedDigest != state.expectedDigest {
			return fmt.Errorf(
				"cannot safely recover migration publication: %s content changed; preserved at %s",
				state.finalPath,
				state.quarantinePath,
			)
		}
		return removeRollbackFiles(state.quarantineDir, state.quarantinePath)
	case state.stagedErr != nil:
		return fmt.Errorf("inspect staged migration file: %w", state.stagedErr)
	case state.quarantineErr != nil:
		return fmt.Errorf("inspect quarantined migration file: %w", state.quarantineErr)
	case state.stagedDigest != state.expectedDigest:
		return fmt.Errorf(
			"cannot safely recover migration publication: staging file content changed; preserved %s and %s",
			state.stagedPath,
			state.quarantinePath,
		)
	case state.quarantinedDigest != state.expectedDigest:
		return fmt.Errorf(
			"cannot safely recover migration publication: %s content changed; preserved at %s",
			state.finalPath,
			state.quarantinePath,
		)
	default:
		return removeRollbackFiles(state.quarantineDir, state.quarantinePath, state.stagedPath)
	}
}

func syncQuarantineMove(finalPath, quarantineDir string) error {
	if err := fsdurable.SyncDir(filepath.Dir(finalPath)); err != nil {
		return fmt.Errorf("sync migration directory after quarantine move: %w", err)
	}
	if err := fsdurable.SyncDir(quarantineDir); err != nil {
		return fmt.Errorf("sync migration rollback quarantine: %w", err)
	}
	return nil
}

func removeRollbackFiles(quarantineDir string, paths ...string) error {
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("remove %s: %w", path, err)
		}
	}
	return removeEmptyQuarantineDir(quarantineDir)
}

func fileDigest(path string) (string, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	return contentDigest(contents), nil
}

func removeEmptyQuarantineDir(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func removePublicationJournal(journalPath string) error {
	return removePublicationJournalWithRetirer(journalPath, retirePublicationJournal)
}

func removePublicationJournalWithRetirer(
	journalPath string,
	retire func(string, string) error,
) error {
	journalTemps, err := filepath.Glob(journalPath + ".*.tmp")
	if err != nil {
		return fmt.Errorf("find migration publication journal backups: %w", err)
	}
	cleanupPath := journalPath + publicationCleanupSuffix
	if err := retire(journalPath, cleanupPath); err != nil {
		return fmt.Errorf("retire migration publication journal: %w", err)
	}
	parent := filepath.Dir(journalPath)
	if err := fsdurable.SyncDir(parent); err != nil {
		return fmt.Errorf("sync retired migration publication journal: %w", err)
	}
	paths := append(
		[]string{publicationCommitMarkerPath(journalPath), cleanupPath},
		journalTemps...,
	)
	if err := removeFiles(paths); err != nil {
		return fmt.Errorf("remove retired migration publication metadata: %w", err)
	}
	if err := fsdurable.SyncDir(parent); err != nil {
		return fmt.Errorf("sync migration publication journal directory: %w", err)
	}
	return nil
}

func retirePublicationJournal(journalPath, cleanupPath string) error {
	_, err := os.Stat(journalPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return fsdurable.ReplaceFile(journalPath, cleanupPath)
}

func removeOrphanPublicationTemps(dir, journalPath string) error {
	stagedPaths, err := filepath.Glob(filepath.Join(dir, stagedMigrationPattern))
	if err != nil {
		return fmt.Errorf("find orphan migration staging files: %w", err)
	}
	journalTemps, err := filepath.Glob(journalPath + ".*.tmp")
	if err != nil {
		return fmt.Errorf("find orphan migration publication journals: %w", err)
	}
	orphanPaths := slices.Concat(
		stagedPaths,
		journalTemps,
		[]string{
			publicationCommitMarkerPath(journalPath),
			journalPath + publicationCleanupSuffix,
		},
	)
	if err := removeFiles(orphanPaths); err != nil {
		return fmt.Errorf("remove orphan migration publication files: %w", err)
	}
	if len(stagedPaths) > 0 {
		if err := fsdurable.SyncDir(dir); err != nil {
			return fmt.Errorf("sync migration directory after orphan cleanup: %w", err)
		}
	}
	if len(journalTemps) > 0 {
		if err := fsdurable.SyncDir(filepath.Dir(journalPath)); err != nil {
			return fmt.Errorf("sync publication journal directory after orphan cleanup: %w", err)
		}
	}
	return nil
}

func writePublicationCommitMarker(
	dir string,
	journal publicationJournal,
) error {
	journalPath, err := publicationJournalPath(dir)
	if err != nil {
		return err
	}
	contents, err := publicationJournalDigest(journal)
	if err != nil {
		return err
	}
	markerPath := publicationCommitMarkerPath(journalPath)
	parent := filepath.Dir(markerPath)
	temp, err := os.CreateTemp(parent, filepath.Base(markerPath)+".*.tmp")
	if err != nil {
		return err
	}
	tempPath := temp.Name()
	if err := temp.Chmod(0600); err != nil {
		return errors.Join(err, temp.Close(), removeFiles([]string{tempPath}))
	}
	if _, err := temp.Write(contents); err != nil {
		return errors.Join(err, temp.Close(), removeFiles([]string{tempPath}))
	}
	if err := temp.Sync(); err != nil {
		return errors.Join(err, temp.Close(), removeFiles([]string{tempPath}))
	}
	if err := temp.Close(); err != nil {
		return errors.Join(err, removeFiles([]string{tempPath}))
	}
	mode, err := detectPublicationMode(tempPath)
	if err != nil {
		return errors.Join(err, removeFiles([]string{tempPath}))
	}
	if err := publishStagedFile(tempPath, markerPath, mode); err != nil {
		return errors.Join(err, removeFiles([]string{tempPath}))
	}
	if mode != publicationModeWriteThroughMove {
		if err := removeFiles([]string{tempPath}); err != nil {
			return err
		}
	}
	return fsdurable.SyncDir(parent)
}

func publicationJournalDigest(journal publicationJournal) ([]byte, error) {
	contents, err := json.Marshal(journal)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(contents)
	return sum[:], nil
}

func publicationCommitMarkerPath(journalPath string) string {
	return journalPath + publicationCommitMarkerSuffix
}

func writePublicationJournal(dir string, journal publicationJournal) error {
	return writePublicationJournalWithPublisher(
		dir,
		journal,
		func(tempPath, journalPath string) error {
			mode, err := detectPublicationMode(tempPath)
			if err != nil {
				return err
			}
			return publishStagedFile(tempPath, journalPath, mode)
		},
	)
}

func writePublicationJournalWithLink(
	dir string,
	journal publicationJournal,
	link func(string, string) error,
) error {
	return writePublicationJournalWithPublisher(
		dir,
		journal,
		func(tempPath, journalPath string) error {
			if err := link(tempPath, journalPath); err != nil {
				if copyErr := copyFileExclusive(tempPath, journalPath); copyErr != nil {
					return errors.Join(err, copyErr)
				}
			}
			return nil
		},
	)
}

func writePublicationJournalWithPublisher(
	dir string,
	journal publicationJournal,
	publish func(string, string) error,
) error {
	journalPath, err := publicationJournalPath(dir)
	if err != nil {
		return err
	}
	contents, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	parent := filepath.Dir(journalPath)
	file, err := os.CreateTemp(parent, filepath.Base(journalPath)+".*.tmp")
	if err != nil {
		return err
	}
	tempPath := file.Name()
	if err := file.Chmod(0600); err != nil {
		return errors.Join(err, file.Close(), removeFiles([]string{tempPath}))
	}
	if _, err := file.Write(contents); err != nil {
		return errors.Join(err, file.Close(), removeFiles([]string{tempPath}))
	}
	if err := file.Sync(); err != nil {
		return errors.Join(err, file.Close(), removeFiles([]string{tempPath}))
	}
	if err := file.Close(); err != nil {
		return errors.Join(err, removeFiles([]string{tempPath}))
	}
	if err := publish(tempPath, journalPath); err != nil {
		return errors.Join(err, removeFiles([]string{tempPath}))
	}
	return fsdurable.SyncDir(parent)
}

func readPublicationJournal(path string) (publicationJournal, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return publicationJournal{}, err
	}
	journal, decodeErr := decodePublicationJournal(contents)
	if decodeErr == nil {
		return journal, nil
	}
	backups, globErr := filepath.Glob(path + ".*.tmp")
	if globErr != nil {
		return publicationJournal{}, errors.Join(decodeErr, globErr)
	}
	for _, backup := range backups {
		contents, err := os.ReadFile(backup)
		if err != nil {
			continue
		}
		journal, err := decodePublicationJournal(contents)
		if err == nil {
			return journal, nil
		}
	}
	return publicationJournal{}, decodeErr
}

func decodePublicationJournal(contents []byte) (publicationJournal, error) {
	decoder := json.NewDecoder(bytes.NewReader(contents))
	decoder.DisallowUnknownFields()
	var journal publicationJournal
	if err := decoder.Decode(&journal); err != nil {
		return publicationJournal{}, fmt.Errorf("decode migration publication journal: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return publicationJournal{}, err
	}
	if err := validatePublicationJournal(journal); err != nil {
		return publicationJournal{}, err
	}
	return journal, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("decode migration publication journal: trailing JSON data")
	}
	return nil
}

func validatePublicationJournal(journal publicationJournal) error {
	if journal.Version != publicationJournalVersion {
		return fmt.Errorf(
			"unsupported migration publication journal version: %d",
			journal.Version,
		)
	}
	if len(journal.Entries) == 0 {
		return errors.New("migration publication journal has no entries")
	}
	if err := validatePublicationCommit(journal); err != nil {
		return err
	}
	stagedNames := make([]string, 0, len(journal.Entries))
	finalNames := make([]string, 0, len(journal.Entries))
	for _, entry := range journal.Entries {
		if err := validatePublicationEntry(entry); err != nil {
			return err
		}
		if slices.Contains(stagedNames, entry.Staged) ||
			slices.Contains(finalNames, entry.Final) {
			return errors.New("migration publication journal contains duplicate paths")
		}
		stagedNames = append(stagedNames, entry.Staged)
		finalNames = append(finalNames, entry.Final)
	}
	return nil
}

func validatePublicationCommit(journal publicationJournal) error {
	switch journal.CommitMode {
	case publicationCommitModeAtlasSum:
		if len(journal.Sum) == 0 {
			return errors.New("migration publication journal has no checksum")
		}
	case publicationCommitModeMarker:
		if len(journal.Sum) != 0 {
			return errors.New("marker-based publication journal contains atlas checksum")
		}
	default:
		return fmt.Errorf(
			"unsupported migration publication commit mode: %q",
			journal.CommitMode,
		)
	}
	return nil
}

func validatePublicationEntry(entry publicationEntry) error {
	if filepath.Base(entry.Staged) != entry.Staged ||
		!strings.HasPrefix(entry.Staged, ".ptah-migrate-diff-") ||
		!strings.HasSuffix(entry.Staged, ".tmp") {
		return fmt.Errorf("invalid staged migration publication path: %q", entry.Staged)
	}
	if filepath.Base(entry.Final) != entry.Final ||
		entry.Final == "." ||
		entry.Final == "" {
		return fmt.Errorf("invalid final migration publication path: %q", entry.Final)
	}
	if entry.Mode != string(publicationModeHardLink) &&
		entry.Mode != string(publicationModeCopy) &&
		entry.Mode != string(publicationModeWriteThroughMove) {
		return fmt.Errorf("invalid migration publication mode: %q", entry.Mode)
	}
	digest, err := hex.DecodeString(entry.Digest)
	if err != nil || len(digest) != sha256.Size {
		return fmt.Errorf("invalid migration publication digest: %q", entry.Digest)
	}
	return nil
}

func publicationJournalPath(dir string) (string, error) {
	canonicalDir, err := canonicalMigrationDir(dir)
	if err != nil {
		return "", fmt.Errorf("resolve migration publication journal path: %w", err)
	}
	return filepath.Join(
		filepath.Dir(canonicalDir),
		"."+filepath.Base(canonicalDir)+publicationJournalSuffix,
	), nil
}

func publicationStagedPaths(dir string, journal publicationJournal) []string {
	paths := make([]string, 0, len(journal.Entries))
	for _, entry := range journal.Entries {
		paths = append(paths, filepath.Join(dir, entry.Staged))
	}
	return paths
}

// writeDirSum atomically refreshes atlas.sum after every complete migration
// file has been published. The sum is the commit marker for the batch.
func writeDirSum(dir string, sum *migratesum.SumFile) (string, error) {
	sumPath := filepath.Join(dir, migratesum.AtlasFileName)
	if err := migratesum.WritePrecomputedWithFormat(
		dir,
		migrator.MigrationDirFormatAtlas,
		sum,
	); err != nil {
		return "", fmt.Errorf("write atlas.sum: %w", err)
	}
	return sumPath, nil
}

func removeFiles(paths []string) error {
	var resultErr error
	for _, path := range paths {
		if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove %s: %w", path, err))
		}
	}
	return resultErr
}

func stageArtifactBatch(
	dir string,
	artifacts []PublicationArtifact,
) (migrationBatch, error) {
	if len(artifacts) == 0 {
		return migrationBatch{}, errors.New("artifact publication batch is empty")
	}
	batch := migrationBatch{
		paths:       make([]string, 0, len(artifacts)),
		stagedPaths: make([]string, 0, len(artifacts)),
		digests:     make([]string, 0, len(artifacts)),
	}
	names := make([]string, 0, len(artifacts))
	for _, artifact := range artifacts {
		if filepath.Base(artifact.Name) != artifact.Name ||
			artifact.Name == "." ||
			artifact.Name == "" {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("invalid artifact publication name %q", artifact.Name),
				removeFiles(batch.stagedPaths),
			)
		}
		if slices.Contains(names, artifact.Name) {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("duplicate artifact publication name %q", artifact.Name),
				removeFiles(batch.stagedPaths),
			)
		}
		stagedPath, err := stageArtifactFile(dir, artifact.Contents)
		if err != nil {
			return migrationBatch{}, errors.Join(err, removeFiles(batch.stagedPaths))
		}
		names = append(names, artifact.Name)
		batch.paths = append(batch.paths, filepath.Join(dir, artifact.Name))
		batch.stagedPaths = append(batch.stagedPaths, stagedPath)
		batch.digests = append(batch.digests, contentDigest(artifact.Contents))
	}
	mode, err := detectPublicationMode(batch.stagedPaths[0])
	if err != nil {
		return migrationBatch{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	batch.mode = mode
	return batch, nil
}

func stageMigrationBatch(dir, name string, contents []MigrationFileContent) (migrationBatch, error) {
	if len(contents) == 0 {
		return migrationBatch{}, errors.New("migration SQL is empty")
	}
	for _, content := range contents {
		if strings.TrimSpace(content.SQL) == "" {
			return migrationBatch{}, errors.New("migration SQL is empty")
		}
	}
	version, err := nextMigrationVersion(dir)
	if err != nil {
		return migrationBatch{}, err
	}
	return stageMigrationBatchAt(dir, name, version, contents)
}

func stageMigrationBatchAt(
	dir, name string,
	version int64,
	contents []MigrationFileContent,
) (migrationBatch, error) {
	return stageMigrationBatchAtWithModeDetector(
		dir,
		name,
		version,
		contents,
		detectPublicationMode,
	)
}

func stageMigrationBatchAtWithModeDetector(
	dir, name string,
	version int64,
	contents []MigrationFileContent,
	detectMode func(string) (publicationMode, error),
) (migrationBatch, error) {
	batch := migrationBatch{
		paths:       make([]string, 0, len(contents)),
		stagedPaths: make([]string, 0, len(contents)),
	}
	for i, content := range contents {
		fileVersion := version + int64(i)
		slug := migrationSlug(name + content.NameSuffix)
		path := filepath.Join(dir, fmt.Sprintf("%d_%s.sql", fileVersion, slug))
		stagedPath, err := stageMigrationFile(dir, content.SQL)
		if err != nil {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("stage migration file: %w", err),
				removeFiles(batch.stagedPaths),
			)
		}
		batch.paths = append(batch.paths, path)
		batch.stagedPaths = append(batch.stagedPaths, stagedPath)
	}
	mode, err := detectMode(batch.stagedPaths[0])
	if err != nil {
		return migrationBatch{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	batch.mode = mode
	if _, err := readStagedMigrationContents(&batch); err != nil {
		return migrationBatch{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	return batch, nil
}

func detectPublicationMode(stagedPath string) (publicationMode, error) {
	return platformPublicationMode(stagedPath)
}

func detectPublicationModeWithLink(
	stagedPath string,
	link func(string, string) error,
) (publicationMode, error) {
	probePath := stagedPath + ".link-probe"
	if err := link(stagedPath, probePath); err != nil {
		return publicationModeCopy, nil
	}
	if err := os.Remove(probePath); err != nil {
		return "", fmt.Errorf("remove migration publication link probe: %w", err)
	}
	return publicationModeHardLink, nil
}

func stageMigrationFile(dir, migrationSQL string) (string, error) {
	return stageArtifactFile(dir, []byte(migrationSQL))
}

func stageArtifactFile(dir string, contents []byte) (string, error) {
	file, err := os.CreateTemp(dir, stagedMigrationPattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Chmod(0644); err != nil {
		return "", errors.Join(err, file.Close(), removeFiles([]string{path}))
	}
	if _, err := file.Write(contents); err != nil {
		return "", errors.Join(err, file.Close(), removeFiles([]string{path}))
	}
	if err := file.Sync(); err != nil {
		return "", errors.Join(err, file.Close(), removeFiles([]string{path}))
	}
	if err := file.Close(); err != nil {
		return "", errors.Join(err, removeFiles([]string{path}))
	}
	return path, nil
}

func nextMigrationVersion(dir string) (int64, error) {
	return nextMigrationVersionFS(os.DirFS(dir))
}

func nextMigrationVersionFS(fsys fs.FS) (int64, error) {
	files, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return 0, err
	}
	version := migrator.GetNextMigrationVersion()
	for _, file := range files {
		if file.Version >= version {
			version = file.Version + 1
		}
	}
	return version, nil
}

var migrationSlugInvalidChars = regexp.MustCompile(`[^a-z0-9_]+`)

func migrationSlug(name string) string {
	slug := strings.ToLower(strings.TrimSpace(name))
	slug = strings.ReplaceAll(slug, "-", "_")
	slug = strings.ReplaceAll(slug, " ", "_")
	slug = migrationSlugInvalidChars.ReplaceAllString(slug, "")
	slug = strings.Trim(slug, "_")
	if slug == "" {
		return "migration"
	}
	return slug
}
