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
	"strconv"
	"strings"
	"time"

	"go.5x5.cz/ptah/internal/fsdurable"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

const (
	publicationJournalVersion     = 5
	publicationJournalSuffix      = ".ptah-migrate-diff.pending"
	publicationCommitMarkerSuffix = ".committed"
	publicationCleanupSuffix      = ".cleanup"
	publicationRollbackSuffix     = ".rollback"
	stagedMigrationPattern        = ".ptah-migrate-diff-*.tmp"
	stagedMigrationPrefix         = ".ptah-migrate-diff-"
	stagedMigrationSuffix         = ".tmp"

	publicationCommitModeAtlasSum = "atlas-sum"
	publicationCommitModeMarker   = "journal-marker"

	publishedFileMode fs.FileMode = 0o644
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

// migrationBatch names the files of one publication batch as direct children of
// the rooted migration directory. Paths are display data derived from the
// handle's lexical path; no publication step resolves them again.
type migrationBatch struct {
	paths       []string
	names       []string
	stagedNames []string
	digests     []string
	mode        publicationMode
}

// writeDiffArtifacts durably publishes one batch of migration files and then
// atlas.sum, entirely through the rooted migration-directory handle. A journal
// beside the migration directory makes an interrupted publication recoverable
// by the next lock holder.
func writeDiffArtifacts(
	ctx context.Context,
	w *migrationWriterDir,
	name string,
	contents []MigrationFileContent,
	baseSnapshot fsnapshot.Snapshot,
	prepare func([]string) error,
) (DiffResult, error) {
	return writeDiffArtifactsWithSumWriter(
		ctx,
		w,
		name,
		contents,
		baseSnapshot,
		prepare,
		publishDirSum,
	)
}

func writeDiffArtifactsWithSumWriter(
	ctx context.Context,
	w *migrationWriterDir,
	name string,
	contents []MigrationFileContent,
	baseSnapshot fsnapshot.Snapshot,
	prepare func([]string) error,
	writeSum func(*migrationWriterDir, *migratesum.SumFile) (string, error),
) (DiffResult, error) {
	if err := ctx.Err(); err != nil {
		return DiffResult{}, err
	}
	if err := recoverPendingPublication(w); err != nil {
		return DiffResult{}, fmt.Errorf("recover previous migration artifact publication: %w", err)
	}
	version, err := nextMigrationVersionFS(baseSnapshot, len(contents))
	if err != nil {
		return DiffResult{}, err
	}
	batch, err := stageMigrationBatchAt(w, name, version, contents)
	if err != nil {
		return DiffResult{}, err
	}
	if err := prepareStagedMigrationBatch(ctx, w, batch, prepare); err != nil {
		return DiffResult{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	stagedContents, err := readStagedMigrationContents(w, &batch)
	if err != nil {
		return DiffResult{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	publishedSnapshot, sum, err := preparePublicationSnapshot(
		baseSnapshot,
		batch,
		stagedContents,
	)
	if err != nil {
		return DiffResult{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	journal, err := beginPublication(w, batch, sum.Bytes())
	if err != nil {
		return DiffResult{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	published, err := publishMigrationBatchContext(ctx, w, batch)
	if err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(w, batch, published))
	}
	if err := verifyMigrationDirUnchanged(w, publishedSnapshot); err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(w, batch, published))
	}
	if err := ctx.Err(); err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(w, batch, published))
	}
	sumPath, err := writeSum(w, sum)
	if err != nil {
		if migratesum.IsCommitUncertain(err) {
			return DiffResult{}, fmt.Errorf(
				"write atlas.sum; migration publication journal retained for recovery: %w",
				err,
			)
		}
		return DiffResult{}, errors.Join(err, abortPendingPublication(w, batch, published))
	}
	result := DiffResult{MigrationPaths: batch.paths, SumPath: sumPath}
	committed, err := publicationCommitted(w, journal)
	if err != nil {
		return result, fmt.Errorf("verify migration publication commit marker: %w", err)
	}
	if !committed {
		return result, errors.New("atlas.sum does not match the journaled migration publication")
	}
	if err := finalizeCommittedPublication(w, journal); err != nil {
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
	w, err := createMigrationWriterDir(nil, dir)
	if err != nil {
		return nil, err
	}
	paths, publishErr := publishArtifactsLocked(ctx, w, artifacts)
	return paths, errors.Join(publishErr, w.Close())
}

func publishArtifactsLocked(
	ctx context.Context,
	w *migrationWriterDir,
	artifacts []PublicationArtifact,
) ([]string, error) {
	if err := recoverPendingPublication(w); err != nil {
		return nil, fmt.Errorf("recover previous artifact publication: %w", err)
	}
	batch, err := stageArtifactBatch(w, artifacts)
	if err != nil {
		return nil, err
	}
	journal, err := beginMarkerPublication(w, batch)
	if err != nil {
		return nil, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	published, err := publishMigrationBatchContext(ctx, w, batch)
	if err != nil {
		return nil, errors.Join(err, abortPendingPublication(w, batch, published))
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, abortPendingPublication(w, batch, published))
	}
	if err := writePublicationCommitMarker(w, journal); err != nil {
		committed, commitErr := publicationCommitted(w, journal)
		if committed && commitErr == nil {
			return nil, fmt.Errorf(
				"write artifact publication commit marker; journal retained for recovery: %w",
				err,
			)
		}
		return nil, errors.Join(err, abortPendingPublication(w, batch, published))
	}
	committed, err := publicationCommitted(w, journal)
	if err != nil {
		return nil, fmt.Errorf("verify artifact publication commit marker: %w", err)
	}
	if !committed {
		return nil, errors.New("artifact publication commit marker does not match its journal")
	}
	if err := finalizeCommittedPublication(w, journal); err != nil {
		return slices.Clone(batch.paths), fmt.Errorf("finalize artifact publication: %w", err)
	}
	return slices.Clone(batch.paths), nil
}

// prepareStagedMigrationBatch runs the caller's editor hook over the staged
// files and then re-validates each one through the rooted handle.
//
// The editor is an external program, so the hook has to receive pathnames.
// Everything after it goes back through the handle and re-establishes the
// staged file's identity there, so an editor that replaced the directory under
// itself cannot hand a foreign file to the publication step.
func prepareStagedMigrationBatch(
	ctx context.Context,
	w *migrationWriterDir,
	batch migrationBatch,
	prepare func([]string) error,
) error {
	if prepare != nil {
		stagedPaths := make([]string, 0, len(batch.stagedNames))
		for _, name := range batch.stagedNames {
			stagedPaths = append(stagedPaths, filepath.Join(w.path, name))
		}
		if err := prepare(stagedPaths); err != nil {
			return fmt.Errorf("prepare migration files for publication: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, name := range batch.stagedNames {
		if err := syncPreparedStagedFile(w, name); err != nil {
			return err
		}
	}
	return nil
}

func syncPreparedStagedFile(w *migrationWriterDir, name string) error {
	info, err := w.dir.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect staged migration file after preparation: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf(
			"staged migration file is not a regular file: %s",
			filepath.Join(w.path, name),
		)
	}
	file, err := w.dir.OpenFile(name, os.O_RDWR, 0)
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
			fmt.Errorf(
				"staged migration file changed while being opened: %s",
				filepath.Join(w.path, name),
			),
			file.Close(),
		)
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("sync staged migration file after preparation: %w", err)
	}
	return nil
}

func readStagedMigrationContents(
	w *migrationWriterDir,
	batch *migrationBatch,
) ([]MigrationFileContent, error) {
	contents := make([]MigrationFileContent, len(batch.stagedNames))
	batch.digests = make([]string, len(batch.stagedNames))
	for i, name := range batch.stagedNames {
		data, err := w.dir.ReadFile(name)
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
	files := make(map[string][]byte, len(batch.names))
	for i, name := range batch.names {
		files[name] = []byte(contents[i].SQL)
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
	w *migrationWriterDir,
	batch migrationBatch,
	sum []byte,
) (publicationJournal, error) {
	// Persist the staging directory entries before making the journal durable.
	// Once the journal exists, recovery relies on the staging links to prove
	// ownership of any published final paths.
	if err := w.dir.Sync(); err != nil {
		return publicationJournal{}, fmt.Errorf("sync staged migration files: %w", err)
	}
	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeAtlasSum,
		Entries:    publicationEntries(batch),
		Sum:        slices.Clone(sum),
	}
	if err := writePublicationJournal(w, journal); err != nil {
		return publicationJournal{}, fmt.Errorf("write migration publication journal: %w", err)
	}
	return journal, nil
}

func beginMarkerPublication(
	w *migrationWriterDir,
	batch migrationBatch,
) (publicationJournal, error) {
	if err := w.dir.Sync(); err != nil {
		return publicationJournal{}, fmt.Errorf("sync staged artifact files: %w", err)
	}
	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeMarker,
		Entries:    publicationEntries(batch),
	}
	if err := writePublicationJournal(w, journal); err != nil {
		return publicationJournal{}, fmt.Errorf("write artifact publication journal: %w", err)
	}
	return journal, nil
}

func publicationEntries(batch migrationBatch) []publicationEntry {
	entries := make([]publicationEntry, len(batch.names))
	for i := range batch.names {
		entries[i] = publicationEntry{
			Staged: batch.stagedNames[i],
			Final:  batch.names[i],
			Mode:   string(batch.mode),
			Digest: batch.digests[i],
		}
	}
	return entries
}

func publishMigrationBatchContext(
	ctx context.Context,
	w *migrationWriterDir,
	batch migrationBatch,
) (int, error) {
	for i, stagedName := range batch.stagedNames {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		err := publishStagedFile(w.dir, stagedName, batch.names[i], batch.mode)
		if err != nil {
			if errors.Is(err, fs.ErrExist) {
				return i, fmt.Errorf(
					"migration directory changed during publication: %s already exists",
					batch.paths[i],
				)
			}
			return i, fmt.Errorf("publish migration file: %w", err)
		}
	}
	if err := w.dir.Sync(); err != nil {
		return len(batch.names), fmt.Errorf("sync published migration files: %w", err)
	}
	return len(batch.names), nil
}

// publishStagedFile commits one staged entry at its final name through the
// rooted handle. Every mode is conditional on the destination being absent: a
// hard link and an exclusive create both fail with fs.ErrExist, and the move
// mode states the expectation to the rooted commit primitive. None of them can
// overwrite a file that appeared after the caller looked.
func publishStagedFile(
	d *pathguard.OpenedDirectory,
	stagedName, finalName string,
	mode publicationMode,
) error {
	switch mode {
	case publicationModeHardLink:
		return d.Link(stagedName, finalName)
	case publicationModeCopy:
		return copyFileExclusive(d, stagedName, finalName)
	case publicationModeWriteThroughMove:
		info, err := d.Lstat(stagedName)
		if err != nil {
			return err
		}
		return d.PublishFile(
			stagedName,
			finalName,
			info,
			publishedFileMode,
			fsdurable.ExpectAbsent(),
		)
	default:
		return fmt.Errorf("unsupported migration publication mode %q", mode)
	}
}

func copyFileExclusive(
	d *pathguard.OpenedDirectory,
	sourceName, destinationName string,
) (resultErr error) {
	contents, err := d.ReadFile(sourceName)
	if err != nil {
		return err
	}
	destination, err := d.OpenFile(
		destinationName,
		os.O_WRONLY|os.O_CREATE|os.O_EXCL,
		publishedFileMode,
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
				removeRootedFiles(d, []string{destinationName}),
			)
		}
	}()
	if _, err := destination.Write(contents); err != nil {
		return err
	}
	if err := destination.Chmod(publishedFileMode); err != nil {
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

func abortPendingPublication(w *migrationWriterDir, batch migrationBatch, published int) error {
	for i := range published {
		if err := rollBackPublicationEntry(
			w,
			batch.stagedNames[i],
			batch.names[i],
			batch.digests[i],
		); err != nil {
			return fmt.Errorf("roll back published migration files: %w", err)
		}
	}
	if err := removeStagedFiles(w, batch.stagedNames[published:]); err != nil {
		return fmt.Errorf("remove rolled back migration staging files: %w", err)
	}
	if err := w.dir.Sync(); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return removePublicationJournal(w)
}

func recoverPendingPublication(w *migrationWriterDir) error {
	journal, err := readPublicationJournal(w)
	if errors.Is(err, fs.ErrNotExist) {
		return removeOrphanPublicationTemps(w)
	}
	if err != nil {
		return err
	}
	if !w.Exists() {
		return fmt.Errorf(
			"cannot recover migration publication: %w",
			w.missingDirError(),
		)
	}
	committed, err := publicationCommitted(w, journal)
	if err != nil {
		return err
	}
	if committed {
		return finalizeCommittedPublication(w, journal)
	}
	return rollBackPendingPublication(w, journal)
}

// RecoverPendingPublicationLocked resolves an interrupted artifact publication.
// The caller must hold the migration-directory lock for dir.
func RecoverPendingPublicationLocked(dir string) error {
	w, err := openMigrationWriterDir(nil, dir)
	if err != nil {
		return err
	}
	return errors.Join(recoverPendingPublication(w), w.Close())
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
		return RecoverPendingPublicationLocked(dir)
	}
	return WithMigrationDirectoryLock(ctx, dir, 0, func(context.Context) error {
		return RecoverPendingPublicationLocked(dir)
	})
}

func publicationCommitted(w *migrationWriterDir, journal publicationJournal) (bool, error) {
	switch journal.CommitMode {
	case publicationCommitModeAtlasSum:
		if !w.Exists() {
			return false, w.missingDirError()
		}
		contents, err := w.dir.ReadFile(migratesum.AtlasFileName)
		if errors.Is(err, fs.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return bytes.Equal(contents, journal.Sum), nil
	case publicationCommitModeMarker:
		contents, err := w.parent.ReadFile(publicationCommitMarkerName(w))
		if errors.Is(err, fs.ErrNotExist) {
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
	w *migrationWriterDir,
	journal publicationJournal,
) error {
	if err := removeStagedFiles(w, publicationStagedNames(journal)); err != nil {
		return fmt.Errorf("remove committed migration staging files: %w", err)
	}
	if err := w.dir.Sync(); err != nil {
		return fmt.Errorf("sync committed migration directory: %w", err)
	}
	return removePublicationJournal(w)
}

func rollBackPendingPublication(
	w *migrationWriterDir,
	journal publicationJournal,
) error {
	for _, entry := range journal.Entries {
		if err := rollBackPublicationEntry(
			w,
			entry.Staged,
			entry.Final,
			entry.Digest,
		); err != nil {
			return err
		}
	}
	if err := w.dir.Sync(); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return removePublicationJournal(w)
}

// rollBackPublicationEntry withdraws one published entry through the rooted
// handle. The published file is first moved aside under a name only this
// transaction knows, conditionally on that name being free, so the content
// check that follows reads bytes nobody else can still reach by the final name.
func rollBackPublicationEntry(
	w *migrationWriterDir,
	stagedName, finalName, expectedDigest string,
) error {
	quarantineName := stagedName + publicationRollbackSuffix
	if err := quarantinePublishedMigration(w, finalName, quarantineName); err != nil {
		return err
	}
	stagedDigest, stagedErr := rootedFileDigest(w.dir, stagedName)
	quarantinedDigest, quarantineErr := rootedFileDigest(w.dir, quarantineName)
	return reconcileRollbackFiles(rollbackFileState{
		writer:            w,
		stagedName:        stagedName,
		finalName:         finalName,
		quarantineName:    quarantineName,
		expectedDigest:    expectedDigest,
		stagedDigest:      stagedDigest,
		stagedErr:         stagedErr,
		quarantinedDigest: quarantinedDigest,
		quarantineErr:     quarantineErr,
	})
}

func quarantinePublishedMigration(
	w *migrationWriterDir,
	finalName, quarantineName string,
) error {
	if _, err := w.dir.Lstat(quarantineName); err == nil {
		return nil
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("inspect quarantined migration file: %w", err)
	}
	info, err := w.dir.Lstat(finalName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect published migration file: %w", err)
	}
	if err := w.dir.PublishFile(
		finalName,
		quarantineName,
		info,
		info.Mode().Perm(),
		fsdurable.ExpectAbsent(),
	); err != nil {
		return fmt.Errorf("quarantine published migration file: %w", err)
	}
	return w.dir.Sync()
}

type rollbackFileState struct {
	writer            *migrationWriterDir
	stagedName        string
	finalName         string
	quarantineName    string
	expectedDigest    string
	stagedDigest      string
	stagedErr         error
	quarantinedDigest string
	quarantineErr     error
}

func (s rollbackFileState) display(name string) string {
	return filepath.Join(s.writer.path, name)
}

func reconcileRollbackFiles(state rollbackFileState) error {
	switch {
	case errors.Is(state.stagedErr, fs.ErrNotExist) && errors.Is(state.quarantineErr, fs.ErrNotExist):
		return nil
	case state.stagedErr == nil && errors.Is(state.quarantineErr, fs.ErrNotExist):
		if state.stagedDigest != state.expectedDigest {
			return fmt.Errorf(
				"cannot safely recover migration publication: staging file content changed; preserved %s",
				state.display(state.stagedName),
			)
		}
		return removeRootedFiles(state.writer.dir, []string{state.stagedName})
	case errors.Is(state.stagedErr, fs.ErrNotExist) &&
		state.quarantineErr == nil:
		if state.quarantinedDigest != state.expectedDigest {
			return fmt.Errorf(
				"cannot safely recover migration publication: %s content changed; preserved at %s",
				state.display(state.finalName),
				state.display(state.quarantineName),
			)
		}
		return removeRootedFiles(state.writer.dir, []string{state.quarantineName})
	case state.stagedErr != nil:
		return fmt.Errorf("inspect staged migration file: %w", state.stagedErr)
	case state.quarantineErr != nil:
		return fmt.Errorf("inspect quarantined migration file: %w", state.quarantineErr)
	case state.stagedDigest != state.expectedDigest:
		return fmt.Errorf(
			"cannot safely recover migration publication: staging file content changed; preserved %s and %s",
			state.display(state.stagedName),
			state.display(state.quarantineName),
		)
	case state.quarantinedDigest != state.expectedDigest:
		return fmt.Errorf(
			"cannot safely recover migration publication: %s content changed; preserved at %s",
			state.display(state.finalName),
			state.display(state.quarantineName),
		)
	default:
		return removeRootedFiles(
			state.writer.dir,
			[]string{state.quarantineName, state.stagedName},
		)
	}
}

func rootedFileDigest(d *pathguard.OpenedDirectory, name string) (string, error) {
	contents, err := d.ReadFile(name)
	if err != nil {
		return "", err
	}
	return contentDigest(contents), nil
}

func removePublicationJournal(w *migrationWriterDir) error {
	return removePublicationJournalWithRetirer(w, retirePublicationJournal)
}

func removePublicationJournalWithRetirer(
	w *migrationWriterDir,
	retire func(*pathguard.OpenedDirectory, string, string) error,
) error {
	journalName := publicationJournalName(w)
	journalTemps, err := rootedGlob(w.parent, journalName+".*.tmp")
	if err != nil {
		return fmt.Errorf("find migration publication journal backups: %w", err)
	}
	cleanupName := journalName + publicationCleanupSuffix
	if err := retire(w.parent, journalName, cleanupName); err != nil {
		return fmt.Errorf("retire migration publication journal: %w", err)
	}
	if err := w.parent.Sync(); err != nil {
		return fmt.Errorf("sync retired migration publication journal: %w", err)
	}
	names := append(
		[]string{publicationCommitMarkerName(w), cleanupName},
		journalTemps...,
	)
	if err := removeRootedFiles(w.parent, names); err != nil {
		return fmt.Errorf("remove retired migration publication metadata: %w", err)
	}
	if err := w.parent.Sync(); err != nil {
		return fmt.Errorf("sync migration publication journal directory: %w", err)
	}
	return nil
}

func retirePublicationJournal(
	d *pathguard.OpenedDirectory,
	journalName, cleanupName string,
) error {
	info, err := d.Lstat(journalName)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return d.PublishFile(
		journalName,
		cleanupName,
		info,
		info.Mode().Perm(),
		fsdurable.ExpectAbsent(),
	)
}

func removeOrphanPublicationTemps(w *migrationWriterDir) error {
	journalName := publicationJournalName(w)
	journalTemps, err := rootedGlob(w.parent, journalName+".*.tmp")
	if err != nil {
		return fmt.Errorf("find orphan migration publication journals: %w", err)
	}
	orphanNames := slices.Concat(
		journalTemps,
		[]string{
			publicationCommitMarkerName(w),
			journalName + publicationCleanupSuffix,
		},
	)
	if err := removeRootedFiles(w.parent, orphanNames); err != nil {
		return fmt.Errorf("remove orphan migration publication files: %w", err)
	}
	if len(journalTemps) > 0 {
		if err := w.parent.Sync(); err != nil {
			return fmt.Errorf("sync publication journal directory after orphan cleanup: %w", err)
		}
	}
	if !w.Exists() {
		return nil
	}
	stagedNames, err := rootedGlob(w.dir, stagedMigrationPattern)
	if err != nil {
		return fmt.Errorf("find orphan migration staging files: %w", err)
	}
	if err := removeRootedFiles(w.dir, stagedNames); err != nil {
		return fmt.Errorf("remove orphan migration publication files: %w", err)
	}
	if len(stagedNames) > 0 {
		if err := w.dir.Sync(); err != nil {
			return fmt.Errorf("sync migration directory after orphan cleanup: %w", err)
		}
	}
	return nil
}

func writePublicationCommitMarker(
	w *migrationWriterDir,
	journal publicationJournal,
) error {
	contents, err := publicationJournalDigest(journal)
	if err != nil {
		return err
	}
	markerName := publicationCommitMarkerName(w)
	tempName, err := stageRootedFile(w.parent, markerName+".*.tmp", contents, 0o600)
	if err != nil {
		return err
	}
	mode, err := detectPublicationMode(w.parent, tempName)
	if err != nil {
		return errors.Join(err, removeRootedFiles(w.parent, []string{tempName}))
	}
	if err := publishStagedFile(w.parent, tempName, markerName, mode); err != nil {
		return errors.Join(err, removeRootedFiles(w.parent, []string{tempName}))
	}
	if mode != publicationModeWriteThroughMove {
		if err := removeRootedFiles(w.parent, []string{tempName}); err != nil {
			return err
		}
	}
	return w.parent.Sync()
}

func publicationJournalDigest(journal publicationJournal) ([]byte, error) {
	contents, err := json.Marshal(journal)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(contents)
	return sum[:], nil
}

func publicationJournalName(w *migrationWriterDir) string {
	return "." + w.name + publicationJournalSuffix
}

func publicationCommitMarkerName(w *migrationWriterDir) string {
	return publicationJournalName(w) + publicationCommitMarkerSuffix
}

func writePublicationJournal(w *migrationWriterDir, journal publicationJournal) error {
	return writePublicationJournalWithPublisher(
		w,
		journal,
		func(tempName, journalName string) error {
			mode, err := detectPublicationMode(w.parent, tempName)
			if err != nil {
				return err
			}
			return publishStagedFile(w.parent, tempName, journalName, mode)
		},
	)
}

func writePublicationJournalWithLink(
	w *migrationWriterDir,
	journal publicationJournal,
	link func(string, string) error,
) error {
	return writePublicationJournalWithPublisher(
		w,
		journal,
		func(tempName, journalName string) error {
			if err := link(tempName, journalName); err != nil {
				if copyErr := copyFileExclusive(w.parent, tempName, journalName); copyErr != nil {
					return errors.Join(err, copyErr)
				}
			}
			return nil
		},
	)
}

func writePublicationJournalWithPublisher(
	w *migrationWriterDir,
	journal publicationJournal,
	publish func(string, string) error,
) error {
	contents, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	journalName := publicationJournalName(w)
	tempName, err := stageRootedFile(w.parent, journalName+".*.tmp", contents, 0o600)
	if err != nil {
		return err
	}
	if err := publish(tempName, journalName); err != nil {
		return errors.Join(err, removeRootedFiles(w.parent, []string{tempName}))
	}
	return w.parent.Sync()
}

func readPublicationJournal(w *migrationWriterDir) (publicationJournal, error) {
	journalName := publicationJournalName(w)
	contents, err := w.parent.ReadFile(journalName)
	if err != nil {
		return publicationJournal{}, err
	}
	journal, decodeErr := decodePublicationJournal(contents)
	if decodeErr == nil {
		return journal, nil
	}
	backups, globErr := rootedGlob(w.parent, journalName+".*.tmp")
	if globErr != nil {
		return publicationJournal{}, errors.Join(decodeErr, globErr)
	}
	for _, backup := range backups {
		contents, err := w.parent.ReadFile(backup)
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
		!strings.HasPrefix(entry.Staged, stagedMigrationPrefix) ||
		!strings.HasSuffix(entry.Staged, stagedMigrationSuffix) {
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

func publicationStagedNames(journal publicationJournal) []string {
	names := make([]string, 0, len(journal.Entries))
	for _, entry := range journal.Entries {
		names = append(names, entry.Staged)
	}
	return names
}

// publishDirSum commits atlas.sum through the same rooted handle that published
// the migration files, conditionally on the checksum state the transaction
// observed. A concurrent writer that replaced atlas.sum after the migration
// snapshot was verified is reported instead of overwritten.
func publishDirSum(w *migrationWriterDir, sum *migratesum.SumFile) (string, error) {
	if sum == nil {
		return "", errors.New("migration checksum must not be nil")
	}
	sumPath := filepath.Join(w.path, migratesum.AtlasFileName)
	destination, err := expectedSumDestination(w)
	if err != nil {
		return "", fmt.Errorf("write %s: %w", migratesum.AtlasFileName, err)
	}
	tempName, err := stageRootedFile(
		w.dir,
		"."+migratesum.AtlasFileName+".*.tmp",
		sum.Bytes(),
		publishedFileMode,
	)
	if err != nil {
		return "", fmt.Errorf("write %s: %w", migratesum.AtlasFileName, err)
	}
	info, err := w.dir.Lstat(tempName)
	if err != nil {
		return "", errors.Join(
			fmt.Errorf("write %s: %w", migratesum.AtlasFileName, err),
			removeRootedFiles(w.dir, []string{tempName}),
		)
	}
	publishErr := w.dir.PublishFile(
		tempName,
		migratesum.AtlasFileName,
		info,
		publishedFileMode,
		destination,
	)
	if publishErr == nil {
		return sumPath, nil
	}
	if errors.Is(publishErr, fsdurable.ErrReplacementCommitted) {
		// The rename took effect; only the durability barrier after it failed.
		// That is exactly the commit-uncertain contract the checksum writer has
		// always reported, so the journal is retained for recovery.
		return "", &migratesum.CommitUncertainError{
			Err: fmt.Errorf("write %s: %w", migratesum.AtlasFileName, publishErr),
		}
	}
	return "", errors.Join(
		fmt.Errorf("write %s: %w", migratesum.AtlasFileName, publishErr),
		removeRootedFiles(w.dir, []string{tempName}),
	)
}

// expectedSumDestination captures the checksum state this commit replaces. It
// is read through the rooted handle immediately before the staged sum is
// published, so the window between the two is closed by the conditional commit
// rather than by trusting the read.
func expectedSumDestination(w *migrationWriterDir) (fsdurable.Destination, error) {
	info, err := w.dir.Lstat(migratesum.AtlasFileName)
	if errors.Is(err, fs.ErrNotExist) {
		return fsdurable.ExpectAbsent(), nil
	}
	if err != nil {
		return fsdurable.Destination{}, err
	}
	if !info.Mode().IsRegular() {
		return fsdurable.Destination{}, fmt.Errorf(
			"%s is not a regular file",
			filepath.Join(w.path, migratesum.AtlasFileName),
		)
	}
	return fsdurable.ExpectFile(info), nil
}

func removeStagedFiles(w *migrationWriterDir, names []string) error {
	if !w.Exists() {
		return nil
	}
	return removeRootedFiles(w.dir, names)
}

func removeRootedFiles(d *pathguard.OpenedDirectory, names []string) error {
	var resultErr error
	for _, name := range names {
		if err := d.Remove(name); err != nil && !errors.Is(err, fs.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove %s: %w", name, err))
		}
	}
	return resultErr
}

// rootedGlob lists the direct children of d whose names match pattern. It
// replaces filepath.Glob so the scan cannot follow a replaced pathname out of
// the opened directory.
func rootedGlob(d *pathguard.OpenedDirectory, pattern string) ([]string, error) {
	entries, err := d.ReadDir(".")
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil, nil
		}
		return nil, err
	}
	var matched []string
	for _, entry := range entries {
		ok, err := filepath.Match(pattern, entry.Name())
		if err != nil {
			return nil, err
		}
		if ok {
			matched = append(matched, entry.Name())
		}
	}
	return matched, nil
}

func stageArtifactBatch(
	w *migrationWriterDir,
	artifacts []PublicationArtifact,
) (migrationBatch, error) {
	if len(artifacts) == 0 {
		return migrationBatch{}, errors.New("artifact publication batch is empty")
	}
	batch := migrationBatch{
		paths:       make([]string, 0, len(artifacts)),
		names:       make([]string, 0, len(artifacts)),
		stagedNames: make([]string, 0, len(artifacts)),
		digests:     make([]string, 0, len(artifacts)),
	}
	for _, artifact := range artifacts {
		if filepath.Base(artifact.Name) != artifact.Name ||
			artifact.Name == "." ||
			artifact.Name == "" {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("invalid artifact publication name %q", artifact.Name),
				removeStagedFiles(w, batch.stagedNames),
			)
		}
		if slices.Contains(batch.names, artifact.Name) {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("duplicate artifact publication name %q", artifact.Name),
				removeStagedFiles(w, batch.stagedNames),
			)
		}
		stagedName, err := stageRootedFile(
			w.dir,
			stagedMigrationPattern,
			artifact.Contents,
			publishedFileMode,
		)
		if err != nil {
			return migrationBatch{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
		}
		batch.paths = append(batch.paths, filepath.Join(w.path, artifact.Name))
		batch.names = append(batch.names, artifact.Name)
		batch.stagedNames = append(batch.stagedNames, stagedName)
		batch.digests = append(batch.digests, contentDigest(artifact.Contents))
	}
	mode, err := detectPublicationMode(w.dir, batch.stagedNames[0])
	if err != nil {
		return migrationBatch{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	batch.mode = mode
	return batch, nil
}

func stageMigrationBatch(
	w *migrationWriterDir,
	name string,
	contents []MigrationFileContent,
) (migrationBatch, error) {
	if len(contents) == 0 {
		return migrationBatch{}, errors.New("migration SQL is empty")
	}
	for _, content := range contents {
		if strings.TrimSpace(content.SQL) == "" {
			return migrationBatch{}, errors.New("migration SQL is empty")
		}
	}
	fsys, err := w.FS()
	if err != nil {
		return migrationBatch{}, err
	}
	version, err := nextMigrationVersionFS(fsys, len(contents))
	if err != nil {
		return migrationBatch{}, err
	}
	return stageMigrationBatchAt(w, name, version, contents)
}

func stageMigrationBatchAt(
	w *migrationWriterDir,
	name string,
	version int64,
	contents []MigrationFileContent,
) (migrationBatch, error) {
	return stageMigrationBatchAtWithModeDetector(
		w,
		name,
		version,
		contents,
		detectPublicationMode,
	)
}

func stageMigrationBatchAtWithModeDetector(
	w *migrationWriterDir,
	name string,
	version int64,
	contents []MigrationFileContent,
	detectMode func(*pathguard.OpenedDirectory, string) (publicationMode, error),
) (migrationBatch, error) {
	if !w.Exists() {
		return migrationBatch{}, w.missingDirError()
	}
	batch := migrationBatch{
		paths:       make([]string, 0, len(contents)),
		names:       make([]string, 0, len(contents)),
		stagedNames: make([]string, 0, len(contents)),
	}
	for i, content := range contents {
		fileVersion := version + int64(i)
		slug := migrationSlug(name + content.NameSuffix)
		fileName := fmt.Sprintf("%d_%s.sql", fileVersion, slug)
		stagedName, err := stageRootedFile(
			w.dir,
			stagedMigrationPattern,
			[]byte(content.SQL),
			publishedFileMode,
		)
		if err != nil {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("stage migration file: %w", err),
				removeStagedFiles(w, batch.stagedNames),
			)
		}
		batch.paths = append(batch.paths, filepath.Join(w.path, fileName))
		batch.names = append(batch.names, fileName)
		batch.stagedNames = append(batch.stagedNames, stagedName)
	}
	mode, err := detectMode(w.dir, batch.stagedNames[0])
	if err != nil {
		return migrationBatch{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	batch.mode = mode
	if _, err := readStagedMigrationContents(w, &batch); err != nil {
		return migrationBatch{}, errors.Join(err, removeStagedFiles(w, batch.stagedNames))
	}
	return batch, nil
}

func detectPublicationMode(
	d *pathguard.OpenedDirectory,
	stagedName string,
) (publicationMode, error) {
	return platformPublicationMode(d, stagedName)
}

func detectPublicationModeWithLink(
	d *pathguard.OpenedDirectory,
	stagedName string,
	link func(string, string) error,
) (publicationMode, error) {
	probeName := stagedName + ".link-probe"
	if err := link(stagedName, probeName); err != nil {
		return publicationModeCopy, nil
	}
	if err := d.Remove(probeName); err != nil {
		return "", fmt.Errorf("remove migration publication link probe: %w", err)
	}
	return publicationModeHardLink, nil
}

// stageRootedFile writes contents into a new exclusively owned file inside the
// opened directory and returns its name. Every staged file is created through
// the handle, so no staging step can land outside the directory the caller
// opened.
func stageRootedFile(
	d *pathguard.OpenedDirectory,
	pattern string,
	contents []byte,
	mode fs.FileMode,
) (string, error) {
	file, name, err := d.CreateTemp(pattern)
	if err != nil {
		return "", err
	}
	if err := file.Chmod(mode); err != nil {
		return "", errors.Join(err, file.Close(), removeRootedFiles(d, []string{name}))
	}
	if _, err := file.Write(contents); err != nil {
		return "", errors.Join(err, file.Close(), removeRootedFiles(d, []string{name}))
	}
	if err := file.Sync(); err != nil {
		return "", errors.Join(err, file.Close(), removeRootedFiles(d, []string{name}))
	}
	if err := file.Close(); err != nil {
		return "", errors.Join(err, removeRootedFiles(d, []string{name}))
	}
	return name, nil
}

// MigrationVersion returns the migration version this compatibility surface
// stamps: the UTC `yyyyMMddHHmmss` value the pinned community binary v1.3.0
// writes, in every case.
//
// "In every case" is measured, not assumed. A directory holding one
// future-dated `29991231235959_future.sql` still gets today's stamp from that
// binary -- a version that sorts BEFORE the migration already there. It does
// not bump past its neighbours and it does not refuse, so neither does this.
//
// The fallback is unreachable in practice: Format produces fourteen digits,
// which fits an int64 until the year 922337. It exists so a clock that somehow
// yields an unparseable stamp degrades to a usable version rather than an
// error from a function with nothing to report.
func MigrationVersion() int64 {
	version, err := strconv.ParseInt(time.Now().UTC().Format("20060102150405"), 10, 64)
	if err != nil {
		return migrator.GetNextMigrationVersion()
	}
	return version
}

// nextMigrationVersionFS returns the first version at which count consecutive
// migration versions are all free.
//
// Only collisions move it. Taking `max(existing) + 1` -- what this did before
// stokaro/ptah#1218 -- differed from the community binary twice over: on an
// empty directory it started from `time.Now().Unix()`, a ten-digit epoch rather
// than a timestamp, and on a directory whose newest version ended in `59`
// seconds it produced `...235960`, which is not a time anyone can parse back.
func nextMigrationVersionFS(fsys fs.FS, count int) (int64, error) {
	files, err := migrator.DiscoverMigrationFiles(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return 0, err
	}
	taken := make(map[int64]struct{}, len(files))
	for _, file := range files {
		taken[file.Version] = struct{}{}
	}
	version := MigrationVersion()
	for collidesWithTakenVersions(taken, version, count) {
		version++
	}
	return version, nil
}

// collidesWithTakenVersions reports whether any of the count consecutive
// versions starting at version is already used. A multi-file plan is staged at
// version+0, version+1, ..., so checking only the first would let a later file
// of the batch land on an existing name.
func collidesWithTakenVersions(taken map[int64]struct{}, version int64, count int) bool {
	for i := range int64(max(count, 1)) {
		if _, ok := taken[version+i]; ok {
			return true
		}
	}
	return false
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
