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
	stagedMigrationPrefix         = ".ptah-migrate-diff-"
	stagedMigrationSuffix         = ".tmp"
	stagedMigrationPattern        = stagedMigrationPrefix + "*" + stagedMigrationSuffix
	// rollbackQuarantineSuffix names the file a rollback moves an already
	// published migration aside to. It is a direct child of the migration
	// directory so that creating, moving and removing it all run through the one
	// retained handle; a nested quarantine would need a second handle whose own
	// open is the race this package exists to close.
	rollbackQuarantineSuffix = ".rollback"

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

// migrationBatch names one publication batch relative to the retained migration
// directory handle. Names are what the rooted operations act on; paths are
// rendered from them for reporting and error messages only.
type migrationBatch struct {
	names       []string
	stagedNames []string
	digests     []string
	mode        publicationMode
}

func (b migrationBatch) paths(pub *publicationDir) []string {
	paths := make([]string, 0, len(b.names))
	for _, name := range b.names {
		paths = append(paths, pub.path(name))
	}
	return paths
}

func (b migrationBatch) stagedPaths(pub *publicationDir) []string {
	paths := make([]string, 0, len(b.stagedNames))
	for _, name := range b.stagedNames {
		paths = append(paths, pub.path(name))
	}
	return paths
}

// writeDiffArtifacts durably publishes one batch of migration files and then
// atlas.sum. A journal next to the migration directory makes an interrupted
// publication recoverable by the next lock holder.
func writeDiffArtifacts(
	ctx context.Context,
	pub *publicationDir,
	name string,
	contents []MigrationFileContent,
	baseSnapshot fsnapshot.Snapshot,
	prepare func([]string) error,
) (DiffResult, error) {
	return writeDiffArtifactsWithSumWriter(
		ctx,
		pub,
		name,
		contents,
		baseSnapshot,
		prepare,
		writeDirSum,
	)
}

func writeDiffArtifactsWithSumWriter(
	ctx context.Context,
	pub *publicationDir,
	name string,
	contents []MigrationFileContent,
	baseSnapshot fsnapshot.Snapshot,
	prepare func([]string) error,
	writeSum func(*publicationDir, *migratesum.SumFile) (string, error),
) (DiffResult, error) {
	if err := ctx.Err(); err != nil {
		return DiffResult{}, err
	}
	if err := recoverPendingPublication(pub); err != nil {
		return DiffResult{}, fmt.Errorf("recover previous migration artifact publication: %w", err)
	}
	version, err := nextMigrationVersionFS(baseSnapshot)
	if err != nil {
		return DiffResult{}, err
	}
	batch, err := stageMigrationBatchAt(pub, name, version, contents)
	if err != nil {
		return DiffResult{}, err
	}
	if err := prepareStagedMigrationBatch(ctx, pub, batch, prepare); err != nil {
		return DiffResult{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	stagedContents, err := readStagedMigrationContents(pub, &batch)
	if err != nil {
		return DiffResult{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	publishedSnapshot, sum, err := preparePublicationSnapshot(
		baseSnapshot,
		batch,
		stagedContents,
	)
	if err != nil {
		return DiffResult{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	journal, err := beginPublication(pub, batch, sum.Bytes())
	if err != nil {
		return DiffResult{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	published, err := publishMigrationBatchContext(ctx, pub, batch)
	if err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(pub, batch, published))
	}
	if err := verifyMigrationDirUnchanged(pub, publishedSnapshot); err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(pub, batch, published))
	}
	if err := ctx.Err(); err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(pub, batch, published))
	}
	sumPath, err := writeSum(pub, sum)
	if err != nil {
		if migratesum.IsCommitUncertain(err) {
			return DiffResult{}, fmt.Errorf(
				"write atlas.sum; migration publication journal retained for recovery: %w",
				err,
			)
		}
		return DiffResult{}, errors.Join(err, abortPendingPublication(pub, batch, published))
	}
	result := DiffResult{MigrationPaths: batch.paths(pub), SumPath: sumPath}
	committed, err := publicationCommitted(pub, journal)
	if err != nil {
		return result, fmt.Errorf("verify migration publication commit marker: %w", err)
	}
	if !committed {
		return result, errors.New("atlas.sum does not match the journaled migration publication")
	}
	if err := finalizeCommittedPublication(pub, journal); err != nil {
		return result, fmt.Errorf("finalize migration artifact publication: %w", err)
	}
	return result, nil
}

// PublishArtifactsLocked durably publishes all artifacts as one batch. The
// caller must hold the migration-directory lock for dir. The directory is
// opened once and every write in the batch is addressed through that retained
// handle.
func PublishArtifactsLocked(
	ctx context.Context,
	dir string,
	artifacts []PublicationArtifact,
) (paths []string, resultErr error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pub, err := openPublicationDir(dir)
	if err != nil {
		return nil, err
	}
	defer func() {
		resultErr = errors.Join(resultErr, pub.close())
	}()
	return publishArtifactsIn(ctx, pub, artifacts)
}

func publishArtifactsIn(
	ctx context.Context,
	pub *publicationDir,
	artifacts []PublicationArtifact,
) ([]string, error) {
	if err := recoverPendingPublication(pub); err != nil {
		return nil, fmt.Errorf("recover previous artifact publication: %w", err)
	}
	batch, err := stageArtifactBatch(pub, artifacts)
	if err != nil {
		return nil, err
	}
	journal, err := beginMarkerPublication(pub, batch)
	if err != nil {
		return nil, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	published, err := publishMigrationBatchContext(ctx, pub, batch)
	if err != nil {
		return nil, errors.Join(err, abortPendingPublication(pub, batch, published))
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, abortPendingPublication(pub, batch, published))
	}
	if err := writePublicationCommitMarker(pub, journal); err != nil {
		committed, commitErr := publicationCommitted(pub, journal)
		if committed && commitErr == nil {
			return nil, fmt.Errorf(
				"write artifact publication commit marker; journal retained for recovery: %w",
				err,
			)
		}
		return nil, errors.Join(err, abortPendingPublication(pub, batch, published))
	}
	committed, err := publicationCommitted(pub, journal)
	if err != nil {
		return nil, fmt.Errorf("verify artifact publication commit marker: %w", err)
	}
	if !committed {
		return nil, errors.New("artifact publication commit marker does not match its journal")
	}
	if err := finalizeCommittedPublication(pub, journal); err != nil {
		return batch.paths(pub), fmt.Errorf("finalize artifact publication: %w", err)
	}
	return batch.paths(pub), nil
}

func prepareStagedMigrationBatch(
	ctx context.Context,
	pub *publicationDir,
	batch migrationBatch,
	prepare func([]string) error,
) error {
	if prepare != nil {
		if err := prepare(batch.stagedPaths(pub)); err != nil {
			return fmt.Errorf("prepare migration files for publication: %w", err)
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	for _, name := range batch.stagedNames {
		if err := syncPreparedStagedFile(pub, name); err != nil {
			return err
		}
	}
	return nil
}

func syncPreparedStagedFile(pub *publicationDir, name string) error {
	info, err := pub.dir.Lstat(name)
	if err != nil {
		return fmt.Errorf("inspect staged migration file after preparation: %w", err)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("staged migration file is not a regular file: %s", pub.path(name))
	}
	file, err := pub.dir.OpenFile(name, os.O_RDWR, 0)
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
			fmt.Errorf("staged migration file changed while being opened: %s", pub.path(name)),
			file.Close(),
		)
	}
	if err := errors.Join(file.Sync(), file.Close()); err != nil {
		return fmt.Errorf("sync staged migration file after preparation: %w", err)
	}
	return nil
}

func readStagedMigrationContents(
	pub *publicationDir,
	batch *migrationBatch,
) ([]MigrationFileContent, error) {
	contents := make([]MigrationFileContent, len(batch.stagedNames))
	batch.digests = make([]string, len(batch.stagedNames))
	for i, name := range batch.stagedNames {
		data, err := pub.dir.ReadFile(name)
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
	pub *publicationDir,
	batch migrationBatch,
	sum []byte,
) (publicationJournal, error) {
	// Persist the staging directory entries before making the journal durable.
	// Once the journal exists, recovery relies on the staging links to prove
	// ownership of any published final paths.
	if err := pub.dir.Sync(); err != nil {
		return publicationJournal{}, fmt.Errorf("sync staged migration files: %w", err)
	}

	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeAtlasSum,
		Entries:    publicationEntries(batch),
		Sum:        slices.Clone(sum),
	}
	if err := writePublicationJournal(pub, journal); err != nil {
		return publicationJournal{}, fmt.Errorf("write migration publication journal: %w", err)
	}
	return journal, nil
}

func beginMarkerPublication(
	pub *publicationDir,
	batch migrationBatch,
) (publicationJournal, error) {
	if err := pub.dir.Sync(); err != nil {
		return publicationJournal{}, fmt.Errorf("sync staged artifact files: %w", err)
	}
	journal := publicationJournal{
		Version:    publicationJournalVersion,
		CommitMode: publicationCommitModeMarker,
		Entries:    publicationEntries(batch),
	}
	if err := writePublicationJournal(pub, journal); err != nil {
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
	pub *publicationDir,
	batch migrationBatch,
) (int, error) {
	for i, stagedName := range batch.stagedNames {
		if err := ctx.Err(); err != nil {
			return i, err
		}
		err := publishStagedFile(pub.dir, stagedName, batch.names[i], batch.mode)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				return i, fmt.Errorf(
					"migration directory changed during publication: %s already exists",
					pub.path(batch.names[i]),
				)
			}
			return i, fmt.Errorf("publish migration file: %w", err)
		}
	}
	if err := pub.dir.Sync(); err != nil {
		return len(batch.names), fmt.Errorf("sync published migration files: %w", err)
	}
	return len(batch.names), nil
}

func publishStagedFile(
	root *pathguard.OpenedDirectory,
	stagedName, finalName string,
	mode publicationMode,
) error {
	switch mode {
	case publicationModeHardLink:
		return root.Link(stagedName, finalName)
	case publicationModeCopy:
		return copyFileExclusive(root, stagedName, finalName)
	case publicationModeWriteThroughMove:
		return root.MoveFileNoReplace(stagedName, finalName)
	default:
		return fmt.Errorf("unsupported migration publication mode %q", mode)
	}
}

func copyFileExclusive(
	root *pathguard.OpenedDirectory,
	sourceName, destinationName string,
) (resultErr error) {
	contents, err := root.ReadFile(sourceName)
	if err != nil {
		return err
	}
	destination, err := root.OpenFile(
		destinationName,
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
				removeNames(root, []string{destinationName}),
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

func abortPendingPublication(pub *publicationDir, batch migrationBatch, published int) error {
	for i := range published {
		if err := rollBackPublicationEntry(
			pub,
			batch.stagedNames[i],
			batch.names[i],
			batch.digests[i],
		); err != nil {
			return fmt.Errorf("roll back published migration files: %w", err)
		}
	}
	if err := removeNames(pub.dir, batch.stagedNames[published:]); err != nil {
		return fmt.Errorf("remove rolled back migration staging files: %w", err)
	}
	if err := pub.dir.Sync(); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return removePublicationJournal(pub)
}

func recoverPendingPublication(pub *publicationDir) error {
	journal, err := readPublicationJournal(pub)
	if errors.Is(err, os.ErrNotExist) {
		return removeOrphanPublicationTemps(pub)
	}
	if err != nil {
		return err
	}
	committed, err := publicationCommitted(pub, journal)
	if err != nil {
		return err
	}
	if committed {
		return finalizeCommittedPublication(pub, journal)
	}
	return rollBackPendingPublication(pub, journal)
}

// RecoverPendingPublicationLocked resolves an interrupted artifact publication.
// The caller must hold the migration-directory lock for dir.
func RecoverPendingPublicationLocked(dir string) (resultErr error) {
	pub, err := openPublicationDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		// A directory that does not exist holds neither staged files nor
		// published ones, so there is nothing to recover. Any journal left in
		// the parent is resolved by the next run that creates the directory,
		// which is also the first run able to act on the entries it names.
		return nil
	}
	if err != nil {
		return err
	}
	defer func() {
		resultErr = errors.Join(resultErr, pub.close())
	}()
	return recoverPendingPublication(pub)
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

func publicationCommitted(pub *publicationDir, journal publicationJournal) (bool, error) {
	switch journal.CommitMode {
	case publicationCommitModeAtlasSum:
		contents, err := pub.dir.ReadFile(migratesum.AtlasFileName)
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		if err != nil {
			return false, err
		}
		return bytes.Equal(contents, journal.Sum), nil
	case publicationCommitModeMarker:
		contents, err := pub.parent.ReadFile(pub.commitMarkerName())
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
	pub *publicationDir,
	journal publicationJournal,
) error {
	if err := removeNames(pub.dir, publicationStagedNames(journal)); err != nil {
		return fmt.Errorf("remove committed migration staging files: %w", err)
	}
	if err := pub.dir.Sync(); err != nil {
		return fmt.Errorf("sync committed migration directory: %w", err)
	}
	return removePublicationJournal(pub)
}

func rollBackPendingPublication(
	pub *publicationDir,
	journal publicationJournal,
) error {
	for _, entry := range journal.Entries {
		if err := rollBackPublicationEntry(
			pub,
			entry.Staged,
			entry.Final,
			entry.Digest,
		); err != nil {
			return err
		}
	}
	if err := pub.dir.Sync(); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return removePublicationJournal(pub)
}

func rollBackPublicationEntry(
	pub *publicationDir,
	stagedName, finalName, expectedDigest string,
) error {
	quarantineName := stagedName + rollbackQuarantineSuffix

	if err := quarantinePublishedMigration(pub, finalName, quarantineName); err != nil {
		return err
	}
	stagedDigest, stagedErr := fileDigest(pub, stagedName)
	quarantinedDigest, quarantineErr := fileDigest(pub, quarantineName)
	return reconcileRollbackFiles(pub, rollbackFileState{
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

func quarantinePublishedMigration(pub *publicationDir, finalName, quarantineName string) error {
	if _, err := pub.dir.Stat(quarantineName); errors.Is(err, os.ErrNotExist) {
		moveErr := pub.dir.MoveFileNoReplace(finalName, quarantineName)
		switch {
		case moveErr == nil:
			if err := pub.dir.Sync(); err != nil {
				return fmt.Errorf("sync migration directory after quarantine move: %w", err)
			}
		case errors.Is(moveErr, os.ErrNotExist):
		default:
			return fmt.Errorf("quarantine published migration file: %w", moveErr)
		}
	} else if err != nil {
		return fmt.Errorf("inspect quarantined migration file: %w", err)
	}
	return nil
}

type rollbackFileState struct {
	stagedName        string
	finalName         string
	quarantineName    string
	expectedDigest    string
	stagedDigest      string
	stagedErr         error
	quarantinedDigest string
	quarantineErr     error
}

func reconcileRollbackFiles(pub *publicationDir, state rollbackFileState) error {
	switch {
	case errors.Is(state.stagedErr, os.ErrNotExist) && errors.Is(state.quarantineErr, os.ErrNotExist):
		return nil
	case state.stagedErr == nil && errors.Is(state.quarantineErr, os.ErrNotExist):
		if state.stagedDigest != state.expectedDigest {
			return fmt.Errorf(
				"cannot safely recover migration publication: staging file content changed; preserved %s",
				pub.path(state.stagedName),
			)
		}
		return removeNames(pub.dir, []string{state.stagedName})
	case errors.Is(state.stagedErr, os.ErrNotExist) &&
		state.quarantineErr == nil:
		if state.quarantinedDigest != state.expectedDigest {
			return fmt.Errorf(
				"cannot safely recover migration publication: %s content changed; preserved at %s",
				pub.path(state.finalName),
				pub.path(state.quarantineName),
			)
		}
		return removeNames(pub.dir, []string{state.quarantineName})
	case state.stagedErr != nil:
		return fmt.Errorf("inspect staged migration file: %w", state.stagedErr)
	case state.quarantineErr != nil:
		return fmt.Errorf("inspect quarantined migration file: %w", state.quarantineErr)
	case state.stagedDigest != state.expectedDigest:
		return fmt.Errorf(
			"cannot safely recover migration publication: staging file content changed; preserved %s and %s",
			pub.path(state.stagedName),
			pub.path(state.quarantineName),
		)
	case state.quarantinedDigest != state.expectedDigest:
		return fmt.Errorf(
			"cannot safely recover migration publication: %s content changed; preserved at %s",
			pub.path(state.finalName),
			pub.path(state.quarantineName),
		)
	default:
		return removeNames(pub.dir, []string{state.quarantineName, state.stagedName})
	}
}

func fileDigest(pub *publicationDir, name string) (string, error) {
	contents, err := pub.dir.ReadFile(name)
	if err != nil {
		return "", err
	}
	return contentDigest(contents), nil
}

func removePublicationJournal(pub *publicationDir) error {
	return removePublicationJournalWithRetirer(pub, func(journalName, cleanupName string) error {
		if _, err := pub.parent.Lstat(journalName); errors.Is(err, os.ErrNotExist) {
			return nil
		} else if err != nil {
			return err
		}
		return pub.parent.ReplaceFile(journalName, cleanupName)
	})
}

func removePublicationJournalWithRetirer(
	pub *publicationDir,
	retire func(string, string) error,
) error {
	journalTemps, err := listNames(pub.parent, pub.journalName+".", stagedMigrationSuffix)
	if err != nil {
		return fmt.Errorf("find migration publication journal backups: %w", err)
	}
	cleanupName := pub.journalCleanupName()
	if err := retire(pub.journalName, cleanupName); err != nil {
		return fmt.Errorf("retire migration publication journal: %w", err)
	}
	if err := pub.parent.Sync(); err != nil {
		return fmt.Errorf("sync retired migration publication journal: %w", err)
	}
	names := append(
		[]string{pub.commitMarkerName(), cleanupName},
		journalTemps...,
	)
	if err := removeNames(pub.parent, names); err != nil {
		return fmt.Errorf("remove retired migration publication metadata: %w", err)
	}
	if err := pub.parent.Sync(); err != nil {
		return fmt.Errorf("sync migration publication journal directory: %w", err)
	}
	return nil
}

func removeOrphanPublicationTemps(pub *publicationDir) error {
	stagedNames, err := listNames(pub.dir, stagedMigrationPrefix, stagedMigrationSuffix)
	if err != nil {
		return fmt.Errorf("find orphan migration staging files: %w", err)
	}
	journalTemps, err := listNames(pub.parent, pub.journalName+".", stagedMigrationSuffix)
	if err != nil {
		return fmt.Errorf("find orphan migration publication journals: %w", err)
	}
	orphanJournalNames := slices.Concat(
		journalTemps,
		[]string{pub.commitMarkerName(), pub.journalCleanupName()},
	)
	if err := errors.Join(
		removeNames(pub.dir, stagedNames),
		removeNames(pub.parent, orphanJournalNames),
	); err != nil {
		return fmt.Errorf("remove orphan migration publication files: %w", err)
	}
	if len(stagedNames) > 0 {
		if err := pub.dir.Sync(); err != nil {
			return fmt.Errorf("sync migration directory after orphan cleanup: %w", err)
		}
	}
	if len(journalTemps) > 0 {
		if err := pub.parent.Sync(); err != nil {
			return fmt.Errorf("sync publication journal directory after orphan cleanup: %w", err)
		}
	}
	return nil
}

func writePublicationCommitMarker(
	pub *publicationDir,
	journal publicationJournal,
) error {
	contents, err := publicationJournalDigest(journal)
	if err != nil {
		return err
	}
	markerName := pub.commitMarkerName()
	tempName, err := stageMetadataFile(pub.parent, markerName, contents)
	if err != nil {
		return err
	}
	mode, err := detectPublicationMode(pub.parent, tempName)
	if err != nil {
		return errors.Join(err, removeNames(pub.parent, []string{tempName}))
	}
	if err := publishStagedFile(pub.parent, tempName, markerName, mode); err != nil {
		return errors.Join(err, removeNames(pub.parent, []string{tempName}))
	}
	if mode != publicationModeWriteThroughMove {
		if err := removeNames(pub.parent, []string{tempName}); err != nil {
			return err
		}
	}
	return pub.parent.Sync()
}

func publicationJournalDigest(journal publicationJournal) ([]byte, error) {
	contents, err := json.Marshal(journal)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(contents)
	return sum[:], nil
}

func writePublicationJournal(pub *publicationDir, journal publicationJournal) error {
	return writePublicationJournalWithPublisher(
		pub,
		journal,
		func(tempName, journalName string) error {
			mode, err := detectPublicationMode(pub.parent, tempName)
			if err != nil {
				return err
			}
			return publishStagedFile(pub.parent, tempName, journalName, mode)
		},
	)
}

func writePublicationJournalWithLink(
	pub *publicationDir,
	journal publicationJournal,
	link func(string, string) error,
) error {
	return writePublicationJournalWithPublisher(
		pub,
		journal,
		func(tempName, journalName string) error {
			if err := link(tempName, journalName); err != nil {
				if copyErr := copyFileExclusive(pub.parent, tempName, journalName); copyErr != nil {
					return errors.Join(err, copyErr)
				}
			}
			return nil
		},
	)
}

func writePublicationJournalWithPublisher(
	pub *publicationDir,
	journal publicationJournal,
	publish func(string, string) error,
) error {
	contents, err := json.Marshal(journal)
	if err != nil {
		return err
	}
	tempName, err := stageMetadataFile(pub.parent, pub.journalName, contents)
	if err != nil {
		return err
	}
	if err := publish(tempName, pub.journalName); err != nil {
		return errors.Join(err, removeNames(pub.parent, []string{tempName}))
	}
	return pub.parent.Sync()
}

// stageMetadataFile writes contents to a private temporary sibling of name and
// returns the staged name inside root.
func stageMetadataFile(
	root *pathguard.OpenedDirectory,
	name string,
	contents []byte,
) (string, error) {
	file, tempName, err := root.CreateTemp(name + ".*" + stagedMigrationSuffix)
	if err != nil {
		return "", err
	}
	if err := file.Chmod(0600); err != nil {
		return "", errors.Join(err, file.Close(), removeNames(root, []string{tempName}))
	}
	if _, err := file.Write(contents); err != nil {
		return "", errors.Join(err, file.Close(), removeNames(root, []string{tempName}))
	}
	if err := file.Sync(); err != nil {
		return "", errors.Join(err, file.Close(), removeNames(root, []string{tempName}))
	}
	if err := file.Close(); err != nil {
		return "", errors.Join(err, removeNames(root, []string{tempName}))
	}
	return tempName, nil
}

func readPublicationJournal(pub *publicationDir) (publicationJournal, error) {
	contents, err := pub.parent.ReadFile(pub.journalName)
	if err != nil {
		return publicationJournal{}, err
	}
	journal, decodeErr := decodePublicationJournal(contents)
	if decodeErr == nil {
		return journal, nil
	}
	backups, listErr := listNames(pub.parent, pub.journalName+".", stagedMigrationSuffix)
	if listErr != nil {
		return publicationJournal{}, errors.Join(decodeErr, listErr)
	}
	for _, backup := range backups {
		contents, err := pub.parent.ReadFile(backup)
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

// writeDirSum atomically refreshes atlas.sum after every complete migration
// file has been published. The sum is the commit marker for the batch.
func writeDirSum(pub *publicationDir, sum *migratesum.SumFile) (string, error) {
	if err := migratesum.WritePrecomputedWithFormatIn(
		pub.dir,
		migrator.MigrationDirFormatAtlas,
		sum,
	); err != nil {
		return "", fmt.Errorf("write atlas.sum: %w", err)
	}
	return pub.path(migratesum.AtlasFileName), nil
}

func removeNames(root *pathguard.OpenedDirectory, names []string) error {
	var resultErr error
	for _, name := range names {
		if err := root.Remove(name); err != nil && !errors.Is(err, os.ErrNotExist) {
			resultErr = errors.Join(resultErr, fmt.Errorf("remove %s: %w", name, err))
		}
	}
	return resultErr
}

func stageArtifactBatch(
	pub *publicationDir,
	artifacts []PublicationArtifact,
) (migrationBatch, error) {
	if len(artifacts) == 0 {
		return migrationBatch{}, errors.New("artifact publication batch is empty")
	}
	batch := migrationBatch{
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
				removeNames(pub.dir, batch.stagedNames),
			)
		}
		if slices.Contains(batch.names, artifact.Name) {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("duplicate artifact publication name %q", artifact.Name),
				removeNames(pub.dir, batch.stagedNames),
			)
		}
		stagedName, err := stageArtifactFile(pub, artifact.Contents)
		if err != nil {
			return migrationBatch{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
		}
		batch.names = append(batch.names, artifact.Name)
		batch.stagedNames = append(batch.stagedNames, stagedName)
		batch.digests = append(batch.digests, contentDigest(artifact.Contents))
	}
	mode, err := detectPublicationMode(pub.dir, batch.stagedNames[0])
	if err != nil {
		return migrationBatch{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	batch.mode = mode
	return batch, nil
}

func stageMigrationBatch(
	pub *publicationDir,
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
	version, err := nextMigrationVersionFS(pub.fsys())
	if err != nil {
		return migrationBatch{}, err
	}
	return stageMigrationBatchAt(pub, name, version, contents)
}

func stageMigrationBatchAt(
	pub *publicationDir,
	name string,
	version int64,
	contents []MigrationFileContent,
) (migrationBatch, error) {
	return stageMigrationBatchAtWithModeDetector(
		pub,
		name,
		version,
		contents,
		detectPublicationMode,
	)
}

func stageMigrationBatchAtWithModeDetector(
	pub *publicationDir,
	name string,
	version int64,
	contents []MigrationFileContent,
	detectMode func(*pathguard.OpenedDirectory, string) (publicationMode, error),
) (migrationBatch, error) {
	batch := migrationBatch{
		names:       make([]string, 0, len(contents)),
		stagedNames: make([]string, 0, len(contents)),
	}
	for i, content := range contents {
		fileVersion := version + int64(i)
		slug := migrationSlug(name + content.NameSuffix)
		stagedName, err := stageMigrationFile(pub, content.SQL)
		if err != nil {
			return migrationBatch{}, errors.Join(
				fmt.Errorf("stage migration file: %w", err),
				removeNames(pub.dir, batch.stagedNames),
			)
		}
		batch.names = append(batch.names, fmt.Sprintf("%d_%s.sql", fileVersion, slug))
		batch.stagedNames = append(batch.stagedNames, stagedName)
	}
	mode, err := detectMode(pub.dir, batch.stagedNames[0])
	if err != nil {
		return migrationBatch{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	batch.mode = mode
	if _, err := readStagedMigrationContents(pub, &batch); err != nil {
		return migrationBatch{}, errors.Join(err, removeNames(pub.dir, batch.stagedNames))
	}
	return batch, nil
}

func detectPublicationMode(
	root *pathguard.OpenedDirectory,
	stagedName string,
) (publicationMode, error) {
	return platformPublicationMode(root, stagedName)
}

func detectPublicationModeWithLink(
	root *pathguard.OpenedDirectory,
	stagedName string,
	link func(string, string) error,
) (publicationMode, error) {
	probeName := stagedName + ".link-probe"
	if err := link(stagedName, probeName); err != nil {
		return publicationModeCopy, nil
	}
	if err := root.Remove(probeName); err != nil {
		return "", fmt.Errorf("remove migration publication link probe: %w", err)
	}
	return publicationModeHardLink, nil
}

func stageMigrationFile(pub *publicationDir, migrationSQL string) (string, error) {
	return stageArtifactFile(pub, []byte(migrationSQL))
}

func stageArtifactFile(pub *publicationDir, contents []byte) (string, error) {
	file, name, err := pub.dir.CreateTemp(stagedMigrationPattern)
	if err != nil {
		return "", err
	}
	if err := file.Chmod(0644); err != nil {
		return "", errors.Join(err, file.Close(), removeNames(pub.dir, []string{name}))
	}
	if _, err := file.Write(contents); err != nil {
		return "", errors.Join(err, file.Close(), removeNames(pub.dir, []string{name}))
	}
	if err := file.Sync(); err != nil {
		return "", errors.Join(err, file.Close(), removeNames(pub.dir, []string{name}))
	}
	if err := file.Close(); err != nil {
		return "", errors.Join(err, removeNames(pub.dir, []string{name}))
	}
	return name, nil
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
