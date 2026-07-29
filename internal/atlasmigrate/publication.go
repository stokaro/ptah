package atlasmigrate

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/stokaro/ptah/internal/fsdurable"
	"github.com/stokaro/ptah/internal/migratesum"
	"github.com/stokaro/ptah/migration/migrator"
)

const (
	publicationJournalVersion = 1
	publicationJournalSuffix  = ".ptah-migrate-diff.pending"
	stagedMigrationPattern    = ".ptah-migrate-diff-*.tmp"
)

type publicationEntry struct {
	Staged string `json:"staged"`
	Final  string `json:"final"`
}

type publicationJournal struct {
	Version int                `json:"version"`
	Entries []publicationEntry `json:"entries"`
}

type migrationBatch struct {
	paths       []string
	stagedPaths []string
}

// writeDiffArtifacts durably publishes one batch of migration files and then
// atlas.sum. A journal next to the migration directory makes an interrupted
// publication recoverable by the next lock holder.
func writeDiffArtifacts(
	ctx context.Context,
	dir, name string,
	contents []MigrationFileContent,
) (DiffResult, error) {
	if err := ctx.Err(); err != nil {
		return DiffResult{}, err
	}
	if err := recoverPendingPublication(dir); err != nil {
		return DiffResult{}, fmt.Errorf("recover previous migration artifact publication: %w", err)
	}
	batch, err := stageMigrationBatch(dir, name, contents)
	if err != nil {
		return DiffResult{}, err
	}
	if err := beginPublication(dir, batch); err != nil {
		return DiffResult{}, errors.Join(err, removeFiles(batch.stagedPaths))
	}
	published, err := publishMigrationBatch(dir, batch)
	if err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	if err := ctx.Err(); err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	sumPath, err := writeDirSum(dir)
	if err != nil {
		return DiffResult{}, errors.Join(err, abortPendingPublication(dir, batch, published))
	}
	result := DiffResult{MigrationPaths: batch.paths, SumPath: sumPath}
	if err := recoverPendingPublication(dir); err != nil {
		return result, fmt.Errorf("finalize migration artifact publication: %w", err)
	}
	return result, nil
}

func beginPublication(dir string, batch migrationBatch) error {
	entries := make([]publicationEntry, len(batch.paths))
	for i := range batch.paths {
		entries[i] = publicationEntry{
			Staged: filepath.Base(batch.stagedPaths[i]),
			Final:  filepath.Base(batch.paths[i]),
		}
	}
	journal := publicationJournal{
		Version: publicationJournalVersion,
		Entries: entries,
	}
	if err := writePublicationJournal(dir, journal); err != nil {
		return fmt.Errorf("write migration publication journal: %w", err)
	}
	return nil
}

func publishMigrationBatch(dir string, batch migrationBatch) (int, error) {
	for i, stagedPath := range batch.stagedPaths {
		if err := os.Link(stagedPath, batch.paths[i]); err != nil {
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

func abortPendingPublication(dir string, batch migrationBatch, published int) error {
	if err := removeFiles(batch.paths[:published]); err != nil {
		return fmt.Errorf("roll back published migration files: %w", err)
	}
	if err := removeFiles(batch.stagedPaths); err != nil {
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
	committed, err := publicationCommitted(dir)
	if err != nil {
		return err
	}
	if committed {
		return finalizeCommittedPublication(dir, journalPath, journal)
	}
	return rollBackPendingPublication(dir, journalPath, journal)
}

func publicationCommitted(dir string) (bool, error) {
	result, err := migratesum.VerifyWithFormat(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
	if errors.Is(err, migratesum.ErrSumFileMissing) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("verify migration publication commit marker: %w", err)
	}
	return result.OK(), nil
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
		if err := rollBackPublicationEntry(stagedPath, finalPath); err != nil {
			return err
		}
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return removePublicationJournal(journalPath)
}

func rollBackPublicationEntry(stagedPath, finalPath string) error {
	stagedInfo, stagedErr := os.Stat(stagedPath)
	finalInfo, finalErr := os.Stat(finalPath)
	switch {
	case errors.Is(stagedErr, os.ErrNotExist) && errors.Is(finalErr, os.ErrNotExist):
		return nil
	case stagedErr == nil && errors.Is(finalErr, os.ErrNotExist):
		return removeFiles([]string{stagedPath})
	case errors.Is(stagedErr, os.ErrNotExist) && finalErr == nil:
		return fmt.Errorf(
			"cannot safely recover migration publication: staging file missing for %s",
			finalPath,
		)
	case stagedErr != nil:
		return fmt.Errorf("inspect staged migration file: %w", stagedErr)
	case finalErr != nil:
		return fmt.Errorf("inspect published migration file: %w", finalErr)
	case !os.SameFile(stagedInfo, finalInfo):
		return fmt.Errorf(
			"cannot safely recover migration publication: %s is not the journaled staging file",
			finalPath,
		)
	default:
		return removeFiles([]string{finalPath, stagedPath})
	}
}

func removePublicationJournal(journalPath string) error {
	if err := removeFiles([]string{journalPath}); err != nil {
		return fmt.Errorf("remove migration publication journal: %w", err)
	}
	if err := fsdurable.SyncDir(filepath.Dir(journalPath)); err != nil {
		return fmt.Errorf("sync migration publication journal directory: %w", err)
	}
	return nil
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
	if err := removeFiles(append(stagedPaths, journalTemps...)); err != nil {
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

func writePublicationJournal(dir string, journal publicationJournal) error {
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
	if err := os.Link(tempPath, journalPath); err != nil {
		return errors.Join(err, removeFiles([]string{tempPath}))
	}
	if err := removeFiles([]string{tempPath}); err != nil {
		return err
	}
	return fsdurable.SyncDir(parent)
}

func readPublicationJournal(path string) (publicationJournal, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return publicationJournal{}, err
	}
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
	stagedNames := make([]string, 0, len(journal.Entries))
	finalNames := make([]string, 0, len(journal.Entries))
	for _, entry := range journal.Entries {
		if filepath.Base(entry.Staged) != entry.Staged ||
			!strings.HasPrefix(entry.Staged, ".ptah-migrate-diff-") ||
			!strings.HasSuffix(entry.Staged, ".tmp") {
			return fmt.Errorf("invalid staged migration publication path: %q", entry.Staged)
		}
		if filepath.Base(entry.Final) != entry.Final ||
			!strings.HasSuffix(entry.Final, ".sql") {
			return fmt.Errorf("invalid final migration publication path: %q", entry.Final)
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
func writeDirSum(dir string) (string, error) {
	sumPath := filepath.Join(dir, migratesum.AtlasFileName)
	if _, err := migratesum.WriteWithFormat(dir, migrator.MigrationDirFormatAtlas); err != nil {
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

// writeMigrationFiles writes every planned migration file with consecutive
// versions. It is retained as a focused white-box primitive; production
// publication uses writeDiffArtifacts so the batch is journaled through the
// atlas.sum commit.
func writeMigrationFiles(dir, name string, contents []MigrationFileContent) ([]string, error) {
	batch, err := stageMigrationBatch(dir, name, contents)
	if err != nil {
		return nil, err
	}
	published, err := publishMigrationBatch(dir, batch)
	if err != nil {
		return nil, errors.Join(err, rollBackUnjournaledBatch(dir, batch, published))
	}
	if err := removeFiles(batch.stagedPaths); err != nil {
		return nil, errors.Join(err, removeFiles(batch.paths))
	}
	return batch.paths, nil
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
	return batch, nil
}

// writeMigrationFilesAt stages and syncs one complete batch before publishing
// each final path without overwriting an existing migration.
func writeMigrationFilesAt(
	dir, name string,
	version int64,
	contents []MigrationFileContent,
) ([]string, error) {
	batch, err := stageMigrationBatchAt(dir, name, version, contents)
	if err != nil {
		return nil, err
	}
	published, err := publishMigrationBatch(dir, batch)
	if err != nil {
		return nil, errors.Join(err, rollBackUnjournaledBatch(dir, batch, published))
	}
	if err := removeFiles(batch.stagedPaths); err != nil {
		return nil, errors.Join(err, removeFiles(batch.paths))
	}
	return batch.paths, nil
}

func rollBackUnjournaledBatch(dir string, batch migrationBatch, published int) error {
	if err := removeFiles(batch.paths[:published]); err != nil {
		return fmt.Errorf("roll back published migration files: %w", err)
	}
	if err := removeFiles(batch.stagedPaths); err != nil {
		return fmt.Errorf("remove rolled back migration staging files: %w", err)
	}
	if err := fsdurable.SyncDir(dir); err != nil {
		return fmt.Errorf("sync rolled back migration directory: %w", err)
	}
	return nil
}

func stageMigrationFile(dir, migrationSQL string) (string, error) {
	file, err := os.CreateTemp(dir, stagedMigrationPattern)
	if err != nil {
		return "", err
	}
	path := file.Name()
	if err := file.Chmod(0644); err != nil {
		return "", errors.Join(err, file.Close(), removeFiles([]string{path}))
	}
	if _, err := file.WriteString(migrationSQL); err != nil {
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
	files, err := migrator.DiscoverMigrationFiles(os.DirFS(dir), migrator.MigrationDirFormatAtlas)
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
