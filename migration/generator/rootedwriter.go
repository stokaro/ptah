package generator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/fsnapshot"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationsnapshot"
	"go.5x5.cz/ptah/internal/migrationversion"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file holds the generator's writer transactions -- creating a skeleton
// migration, publishing a planned one, and writing a checkpoint or data
// migration -- expressed against one rooted migration-directory handle each.
//
// The shape is the point (stokaro/ptah#1118). Both used to resolve the output
// directory to a string and then reopen it by pathname for every subsequent
// step: the mkdir, the version scan, each exclusive create, the atlas.sum
// commit, the pre-publication snapshot and the publication itself. Measured on
// master, a `ptah migrations create` racing a symlink swap of its own migration
// directory published the migration file and a staged atlas.sum into a
// directory outside AllowedOutputRoot in 4 of 40 runs, and a plan whose
// directory was replaced between planning and publication wrote both migration
// files into the replacement while reporting success.
//
// So: bind once, then never name the directory again. AllowedOutputRoot is
// opened rather than merely compared against, and every read and write below
// goes through that handle, which is why a replacement can no longer redirect
// one step of a transaction the earlier steps already validated.

// afterMigrationWriterBound runs once a writer transaction has bound its
// migration directory and before it reads or writes anything through it. It is
// nil outside tests.
//
// It exists because neither transaction has a product callback at that moment,
// and that moment is exactly what the rooted handle defends: a replacement
// landing in the window a pathname-based writer would resolve again. A test
// that swaps before the call can only exercise the open.
var afterMigrationWriterBound func()

func notifyMigrationWriterBound() {
	if afterMigrationWriterBound != nil {
		afterMigrationWriterBound()
	}
}

// afterMigrationPublicationVerified runs once a planned publication has
// revalidated the directory it holds and compared its contents, and before it
// writes anything. It is nil outside tests.
//
// The planned writer binds its directory during planning rather than during
// publication, so afterMigrationWriterBound no longer names the window that
// matters for it: by the time WriteFilesContext runs, the handle is old. What
// the handle uniquely defends is the gap between "verified" and "committed",
// and a replacement landing there is what this hook lets a test stage.
var afterMigrationPublicationVerified func()

func notifyMigrationPublicationVerified() {
	if afterMigrationPublicationVerified != nil {
		afterMigrationPublicationVerified()
	}
}

// afterMigrationFileNamesChosen runs once a writer transaction has settled on
// the file names it is about to create and before it creates the first of them.
// It is nil outside tests.
//
// It names the destination-replacement window for the migration files
// themselves, which is a different window from the two hooks above: those stage
// a replacement of the DIRECTORY, and this one lets a test stage a replacement
// of the exact final name inside the directory the writer holds. A fixture that
// pre-places a file at that name before the call is answered by the binding
// itself, so it measures the open rather than the commit; only a file that
// appears after the name is chosen exercises the exclusive create as a
// conditional commit (stokaro/ptah#1118).
var afterMigrationFileNamesChosen func(names []string)

func notifyMigrationFileNamesChosen(names ...string) {
	if afterMigrationFileNamesChosen != nil {
		afterMigrationFileNamesChosen(names)
	}
}

// openOutputRoot opens the confinement root a writer transaction runs inside,
// or returns nil for the direct-CLI shape where an explicit absolute output
// directory is the operator's own choice of destination.
//
// Opening it is what makes AllowedOutputRoot mean something after resolution.
// It used to be a string compared against once by pathguard.ResolveWithinRoot
// and then dropped, so a directory replaced afterwards escaped a boundary the
// caller believed was still being enforced.
func openOutputRoot(allowedOutputRoot string) (*pathguard.OpenedDirectory, error) {
	if allowedOutputRoot == "" {
		return nil, nil
	}
	root, err := pathguard.OpenDirectory(allowedOutputRoot)
	if err != nil {
		return nil, fmt.Errorf("open allowed output root: %w", err)
	}
	return root, nil
}

func closeOutputRoot(root *pathguard.OpenedDirectory) error {
	if root == nil {
		return nil
	}
	return root.Close()
}

// bindMigrationOutputDir creates the levels above the migration directory and
// binds the directory itself, without creating it. The caller compares the
// directory's absence against what it planned for before materializing it.
//
// Creating every missing level, not just the leaf, is measured behavior rather
// than convenience: `--dir file://a/b` with no `a` must create `a`, `a/b`, the
// migration file and atlas.sum and exit 0, which is what the pinned community
// binary v1.3.0 does at two and at three missing levels (stokaro/ptah#1241 item
// 4). What must not relax is a path component that exists and is not a
// directory: the parent MkdirAll refuses that with ENOTDIR, and a leaf that is a
// regular file fails the rooted open, so both stay exit 1.
func bindMigrationOutputDir(
	root *pathguard.OpenedDirectory,
	outputDir string,
) (*atlasmigrate.MigrationWriter, error) {
	if err := atlasmigrate.EnsureMigrationParent(root, outputDir); err != nil {
		return nil, err
	}
	writer, err := atlasmigrate.OpenMigrationWriter(root, outputDir)
	if err != nil {
		return nil, err
	}
	notifyMigrationWriterBound()
	return writer, nil
}

// bindPlannedMigrationDir binds the migration directory a plan will publish
// into, for the plan to hold until its publication attempt returns.
//
// The confinement root is opened, consulted for the bind, and then closed: the
// parent and directory handles were opened through it and stay valid on their
// own, so the plan keeps two descriptors rather than three and the root is not
// pinned for the caller's whole pre-publication window. What the root
// contributes is refusing, here, a directory that resolves outside it -- after
// this point the binding is the boundary.
func bindPlannedMigrationDir(
	allowedOutputRoot, outputDir string,
) (*atlasmigrate.MigrationWriter, error) {
	root, err := openOutputRoot(allowedOutputRoot)
	if err != nil {
		return nil, err
	}
	writer, err := bindMigrationOutputDir(root, outputDir)
	if err != nil {
		return nil, errors.Join(err, closeOutputRoot(root))
	}
	if err := closeOutputRoot(root); err != nil {
		return nil, errors.Join(err, writer.Close())
	}
	return writer, nil
}

// writeEmptyMigration creates the skeleton files for one migration through a
// rooted handle bound for the whole transaction.
func writeEmptyMigration(
	root *pathguard.OpenedDirectory,
	outputDir, name string,
	dirFormat migrator.MigrationDirFormat,
) (*MigrationFiles, error) {
	writer, err := bindMigrationOutputDir(root, outputDir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = writer.Close() }()
	if err := writer.Create(); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	files, err := writeEmptyMigrationFiles(writer, name, dirFormat)
	if err != nil {
		return nil, err
	}
	if err := writer.SyncDir(); err != nil {
		return nil, fmt.Errorf("failed to flush output directory: %w", err)
	}
	return files, nil
}

func writeEmptyMigrationFiles(
	writer *atlasmigrate.MigrationWriter,
	name string,
	dirFormat migrator.MigrationDirFormat,
) (*MigrationFiles, error) {
	names, err := migrationDirNames(writer)
	if err != nil {
		return nil, fmt.Errorf("failed to read output directory: %w", err)
	}
	if dirFormat == migrator.MigrationDirFormatAtlas {
		return writeEmptyAtlasMigration(writer, names, name)
	}
	return writeEmptyPtahMigration(writer, names, name)
}

// writeEmptyAtlasMigration writes one Atlas-style file and commits atlas.sum
// over it. The checksum is computed from the same handle the file was created
// through, so it describes the directory this transaction wrote into rather
// than whatever the pathname resolves to by the time the sum is written.
func writeEmptyAtlasMigration(
	writer *atlasmigrate.MigrationWriter,
	names []string,
	name string,
) (*MigrationFiles, error) {
	version, err := firstFreeAtlasVersion(names, nextAtlasMigrationVersion())
	if err != nil {
		return nil, err
	}
	for {
		// The retry below advances the version, so the bound is re-checked here
		// rather than only on the value the scan chose.
		if err := migrationversion.Check(version, migrator.MigrationDirFormatAtlas); err != nil {
			return nil, err
		}
		fileName := atlasEmptyMigrationFileName(version, name)
		notifyMigrationFileNamesChosen(fileName)
		err := writer.WriteNew(fileName, "")
		if errors.Is(err, fs.ErrExist) {
			version++
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to write atlas migration file: %w", err)
		}
		if err := publishMigrationDirSum(writer, migrator.MigrationDirFormatAtlas); err != nil {
			return nil, errors.Join(
				fmt.Errorf("failed to write atlas migration checksum: %w", err),
				writer.Remove(fileName),
			)
		}
		return migrationFilesFromPairs([]MigrationFilePair{{
			UpFile:  filepath.Join(writer.Path(), fileName),
			Version: version,
		}}), nil
	}
}

// writeEmptyPtahMigration writes the paired up/down skeleton. A down file that
// cannot be created withdraws the up file it already wrote, because a migration
// with no rollback half is worse than none.
func writeEmptyPtahMigration(
	writer *atlasmigrate.MigrationWriter,
	names []string,
	name string,
) (*MigrationFiles, error) {
	generatedAt := time.Now().UTC().Format(time.RFC3339)
	upSQL := emptyMigrationSQL(name, generatedAt, "UP")
	downSQL := emptyMigrationSQL(name, generatedAt, "DOWN")
	version, err := nextAvailablePtahVersion(names, migrator.GetNextMigrationVersion(), name)
	if err != nil {
		return nil, err
	}
	for {
		// The two retries below advance the version, so the bound is re-checked
		// here rather than only on the value the scan chose.
		if err := migrationversion.Check(version, migrator.MigrationDirFormatPtah); err != nil {
			return nil, err
		}
		upName := migrator.GenerateMigrationFileName(version, name, "up")
		downName := migrator.GenerateMigrationFileName(version, name, "down")
		notifyMigrationFileNamesChosen(upName, downName)

		upErr := writer.WriteNew(upName, upSQL)
		if errors.Is(upErr, fs.ErrExist) {
			version++
			continue
		}
		if upErr != nil {
			return nil, fmt.Errorf("failed to write up migration file: %w", upErr)
		}

		downErr := writer.WriteNew(downName, downSQL)
		if errors.Is(downErr, fs.ErrExist) {
			version++
			if removeErr := writer.Remove(upName); removeErr != nil {
				return nil, fmt.Errorf("failed to withdraw up migration file: %w", removeErr)
			}
			continue
		}
		if downErr != nil {
			return nil, errors.Join(
				fmt.Errorf("failed to write down migration file: %w", downErr),
				writer.Remove(upName),
			)
		}

		return migrationFilesFromPairs([]MigrationFilePair{{
			UpFile:   filepath.Join(writer.Path(), upName),
			DownFile: filepath.Join(writer.Path(), downName),
			Version:  version,
		}}), nil
	}
}

// publishMigrationDirSum recomputes the layout's integrity file from the bound
// handle and commits it conditionally on the checksum state that read observed.
//
// Reading and writing through the same handle is what binds the checksum to the
// snapshot it describes: computed over a pathname, it would cover whatever the
// name resolves to at that instant, which is not necessarily the directory this
// transaction created its files in.
func publishMigrationDirSum(
	writer *atlasmigrate.MigrationWriter,
	format migrator.MigrationDirFormat,
) error {
	fsys, err := writer.FS()
	if err != nil {
		return err
	}
	sum, err := migratesum.ComputeWithFormat(fsys, format)
	if err != nil {
		return err
	}
	if _, err := writer.PublishSum(format, sum); err != nil {
		return err
	}
	return nil
}

// writeRootedAtlasCheckpoint is the Atlas-layout checkpoint writer's whole
// transaction: bind the directory once, create it if it is missing, create the
// checkpoint exclusively, and commit atlas.sum over it -- all through that one
// handle.
//
// It used to run entirely on pathnames, and the split it allowed was measured
// rather than argued: with the directory renamed aside after the checkpoint file
// was created, `WriteAtlasCheckpointFile` returned nil having left the
// checkpoint in the retained directory and written atlas.sum into the directory
// that took over the pathname. The retained directory is then uncovered, so
// every reader rejects it, and the impostor carries a checksum for a snapshot it
// never held (stokaro/ptah#1118).
func writeRootedAtlasCheckpoint(
	outputDir string,
	version int64,
	description, upSQL string,
	authorizedMigrationsFS fs.FS,
) (path string, err error) {
	writer, err := bindMigrationOutputDir(nil, outputDir)
	if err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}
	defer func() { err = errors.Join(err, writer.Close()) }()
	authorized, err := bindAuthorizedMigrations(writer, authorizedMigrationsFS)
	if err != nil {
		return "", err
	}
	if err := writer.Create(); err != nil {
		return "", fmt.Errorf("failed to create output directory: %w", err)
	}

	name, contents := AtlasCheckpointArtifact(version, description, upSQL)
	notifyMigrationFileNamesChosen(name)
	if writeErr := writer.WriteNew(name, contents); writeErr != nil {
		if errors.Is(writeErr, fs.ErrExist) {
			return "", fmt.Errorf("checkpoint file %s already exists", filepath.Join(writer.Path(), name))
		}
		return "", fmt.Errorf("failed to write atlas checkpoint file: %w", writeErr)
	}
	if sumErr := publishAuthorizedMigrationDirSum(
		writer,
		migrator.MigrationDirFormatAtlas,
		authorized,
		map[string][]byte{name: []byte(contents)},
	); sumErr != nil {
		// The checkpoint is only safe to leave behind once atlas.sum covers it;
		// an uncovered file makes the whole directory fail verification. The
		// withdrawal goes through the same handle the file was created through,
		// so it cannot delete a same-named file in some other directory.
		return "", errors.Join(
			fmt.Errorf("failed to write atlas checkpoint checksum: %w", sumErr),
			writer.Remove(name),
		)
	}
	if err := writer.SyncDir(); err != nil {
		return "", fmt.Errorf("failed to flush output directory: %w", err)
	}
	return filepath.Join(writer.Path(), name), nil
}

// writeRootedMigrationPair is the paired-layout counterpart: both halves and
// ptah.sum committed through one binding of the output directory.
//
// The refusal is the exclusive create itself rather than a preceding existence
// check. Two separate steps left a window in which the name a writer had just
// observed free could be taken by someone else, and the write then overwrote
// them; here the same syscall that creates the file is the one that observes the
// name, so a file that appeared in between is reported and never overwritten.
//
// A down half that cannot be created withdraws the up half, because a migration
// with no rollback is worse than none -- the same rule the skeleton writer
// follows next door.
func writeRootedMigrationPair(
	outputDir string,
	version int64,
	description, upSQL, downSQL, kind string,
	nameFor func(version int64, description, direction string) string,
	authorizedMigrationsFS fs.FS,
) (upPath, downPath string, err error) {
	writer, err := bindMigrationOutputDir(nil, outputDir)
	if err != nil {
		return "", "", fmt.Errorf("failed to create output directory: %w", err)
	}
	defer func() { err = errors.Join(err, writer.Close()) }()
	authorized, err := bindAuthorizedMigrations(writer, authorizedMigrationsFS)
	if err != nil {
		return "", "", err
	}
	if err := writer.Create(); err != nil {
		return "", "", fmt.Errorf("failed to create output directory: %w", err)
	}

	upName := nameFor(version, description, "up")
	downName := nameFor(version, description, "down")
	notifyMigrationFileNamesChosen(upName, downName)

	taken := fmt.Errorf("%s files for version %d already exist", kind, version)
	if upErr := writer.WriteNew(upName, upSQL); upErr != nil {
		if errors.Is(upErr, fs.ErrExist) {
			return "", "", taken
		}
		return "", "", fmt.Errorf("failed to write %s up file: %w", kind, upErr)
	}
	if downErr := writer.WriteNew(downName, downSQL); downErr != nil {
		withdrawn := writer.Remove(upName)
		if errors.Is(downErr, fs.ErrExist) {
			return "", "", errors.Join(taken, withdrawn)
		}
		return "", "", errors.Join(
			fmt.Errorf("failed to write %s down file: %w", kind, downErr),
			withdrawn,
		)
	}

	sumName, err := migratesum.FileNameForFormat(migrator.MigrationDirFormatPtah)
	if err != nil {
		return "", "", err
	}
	if sumErr := publishAuthorizedMigrationDirSum(
		writer,
		migrator.MigrationDirFormatPtah,
		authorized,
		map[string][]byte{
			upName:   []byte(upSQL),
			downName: []byte(downSQL),
		},
	); sumErr != nil {
		return "", "", errors.Join(
			fmt.Errorf("failed to rewrite %s: %w", sumName, sumErr),
			writer.Remove(upName),
			writer.Remove(downName),
		)
	}
	if err := writer.SyncDir(); err != nil {
		return "", "", fmt.Errorf("failed to flush output directory: %w", err)
	}
	return filepath.Join(writer.Path(), upName), filepath.Join(writer.Path(), downName), nil
}

type authorizedMigrationState struct {
	enabled  bool
	snapshot fsnapshot.Snapshot
}

func bindAuthorizedMigrations(
	writer *atlasmigrate.MigrationWriter,
	authorized fs.FS,
) (authorizedMigrationState, error) {
	if authorized == nil {
		return authorizedMigrationState{}, nil
	}
	expected, err := migrationsnapshot.Capture(authorized)
	if err != nil {
		return authorizedMigrationState{}, fmt.Errorf("capture authorized migration directory: %w", err)
	}
	if err := verifyAuthorizedMigrationSnapshot(writer, expected); err != nil {
		return authorizedMigrationState{}, err
	}
	return authorizedMigrationState{enabled: true, snapshot: expected}, nil
}

func verifyAuthorizedMigrationSnapshot(
	writer *atlasmigrate.MigrationWriter,
	expected fsnapshot.Snapshot,
) error {
	current := fsnapshot.Snapshot{}
	if writer.Exists() {
		fsys, err := writer.FS()
		if err != nil {
			return fmt.Errorf("open migration directory before checkpoint publication: %w", err)
		}
		current, err = migrationsnapshot.Capture(fsys)
		if err != nil {
			return fmt.Errorf("capture migration directory before checkpoint publication: %w", err)
		}
	}
	if !expected.Equal(current) {
		return ErrMigrationDirectoryChanged
	}
	return nil
}

func publishAuthorizedMigrationDirSum(
	writer *atlasmigrate.MigrationWriter,
	format migrator.MigrationDirFormat,
	authorized authorizedMigrationState,
	newFiles map[string][]byte,
) error {
	if !authorized.enabled {
		return publishMigrationDirSum(writer, format)
	}
	expected, err := authorized.snapshot.WithFiles(newFiles)
	if err != nil {
		return fmt.Errorf("build authorized checkpoint snapshot: %w", err)
	}
	if err := verifyAuthorizedMigrationSnapshot(writer, expected); err != nil {
		return err
	}
	// Compute from the authorized state rather than reopening the live directory.
	// A concurrent edit after the comparison can make this sum fail verification,
	// but it cannot make the new sum legitimize bytes the checkpoint never used.
	sum, err := migratesum.ComputeWithFormat(expected, format)
	if err != nil {
		return err
	}
	_, err = writer.PublishSum(format, sum)
	return err
}

// publishPlannedMigration commits a planned batch through the handle the
// transaction already verified the directory with.
func publishPlannedMigration(
	ctx context.Context,
	writer *atlasmigrate.MigrationWriter,
	reportFormat string,
	specs []generatedMigrationSpec,
) (*MigrationFiles, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	artifacts, pairs, err := renderMigrationArtifacts(writer.Path(), reportFormat, specs)
	if err != nil {
		return nil, err
	}
	if err := writer.Create(); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	if _, err := writer.PublishArtifacts(ctx, artifacts); err != nil {
		return nil, err
	}
	return migrationFilesFromPairs(pairs), nil
}

// migrationDirNames lists the migration directory's file names through the
// bound handle. An absent directory lists as empty, which is the same answer
// the version scan wants.
func migrationDirNames(writer *atlasmigrate.MigrationWriter) ([]string, error) {
	entries, err := writer.Entries()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		names = append(names, entry.Name())
	}
	return names, nil
}
