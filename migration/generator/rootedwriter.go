package generator

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"time"

	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

// This file holds the generator's two writer transactions -- creating a
// skeleton migration and publishing a planned one -- expressed against one
// rooted migration-directory handle each.
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
// into, for the plan to hold until it publishes.
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
	version := nextAvailableAtlasVersion(names, nextAtlasMigrationVersion())
	for {
		fileName := atlasEmptyMigrationFileName(version, name)
		err := writer.WriteNew(fileName, "")
		if errors.Is(err, fs.ErrExist) {
			version++
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("failed to write atlas migration file: %w", err)
		}
		if err := publishMigrationDirSum(writer); err != nil {
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
	version := nextAvailablePtahVersion(names, migrator.GetNextMigrationVersion(), name)
	for {
		upName := migrator.GenerateMigrationFileName(version, name, "up")
		downName := migrator.GenerateMigrationFileName(version, name, "down")

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

// publishMigrationDirSum recomputes atlas.sum from the bound handle and commits
// it conditionally on the checksum state that read observed.
func publishMigrationDirSum(writer *atlasmigrate.MigrationWriter) error {
	fsys, err := writer.FS()
	if err != nil {
		return err
	}
	sum, err := migratesum.ComputeWithFormat(fsys, migrator.MigrationDirFormatAtlas)
	if err != nil {
		return err
	}
	if _, err := writer.PublishAtlasSum(sum); err != nil {
		return err
	}
	return nil
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
