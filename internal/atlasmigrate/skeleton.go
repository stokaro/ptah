package atlasmigrate

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/atlasmigrateimport"
	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/migrationversion"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

// WriteSkeletonMigration writes one foreign layout's empty migration into dir
// and rewrites atlas.sum over that layout's covered set. It returns the created
// files in creation order.
//
// Every step goes through one rooted handle, opened once. `migrate new` used to
// resolve dir to a string and then reopen it by pathname for the mkdir, for each
// file, for the covered-set listing and for the checksum commit, so a directory
// or ancestor replaced after the path was validated could take any of those
// writes somewhere the gate never looked (stokaro/ptah#1118). Creating a missing
// directory goes through the same boundary, so it is materialized inside the
// opened root rather than wherever the pathname happens to point.
//
// A nil root keeps direct-CLI behavior, where an explicit absolute --dir is the
// operator's own choice of destination. The handle still exists; it is simply
// not confined to a project root.
//
// The version is the UTC stamp both binaries write. A name that already exists
// advances it to the next SECOND and retries the whole set, rather than failing
// the command for a directory that merely already holds this second's
// migration. The advance goes through [migrationversion.Writable] because a
// plain increment off a name written at :59 produces `...235960`, sixty seconds
// past the minute, a version no reader parses back as the time it looks like
// (stokaro/ptah#938).
func WriteSkeletonMigration(
	root *pathguard.OpenedDirectory,
	dir string,
	format atlasmigrateimport.Format,
	name string,
) ([]string, error) {
	w, err := createMigrationWriterDir(root, dir)
	if err != nil {
		return nil, err
	}
	defer func() { _ = w.Close() }()
	if afterSkeletonDirBound != nil {
		afterSkeletonDirBound()
	}

	version := MigrationVersion()
	for {
		writable, err := migrationversion.Writable(version, migrator.MigrationDirFormatAtlas)
		if err != nil {
			return nil, err
		}
		version = writable
		files, err := atlasmigrateimport.SkeletonFiles(format, version, name)
		if err != nil {
			return nil, err
		}
		written, err := createSkeletonFiles(w, files)
		if errors.Is(err, fs.ErrExist) {
			version++
			continue
		}
		if err != nil {
			return nil, err
		}
		if err := rehashSkeletonDir(w, format); err != nil {
			return nil, errors.Join(err, removeRootedFiles(w.dir, written))
		}
		paths := make([]string, 0, len(written))
		for _, entry := range written {
			paths = append(paths, filepath.Join(w.Path(), entry))
		}
		return paths, nil
	}
}

// afterSkeletonDirBound runs once the migration directory is bound and before
// anything is written. It is nil outside tests.
//
// It exists because this path has no product callback to hang the rooted-writer
// regression on, and the thing that has to be measured is a replacement that
// happens AFTER the directory was validated -- exactly what a pre-swap test
// cannot show, since that only exercises the open. `migrate diff` gets the same
// moment for free from its editor-preparation callback; this is the equivalent
// seam, kept unexported and unset in production.
var afterSkeletonDirBound func()

// createSkeletonFiles writes one layout's file set through the rooted handle,
// removing whatever it already created if a later file cannot be created. A
// half-written pair is worse than none: a rehash would then cover the up file
// of a migration with no rollback half.
//
// fs.ErrExist is returned unwrapped so the caller can tell "this second is
// taken" from a real failure and retry the whole set at the next second.
func createSkeletonFiles(w *migrationWriterDir, files []atlasmigrateimport.SkeletonFile) ([]string, error) {
	written := make([]string, 0, len(files))
	for _, file := range files {
		if err := writeExclusiveRootedFile(w.dir, file.Name, file.Content); err != nil {
			return nil, errors.Join(err, removeRootedFiles(w.dir, written))
		}
		written = append(written, file.Name)
	}
	return written, nil
}

func writeExclusiveRootedFile(d *pathguard.OpenedDirectory, name, content string) error {
	file, err := d.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, publishedFileMode)
	if err != nil {
		return err
	}
	if _, err := file.WriteString(content); err != nil {
		return errors.Join(err, file.Close(), d.Remove(name))
	}
	if err := file.Close(); err != nil {
		return errors.Join(err, d.Remove(name))
	}
	return nil
}

// rehashSkeletonDir rewrites atlas.sum over the covered set of the layout just
// written into, reading that set through the same handle the files were written
// through.
func rehashSkeletonDir(w *migrationWriterDir, format atlasmigrateimport.Format) error {
	fsys, err := w.FS()
	if err != nil {
		return err
	}
	names, err := atlasmigrateimport.SumFileNames(fsys, format)
	if err != nil {
		return err
	}
	sum, err := migratesum.ComputeAtlasFiles(fsys, names)
	if err != nil {
		return err
	}
	if _, err := publishDirSum(w, sum); err != nil {
		return fmt.Errorf("write %s: %w", migratesum.AtlasFileName, err)
	}
	return nil
}
