package atlasmigrate

import (
	"context"
	"errors"
	"io/fs"

	"go.5x5.cz/ptah/internal/migratesum"
	"go.5x5.cz/ptah/internal/pathguard"
	"go.5x5.cz/ptah/migration/migrator"
)

// MigrationWriter is the rooted migration-directory capability a writer
// transaction outside this package runs through: one binding of the directory
// and its parent, reused for every read, create, publication and rollback the
// transaction performs.
//
// It exists because the native generator had the shape stokaro/ptah#1118
// describes. `ptah migrations create` resolved its output directory once, then
// reopened it by pathname for the mkdir, for the version scan, for each file it
// created and for the atlas.sum commit; `ptah migrations generate` verified the
// directory's contents by pathname and then let the publication resolve the
// pathname a second time. A directory or ancestor replaced between any two of
// those steps takes the write somewhere the verification never looked.
//
// The methods are deliberately thin: this type is the boundary, not a second
// filesystem abstraction. Everything behind it is the same migrationWriterDir
// the `migrate diff` publication already runs through, so both writers share one
// answer to "which filesystem object is this transaction committing to".
//
// A nil root keeps direct-CLI behavior, where an explicit absolute directory is
// the operator's own choice of destination. The binding still happens; it is
// simply not confined to a project root.
type MigrationWriter struct {
	dir *migrationWriterDir
}

// OpenMigrationWriter binds dir and its parent without creating dir. A missing
// migration directory is reported through Exists rather than as an error, so a
// caller can compare the directory's absence against what it planned for before
// deciding to materialize it.
//
// The parent must already exist; call EnsureMigrationParent first when the
// caller accepts a directory nested below missing levels.
func OpenMigrationWriter(
	root *pathguard.OpenedDirectory,
	dir string,
) (*MigrationWriter, error) {
	bound, err := openMigrationWriterDir(root, dir)
	if err != nil {
		return nil, err
	}
	return &MigrationWriter{dir: bound}, nil
}

// EnsureMigrationParent creates dir's parent, and every level missing above it,
// through root when one is supplied.
func EnsureMigrationParent(root *pathguard.OpenedDirectory, dir string) error {
	return ensureMigrationDirParent(root, dir)
}

// Close releases the directory and parent handles.
func (w *MigrationWriter) Close() error {
	return w.dir.Close()
}

// Path returns the stable lexical path selected for the migration directory. It
// is display and result data only; no write step resolves it again.
func (w *MigrationWriter) Path() string {
	return w.dir.Path()
}

// Exists reports whether the migration directory was present when the handles
// were bound, or has been materialized by Create since.
func (w *MigrationWriter) Exists() bool {
	return w.dir.Exists()
}

// Create materializes a missing migration directory through the parent handle
// bound at open time, so it is created inside the opened root rather than
// wherever the pathname happens to point by now. Creating a directory that
// already exists is not an error.
//
// It is the same materialization the creating binding performs, so a directory
// this package creates for itself and one an outside caller creates through the
// boundary land in exactly the same place.
func (w *MigrationWriter) Create() error {
	return w.dir.create()
}

// Revalidate reports whether the pathname this writer was selected by still
// names the object it holds, or -- for a migration directory that was absent
// when the writer bound -- whether that name is still free in the bound parent.
//
// It is the check a transaction runs when it has to leave a window open between
// binding the directory and committing to it, which the planned-migration
// writer does: the plan is built by one exported call and published by another,
// so the caller owns the time in between. Holding the handle across that window
// is what makes the answer sound rather than a bet on the operating system not
// reissuing an identifier (stokaro/ptah#1118).
func (w *MigrationWriter) Revalidate() error {
	return w.dir.revalidate()
}

// FS returns the escape-resistant filesystem rooted at the migration directory.
func (w *MigrationWriter) FS() (fs.FS, error) {
	return w.dir.FS()
}

// Entries lists the migration directory through the bound handle. A directory
// that does not exist lists as empty, because a version scan over an absent
// directory has the same answer as one over an empty directory.
func (w *MigrationWriter) Entries() ([]fs.DirEntry, error) {
	if !w.dir.Exists() {
		return nil, nil
	}
	return w.dir.dir.ReadDir(".")
}

// WriteNew creates name with contents through the bound handle, failing with
// fs.ErrExist when the name is taken. The exclusive create is what makes the
// commit conditional on the destination being absent: a file that appeared
// after the caller chose the name is reported, never overwritten.
func (w *MigrationWriter) WriteNew(name, contents string) error {
	if !w.dir.Exists() {
		return w.dir.missingDirError()
	}
	return writeExclusiveRootedFile(w.dir.dir, name, contents)
}

// Remove deletes name through the bound handle, for withdrawing a file this
// transaction created after a later step failed.
func (w *MigrationWriter) Remove(name string) error {
	if !w.dir.Exists() {
		return nil
	}
	err := w.dir.dir.Remove(name)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// PublishSum commits the integrity file the layout selects -- atlas.sum for the
// Atlas layout, ptah.sum for the paired one -- through the bound handle,
// conditionally on the checksum state this transaction observed, and returns
// its path. A concurrent writer that replaced it after the snapshot was taken is
// reported instead of overwritten.
//
// The layout is a parameter rather than a second method because the two layouts
// differ only in the file name: both must reach the destination through the same
// handle the migration files were created through, or the checksum describes a
// directory this transaction did not write into (stokaro/ptah#1118).
func (w *MigrationWriter) PublishSum(
	format migrator.MigrationDirFormat,
	sum *migratesum.SumFile,
) (string, error) {
	if !w.dir.Exists() {
		return "", w.dir.missingDirError()
	}
	sumName, err := migratesum.FileNameForFormat(format)
	if err != nil {
		return "", err
	}
	return publishDirSumAs(w.dir, sumName, sum)
}

// PublishArtifacts durably publishes artifacts as one journaled batch through
// the bound handle. The caller must hold the migration-directory lock.
func (w *MigrationWriter) PublishArtifacts(
	ctx context.Context,
	artifacts []PublicationArtifact,
) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !w.dir.Exists() {
		return nil, w.dir.missingDirError()
	}
	return publishArtifactsLocked(ctx, w.dir, artifacts)
}

// SyncDir flushes the migration directory's entry changes where the platform
// supports it, so files created through WriteNew are durable before the command
// reports them.
func (w *MigrationWriter) SyncDir() error {
	if !w.dir.Exists() {
		return nil
	}
	return w.dir.dir.Sync()
}
