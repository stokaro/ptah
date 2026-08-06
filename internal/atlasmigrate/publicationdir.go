package atlasmigrate

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"go.5x5.cz/ptah/internal/pathguard"
)

// publicationDir is the retained rooted view of one migration directory and of
// the directory that holds its publication journal.
//
// Every staged, published, journal, commit-marker, checksum, rollback and
// cleanup operation in this package is addressed by a name relative to one of
// these two handles. Both handles are opened once, before the directory is
// snapshotted, so replacing either pathname afterwards -- with a rename, a
// symlink or a fresh directory -- cannot redirect a later write: os.Root keeps
// referencing the filesystem object it was opened on, wherever that object is
// moved to (stokaro/ptah#895).
//
// The journal deliberately lives in the parent directory, which is why the
// parent needs its own retained handle rather than being reached through
// filepath.Dir of a pathname that may since have been replaced.
type publicationDir struct {
	dir    *pathguard.OpenedDirectory
	parent *pathguard.OpenedDirectory
	// displayPath is the migration directory as the caller named it. It is used
	// for reported paths and error messages only; it is never reopened.
	displayPath string
	// journalDir is the display path of the directory holding the journal.
	journalDir string
	// journalName is the journal's name inside parent.
	journalName string
}

// openPublicationDir binds the parent of dir first and then opens dir only
// through that handle, so neither open can be redirected by replacing the
// other's pathname. The caller owns the result and must close it.
func openPublicationDir(dir string) (*publicationDir, error) {
	if strings.TrimSpace(dir) == "" {
		return nil, errors.New("migration directory is required")
	}
	canonical, err := canonicalMigrationDir(dir)
	if err != nil {
		return nil, fmt.Errorf("resolve migration directory: %w", err)
	}
	journalDir := filepath.Dir(canonical)
	base := filepath.Base(canonical)
	parent, err := pathguard.OpenDirectory(journalDir)
	if err != nil {
		return nil, fmt.Errorf("open migration directory parent: %w", err)
	}
	opened, err := parent.OpenDirectory(base)
	if err != nil {
		return nil, errors.Join(
			fmt.Errorf("open migration directory: %w", err),
			parent.Close(),
		)
	}
	return &publicationDir{
		dir:         opened,
		parent:      parent,
		displayPath: filepath.Clean(dir),
		journalDir:  journalDir,
		journalName: "." + base + publicationJournalSuffix,
	}, nil
}

func (p *publicationDir) close() error {
	return errors.Join(p.dir.Close(), p.parent.Close())
}

// fsys returns an escape-resistant view of the retained migration directory.
func (p *publicationDir) fsys() fs.FS {
	return p.dir.FS()
}

// path renders a name inside the migration directory for display. The result is
// never reopened; rooted operations always take the bare name.
func (p *publicationDir) path(name string) string {
	return filepath.Join(p.displayPath, name)
}

// journalPath renders the publication journal's pathname for display.
func (p *publicationDir) journalPath() string {
	return filepath.Join(p.journalDir, p.journalName)
}

// commitMarkerName is the journal's commit marker, a sibling of the journal.
func (p *publicationDir) commitMarkerName() string {
	return p.journalName + publicationCommitMarkerSuffix
}

func (p *publicationDir) journalCleanupName() string {
	return p.journalName + publicationCleanupSuffix
}

// listNames returns the names directly inside root that carry prefix and
// suffix. It replaces filepath.Glob, which resolves a pathname the caller may
// no longer own.
func listNames(root *pathguard.OpenedDirectory, prefix, suffix string) ([]string, error) {
	entries, err := root.ReadDir()
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := entry.Name()
		if len(name) < len(prefix)+len(suffix) {
			continue
		}
		if strings.HasPrefix(name, prefix) && strings.HasSuffix(name, suffix) {
			names = append(names, name)
		}
	}
	return names, nil
}
