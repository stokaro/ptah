// Package agentworkspace is the one place an agent operation learns where it
// may read and write, and the one place it learns what those files currently
// contain.
//
// # Why the workspace is not a tool argument
//
// ADR 0003 records that the model chooses the arguments, so a surface that took
// its root from a tool call would let the untrusted party pick the directory it
// is confined to. Every scope here is resolved from operator configuration --
// the flags and environment of the command the operator started -- before the
// first tool call is served. A tool names an [agentpolicy.ArtifactClass] and a
// path relative to that class; it cannot name a root.
//
// The containment is enforced twice, deliberately, because one of the two
// layers is a check and the other is the kernel. [validateRelativePath] refuses
// the spellings that mean something other than they look like -- absolute
// paths, `..`, backslashes, drive letters, Windows device names -- and the
// [pathguard.OpenedDirectory] the scope holds is an `os.Root` handle bound at
// open time, so even a path that got past the check reaches only the directory
// the operator named. Renaming the directory after it is opened does not
// retarget the handle.
//
// # Digests
//
// A [Scope] answers a content address for everything it holds:
// `sha256:<hex>` over a canonical manifest of every entry and its own hash.
// That is what a patch binds to, what an approval refers to, and what makes a
// concurrent edit between preview and apply a refusal rather than a surprise.
//
// The manifest covers every entry, including files nobody expected to matter.
// A digest that skipped, say, dotfiles would answer "unchanged" for a directory
// that changed, and the whole value of the digest is that it does not.
package agentworkspace

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"

	"go.5x5.cz/ptah/internal/agentdiag"
	"go.5x5.cz/ptah/internal/agentpolicy"
	"go.5x5.cz/ptah/internal/pathguard"
)

// Limits on what one scope will read or one patch will write.
//
// They exist because the caller is an agent that can be told to do something
// silly by a file it read, and because an operation that streams a gigabyte
// into a model's context has failed at something other than security. The
// numbers are deliberately generous for the artifacts Ptah owns: the largest
// migration in this repository's own fixtures is a few kilobytes.
const (
	// MaxFileBytes is the largest single artifact this package will read or
	// write.
	MaxFileBytes = 1 << 20
	// MaxScopeFiles is the largest number of entries a scope may hold before
	// digesting it is refused. A directory past this is not a Ptah artifact
	// directory, and hashing it would be a denial of service with extra steps.
	MaxScopeFiles = 10000
)

var (
	// ErrClassNotConfigured reports a class the operator did not point at a
	// directory. It is distinct from a permission denial: the capability may
	// well be granted, and there is still nowhere to put the file.
	ErrClassNotConfigured = agentdiag.Sentinel(agentdiag.CodeArtifactClassNotConfigured,
		"artifact class is not configured for this workspace")
	// ErrUnsafePath reports a path that does not mean what it looks like.
	ErrUnsafePath = agentdiag.Sentinel(agentdiag.CodeUnsafePath, "unsafe artifact path")
	// ErrTooLarge reports an artifact past [MaxFileBytes], or a scope past
	// [MaxScopeFiles].
	ErrTooLarge = agentdiag.Sentinel(agentdiag.CodeArtifactTooLarge, "artifact too large")
	// ErrNotRegularFile reports a name that exists as something other than a
	// regular file: a directory, a symbolic link, a device.
	ErrNotRegularFile = agentdiag.Sentinel(agentdiag.CodeNotRegularFile, "not a regular file")
)

// ClassConfig points one artifact class at one directory.
//
// One directory per class, not several. A class with two writable roots makes
// `migrations/0001.sql` an ambiguous address unless the patch also names the
// root, and an unambiguous address is the property this whole design is built
// to have. A project that needs two is a project that needs two workspaces.
type ClassConfig struct {
	// Dir is the directory, relative to the workspace root or absolute; either
	// way it must resolve inside the root.
	Dir string
	// Writable reports whether the operator granted writes here. It is recorded
	// on the scope for reporting; the enforcement is the capability broker's.
	Writable bool
}

// Config is everything the operator decided.
type Config struct {
	// Root is the project root every scope must resolve inside. It is required:
	// a workspace with no root is a workspace with no containment, and
	// pathguard's root-binding resolvers treat an empty root as "no check".
	Root string
	// Classes maps each configured artifact class to its directory. A class
	// absent from the map is a class no operation can name.
	Classes map[agentpolicy.ArtifactClass]ClassConfig
	// Dialect is the target the workspace's schema is read for, when the
	// project declares one. Operations that need a dialect still take it
	// explicitly; this is the default a surface reports.
	Dialect string
}

// Workspace is an opened project: a root handle plus one handle per configured
// class.
//
// Every handle is held for the life of the workspace rather than reopened per
// call. That is not an optimization: reopening would reintroduce the window
// between resolving a name and using it, which is the window
// [pathguard.OpenedDirectory] exists to close.
type Workspace struct {
	root    *pathguard.OpenedDirectory
	scopes  map[agentpolicy.ArtifactClass]*Scope
	dialect string
}

// Scope is one artifact class's directory.
type Scope struct {
	class    agentpolicy.ArtifactClass
	dir      *pathguard.OpenedDirectory
	writable bool
}

// Open resolves the configuration into handles, refusing anything that does not
// resolve inside the root.
//
// Every failure here is a configuration error the operator can act on, and none
// of them is recoverable at call time: a workspace that opened with a class
// pointing outside the root would be a workspace whose later checks are
// decoration.
func Open(cfg Config) (*Workspace, error) {
	if cfg.Root == "" {
		return nil, errors.New("workspace root is required")
	}
	root, err := pathguard.OpenDirectory(cfg.Root)
	if err != nil {
		return nil, fmt.Errorf("open workspace root %q: %w", cfg.Root, err)
	}
	workspace := &Workspace{
		root:    root,
		scopes:  make(map[agentpolicy.ArtifactClass]*Scope, len(cfg.Classes)),
		dialect: cfg.Dialect,
	}
	for _, class := range agentpolicy.ArtifactClasses() {
		classCfg, configured := cfg.Classes[class]
		if !configured {
			continue
		}
		scope, scopeErr := workspace.openScope(class, classCfg)
		if scopeErr != nil {
			return nil, errors.Join(scopeErr, workspace.Close())
		}
		workspace.scopes[class] = scope
	}
	return workspace, nil
}

// openScope binds one class directory inside the root.
func (w *Workspace) openScope(class agentpolicy.ArtifactClass, cfg ClassConfig) (*Scope, error) {
	if cfg.Dir == "" {
		return nil, fmt.Errorf("artifact class %q names no directory", class)
	}
	dir, err := pathguard.OpenDirectoryWithinRoot(cfg.Dir, w.root.Path())
	if err != nil {
		return nil, fmt.Errorf("open %s directory %q: %w", class, cfg.Dir, err)
	}
	return &Scope{class: class, dir: dir, writable: cfg.Writable}, nil
}

// Close releases every handle. Releasing twice is a no-op, so a deferred Close
// beside the constructor is always correct.
func (w *Workspace) Close() error {
	var errs []error
	for _, scope := range w.scopes {
		errs = append(errs, scope.dir.Close())
	}
	if w.root != nil {
		errs = append(errs, w.root.Close())
	}
	return errors.Join(errs...)
}

// Root is the project root's resolved path.
func (w *Workspace) Root() string {
	return w.root.Path()
}

// Dialect is the workspace's declared target, or the empty string.
func (w *Workspace) Dialect() string {
	return w.dialect
}

// Classes lists the configured classes in a stable order.
func (w *Workspace) Classes() []agentpolicy.ArtifactClass {
	classes := make([]agentpolicy.ArtifactClass, 0, len(w.scopes))
	for _, class := range agentpolicy.ArtifactClasses() {
		if _, configured := w.scopes[class]; configured {
			classes = append(classes, class)
		}
	}
	return classes
}

// Scope returns one class's directory, refusing a class the operator did not
// configure.
func (w *Workspace) Scope(class agentpolicy.ArtifactClass) (*Scope, error) {
	scope, configured := w.scopes[class]
	if !configured {
		return nil, fmt.Errorf("%q: %w", class, ErrClassNotConfigured)
	}
	return scope, nil
}

// Class names the scope's artifact class.
func (s *Scope) Class() agentpolicy.ArtifactClass {
	return s.class
}

// Path is the scope directory's resolved path, for display and for the
// operations that take a path rather than a handle.
//
// It is display data. Nothing in this package re-resolves it to reach a file:
// every read and write goes through the bound handle, which is what makes
// renaming the directory afterwards harmless.
func (s *Scope) Path() string {
	return s.dir.Path()
}

// Writable reports the operator's intent, for a surface that lists what this
// session can do. It is not the authorization: [agentpolicy] decides that.
func (s *Scope) Writable() bool {
	return s.writable
}

// Directory exposes the bound handle to the packages that write through it.
func (s *Scope) Directory() *pathguard.OpenedDirectory {
	return s.dir
}

// Revalidate reports whether the directory the handle was bound to is still the
// directory its pathname names.
func (s *Scope) Revalidate() error {
	return s.dir.Revalidate()
}

// Entry is one file the scope holds.
type Entry struct {
	// Path is the slash-separated path relative to the scope directory.
	Path string `json:"path"`
	// Size is the file's length in bytes.
	Size int64 `json:"size"`
	// Digest is `sha256:<hex>` of the file's bytes, absent for an entry that is
	// not a regular file.
	Digest string `json:"digest,omitempty"`
	// Kind names what the entry is when it is not a regular file, so a symbolic
	// link in a migration directory is reported rather than silently skipped.
	Kind string `json:"kind,omitempty"`
}

// List reports every entry the scope holds, sorted by path.
func (s *Scope) List() ([]Entry, error) {
	fsys := s.dir.FS()
	entries := make([]Entry, 0, 16)
	err := fs.WalkDir(fsys, ".", func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if name == "." || entry.IsDir() {
			return nil
		}
		if len(entries) >= MaxScopeFiles {
			return fmt.Errorf("%s holds more than %d files: %w", s.class, MaxScopeFiles, ErrTooLarge)
		}
		listed, listErr := entryFor(fsys, name, entry)
		if listErr != nil {
			return listErr
		}
		entries = append(entries, listed)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list %s: %w", s.class, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Path < entries[j].Path })
	return entries, nil
}

// entryFor describes one walked entry, hashing it when it is a regular file.
func entryFor(fsys fs.FS, name string, entry fs.DirEntry) (Entry, error) {
	info, err := entry.Info()
	if err != nil {
		return Entry{}, err
	}
	if !info.Mode().IsRegular() {
		return Entry{Path: name, Kind: kindOf(info.Mode())}, nil
	}
	digest, err := hashFile(fsys, name)
	if err != nil {
		return Entry{}, err
	}
	return Entry{Path: name, Size: info.Size(), Digest: digest}, nil
}

// kindOf names a non-regular entry for a report.
func kindOf(mode fs.FileMode) string {
	switch {
	case mode&fs.ModeSymlink != 0:
		return "symlink"
	case mode&fs.ModeDevice != 0:
		return "device"
	case mode&fs.ModeNamedPipe != 0:
		return "pipe"
	case mode&fs.ModeSocket != 0:
		return "socket"
	}
	return "irregular"
}

// hashFile digests one file's bytes, refusing one past [MaxFileBytes].
func hashFile(fsys fs.FS, name string) (string, error) {
	file, err := fsys.Open(name)
	if err != nil {
		return "", err
	}
	defer file.Close() //nolint:errcheck // a read-only handle's close cannot fail meaningfully

	hash := sha256.New()
	written, err := io.Copy(hash, io.LimitReader(file, MaxFileBytes+1))
	if err != nil {
		return "", err
	}
	if written > MaxFileBytes {
		return "", fmt.Errorf("%s is larger than %d bytes: %w", name, MaxFileBytes, ErrTooLarge)
	}
	return "sha256:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// Digest is the scope's content address: `sha256:<hex>` over a canonical
// manifest naming every entry and its own hash.
//
// The manifest is hashed rather than the concatenated file bytes so that moving
// content between two files changes the answer. Order is fixed by sorting, so
// two machines reading the same directory agree.
func (s *Scope) Digest() (string, error) {
	entries, err := s.List()
	if err != nil {
		return "", err
	}
	return DigestOf(entries), nil
}

// DigestOf computes the manifest digest for a listed set of entries.
//
// It is exported so a caller that already holds the listing -- a preview
// computing what the scope will become -- digests the same way rather than
// through a second implementation that agrees until it does not.
func DigestOf(entries []Entry) string {
	sorted := make([]Entry, len(entries))
	copy(sorted, entries)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Path < sorted[j].Path })

	manifest := &strings.Builder{}
	for _, entry := range sorted {
		fmt.Fprintf(manifest, "%s %s\n", entry.Path, manifestValue(entry))
	}
	sum := sha256.Sum256([]byte(manifest.String()))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// manifestValue is what an entry contributes to the manifest line.
func manifestValue(entry Entry) string {
	if entry.Digest != "" {
		return entry.Digest
	}
	return entry.Kind
}

// ReadFile returns one artifact's bytes, refusing anything that is not a
// regular file inside the scope.
func (s *Scope) ReadFile(relative string) ([]byte, error) {
	clean, err := validateRelativePath(relative)
	if err != nil {
		return nil, err
	}
	info, err := s.dir.Lstat(clean)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%s: %w", clean, ErrNotRegularFile)
	}
	if info.Size() > MaxFileBytes {
		return nil, fmt.Errorf("%s is larger than %d bytes: %w", clean, MaxFileBytes, ErrTooLarge)
	}
	return s.dir.ReadFile(clean)
}

// Stat describes one artifact without reading it.
func (s *Scope) Stat(relative string) (fs.FileInfo, error) {
	clean, err := validateRelativePath(relative)
	if err != nil {
		return nil, err
	}
	return s.dir.Lstat(clean)
}

// ResolvePath validates a caller-supplied path and returns its cleaned form.
//
// It is the only way a path from outside this package becomes one this package
// will act on.
func (s *Scope) ResolvePath(relative string) (string, error) {
	return validateRelativePath(relative)
}

// Parent returns the directory handle a name's parent lives in, opening
// intermediate directories through the bound handle.
//
// Publication is a same-directory operation -- fsdurable's conditional renames
// require the staged file and its destination to be direct children of one
// handle -- so a nested path needs its parent bound before anything is staged.
func (s *Scope) Parent(clean string) (*pathguard.OpenedDirectory, string, error) {
	dir, base := path.Split(clean)
	if dir == "" {
		return s.dir, base, nil
	}
	opened, err := s.dir.OpenDirectory(strings.TrimSuffix(dir, "/"))
	if err != nil {
		return nil, "", err
	}
	return opened, base, nil
}
