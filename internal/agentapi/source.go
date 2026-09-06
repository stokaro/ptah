package agentapi

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"

	"ptah.run/internal/agentdiag"
)

// ErrNoSourceScope reports that no directory was configured for schema sources
// to be read from.
var ErrNoSourceScope = agentdiag.Sentinel(agentdiag.CodeNoSourceScope,
	"no schema source directory is configured")

// ErrSourceOutsideScope reports a schema source outside every configured root.
var ErrSourceOutsideScope = agentdiag.Sentinel(agentdiag.CodeUnsafePath,
	"schema source is outside the configured directories")

// ErrSourceNotLocal reports a schema source that would be fetched rather than
// read.
var ErrSourceNotLocal = agentdiag.Sentinel(agentdiag.CodeUnsafePath,
	"schema source is not a local path")

// sourceScope is where a session may read declared schemas from.
//
// It exists because a schema source is a path the model chooses. Without a
// scope, `schema_files` and `root_dirs` are an arbitrary local read wearing the
// name of a schema operation -- a route around filesystem.arbitrary_read, which
// no layer may grant. The operator names the directories; the model names files
// inside them.
//
// An empty scope permits nothing. That is deliberate: a process configured with
// no directory has not been told what an agent may read, and the answer to an
// unasked question is not "everything this process can open".
type sourceScope struct {
	roots []string
}

// newSourceScope resolves the configured roots to absolute, symlink-free paths.
func newSourceScope(roots []string) (sourceScope, error) {
	resolved := make([]string, 0, len(roots))
	for _, root := range roots {
		if strings.TrimSpace(root) == "" {
			continue
		}
		absolute, err := filepath.Abs(root)
		if err != nil {
			return sourceScope{}, fmt.Errorf("resolve schema source root %q: %w", root, err)
		}
		resolvedRoot, err := filepath.EvalSymlinks(absolute)
		if err != nil {
			return sourceScope{}, fmt.Errorf("resolve schema source root %q: %w", root, err)
		}
		resolved = append(resolved, resolvedRoot)
	}
	sort.Strings(resolved)
	return sourceScope{roots: resolved}, nil
}

// empty reports whether the scope permits nothing at all.
func (s sourceScope) empty() bool { return len(s.roots) == 0 }

// permit refuses a path the operator did not open up.
//
// Symlinks are resolved before the comparison, so a link planted inside a
// configured directory cannot name a file outside it. The check is on the path
// rather than through an os.Root handle because the loaders below take plain
// paths; the new decision record states that limit rather than implying a
// containment this cannot provide.
func (s sourceScope) permit(path string) error {
	if err := refuseNonLocal(path); err != nil {
		return err
	}
	if s.empty() {
		return fmt.Errorf("%w: %s", ErrNoSourceScope, path)
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return fmt.Errorf("resolve schema source %q: %w", path, err)
	}
	resolvedPath := resolveExisting(absolute)
	for _, root := range s.roots {
		relative, relErr := filepath.Rel(root, resolvedPath)
		if relErr != nil {
			continue
		}
		if relative == "." || !strings.HasPrefix(relative, "..") {
			return nil
		}
	}
	return fmt.Errorf("%w: %s", ErrSourceOutsideScope, path)
}

// permitAll checks every entry of a source.
func (s sourceScope) permitAll(source SchemaSource) error {
	for _, dir := range source.RootDirs {
		if err := s.permit(dir); err != nil {
			return err
		}
	}
	for _, file := range source.SchemaFiles {
		if err := s.permit(file); err != nil {
			return err
		}
	}
	return nil
}

// refuseNonLocal rejects a source that would be fetched rather than opened.
//
// `oci://` reaches a registry over the network, and network.arbitrary is hard
// denied: no layer may grant it, so a schema operation must not be the way to
// perform it. The refusal is by shape rather than by scheme list, because the
// question is whether the loader would go somewhere, and a scheme this code has
// not heard of is not evidence that it would not.
func refuseNonLocal(path string) error {
	scheme, _, found := strings.Cut(path, "://")
	if !found {
		return nil
	}
	return fmt.Errorf(
		"%w: %s. %s:// is fetched rather than read, and the agent surface grants no "+
			"capability for that. Name a file inside a configured schema source directory",
		ErrSourceNotLocal, path, scheme)
}

// list is the configured roots, for a discovery response.
func (s sourceScope) list() []string {
	roots := make([]string, 0, len(s.roots))
	return append(roots, s.roots...)
}

// resolveExisting resolves symlinks as far as the path exists, and keeps the
// rest as it was written.
//
// A schema source that does not exist is a normal answer -- "this source will
// not load" is what the caller asked about -- so a missing directory inside a
// configured root must stay inside it rather than become unreadable. Only the
// part that exists can be a symlink, so only that part needs resolving, and
// resolving it is what stops a link inside the scope from naming a file
// outside it.
func resolveExisting(absolute string) string {
	existing := absolute
	remainder := ""
	for {
		resolved, err := filepath.EvalSymlinks(existing)
		if err == nil {
			return filepath.Join(resolved, remainder)
		}
		parent := filepath.Dir(existing)
		if parent == existing {
			// Nothing on the path exists. It is already absolute and cleaned,
			// so it is compared as written.
			return absolute
		}
		remainder = filepath.Join(filepath.Base(existing), remainder)
		existing = parent
	}
}
