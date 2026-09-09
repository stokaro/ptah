package schemafile

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"ptah.run/core/schemamodel"
)

// sqlImportMarker is the directive a split SQL export writes into its entry
// point, one per imported file.
//
// `ptah-compat schema inspect --format '{{ sql . | split | write "out" }}'`
// writes out/main.sql holding nothing but these lines, and Atlas documents that
// entry point as the way to reference the whole export
// (`file://path/to/main.sql`). Ptah wrote the directive and read it nowhere, so
// the export round-tripped to an empty schema at exit 0 and a diff against the
// live database then planned a DROP for every object (stokaro/ptah#3110).
const sqlImportMarker = "atlas:import"

// ErrSQLImportCycle reports an entry point that imports itself, directly or
// through a chain.
var ErrSQLImportCycle = errors.New("SQL schema import cycle")

// ErrSQLImportEscapes reports an import that resolves outside the entry point's
// own directory tree.
var ErrSQLImportEscapes = errors.New("SQL schema import escapes the entry point directory")

// maxSQLImportDepth bounds how deep an import chain may nest.
//
// The exports Ptah writes are one level deep. The bound exists so a
// hand-written chain fails with a diagnostic naming the limit rather than
// exhausting the stack, and it is checked even though [ErrSQLImportCycle]
// already refuses the shape a runaway chain usually takes: a chain can be
// acyclic and still unbounded.
const maxSQLImportDepth = 32

// sqlImportPaths returns the paths a SQL document imports, in the order the
// document writes them.
//
// A directive is a whole comment line: `-- atlas:import ./tables/users.sql`.
// Leading whitespace is allowed so an indented entry point is read the same
// way. A line that merely mentions the marker inside a statement is not a
// directive, which is why the prefix is matched on the trimmed line rather than
// searched for anywhere.
func sqlImportPaths(data []byte) ([]string, error) {
	var paths []string
	for index, line := range strings.Split(string(data), "\n") {
		trimmed := strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		rest, ok := strings.CutPrefix(trimmed, "--")
		if !ok {
			continue
		}
		rest = strings.TrimSpace(rest)
		value, ok := strings.CutPrefix(rest, sqlImportMarker)
		if !ok {
			continue
		}
		// `atlas:importer` must not read as `atlas:import` with a path of
		// "er": the marker has to be followed by separating space, or by
		// nothing at all so an empty directive is reported as empty.
		if value != "" && !strings.HasPrefix(value, " ") && !strings.HasPrefix(value, "\t") {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return nil, fmt.Errorf("line %d: %s names no file", index+1, sqlImportMarker)
		}
		paths = append(paths, value)
	}
	return paths, nil
}

// resolveSQLImport turns one directive value into an absolute path confined to
// root.
//
// The confinement is deliberate and stricter than a bare join: an entry point is
// an input a user may have received rather than written, and `../../etc/passwd`
// in one is a file read the operator did not ask for. `atlas.hcl`'s `file()` is
// confined for the same reason (stokaro/ptah#1042).
//
// An absolute directive value is refused rather than resolved, on every
// platform. [filepath.IsAbs] answers false on Windows for "/etc/passwd", so the
// leading separator and a drive letter are both refused by shape instead.
func resolveSQLImport(root, from, value string) (string, error) {
	if strings.HasPrefix(value, "/") || strings.HasPrefix(value, `\`) || filepath.IsAbs(value) || hasDriveLetter(value) {
		return "", fmt.Errorf("%w: %q is absolute; write it relative to the importing file", ErrSQLImportEscapes, value)
	}
	joined := filepath.Join(filepath.Dir(from), filepath.FromSlash(value))
	resolved, err := filepath.Abs(joined)
	if err != nil {
		return "", fmt.Errorf("resolve %s %q: %w", sqlImportMarker, value, err)
	}
	within, err := filepath.Rel(root, resolved)
	if err != nil || within == ".." || strings.HasPrefix(within, ".."+string(filepath.Separator)) {
		return "", fmt.Errorf("%w: %q resolves outside %s", ErrSQLImportEscapes, value, root)
	}
	return resolved, nil
}

// hasDriveLetter reports a Windows drive-qualified path such as `C:\schema.sql`.
//
// It is checked on every platform so the refusal does not depend on the machine
// reading the document.
func hasDriveLetter(value string) bool {
	if len(value) < 2 || value[1] != ':' {
		return false
	}
	c := value[0]
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')
}

// loadSQLWithImports reads a SQL schema file and merges everything it imports.
//
// The entry point's own statements are merged first, then each import in the
// order written, so a reader sees the document in the order the file lists it.
// visited holds the absolute paths already on this branch of the chain, which is
// what makes a cycle an error rather than a hang.
func loadSQLWithImports(root, path string, opts Options, visited map[string]struct{}, depth int) (*schemamodel.Database, error) {
	if depth > maxSQLImportDepth {
		return nil, fmt.Errorf("%s chain is deeper than %d files", sqlImportMarker, maxSQLImportDepth)
	}
	if _, seen := visited[path]; seen {
		return nil, fmt.Errorf("%w: %s imports itself", ErrSQLImportCycle, path)
	}
	visited[path] = struct{}{}
	defer delete(visited, path)

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read SQL schema file: %w", err)
	}
	imports, err := sqlImportPaths(data)
	if err != nil {
		return nil, fmt.Errorf("parse SQL schema file %s: %w", path, err)
	}

	merged, err := loadSQLFile(path, opts)
	if err != nil {
		return nil, err
	}
	for _, value := range imports {
		resolved, err := resolveSQLImport(root, path, value)
		if err != nil {
			return nil, err
		}
		info, err := os.Stat(resolved)
		if err != nil {
			if os.IsNotExist(err) {
				return nil, fmt.Errorf("%s %q: schema file does not exist: %s", sqlImportMarker, value, resolved)
			}
			return nil, fmt.Errorf("%s %q: %w", sqlImportMarker, value, err)
		}
		if info.IsDir() {
			return nil, isDirectoryError(resolved)
		}
		if !strings.EqualFold(filepath.Ext(resolved), dirSQLExtension) {
			return nil, fmt.Errorf(
				"%s %q: only %s files can be imported", sqlImportMarker, value, dirSQLExtension)
		}
		imported, err := loadSQLWithImports(root, resolved, opts, visited, depth+1)
		if err != nil {
			return nil, err
		}
		appendDatabase(merged, imported)
	}
	return merged, nil
}

// loadSQLFileTree is the SQL arm of [parseSchemaFile]: one file plus everything
// it imports.
//
// The entry point's own directory is the confinement root, so an export stays
// readable wherever it was unpacked while a directive cannot reach past it.
//
// A file that imports nothing takes the same path it always did, and is neither
// re-finalized nor merged: only a document that actually pulled in another one
// needs the merge pass, and running it unconditionally would change what a
// single file means.
func loadSQLFileTree(resolved string, opts Options) (*schemamodel.Database, error) {
	data, err := os.ReadFile(resolved)
	if err != nil {
		return nil, fmt.Errorf("read SQL schema file: %w", err)
	}
	imports, err := sqlImportPaths(data)
	if err != nil {
		return nil, fmt.Errorf("parse SQL schema file %s: %w", resolved, err)
	}
	if len(imports) == 0 {
		return loadSQLFile(resolved, opts)
	}
	merged, err := loadSQLWithImports(filepath.Dir(resolved), resolved, opts, make(map[string]struct{}), 0)
	if err != nil {
		return nil, err
	}
	schemamodel.Finalize(merged)
	return merged, nil
}
