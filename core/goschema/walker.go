package goschema

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/goannotationsource"
)

// ParseDir walks one Go entity root on the host filesystem and returns the
// finalized [go.5x5.cz/ptah/core/schemamodel.Database] its annotations declare.
//
// The walk is recursive from rootDir and reads every .go file except _test.go
// files, skipping hidden directories and directories named exactly "vendor".
// A symlinked or otherwise non-regular Go source file is refused rather than
// followed. A file that fails to parse does not stop the walk: the failures
// are collected and returned as one error that names every failing file and
// unwraps to the individual refusals, so a caller is never told about the
// first one alone.
//
// The result has been through the finalize pipeline — embedded fields
// expanded, identical repeated declarations folded, conflicting ones reported
// as an error, tables and functions ordered by their dependencies — so it can
// go straight to the renderer or the diff and migration layers. For an
// un-finalized schema to compose with other authoring sources, use
// [ParseDirRaw]; to finalize several roots together, use [ParseDirs].
//
// Managed-data annotations record an absolute SourceDir anchored at rootDir,
// so [go.5x5.cz/ptah/core/schemamodel.LoadManagedRows] resolves them from any
// working directory.
func ParseDir(rootDir string) (*schemamodel.Database, error) {
	result, err := ParseDirRaw(rootDir)
	if err != nil {
		return nil, err
	}
	return schemamodel.MergeAccumulated(result)
}

// ParseFS walks one Go entity root inside fsys and returns the finalized
// [go.5x5.cz/ptah/core/schemamodel.Database] its annotations declare. It is
// [ParseDir] for a caller holding a filesystem — an embed.FS, a
// testing/fstest.MapFS, a source snapshot — rather than a host directory:
// file selection, the refusal of symlinked and non-regular sources, the
// collected per-file parse errors, and the finalize pipeline are all the same.
//
// One thing differs by necessity: with no host root to anchor to,
// managed-data annotations keep the filesystem-relative SourceDir they were
// parsed with. Resolve them by passing the host location of fsys as the
// rootDir argument of [go.5x5.cz/ptah/core/schemamodel.LoadManagedRows].
func ParseFS(fsys fs.FS, rootDir string) (*schemamodel.Database, error) {
	result := schemamodel.NewDatabase()
	if err := accumulateGoFiles(result, fsys, rootDir); err != nil {
		return nil, err
	}
	return schemamodel.MergeAccumulated(result)
}

// ParseDirs parses several Go entity roots into a single composite schema.
//
// Each root is walked like ParseDir, every file's schema objects from every root
// are accumulated together, and the finalize pipeline (deduplicate, expand
// embedded fields, build the dependency graph, sort) runs once over the combined
// set — so a table in one root can reference a table in another. It is the
// multi-root form of ParseDir, for a composite desired-state schema assembled
// from several Go packages (for example a shared "common" package plus
// per-service tables).
//
// Identical definitions across roots are deduplicated. Every named object kind
// is checked by its stable database identity, and definitions of the same
// object that differ return a descriptive conflict error. With a single root,
// ParseDirs delegates to ParseDir and uses the same strict collision semantics
// without allocating a second database accumulator.
func ParseDirs(roots ...string) (*schemamodel.Database, error) {
	if len(roots) == 1 {
		return ParseDir(roots[0])
	}

	sources := make([]*schemamodel.Database, 0, len(roots))
	for _, root := range roots {
		source, err := ParseDirRaw(root)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	return schemamodel.Merge(sources...)
}

// ParseDirRaw parses one Go entity root into an un-finalized schema: it
// accumulates every file's schema objects but does NOT run the
// finalize pipeline (deduplication, embedded-field expansion, dependency
// ordering).
//
// It exists to feed schemamodel.Merge when composing a Go schema with schemas from
// other source kinds (YAML, HCL): parsing every Go root independently preserves
// its local type namespace until Merge applies the cross-source collision
// policy. Merge also accepts finalized schemas, but raw roots avoid unnecessary
// expansion and deduplication work. For a directly usable, finalized schema use
// ParseDir or ParseDirs instead.
func ParseDirRaw(root string) (*schemamodel.Database, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	result := schemamodel.NewDatabase()
	if err := accumulateGoFiles(result, os.DirFS(absoluteRoot), "."); err != nil {
		return nil, err
	}
	bindManagedDataSourceRoot(result, absoluteRoot)
	return result, nil
}

func bindManagedDataSourceRoot(result *schemamodel.Database, root string) {
	for index := range result.ManagedData {
		result.ManagedData[index].SourceDir = filepath.Join(
			root,
			result.ManagedData[index].SourceDir,
		)
	}
}

// accumulateGoFiles walks the selected Go annotation sources under rootDir in
// fsys and appends each parsed file's schema objects onto result without
// finalizing. It is the shared, pre-finalize body of ParseFS and ParseDirs, so
// multiple roots can accumulate into one result before a single finalize pass.
func accumulateGoFiles(result *schemamodel.Database, fsys fs.FS, rootDir string) error {
	var parseErrors []error

	err := fs.WalkDir(fsys, rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path != rootDir && goannotationsource.SkipDirectory(d.Name()) {
				return fs.SkipDir
			}
			return nil
		}
		if !goannotationsource.IsSource(path) {
			return nil
		}
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("refuse to parse symlinked Go source %s", path)
		}
		info, err := d.Info()
		if err != nil {
			return fmt.Errorf("stat Go source %s: %w", path, err)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("refuse to parse non-regular Go source %s", path)
		}

		database, err := parseDatabaseFile(fsys, path)
		if err != nil {
			parseErrors = append(parseErrors, err)
			return nil
		}

		schemamodel.AppendDatabase(result, &database)

		return nil
	})

	if err != nil {
		return err
	}
	return errors.Join(parseErrors...)
}

func parseDatabaseFile(fsys fs.FS, path string) (schemamodel.Database, error) {
	file, err := fsys.Open(path)
	if err != nil {
		return schemamodel.Database{}, err
	}
	defer file.Close()

	return ParseSource(path, bufio.NewReader(file))
}
