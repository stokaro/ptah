package goschema

import (
	"bufio"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"go.5x5.cz/ptah/internal/goannotationsource"
)

// ParseDir parses all Go files in the given root directory and its subdirectories
// to find all entity definitions and build a complete database schema.
//
// This function performs a comprehensive analysis of the Go codebase to extract database
// schema information. It walks through the directory tree recursively, parsing each Go file
// to discover entity definitions, and then processes the results to build a coherent
// database schema with proper dependency ordering.
//
// The parsing process includes:
//   - Recursive directory traversal starting from rootDir
//   - Filtering to include only .go files (excluding tests, hidden directories,
//     and directories named exactly vendor)
//   - Extraction of tables, fields, indexes, enums, and embedded fields
//   - Deduplication of entities found in multiple files
//   - Dependency analysis based on foreign key relationships
//   - Topological sorting to determine proper table creation order
//
// Parameters:
//   - rootDir: The root directory to start parsing from (e.g., "./entities", "./models")
//
// Returns:
//   - *PackageParseResult: Complete schema information with dependency ordering
//   - error: Any error encountered during parsing or file system operations
//
// Example:
//
//	result, err := ParseDir("./internal/entities")
//	if err != nil {
//		return fmt.Errorf("failed to parse entities: %w", err)
//	}
//
//	// Generate migration statements in proper order
//	statements, err := renderer.GetOrderedCreateStatements(result, "postgresql")
//	if err != nil {
//		return fmt.Errorf("failed to render schema: %w", err)
//	}
func ParseDir(rootDir string) (*Database, error) {
	result, err := ParseDirRaw(rootDir)
	if err != nil {
		return nil, err
	}
	return mergeAccumulatedDatabase(result)
}

// ParseFS parses all Go files in the given root directory and its subdirectories within the provided filesystem.
//
// This function is similar to ParseDir, but it operates on a provided filesystem rather than the host filesystem.
// It's useful for parsing entities within an embedded filesystem, such as a Go module or a virtual filesystem.
//
// Parameters:
//   - fsys: The filesystem to search for Go files
//   - rootDir: The root directory within the filesystem to start parsing from
//
// Returns:
//   - *PackageParseResult: Complete schema information with dependency ordering
//   - error: Any error encountered during parsing or file system operations
//
// Example:
//
//	//go:embed entities
//	var entities embed.FS
//
//	result, err := ParseFS(entities, ".")
//	if err != nil {
//		return fmt.Errorf("failed to parse entities: %w", err)
//	}
//
//	// Generate migration statements in proper order
//	statements, err := renderer.GetOrderedCreateStatements(result, "postgresql")
//	if err != nil {
//		return fmt.Errorf("failed to render schema: %w", err)
//	}
func ParseFS(fsys fs.FS, rootDir string) (*Database, error) {
	result := newDatabase()
	if err := accumulateGoFiles(result, fsys, rootDir); err != nil {
		return nil, err
	}
	return mergeAccumulatedDatabase(result)
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
func ParseDirs(roots ...string) (*Database, error) {
	if len(roots) == 1 {
		return ParseDir(roots[0])
	}

	sources := make([]*Database, 0, len(roots))
	for _, root := range roots {
		source, err := ParseDirRaw(root)
		if err != nil {
			return nil, err
		}
		sources = append(sources, source)
	}
	return Merge(sources...)
}

// ParseDirRaw parses one Go entity root into an un-finalized schema: it
// accumulates every file's schema objects but does NOT run the
// finalize pipeline (deduplication, embedded-field expansion, dependency
// ordering).
//
// It exists to feed goschema.Merge when composing a Go schema with schemas from
// other source kinds (YAML, HCL): parsing every Go root independently preserves
// its local type namespace until Merge applies the cross-source collision
// policy. Merge also accepts finalized schemas, but raw roots avoid unnecessary
// expansion and deduplication work. For a directly usable, finalized schema use
// ParseDir or ParseDirs instead.
func ParseDirRaw(root string) (*Database, error) {
	absoluteRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	result := newDatabase()
	if err := accumulateGoFiles(result, os.DirFS(absoluteRoot), "."); err != nil {
		return nil, err
	}
	bindManagedDataSourceRoot(result, absoluteRoot)
	return result, nil
}

func bindManagedDataSourceRoot(result *Database, root string) {
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
func accumulateGoFiles(result *Database, fsys fs.FS, rootDir string) error {
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

		appendDatabase(result, &database)

		return nil
	})

	if err != nil {
		return err
	}
	return errors.Join(parseErrors...)
}

func parseDatabaseFile(fsys fs.FS, path string) (Database, error) {
	file, err := fsys.Open(path)
	if err != nil {
		return Database{}, err
	}
	defer file.Close()

	return ParseSource(path, bufio.NewReader(file))
}
