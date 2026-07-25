package goschema

import (
	"bufio"
	"errors"
	"io/fs"
	"os"
	"strings"
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
//   - Filtering to include only .go files (excluding tests and vendor)
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
	return ParseFS(os.DirFS(rootDir), ".")
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
	if err := finalizeDatabase(result); err != nil {
		return nil, err
	}
	return result, nil
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
// Identical definitions across roots are deduplicated; conflicting definitions
// of a named schema, view, materialized view, trigger, constraint, or role are
// an error, matching ParseDir's cross-file behavior. With a single root it is
// equivalent to ParseDir.
func ParseDirs(roots ...string) (*Database, error) {
	result := newDatabase()
	for _, root := range roots {
		if err := accumulateGoFiles(result, os.DirFS(root), "."); err != nil {
			return nil, err
		}
	}
	if err := finalizeDatabase(result); err != nil {
		return nil, err
	}
	return result, nil
}

// accumulateGoFiles walks the non-test, non-vendor Go source files under rootDir
// in fsys and appends each parsed file's schema objects onto result without
// finalizing. It is the shared, pre-finalize body of ParseFS and ParseDirs, so
// multiple roots can accumulate into one result before a single finalize pass.
func accumulateGoFiles(result *Database, fsys fs.FS, rootDir string) error {
	var parseErrors []error

	// Walk through all directories recursively
	err := fs.WalkDir(fsys, rootDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip non-Go files
		if !strings.HasSuffix(path, ".go") {
			return nil
		}

		// Skip test files
		if strings.HasSuffix(path, "_test.go") {
			return nil
		}

		// Skip vendor directories (handle both Unix and Windows path separators)
		if strings.Contains(path, "vendor/") || strings.Contains(path, "vendor\\") {
			return nil
		}

		database, err := parseDatabaseFile(fsys, path)
		if err != nil {
			parseErrors = append(parseErrors, err)
			return nil
		}

		// Add this file's schema objects to the accumulator.
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
