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
	result := &Database{
		Schemas:                    []Schema{},
		Tables:                     []Table{},
		Fields:                     []Field{},
		Indexes:                    []Index{},
		Constraints:                []Constraint{},
		Enums:                      []Enum{},
		EmbeddedFields:             []EmbeddedField{},
		Extensions:                 []Extension{},
		Functions:                  []Function{},
		Views:                      []View{},
		MaterializedViews:          []MaterializedView{},
		Triggers:                   []Trigger{},
		RLSPolicies:                []RLSPolicy{},
		RLSEnabledTables:           []RLSEnabledTable{},
		Roles:                      []Role{},
		Grants:                     []Grant{},
		Dependencies:               make(map[string][]string),
		FunctionDependencies:       make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]SelfReferencingFK),
	}

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

		// Add to result
		result.Schemas = append(result.Schemas, database.Schemas...)
		result.EmbeddedFields = append(result.EmbeddedFields, database.EmbeddedFields...)
		result.Fields = append(result.Fields, database.Fields...)
		result.Indexes = append(result.Indexes, database.Indexes...)
		result.Tables = append(result.Tables, database.Tables...)
		result.Enums = append(result.Enums, database.Enums...)
		result.Extensions = append(result.Extensions, database.Extensions...)
		result.Functions = append(result.Functions, database.Functions...)
		result.RLSPolicies = append(result.RLSPolicies, database.RLSPolicies...)
		result.RLSEnabledTables = append(result.RLSEnabledTables, database.RLSEnabledTables...)
		result.Roles = append(result.Roles, database.Roles...)
		result.Constraints = append(result.Constraints, database.Constraints...)
		result.Views = append(result.Views, database.Views...)
		result.MaterializedViews = append(result.MaterializedViews, database.MaterializedViews...)
		result.Triggers = append(result.Triggers, database.Triggers...)
		result.Grants = append(result.Grants, database.Grants...)

		return nil
	})

	if err != nil {
		return nil, err
	}
	if err := errors.Join(parseErrors...); err != nil {
		return nil, err
	}

	if err := validateDuplicateSchemaObjectDefinitions(result); err != nil {
		return nil, err
	}

	// deduplicate entities (same table/field defined in multiple files)
	Deduplicate(result)
	normalizeTableScopedNames(result)

	// Process embedded fields BEFORE building dependency graph
	// This ensures that foreign keys from embedded fields are included in dependency analysis
	result.Fields = processEmbeddedFields(result.EmbeddedFields, result.Fields)

	// Build dependency graph for foreign key ordering
	buildDependencyGraph(result)

	// Sort tables by dependency order
	sortTablesByDependencies(result)

	// Sort functions by dependency order
	sortFunctionsByDependencies(result)

	return result, nil
}

func parseDatabaseFile(fsys fs.FS, path string) (Database, error) {
	file, err := fsys.Open(path)
	if err != nil {
		return Database{}, err
	}
	defer file.Close()

	return ParseSource(path, bufio.NewReader(file))
}
