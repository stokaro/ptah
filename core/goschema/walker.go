package goschema

import (
	"os"
	"path/filepath"
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
//	statements := GetOrderedCreateStatements(result, "postgresql")
func ParseDir(rootDir string) (*Database, error) {
	result := &Database{
		Tables:               []Table{},
		Fields:               []Field{},
		Indexes:              []Index{},
		Enums:                []Enum{},
		EmbeddedFields:       []EmbeddedField{},
		Extensions:           []Extension{},
		Functions:            []Function{},
		RLSPolicies:          []RLSPolicy{},
		RLSEnabledTables:     []RLSEnabledTable{},
		Dependencies:         make(map[string][]string),
		FunctionDependencies: make(map[string][]string),
	}

	// Walk through all directories recursively
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
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

		// Parse the file
		database := ParseFile(path)

		// Add to result
		result.EmbeddedFields = append(result.EmbeddedFields, database.EmbeddedFields...)
		result.Fields = append(result.Fields, database.Fields...)
		result.Indexes = append(result.Indexes, database.Indexes...)
		result.Tables = append(result.Tables, database.Tables...)
		result.Enums = append(result.Enums, database.Enums...)
		result.Extensions = append(result.Extensions, database.Extensions...)
		result.Functions = append(result.Functions, database.Functions...)
		result.RLSPolicies = append(result.RLSPolicies, database.RLSPolicies...)
		result.RLSEnabledTables = append(result.RLSEnabledTables, database.RLSEnabledTables...)

		return nil
	})

	if err != nil {
		return nil, err
	}

	// deduplicate entities (same table/field defined in multiple files)
	deduplicate(result)

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
