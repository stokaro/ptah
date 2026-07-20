package compare

import (
	"sort"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// Extensions performs comprehensive extension comparison between generated and database schemas.
//
// This function compares PostgreSQL extensions defined in the target schema (from Go struct annotations)
// with extensions currently installed in the database. It identifies which extensions need to be
// added or removed to bring the database in line with the target schema.
//
// # Extension Ignore Functionality
//
// The function supports ignoring specific extensions through the opts parameter:
//   - Ignored extensions are filtered out before comparison
//   - Ignored extensions will never be marked for removal
//   - Ignored extensions can still be created if defined in the target schema
//   - If opts is nil, default options are used (ignores "plpgsql")
//
// # Comparison Process
//
// The function performs comparison in three phases:
//  1. **Extension Filtering**: Removes ignored extensions from consideration
//  2. **Extension Discovery**: Creates lookup maps for efficient extension comparison
//  3. **Extension Diff Analysis**: Identifies added and removed extensions
//
// # PostgreSQL Extension Considerations
//
// Extensions in PostgreSQL are database-wide objects that provide additional functionality:
//   - **pg_trgm**: Trigram similarity search and GIN operator classes
//   - **btree_gin**: GIN indexes for btree-compatible data types
//   - **postgis**: Geographic data types and functions
//   - **uuid-ossp**: UUID generation functions
//   - **plpgsql**: Procedural language (usually pre-installed, commonly ignored)
//
// # Extension Detection
//
// The function now fully supports extension detection from the database schema, enabling
// accurate comparison between target and current state. This allows for proper extension
// lifecycle management including both addition and removal operations.
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from executor introspection (includes extensions)
//   - diff: SchemaDiff structure to populate with discovered differences
//   - opts: Configuration options for comparison (can be nil for defaults)
//
// # Side Effects
//
// Modifies the provided diff parameter by populating:
//   - diff.ExtensionsAdded: Extensions that need to be created
//   - diff.ExtensionsRemoved: Extensions that exist in database but not in target schema
//
// # Example Usage
//
//	// Extensions defined in Go annotations
//	//migrator:schema:extension name="pg_trgm" if_not_exists="true"
//	//migrator:schema:extension name="btree_gin" if_not_exists="true"
//	type DatabaseExtensions struct{}
//
//	// Database has pg_trgm installed but not btree_gin
//	// Results in diff.ExtensionsAdded = ["btree_gin"]
//
//	// Using custom ignore options
//	opts := config.WithIgnoredExtensions("plpgsql", "adminpack")
//	Extensions(generated, database, diff, opts)
//
// # Output Consistency
//
// Results are sorted alphabetically for consistent output across multiple runs,
// ensuring deterministic migration generation and reliable testing.
func Extensions(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff, opts *config.CompareOptions) {
	// Use default options if none provided
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}

	// Initialize slices to ensure they're never nil
	diff.ExtensionsAdded = []string{}
	diff.ExtensionsRemoved = []string{}

	// Create maps for quick lookup, filtering out ignored extensions
	genExtensions := make(map[string]goschema.Extension)
	for _, extension := range generated.Extensions {
		if !opts.IsExtensionIgnored(extension.Name) {
			genExtensions[extension.Name] = extension
		}
	}

	// Create map of database extensions for efficient lookup, filtering out ignored extensions
	dbExtensions := make(map[string]types.DBExtension)
	for _, extension := range database.Extensions {
		if !opts.IsExtensionIgnored(extension.Name) {
			dbExtensions[extension.Name] = extension
		}
	}

	// Find added extensions (exist in generated schema but not in database)
	// Note: Ignored extensions are already filtered out, so they won't appear here
	for extensionName := range genExtensions {
		if _, exists := dbExtensions[extensionName]; !exists {
			diff.ExtensionsAdded = append(diff.ExtensionsAdded, extensionName)
		}
	}

	// Find removed extensions (exist in database but not in generated schema)
	// Note: Ignored extensions are already filtered out, so they will never be marked for removal
	for extensionName := range dbExtensions {
		if _, exists := genExtensions[extensionName]; !exists {
			diff.ExtensionsRemoved = append(diff.ExtensionsRemoved, extensionName)
		}
	}

	// Sort for consistent output
	sort.Strings(diff.ExtensionsAdded)
	sort.Strings(diff.ExtensionsRemoved)
}
