package compare

import (
	"sort"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

// Extensions performs comprehensive extension comparison between generated and database schemas.
//
// This function compares PostgreSQL extensions defined in the target schema (from Go struct annotations)
// with extensions currently installed in the database. It identifies which extensions need to be
// added, removed, or moved to bring the database in line with the target schema.
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
//  3. **Extension Diff Analysis**: Identifies added, removed, and moved extensions
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
//   - diff.ExtensionsModified: Extensions whose installation schema differs
//
// # Example Usage
//
//	// Extensions defined in Go annotations
//	//ptah:schema:extension name="pg_trgm" if_not_exists="true"
//	//ptah:schema:extension name="btree_gin" if_not_exists="true"
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
func Extensions(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	opts *config.CompareOptions,
	cov Coverage,
) {
	ExtensionsWithSemantics(generated, database, diff, opts, cov, identifier.ForDialect(platform.Postgres))
}

// ExtensionsWithSemantics compares extension identity and installation schema
// using the target database's resolved default schema and identifier rules.
func ExtensionsWithSemantics(
	generated *goschema.Database,
	database *types.DBSchema,
	diff *difftypes.SchemaDiff,
	opts *config.CompareOptions,
	cov Coverage,
	semantics identifier.Semantics,
) {
	semantics = semantics.Normalize("")
	// Extensions are PostgreSQL objects even when the legacy public comparison
	// entry point is dialect-neutral. Keep all supplied identifier rules, but
	// fill the one missing extension-specific rule so an omitted authored
	// placement and an inspected `public` placement remain the same state.
	if semantics.DefaultSchema == "" {
		semantics.DefaultSchema = identifier.ForDialect(platform.Postgres).DefaultSchema
	}
	// Use default options if none provided
	if opts == nil {
		opts = config.DefaultCompareOptions()
	}

	// Initialize slices to ensure they're never nil
	diff.ExtensionsAdded = []string{}
	diff.ExtensionsRemoved = []string{}
	diff.ExtensionsModified = []difftypes.ExtensionDiff{}

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
		databaseExtension, exists := dbExtensions[extensionName]
		if !exists {
			diff.ExtensionsAdded = append(diff.ExtensionsAdded, extensionName)
			continue
		}
		generatedSchema := effectiveExtensionSchema(genExtensions[extensionName].Schema, semantics)
		databaseSchema := effectiveExtensionSchema(databaseExtension.Schema, semantics)
		if generatedSchema != databaseSchema {
			diff.ExtensionsModified = append(diff.ExtensionsModified, difftypes.ExtensionDiff{
				Name:       extensionName,
				FromSchema: databaseSchema,
				ToSchema:   generatedSchema,
			})
		}
	}

	// Find removed extensions (exist in database but not in generated schema)
	// Note: Ignored extensions are already filtered out, so they will never be marked for removal
	for extensionName := range dbExtensions {
		if _, exists := genExtensions[extensionName]; !exists {
			diff.ExtensionsRemoved = append(diff.ExtensionsRemoved, extensionName)
		}
	}

	// A description that does not describe extensions is not a description of a
	// database with no extensions, and a read that did not look for them is not
	// a database that has none. Both directions are dropped here rather than at
	// the two dozen call sites that build a comparison (stokaro/ptah#1276).
	//
	// IF NOT EXISTS is not a convergence guard for extensions now that their
	// installation schema is desired state. If the current side never looked,
	// an extension requested in `extensions` may already exist in `public`;
	// PostgreSQL accepts CREATE EXTENSION IF NOT EXISTS as a no-op and leaves the
	// requested placement unapplied. Withhold every unknown-current addition and
	// report it instead of silently accepting the wrong schema.
	kept, withheld := cov.keepPlannedAdditions(
		coverage.Extension, diff.ExtensionsAdded, globalName, unguardedCreations(),
	)
	diff.ExtensionsAdded = kept
	cov.recordUndecidedAdditions(coverage.Extension, withheld)
	diff.ExtensionsRemoved = cov.keepPlannedRemovals(coverage.Extension, diff.ExtensionsRemoved, globalName)

	// Sort for consistent output
	sort.Strings(diff.ExtensionsAdded)
	sort.Strings(diff.ExtensionsRemoved)
	sort.Slice(diff.ExtensionsModified, func(i, j int) bool {
		return diff.ExtensionsModified[i].Name < diff.ExtensionsModified[j].Name
	})
}

func effectiveExtensionSchema(schema string, semantics identifier.Semantics) string {
	if schema == "" {
		schema = semantics.DefaultSchema
	}
	return semantics.TableIdentityKey(schema)
}
