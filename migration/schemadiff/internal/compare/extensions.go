package compare

import (
	"sort"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	opts *config.CompareOptions,
	cov Coverage,
) {
	ExtensionsWithSemantics(desired, database, diff, opts, cov, identifier.ForDialect(platform.Postgres))
}

// ExtensionsWithSemantics compares extension identity and installation schema
// using the target database's resolved default schema and identifier rules.
func ExtensionsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
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
	diff.ExtensionsAdded = make(difftypes.ExtensionChanges, 0)
	diff.ExtensionsRemoved = make(difftypes.ExtensionChanges, 0)
	diff.ExtensionsModified = make([]difftypes.ExtensionDiff, 0)

	// Create maps for quick lookup, filtering out ignored extensions
	genExtensions := make(map[string]schemamodel.Extension)
	for _, extension := range desired.Extensions {
		if !opts.IsExtensionIgnored(extension.Name) {
			genExtensions[extension.Name] = extension
		}
	}

	// Create map of database extensions for efficient lookup, filtering out ignored extensions
	dbExtensions := make(map[string]catalog.Extension)
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
			diff.ExtensionsAdded = append(diff.ExtensionsAdded, genExtensions[extensionName])
			continue
		}
		generatedSchema := effectiveExtensionSchema(genExtensions[extensionName].Schema, semantics)
		databaseSchema := effectiveExtensionSchema(databaseExtension.Schema, semantics)
		// The version is compared only when the declaration names one. An
		// extension declared without a version means "whatever the server
		// installs", and reporting the installed version as a difference from
		// nothing would plan an update on every run against a database that is
		// exactly what was asked for.
		generatedVersion := strings.TrimSpace(genExtensions[extensionName].Version)
		databaseVersion := strings.TrimSpace(databaseExtension.Version)
		versionMoved := generatedVersion != "" && generatedVersion != databaseVersion
		if generatedSchema != databaseSchema || versionMoved {
			change := difftypes.ExtensionDiff{
				Name:        extensionName,
				FromSchema:  databaseSchema,
				ToSchema:    generatedSchema,
				Relocatable: databaseExtension.Relocatable,
			}
			if versionMoved {
				change.FromVersion = databaseVersion
				change.ToVersion = generatedVersion
			}
			diff.ExtensionsModified = append(diff.ExtensionsModified, change)
		}
	}

	// Find removed extensions (exist in database but not in generated schema)
	// Note: Ignored extensions are already filtered out, so they will never be marked for removal
	//
	// An extension a declared column's TYPE comes from is not unrequired merely
	// because no extension annotation names it. `vector(384)` is a declaration
	// that pgvector is needed, and dropping it is the plan contradicting its
	// own CREATE TABLE (stokaro/ptah#2389).
	needed := extensionsDeclaredTypesNeed(desired)
	for extensionName, databaseExtension := range dbExtensions {
		if _, exists := genExtensions[extensionName]; exists {
			continue
		}
		if needed[extensionName] {
			continue
		}
		diff.ExtensionsRemoved = append(diff.ExtensionsRemoved, extensionFromCatalog(databaseExtension))
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
	kept, withheld := keepPlannedAdditions(cov,
		coverage.Extension, diff.ExtensionsAdded, extensionSpelling, extensionDisplay, unguardedCreations(),
	)
	diff.ExtensionsAdded = kept
	cov.recordUndecidedAdditions(withheld)
	diff.ExtensionsRemoved = keepPlannedRemovals(cov, coverage.Extension, diff.ExtensionsRemoved, extensionSpelling)

	// Sort for consistent output
	sortExtensions(diff.ExtensionsAdded)
	sortExtensions(diff.ExtensionsRemoved)
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

// extensionFromCatalog carries an extension the database reported into the
// shape the diff holds, matching what internal/convert/dbschematogo builds for
// the same read -- including IfNotExists, which is true so a down migration can
// re-create the extension without failing on one that survived.
func extensionFromCatalog(reported catalog.Extension) schemamodel.Extension {
	extension := schemamodel.Extension{
		Name:        reported.Name,
		Schema:      reported.Schema,
		IfNotExists: true,
		Version:     reported.Version,
		// Carried rather than recomputed: only the reader has the catalog, and
		// the renderer needs it to tell an extension nothing depends on from
		// one a column type still needs.
		Provides: reported.Provides,
	}
	if reported.Comment != nil {
		extension.Comment = *reported.Comment
	}
	return extension
}

// extensionSpelling is globalName for a change that carries its operand: an
// extension is named globally rather than inside a schema.
func extensionSpelling(extension schemamodel.Extension) (schema string, spellings []string) {
	return globalName(extension.Name)
}

// extensionDisplay names one for a record a person reads.
func extensionDisplay(extension schemamodel.Extension) string { return extension.Name }

// sortExtensions orders by the key the name lists were sorted on.
func sortExtensions(extensions difftypes.ExtensionChanges) {
	sort.Slice(extensions, func(i, j int) bool { return extensions[i].Name < extensions[j].Name })
}
