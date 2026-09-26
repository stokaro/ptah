package compare

import (
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/serverobjects"
	"ptah.run/migration/schemadiff/difftypes"
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
//   - Ignored extensions enter the comparison as coverage records on both
//     sides, so neither side is authoritative about them
//   - Ignored extensions will never be marked for removal
//   - Ignored extensions will never be marked for creation either. The older
//     spelling of this line said they "can still be created if defined in the
//     target schema", which nothing ever did: a `schema diff` from a
//     hand-authored file to a live PostgreSQL database would emit CREATE
//     EXTENSION "plpgsql" on every run
//   - If opts is nil, default options are used (ignores "plpgsql")
//
// # Comparison Process
//
// The function performs comparison in three phases:
//  1. **Coverage**: Records the ignored extensions as objects the desired
//     description does not describe
//  2. **Extension Discovery**: Creates lookup maps for efficient extension comparison
//  3. **Extension Diff Analysis**: Identifies added, removed, and moved
//     extensions, then withholds the changes coverage does not authorize
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
	// An ignored extension enters the comparison as a coverage record rather
	// than as a filter of its own. It goes on BOTH sides, which is what the
	// list has always meant: neither side is authoritative about the object, so
	// nothing is planned for it in either direction -- the words
	// [ptah.run/core/coverage] uses for a record it holds.
	//
	// Recording it on the desired side alone would withhold the removal and
	// leave the addition, and the addition is the half a `schema diff` from a
	// hand-authored file to a live PostgreSQL database meets: the file
	// describes no plpgsql, every server has one, and CREATE EXTENSION
	// "plpgsql" is not a statement anybody asked for (stokaro/ptah#3373).
	//
	// Folded here rather than at every caller because every caller of every
	// entry point would have to fold it, and the first one that forgot would
	// bring back the two filters this replaced.

	// Initialize slices to ensure they're never nil
	diff.ExtensionsAdded = make(difftypes.ExtensionChanges, 0)
	diff.ExtensionsRemoved = make(difftypes.ExtensionChanges, 0)
	diff.ExtensionsModified = make([]difftypes.ExtensionDiff, 0)

	// Both sides enter whole. What an ignored extension changes is which
	// PLANNED change survives the coverage gate below, not which object the
	// comparison can see.
	genExtensions := make(map[string]schemamodel.Extension)
	for _, extension := range desired.Extensions {
		genExtensions[extension.Name] = extension
	}

	dbExtensions := make(map[string]catalog.Extension)
	for _, extension := range database.Extensions {
		dbExtensions[extension.Name] = extension
	}

	// Find added extensions (exist in generated schema but not in database)
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
		// An extension the server installs in every database is the
		// server's, whatever the ignore list says. Only the removal is
		// withheld: where a line of the server lacks the extension, a
		// declaration of it is still created (stokaro/ptah#3687).
		if serverobjects.IsExtension(opts.Dialect, extensionName) {
			continue
		}
		diff.ExtensionsRemoved = append(diff.ExtensionsRemoved, extensionFromCatalog(databaseExtension))
	}

	// An ignored extension leaves both lists before the coverage gate sees
	// them. That order is the whole point: a change coverage withholds is
	// UNDECIDED and gets reported, because neither side could answer for the
	// object -- while an ignored one is DECIDED, by whoever configured the run,
	// and a diagnostic about it would be Ptah asking a question it was already
	// given the answer to.
	//
	// One filter, after the diff rather than before it, so the names the two
	// sides carry are still compared and only the planned change is dropped.
	diff.ExtensionsAdded = withoutIgnoredExtensions(diff.ExtensionsAdded, opts)
	diff.ExtensionsRemoved = withoutIgnoredExtensions(diff.ExtensionsRemoved, opts)

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

// withoutIgnoredExtensions drops the planned changes the configured ignore list
// covers, preserving the input's nil-versus-empty shape for the reason `keep`
// gives.
func withoutIgnoredExtensions(
	planned difftypes.ExtensionChanges,
	opts *config.CompareOptions,
) difftypes.ExtensionChanges {
	return keep(planned, func(change schemamodel.Extension) bool {
		return !opts.IsExtensionIgnored(change.Name)
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
