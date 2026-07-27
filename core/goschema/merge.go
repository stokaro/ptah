package goschema

// newDatabase returns an empty Database with every slice and map field
// initialized, ready to accumulate schema objects from one or more sources.
func newDatabase() *Database {
	return &Database{
		Schemas:                    []Schema{},
		Tables:                     []Table{},
		Fields:                     []Field{},
		Indexes:                    []Index{},
		Constraints:                []Constraint{},
		Enums:                      []Enum{},
		EmbeddedFields:             []EmbeddedField{},
		Extensions:                 []Extension{},
		Functions:                  []Function{},
		Sequences:                  []Sequence{},
		Domains:                    []Domain{},
		CompositeTypes:             []CompositeType{},
		Ranges:                     []Range{},
		Views:                      []View{},
		MaterializedViews:          []MaterializedView{},
		Triggers:                   []Trigger{},
		RLSPolicies:                []RLSPolicy{},
		RLSEnabledTables:           []RLSEnabledTable{},
		Roles:                      []Role{},
		Grants:                     []Grant{},
		ManagedData:                []ManagedData{},
		Dependencies:               make(map[string][]string),
		FunctionDependencies:       make(map[string][]string),
		SelfReferencingForeignKeys: make(map[string][]SelfReferencingFK),
	}
}

// appendDatabase concatenates every schema slice of src onto dst. The derived
// maps (Dependencies, FunctionDependencies, SelfReferencingForeignKeys) are not
// copied: they are rebuilt from the combined slices by finalizeDatabase, so
// carrying them here would only risk duplicating self-referencing entries.
func appendDatabase(dst, src *Database) {
	dst.Schemas = append(dst.Schemas, src.Schemas...)
	dst.Tables = append(dst.Tables, src.Tables...)
	dst.Fields = append(dst.Fields, src.Fields...)
	dst.Indexes = append(dst.Indexes, src.Indexes...)
	dst.Constraints = append(dst.Constraints, src.Constraints...)
	dst.Enums = append(dst.Enums, src.Enums...)
	dst.EmbeddedFields = append(dst.EmbeddedFields, src.EmbeddedFields...)
	dst.Extensions = append(dst.Extensions, src.Extensions...)
	dst.Functions = append(dst.Functions, src.Functions...)
	dst.Sequences = append(dst.Sequences, src.Sequences...)
	dst.Domains = append(dst.Domains, src.Domains...)
	dst.CompositeTypes = append(dst.CompositeTypes, src.CompositeTypes...)
	dst.Ranges = append(dst.Ranges, src.Ranges...)
	dst.Views = append(dst.Views, src.Views...)
	dst.MaterializedViews = append(dst.MaterializedViews, src.MaterializedViews...)
	dst.Triggers = append(dst.Triggers, src.Triggers...)
	dst.RLSPolicies = append(dst.RLSPolicies, src.RLSPolicies...)
	dst.RLSEnabledTables = append(dst.RLSEnabledTables, src.RLSEnabledTables...)
	dst.Roles = append(dst.Roles, src.Roles...)
	dst.Grants = append(dst.Grants, src.Grants...)
	dst.ManagedData = append(dst.ManagedData, src.ManagedData...)
}

// finalizeDatabase runs the shared post-merge pipeline over an accumulator that
// already holds the concatenated schema slices of one or more sources. It
// validates that no two sources define the same object differently,
// deduplicates identical definitions, normalizes table-scoped names, expands
// embedded fields into concrete fields, and then rebuilds the dependency graph
// and ordering. It is the single finalize path shared by ParseFS and Merge.
func finalizeDatabase(result *Database) error {
	if err := validateDuplicateSchemaObjectDefinitions(result); err != nil {
		return err
	}

	// deduplicate entities (same table/field defined in multiple files/sources)
	Deduplicate(result)
	normalizeTableScopedNames(result)

	// Process embedded fields BEFORE building dependency graph so that foreign
	// keys contributed by embedded fields are included in dependency analysis.
	result.Fields = processEmbeddedFields(result.EmbeddedFields, result.Fields)

	// Build dependency graph for foreign key ordering.
	buildDependencyGraph(result)

	// Sort tables and functions by dependency order.
	sortTablesByDependencies(result)
	sortFunctionsByDependencies(result)

	return nil
}

// Merge combines several parsed source schemas into a single finalized Database.
//
// It concatenates every schema slice of the inputs and then runs the same
// finalize pipeline that ParseFS applies to the per-file schemas of one
// directory, so it reconciles cross-source objects exactly as ParseFS reconciles
// cross-file ones. Conflicting definitions of a named schema, view, materialized
// view, trigger, constraint, or role are rejected with an error. Tables, fields,
// and the remaining object kinds are deduplicated first-wins: identical
// definitions across sources collapse to one, and if two sources define the same
// table or field differently the first is kept (matching ParseFS's cross-file
// behavior; stricter table/field conflict detection for composite sources is a
// follow-up). Embedded fields are then expanded and the dependency graph and
// table/function ordering are rebuilt. The derived maps of the inputs are
// ignored and recomputed from the merged slices. A nil input is skipped.
//
// Merge is the reusable form of the merge ParseFS performs internally, so it can
// assemble a composite desired-state schema from heterogeneous sources (several
// Go roots, or a mix of Go, YAML, and HCL) and feed the result to the existing
// render/compare/migrate paths unchanged.
//
// Inputs must be freshly parsed sources whose embedded fields have not yet been
// expanded into concrete fields (as produced by parsing a single file), not the
// already-finalized output of ParseDir/ParseFS: processEmbeddedFields is not
// idempotent, so re-finalizing an already-expanded schema would duplicate the
// generated fields.
func Merge(dbs ...*Database) (*Database, error) {
	result := newDatabase()
	for _, db := range dbs {
		if db == nil {
			continue
		}
		appendDatabase(result, db)
	}
	if err := finalizeDatabase(result); err != nil {
		return nil, err
	}
	return result, nil
}
