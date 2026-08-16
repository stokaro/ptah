package goschema

// newDatabase returns an empty Database with every slice and map field
// initialized, ready to accumulate schema objects from one or more sources.
func newDatabase() *Database {
	return &Database{
		Schemas:           []Schema{},
		Tables:            []Table{},
		Fields:            []Field{},
		Indexes:           []Index{},
		Constraints:       []Constraint{},
		Enums:             []Enum{},
		EmbeddedFields:    []EmbeddedField{},
		Extensions:        []Extension{},
		Functions:         []Function{},
		Sequences:         []Sequence{},
		Domains:           []Domain{},
		CompositeTypes:    []CompositeType{},
		Ranges:            []Range{},
		Views:             []View{},
		MaterializedViews: []MaterializedView{},
		Triggers:          []Trigger{},
		RLSPolicies:       []RLSPolicy{},
		RLSEnabledTables:  []RLSEnabledTable{},
		Roles:             []Role{},
		Grants:            []Grant{},
		ManagedData:       []ManagedData{},
		EmbeddedSources: EmbeddedSources{
			Fields:      []Field{},
			Definitions: []EmbeddedField{},
		},
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
	// A limit one source declared is a limit of the whole composite. Dropping
	// it here would turn "this description does not claim to describe X" into
	// silence, and silence is what a comparator reads as a removal -- the exact
	// failure [go.5x5.cz/ptah/core/coverage] exists to prevent. Merging matches
	// what internal/schemafile already does when it composes documents.
	dst.NotDescribed = dst.NotDescribed.Merge(src.NotDescribed)
	dst.Schemas = append(dst.Schemas, src.Schemas...)
	dst.Tables = append(dst.Tables, src.Tables...)
	dst.Fields = append(dst.Fields, src.Fields...)
	dst.Fields = append(dst.Fields, src.EmbeddedSources.Fields...)
	dst.Indexes = append(dst.Indexes, src.Indexes...)
	dst.Constraints = append(dst.Constraints, src.Constraints...)
	dst.Enums = append(dst.Enums, src.Enums...)
	dst.EmbeddedFields = append(dst.EmbeddedFields, src.EmbeddedFields...)
	dst.EmbeddedFields = append(dst.EmbeddedFields, src.EmbeddedSources.Definitions...)
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

// finalizeDatabase rebuilds the derived state shared by every parsed or merged
// database after the caller has applied its collision policy.
func finalizeDatabase(result *Database) error {
	// Process embedded fields BEFORE building dependency graph so that foreign
	// keys contributed by embedded fields are included in dependency analysis.
	result.Fields = processEmbeddedFields(result.EmbeddedFields, result.Fields)
	validator := compositeDefinitionValidator{
		database: result,
		resolver: newTableScopeResolver(result.Tables),
	}
	if err := validator.fields(); err != nil {
		return err
	}
	result.Fields = deduplicateFields(
		result.Fields,
		validator.resolver,
		compositeDeduplicationScope,
	)
	stashCompositeHelperDefinitions(result)

	// Build dependency graph for foreign key ordering.
	buildDependencyGraph(result)

	// Sort tables and functions by dependency order.
	sortTablesByDependencies(result)
	sortFunctionsByDependencies(result)
	return nil
}

// stashCompositeHelperDefinitions removes source-only composite helpers from
// the public schema surface while retaining them for repeated finalization.
func stashCompositeHelperDefinitions(result *Database) {
	helperStructs := embeddedSourceStructNames(result)
	result.EmbeddedSources.Fields = result.EmbeddedSources.Fields[:0]
	fields := result.Fields[:0]
	for _, field := range result.Fields {
		if helperStructs[field.StructName] {
			result.EmbeddedSources.Fields = append(result.EmbeddedSources.Fields, field)
			if isCompositeHelperType(field.StructName) {
				continue
			}
		}
		field.FieldName = unscopedGoTypeIdentity(field.FieldName)
		fields = append(fields, field)
	}
	result.Fields = fields

	result.EmbeddedSources.Definitions = result.EmbeddedSources.Definitions[:0]
	embeddedFields := result.EmbeddedFields[:0]
	for _, field := range result.EmbeddedFields {
		if helperStructs[field.StructName] {
			result.EmbeddedSources.Definitions = append(result.EmbeddedSources.Definitions, field)
			if isCompositeHelperType(field.StructName) {
				continue
			}
		}
		embeddedFields = append(embeddedFields, field)
	}
	result.EmbeddedFields = embeddedFields
}

func embeddedSourceStructNames(result *Database) map[string]bool {
	tableStructs := make(map[string]bool, len(result.Tables))
	for _, table := range result.Tables {
		tableStructs[table.StructName] = true
	}
	helperStructs := make(map[string]bool)
	for _, embedded := range result.EmbeddedFields {
		if !tableStructs[embedded.StructName] {
			helperStructs[embedded.StructName] = true
		}
		if !tableStructs[embedded.EmbeddedTypeName] {
			helperStructs[embedded.EmbeddedTypeName] = true
		}
	}
	return helperStructs
}

func restoreCompositeHelperDefinitions(result *Database) {
	result.Fields = append(result.Fields, result.EmbeddedSources.Fields...)
	result.EmbeddedFields = append(
		result.EmbeddedFields,
		result.EmbeddedSources.Definitions...,
	)
	result.EmbeddedSources.Fields = nil
	result.EmbeddedSources.Definitions = nil
}

func finalizeAccumulatedDatabase(result *Database) error {
	normalizeTableScopedNames(result)
	if err := validateDuplicateSchemaObjectDefinitions(result); err != nil {
		return err
	}
	deduplicateComposite(result)
	return finalizeDatabase(result)
}

// Merge combines several parsed source schemas into a single finalized Database.
//
// It reconciles source-local Go struct associations, concatenates every schema
// slice of the inputs, and then runs the same strict finalization that ParseFS
// applies to the per-file schemas of one directory. Every named object kind
// uses its stable database identity, with
// schema-qualified names where applicable, table-qualified names for fields,
// indexes, constraints, triggers, and RLS objects, and global names for objects
// such as extensions, functions, enums, and roles. Identical definitions
// collapse to one; definitions with the same identity but different desired
// properties return a descriptive conflict error. Parser-only Go struct and
// field names do not make otherwise identical definitions conflict.
//
// Embedded fields are then expanded and the dependency graph and table/function
// ordering are rebuilt. The derived maps of the inputs are ignored and
// recomputed from the merged slices. A nil input is skipped.
//
// Merge is the reusable form of the merge ParseFS performs internally, so it can
// assemble a composite desired-state schema from heterogeneous sources (several
// Go roots, or a mix of Go, YAML, and HCL) and feed the result to the existing
// render/compare/migrate paths unchanged.
//
// Inputs may be raw parser output or already-finalized schemas from ParseDir,
// ParseFS, or an OCI artifact. Post-expansion conflict validation and
// deduplication make repeated finalization idempotent.
func Merge(dbs ...*Database) (*Database, error) {
	sources := make([]*Database, 0, len(dbs))
	for _, db := range dbs {
		if db == nil {
			continue
		}
		source := newDatabase()
		appendDatabase(source, db)
		sources = append(sources, source)
	}
	if err := reconcileTableOwners(sources); err != nil {
		return nil, err
	}

	result := newDatabase()
	for _, source := range sources {
		appendDatabase(result, source)
	}
	return finalizeMergedDatabase(result)
}

// mergeAccumulatedDatabase reconciles and finalizes a mutable accumulator
// assembled by ParseFS. Merge copies and reconciles its inputs first so callers'
// source schemas are never mutated.
func mergeAccumulatedDatabase(result *Database) (*Database, error) {
	if err := reconcileTableOwners([]*Database{result}); err != nil {
		return nil, err
	}
	return finalizeMergedDatabase(result)
}

func finalizeMergedDatabase(result *Database) (*Database, error) {
	if err := finalizeAccumulatedDatabase(result); err != nil {
		return nil, err
	}
	return result, nil
}
