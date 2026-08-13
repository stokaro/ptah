// Package types defines the schema difference model (SchemaDiff and the
// per-object diffs for tables, columns, enums, functions, triggers, RLS
// policies, roles, and grants) produced by schemadiff and consumed by the
// migration planner.
package types

import (
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform/identifier"
)

// IndexRef identifies an index together with its owning table.
// Table-qualified identity is required for dialects such as MySQL and MariaDB,
// where index names are scoped to a table rather than the database.
type IndexRef struct {
	// Name is the raw, unqualified index identifier. TableName carries the
	// namespace used by schema-scoped dialects.
	Name string `json:"name"`

	// TableName is the qualified name of the table that owns the index.
	TableName string `json:"table_name"`
}

// ConstraintRemovalInfo contains information about a constraint that needs to be
// removed, including the constraint name, the table it belongs to, and its type.
//
// This is needed for databases like MySQL/MariaDB that require both the table
// name and a type-specific drop syntax — e.g. FOREIGN KEY constraints are
// dropped with `ALTER TABLE <table> DROP FOREIGN KEY <name>` rather than
// `DROP CONSTRAINT`. The plain ConstraintsRemoved name list discards the table
// name, which the comparator does know at diff time, so it is preserved here in
// parallel.
type ConstraintRemovalInfo struct {
	// Name is the name of the constraint to be removed
	Name string `json:"name"`

	// TableName is the name of the table that the constraint belongs to
	TableName string `json:"table_name"`

	// Type is the constraint type (FOREIGN KEY, CHECK, UNIQUE, PRIMARY KEY, ...)
	Type string `json:"type"`
}

// ForeignKeyRemovalInfo supplies the local and referenced columns needed to
// order a foreign-key removal around column changes. It is supplemental to a
// matching ConstraintRemovalInfo, correlated by table and constraint name; it
// does not independently describe a schema change and is ignored without that
// base removal. The separate collection keeps ConstraintRemovalInfo a
// comparable three-field identity value.
type ForeignKeyRemovalInfo struct {
	// Name is the foreign-key constraint name.
	Name string `json:"name"`
	// TableName is the local table that owns the foreign key.
	TableName string `json:"table_name"`
	// Columns are the local columns in constraint order.
	Columns []string `json:"columns,omitempty"`
	// ForeignTable is the referenced table.
	ForeignTable string `json:"foreign_table"`
	// ForeignColumns are the referenced columns in constraint order.
	ForeignColumns []string `json:"foreign_columns,omitempty"`
}

// ConstraintAdditionInfo contains the table-qualified definition of a
// constraint that needs to be added, in parallel to the bare ConstraintsAdded
// name list (mirroring ConstraintsRemovedWithTables).
//
// This exists because a field-level FOREIGN KEY whose constraint name repeats
// across several tables cannot be resolved from the name alone. The canonical
// case is an embedded inline-relation mixin: a shared base struct that carries
// `foreign=` fields (e.g. tenant_id with foreign_key_name="fk_entity_tenant")
// and is embedded into many concrete tables, so the same FK name legitimately
// lands on every host table. The mixin's Go struct name is not a table, so a
// planner that re-derives the table from the field's StructName emits
// `ALTER TABLE <MixinStruct> ...` once per host, all collapsed onto the bogus
// name (issue #197). Carrying the concrete table (and the full FK definition)
// here lets the planner emit one correct ALTER TABLE per real host table.
type ConstraintAdditionInfo struct {
	// Name is the name of the constraint to be added.
	Name string `json:"name"`

	// TableName is the concrete database table the constraint is added to.
	TableName string `json:"table_name"`

	// Type is the constraint type (FOREIGN KEY, CHECK, UNIQUE, ...).
	Type string `json:"type"`

	// Columns are the local columns the constraint covers (UNIQUE columns or FK
	// source columns).
	Columns []string `json:"columns,omitempty"`
	// IncludeColumns carries PostgreSQL INCLUDE columns for covering UNIQUE and
	// PRIMARY KEY constraints.
	IncludeColumns []string `json:"include_columns,omitempty"`
	// NullsDistinct carries PostgreSQL UNIQUE NULLS [NOT] DISTINCT state.
	// Nil means the clause was not specified.
	NullsDistinct *bool `json:"nulls_distinct,omitempty"`
	// CheckExpression is the CHECK predicate body (CHECK only).
	CheckExpression string `json:"check_expression,omitempty"`

	// ForeignTable / ForeignColumn / ForeignColumns describe the FK target
	// (FOREIGN KEY only). ForeignColumn is kept for compatibility with older
	// single-column callers; ForeignColumns carries the full referenced column
	// list for composite keys.
	ForeignTable   string   `json:"foreign_table,omitempty"`
	ForeignColumn  string   `json:"foreign_column,omitempty"`
	ForeignColumns []string `json:"foreign_columns,omitempty"`

	// OnDelete / OnUpdate are the referential actions (FOREIGN KEY only).
	OnDelete string `json:"on_delete,omitempty"`
	OnUpdate string `json:"on_update,omitempty"`
}

// SchemaDiff represents comprehensive differences between two database schemas.
//
// This structure captures all types of schema changes that can occur between a target
// schema (generated from Go struct annotations) and an existing database schema.
// It provides a complete picture of what needs to be modified to bring the database
// schema in line with the application's expected schema.
//
// # Structure Organization
//
// The diff is organized by database object type for clear categorization:
//   - Tables: New, removed, and modified table structures
//   - Enums: New, removed, and modified enum types
//   - Indexes: New and removed database indexes
//
// # JSON Serialization
//
// All fields are JSON-serializable for integration with external tools,
// CI/CD pipelines, and migration management systems.
//
// # Example Usage
//
//	diff := &SchemaDiff{
//		TablesAdded: []string{"users", "posts"},
//		TablesModified: []TableDiff{
//			{TableName: "products", ColumnsAdded: []string{"price", "category"}},
//		},
//		EnumsAdded: []string{"status_type"},
//	}
//
//	if diff.HasChanges() {
//		fmt.Printf("Found %d new tables\n", len(diff.TablesAdded))
//	}
type SchemaDiff struct {
	// IdentifierSemantics records live catalog identifier rules used to produce
	// this diff. It is absent for dialect-only comparisons, whose planners use
	// conservative offline defaults.
	IdentifierSemantics *identifier.Semantics `json:"identifier_semantics,omitempty"`

	// TablesAdded contains names of tables that exist in the target schema
	// but not in the current database schema
	TablesAdded []string `json:"tables_added"`

	// TablesRemoved contains names of tables that exist in the current database
	// but not in the target schema (potentially dangerous - data loss)
	TablesRemoved []string `json:"tables_removed"`

	// TablesModified contains detailed information about tables that exist in both
	// schemas but have structural differences (columns, constraints, etc.)
	TablesModified []TableDiff `json:"tables_modified"`

	// EnumsAdded contains names of enum types that exist in the target schema
	// but not in the current database schema
	EnumsAdded []string `json:"enums_added"`

	// EnumsRemoved contains names of enum types that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing data)
	EnumsRemoved []string `json:"enums_removed"`

	// EnumsModified contains detailed information about enum types that exist in both
	// schemas but have different values (additions/removals)
	EnumsModified []EnumDiff `json:"enums_modified"`

	// IndexesAdded contains table-qualified indexes that exist in the target
	// schema but not in the current database schema.
	IndexesAdded []IndexRef `json:"indexes_added"`

	// IndexesRemoved contains table-qualified indexes that exist in the current
	// database but not in the target schema. Removing them may affect query
	// plans or uniqueness protections.
	IndexesRemoved []IndexRef `json:"indexes_removed"`

	// ConstraintBackedIndexRemovals is the subset of IndexesRemoved whose
	// object is enforced by a UNIQUE constraint of the same name on the same
	// table. It records what the object is; each planner spells the statement
	// its engine accepts.
	//
	// PostgreSQL refuses the index spelling for one:
	// `cannot drop index uq_users_email because constraint uq_users_email on
	// table users requires it (SQLSTATE 2BP01)`, measured on 17.10, so its
	// planner drops the constraint instead. MySQL and MariaDB are the opposite
	// case -- a unique key and its index are one catalog row that `DROP INDEX`
	// drops, which is what the pinned community binary v1.3.0 plans there -- so
	// their planners ignore the list and their plans are unchanged by it.
	//
	// The list is not PostgreSQL-only, because the down direction needs it on
	// every engine: what was dropped was a UNIQUE constraint, and a rollback
	// that reverses it into an index addition restores the wrong object (see
	// the generator's reverseIndexRemovals).
	ConstraintBackedIndexRemovals []IndexRef `json:"constraint_backed_index_removals,omitempty"`

	// ExtensionsAdded contains names of PostgreSQL extensions that exist in the target schema
	// but not in the current database schema
	ExtensionsAdded []string `json:"extensions_added"`

	// ExtensionsRemoved contains names of PostgreSQL extensions that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	ExtensionsRemoved []string `json:"extensions_removed"`

	// ExtensionsModified contains PostgreSQL extensions whose installation schema differs.
	// PostgreSQL extension names are database-wide identities; schema is placement, not identity.
	ExtensionsModified []ExtensionDiff `json:"extensions_modified"`

	// FunctionsAdded contains names of PostgreSQL functions that exist in the target schema
	// but not in the current database schema
	FunctionsAdded []string `json:"functions_added"`

	// FunctionsRemoved contains names of PostgreSQL functions that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	FunctionsRemoved []string `json:"functions_removed"`

	// FunctionsModified contains detailed information about functions that exist in both
	// schemas but have different definitions (parameters, body, attributes, etc.)
	FunctionsModified []FunctionDiff `json:"functions_modified"`

	// SequencesAdded contains names of standalone sequences that exist in the target
	// schema but not in the current database schema.
	SequencesAdded []string `json:"sequences_added"`

	// SequencesRemoved contains names of standalone sequences that exist in the current
	// database but not in the target schema (potentially dangerous - may break defaults).
	SequencesRemoved []string `json:"sequences_removed"`

	// SequencesModified contains detailed information about sequences that exist in both
	// schemas but have different options (increment, cache, cycle, ownership, etc.).
	SequencesModified []SequenceDiff `json:"sequences_modified"`

	// DomainsAdded/Removed/Modified track PostgreSQL domain types.
	DomainsAdded    []string     `json:"domains_added"`
	DomainsRemoved  []string     `json:"domains_removed"`
	DomainsModified []DomainDiff `json:"domains_modified"`

	// CompositeTypesAdded/Removed/Modified track PostgreSQL composite types.
	CompositeTypesAdded    []string            `json:"composite_types_added"`
	CompositeTypesRemoved  []string            `json:"composite_types_removed"`
	CompositeTypesModified []CompositeTypeDiff `json:"composite_types_modified"`

	// RangesAdded/Removed/Modified track PostgreSQL range types. PostgreSQL has
	// no ALTER TYPE ... AS RANGE, so a modification is planned as a non-CASCADE
	// DROP TYPE followed by a CREATE TYPE, the same shape domains and composite
	// types already use.
	//
	// Modified used to be absent, and the comparator built name sets only, so
	// changing the subtype of an existing range type produced an empty plan and
	// `schema apply` reported "Schema is synced" while the database still held
	// the old definition (stokaro/ptah#931 item 2).
	RangesAdded    []string    `json:"ranges_added"`
	RangesRemoved  []string    `json:"ranges_removed"`
	RangesModified []RangeDiff `json:"ranges_modified"`

	// ViewsAdded contains names of views that exist in the target schema
	// but not in the current database schema.
	ViewsAdded []string `json:"views_added"`

	// ViewsRemoved contains names of views that exist in the current database
	// but not in the target schema.
	ViewsRemoved []string `json:"views_removed"`

	// ViewsModified contains detailed information about views with changed definitions.
	ViewsModified []ViewDiff `json:"views_modified"`

	// MaterializedViewsAdded contains names of materialized views that exist in the target schema
	// but not in the current database schema.
	MaterializedViewsAdded []string `json:"materialized_views_added"`

	// MaterializedViewsRemoved contains names of materialized views that exist in the current database
	// but not in the target schema.
	MaterializedViewsRemoved []string `json:"materialized_views_removed"`

	// MaterializedViewsModified contains detailed information about changed materialized views.
	MaterializedViewsModified []MaterializedViewDiff `json:"materialized_views_modified"`

	// TriggersAdded contains table-qualified trigger refs that exist in the target schema
	// but not in the current database schema.
	TriggersAdded []TriggerRef `json:"triggers_added"`

	// TriggersRemoved contains table-qualified trigger refs that exist in the current database
	// but not in the target schema.
	TriggersRemoved []TriggerRef `json:"triggers_removed"`

	// TriggersModified contains detailed information about changed triggers.
	TriggersModified []TriggerDiff `json:"triggers_modified"`

	// RLSPoliciesAdded contains RLS policies that exist in the target schema
	// but not in the current database schema.
	//
	// A policy name is scoped to its table, not to the schema, so the name on
	// its own does not identify a policy: two tables may each carry one called
	// "tenant_isolation". The table travels with the name for the same reason
	// it does in TriggersAdded.
	RLSPoliciesAdded []RLSPolicyRef `json:"rls_policies_added"`

	// RLSPoliciesRemoved contains RLS policies that exist in the current database
	// but not in the target schema (may remove an access-control protection)
	RLSPoliciesRemoved []RLSPolicyRef `json:"rls_policies_removed"`

	// RLSPoliciesModified contains detailed information about RLS policies that exist in both
	// schemas but have different definitions (expressions, roles, etc.)
	RLSPoliciesModified []RLSPolicyDiff `json:"rls_policies_modified"`

	// RLSEnabledTablesAdded contains names of tables that need RLS enabled
	RLSEnabledTablesAdded []string `json:"rls_enabled_tables_added"`

	// RLSEnabledTablesRemoved contains names of tables that need RLS disabled
	// (potentially dangerous - removes row-level security)
	RLSEnabledTablesRemoved []string `json:"rls_enabled_tables_removed"`

	// RolesAdded contains names of PostgreSQL roles that exist in the target schema
	// but not in the current database schema
	RolesAdded []string `json:"roles_added"`

	// RolesRemoved contains names of PostgreSQL roles that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	RolesRemoved []string `json:"roles_removed"`

	// RolesModified contains detailed information about roles that exist in both
	// schemas but have different definitions (attributes, passwords, etc.)
	RolesModified []RoleDiff `json:"roles_modified"`

	// GrantsAdded contains PostgreSQL privilege grants that exist in the target
	// schema but not in the current database schema.
	GrantsAdded []GrantRef `json:"grants_added"`

	// GrantsRemoved contains PostgreSQL privilege grants that exist in the current
	// database schema for managed roles but not in the target schema.
	GrantsRemoved []GrantRef `json:"grants_removed"`

	// GrantOptionsAdded contains PostgreSQL privileges whose WITH GRANT OPTION
	// flag exists in the target schema but not in the current database schema.
	GrantOptionsAdded []GrantRef `json:"grant_options_added"`

	// GrantOptionsRevoked contains PostgreSQL privileges whose WITH GRANT OPTION
	// flag exists in the database but not in the target schema.
	GrantOptionsRevoked []GrantRef `json:"grant_options_revoked"`

	// ConstraintsAdded contains names of constraints that exist in the target schema
	// but not in the current database schema
	ConstraintsAdded []string `json:"constraints_added"`

	// ConstraintsAddedWithTables contains the table-qualified definitions of the
	// constraints in ConstraintsAdded. It is NOT index-aligned with
	// ConstraintsAdded — each list is sorted independently (ConstraintsAdded by
	// name, this one by table then name), so consumers must correlate entries
	// by constraint name, never by position. Planners read this to add a
	// field-level FK to its concrete host table rather than re-deriving the
	// table from a Go struct name — which breaks for FK names shared across
	// the many tables that embed an inline-relation mixin (issue #197).
	ConstraintsAddedWithTables []ConstraintAdditionInfo `json:"constraints_added_with_tables"`

	// ConstraintsRemoved contains names of constraints that exist in the current database
	// but not in the target schema (potentially dangerous - may affect data integrity)
	ConstraintsRemoved []string `json:"constraints_removed"`

	// ConstraintsRemovedWithTables contains detailed information about constraints that
	// need to be removed, including the constraint name, owning table, and type. This is
	// used by database dialects that require the table name and a type-specific drop
	// syntax (e.g. MySQL/MariaDB FOREIGN KEY constraints use DROP FOREIGN KEY). It is
	// NOT index-aligned with ConstraintsRemoved — each list is sorted independently
	// (ConstraintsRemoved by name, this one by table then name), so consumers must
	// correlate entries by constraint name, never by position.
	ConstraintsRemovedWithTables []ConstraintRemovalInfo `json:"constraints_removed_with_tables"`

	// ForeignKeysRemovedWithTables carries the local and referenced column
	// definitions needed to order foreign-key drops before column removals. It
	// is a table-qualified subset of ConstraintsRemovedWithTables, correlated by
	// table and constraint name rather than slice position. It is supplemental:
	// an entry without a matching ConstraintsRemovedWithTables value is ignored
	// and does not independently make the diff non-empty.
	ForeignKeysRemovedWithTables []ForeignKeyRemovalInfo `json:"foreign_keys_removed_with_tables"`
}

// EffectiveIdentifierSemantics returns live semantics stored on the diff, or
// conservative offline rules for dialect when the diff has no live metadata.
func (d *SchemaDiff) EffectiveIdentifierSemantics(dialect string) identifier.Semantics {
	if d != nil && d.IdentifierSemantics != nil {
		return d.IdentifierSemantics.Normalize(dialect)
	}
	return identifier.ForDialect(dialect)
}

// HasChanges returns true if the diff contains any schema changes requiring migration.
//
// This method provides a quick way to determine if any migration actions are needed
// without having to check each individual diff category. It's commonly used in
// CI/CD pipelines and automated deployment systems to decide whether to generate
// and apply migrations.
//
// # Return Value
//
// Returns true if any of the following conditions are met:
//   - New tables need to be created
//   - Existing tables need to be removed
//   - Existing tables have structural modifications
//   - New enum types need to be created
//   - Existing enum types need to be removed
//   - Existing enum types have value modifications
//   - New indexes need to be created
//   - Existing indexes need to be removed
//
// # Example Usage
//
//	diff := CompareSchemas(generated, database)
//	if diff.HasChanges() {
//		log.Println("Schema changes detected, generating migration...")
//		statements, err := planner.GenerateSchemaDiffAST(diff, generated, "postgres")
//		if err != nil {
//			return err
//		}
//		// Apply migration statements...
//	} else {
//		log.Println("No schema changes detected")
//	}
func (d *SchemaDiff) HasChanges() bool {
	return d.hasTableChanges() ||
		d.hasEnumChanges() ||
		d.hasIndexChanges() ||
		d.hasExtensionChanges() ||
		d.hasFunctionChanges() ||
		d.hasSequenceChanges() ||
		d.hasUserTypeChanges() ||
		d.hasViewChanges() ||
		d.hasMaterializedViewChanges() ||
		d.hasTriggerChanges() ||
		d.hasRLSChanges() ||
		d.hasRoleChanges() ||
		d.hasConstraintChanges()
}

// hasTableChanges returns true if there are any table-related changes
func (d *SchemaDiff) hasTableChanges() bool {
	return len(d.TablesAdded) > 0 ||
		len(d.TablesRemoved) > 0 ||
		len(d.TablesModified) > 0
}

// hasEnumChanges returns true if there are any enum-related changes
func (d *SchemaDiff) hasEnumChanges() bool {
	return len(d.EnumsAdded) > 0 ||
		len(d.EnumsRemoved) > 0 ||
		len(d.EnumsModified) > 0
}

// hasIndexChanges returns true if there are any index-related changes
func (d *SchemaDiff) hasIndexChanges() bool {
	return len(d.IndexesAdded) > 0 ||
		len(d.IndexesRemoved) > 0
}

// IndexAdditions returns a copy of the added index references.
func (d *SchemaDiff) IndexAdditions() []IndexRef {
	return slices.Clone(d.IndexesAdded)
}

// IndexRemovals returns a copy of the removed index references.
func (d *SchemaDiff) IndexRemovals() []IndexRef {
	return slices.Clone(d.IndexesRemoved)
}

// SetIndexAdditions replaces the added index references with a sorted copy.
func (d *SchemaDiff) SetIndexAdditions(refs []IndexRef) {
	d.IndexesAdded = sortedIndexRefs(refs)
}

// SetIndexRemovals replaces the removed index references with a sorted copy.
func (d *SchemaDiff) SetIndexRemovals(refs []IndexRef) {
	d.IndexesRemoved = sortedIndexRefs(refs)
}

// SetConstraintBackedIndexRemovals replaces the constraint-backed index
// removals with a sorted copy, in the same key order as SetIndexRemovals.
func (d *SchemaDiff) SetConstraintBackedIndexRemovals(refs []IndexRef) {
	d.ConstraintBackedIndexRemovals = sortedIndexRefs(refs)
}

// ConstraintBackedIndexRemovalSet keys ConstraintBackedIndexRemovals for the
// membership test a planner makes once per rendered drop. The references are
// the ones recorded in IndexesRemoved, so an exact match is the right test:
// both come from the same introspected index.
func (d *SchemaDiff) ConstraintBackedIndexRemovalSet() map[IndexRef]struct{} {
	set := make(map[IndexRef]struct{}, len(d.ConstraintBackedIndexRemovals))
	for _, ref := range d.ConstraintBackedIndexRemovals {
		set[ref] = struct{}{}
	}
	return set
}

// IndexRemovalsRebuiltAsUniqueConstraints keys the index removals whose object
// a UNIQUE constraint addition in the same plan puts back.
//
// This is the shape a rollback of a constraint-backed index replacement has:
// the up direction turned a UNIQUE constraint into a plain index, so the down
// direction drops that index and adds the constraint again — and ADD CONSTRAINT
// ... UNIQUE builds an index of the constraint's name, so the two collide on
// one name. PostgreSQL 17.10 answers the add with
// `relation "uq_users_email" already exists (SQLSTATE 42P07)` and MySQL 9.7.1
// with `Error 1061 (42000): Duplicate key name 'uq_users_email'` unless the
// drop runs first, and the planners emit constraint additions before index
// removals. A planner therefore emits the drop with the addition and skips it
// where it would otherwise have landed, which is what this set is for.
//
// The match is exact rather than semantics-folded: both sides name the same
// introspected object, recorded from one IndexRef by the reversal that built
// the addition.
func (d *SchemaDiff) IndexRemovalsRebuiltAsUniqueConstraints() map[IndexRef]struct{} {
	if len(d.ConstraintsAddedWithTables) == 0 || len(d.IndexesRemoved) == 0 {
		return nil
	}
	additions := make(map[IndexRef]struct{}, len(d.ConstraintsAddedWithTables))
	for _, add := range d.ConstraintsAddedWithTables {
		if add.Type != "UNIQUE" || add.TableName == "" {
			continue
		}
		additions[IndexRef{Name: add.Name, TableName: add.TableName}] = struct{}{}
	}
	set := make(map[IndexRef]struct{}, len(additions))
	for _, ref := range d.IndexesRemoved {
		if _, rebuilt := additions[ref]; rebuilt {
			set[ref] = struct{}{}
		}
	}
	return set
}

func sortedIndexRefs(refs []IndexRef) []IndexRef {
	if len(refs) == 0 {
		return nil
	}
	sorted := slices.Clone(refs)
	slices.SortFunc(sorted, func(a, b IndexRef) int {
		if byTable := strings.Compare(a.TableName, b.TableName); byTable != 0 {
			return byTable
		}
		return strings.Compare(a.Name, b.Name)
	})
	return sorted
}

// hasExtensionChanges returns true if there are any extension-related changes
func (d *SchemaDiff) hasExtensionChanges() bool {
	return len(d.ExtensionsAdded) > 0 ||
		len(d.ExtensionsRemoved) > 0 ||
		len(d.ExtensionsModified) > 0
}

// hasFunctionChanges returns true if there are any function-related changes
func (d *SchemaDiff) hasFunctionChanges() bool {
	return len(d.FunctionsAdded) > 0 ||
		len(d.FunctionsRemoved) > 0 ||
		len(d.FunctionsModified) > 0
}

// hasSequenceChanges returns true if there are any sequence-related changes
func (d *SchemaDiff) hasSequenceChanges() bool {
	return len(d.SequencesAdded) > 0 ||
		len(d.SequencesRemoved) > 0 ||
		len(d.SequencesModified) > 0
}

// hasUserTypeChanges returns true if there are any domain/composite/range changes.
func (d *SchemaDiff) hasUserTypeChanges() bool {
	return len(d.DomainsAdded) > 0 || len(d.DomainsRemoved) > 0 || len(d.DomainsModified) > 0 ||
		len(d.CompositeTypesAdded) > 0 || len(d.CompositeTypesRemoved) > 0 || len(d.CompositeTypesModified) > 0 ||
		len(d.RangesAdded) > 0 || len(d.RangesRemoved) > 0 || len(d.RangesModified) > 0
}

func (d *SchemaDiff) hasViewChanges() bool {
	return len(d.ViewsAdded) > 0 ||
		len(d.ViewsRemoved) > 0 ||
		len(d.ViewsModified) > 0
}

func (d *SchemaDiff) hasMaterializedViewChanges() bool {
	return len(d.MaterializedViewsAdded) > 0 ||
		len(d.MaterializedViewsRemoved) > 0 ||
		len(d.MaterializedViewsModified) > 0
}

func (d *SchemaDiff) hasTriggerChanges() bool {
	return len(d.TriggersAdded) > 0 ||
		len(d.TriggersRemoved) > 0 ||
		len(d.TriggersModified) > 0
}

// hasRLSChanges returns true if there are any RLS-related changes
func (d *SchemaDiff) hasRLSChanges() bool {
	return len(d.RLSPoliciesAdded) > 0 ||
		len(d.RLSPoliciesRemoved) > 0 ||
		len(d.RLSPoliciesModified) > 0 ||
		len(d.RLSEnabledTablesAdded) > 0 ||
		len(d.RLSEnabledTablesRemoved) > 0
}

// hasRoleChanges returns true if there are any role-related changes
func (d *SchemaDiff) hasRoleChanges() bool {
	return len(d.RolesAdded) > 0 ||
		len(d.RolesRemoved) > 0 ||
		len(d.RolesModified) > 0 ||
		len(d.GrantsAdded) > 0 ||
		len(d.GrantsRemoved) > 0 ||
		len(d.GrantOptionsAdded) > 0 ||
		len(d.GrantOptionsRevoked) > 0
}

// hasConstraintChanges returns true if there are any constraint-related changes.
//
// The table-qualified lists are consulted as well as the bare name lists. The
// comparator fills both halves together, but a caller that builds a diff from
// the table-qualified halves alone — a planner test, a policy filter that
// rewrites one list — would otherwise hold a diff that carries constraints and
// answers false to HasChanges, and every check built on HasChanges would report
// a synced schema.
func (d *SchemaDiff) hasConstraintChanges() bool {
	return len(d.ConstraintsAdded) > 0 ||
		len(d.ConstraintsRemoved) > 0 ||
		len(d.ConstraintsAddedWithTables) > 0 ||
		len(d.ConstraintsRemovedWithTables) > 0
}

// TableDiff represents structural differences within a specific database table.
//
// This structure captures all types of changes that can occur to a table's structure,
// including column additions, removals, and modifications. It provides detailed
// information needed to generate appropriate ALTER TABLE statements.
//
// # Example Usage
//
//	tableDiff := TableDiff{
//		TableName: "users",
//		ColumnsAdded: []string{"email", "created_at"},
//		ColumnsRemoved: []string{"legacy_field"},
//		ColumnsModified: []ColumnDiff{
//			{ColumnName: "name", Changes: map[string]string{"type": "VARCHAR(100) -> VARCHAR(255)"}},
//		},
//	}
type TableDiff struct {
	// TableName is the name of the table being modified
	TableName string `json:"table_name"`

	// ColumnsAdded contains names of columns that need to be added to the table
	ColumnsAdded []string `json:"columns_added"`

	// ColumnsRemoved contains names of columns that need to be removed from the table
	// (potentially dangerous - may cause data loss)
	ColumnsRemoved []string `json:"columns_removed"`

	// ColumnsModified contains detailed information about columns that exist in both
	// schemas but have different properties (type, constraints, defaults, etc.)
	ColumnsModified []ColumnDiff `json:"columns_modified"`

	// ConstraintsAdded contains names of constraints that need to be added to the table
	ConstraintsAdded []string `json:"constraints_added"`

	// ConstraintsRemoved contains names of constraints that need to be removed from the table
	// (potentially dangerous - may affect data integrity)
	ConstraintsRemoved []string `json:"constraints_removed"`
}

// ColumnDiff represents specific property changes within a database column.
//
// This structure captures the detailed differences between the current column
// definition and the target column definition. Each change is represented as
// a key-value pair showing the transition from old value to new value.
//
// # Change Types
//
// Common change types include:
//   - "type": Data type changes (e.g., "VARCHAR(100) -> VARCHAR(255)")
//   - "nullable": Nullability changes (e.g., "true -> false")
//   - "primary_key": Primary key constraint changes (e.g., "false -> true")
//   - "unique": Unique constraint changes (e.g., "false -> true")
//   - "default": Default value changes (e.g., "'old' -> 'new'")
//
// # Example Usage
//
//	columnDiff := ColumnDiff{
//		ColumnName: "email",
//		Changes: map[string]string{
//			"type": "VARCHAR(100) -> VARCHAR(255)",
//			"nullable": "true -> false",
//			"unique": "false -> true",
//		},
//	}
type ColumnDiff struct {
	// ColumnName is the name of the column being modified
	ColumnName string `json:"column_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// EnumDiff represents changes to enum type values.
//
// This structure captures modifications to enum types, specifically the addition
// and removal of enum values. It's important to note that not all databases
// support enum value removal without recreating the entire enum type.
//
// # Database Limitations
//
//   - **PostgreSQL**: Supports adding enum values but not removing them without recreating the enum
//   - **MySQL/MariaDB**: Supports both adding and removing enum values with ALTER TABLE
//   - **SQLite**: No native enum support - uses CHECK constraints
//
// # Example Usage
//
//	enumDiff := EnumDiff{
//		EnumName: "status_type",
//		ValuesAdded: []string{"pending", "archived"},
//		ValuesRemoved: []string{"deprecated"},
//	}
type EnumDiff struct {
	// EnumName is the name of the enum type being modified
	EnumName string `json:"enum_name"`

	// ValuesAdded contains enum values that need to be added to the enum type
	ValuesAdded []string `json:"values_added"`

	// ValuesRemoved contains enum values that need to be removed from the enum type
	// (may not be supported by all databases - see database limitations above)
	ValuesRemoved []string `json:"values_removed"`
}

// FunctionDiff represents changes to PostgreSQL function definitions.
//
// This structure captures modifications to function definitions, including changes
// to parameters, return types, function body, and function attributes like security
// and volatility. Function modifications typically require dropping and recreating
// the function in PostgreSQL.
//
// # Function Change Types
//
// Common function changes include:
//   - **Parameters**: Changes to function parameter list
//   - **Returns**: Changes to return type
//   - **Body**: Changes to function implementation
//   - **Language**: Changes to function language (rare)
//   - **Security**: Changes between DEFINER and INVOKER
//   - **Volatility**: Changes between STABLE, IMMUTABLE, and VOLATILE
//
// # Example Usage
//
//	functionDiff := FunctionDiff{
//		FunctionName: "get_user_count",
//		Changes: map[string]string{
//			"parameters": "() -> (tenant_id TEXT)",
//			"body": "SELECT COUNT(*) FROM users -> SELECT COUNT(*) FROM users WHERE tenant_id = tenant_id_param",
//			"volatility": "VOLATILE -> STABLE",
//		},
//	}
type FunctionDiff struct {
	// FunctionName is the name of the function being modified
	FunctionName string `json:"function_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// DomainDiff represents changes to a PostgreSQL domain type.
//
// Changes is the human-readable "old -> new" payload. CurrentBaseType is the
// from-side of the same comparison kept as a type spelling rather than prose,
// because a plan that drops this domain has to be ordered by the shape the
// database holds now. See the comment on CurrentBaseType.
type DomainDiff struct {
	DomainName string            `json:"domain_name"`
	Changes    map[string]string `json:"changes"`

	// CurrentBaseType is the base type the domain has in the database being
	// compared against, in that catalog's own spelling -- the from-side of the
	// "type" entry in Changes, structurally rather than as prose.
	//
	// A modification is reconciled as a non-CASCADE DROP followed by a CREATE.
	// The CREATE belongs to the desired definitions, but the DROP executes
	// against the database as it stands, so only the references it holds now
	// can block it, and those are the ones recorded here. The two sides
	// disagree exactly when the modification is what moved a reference.
	//
	// Empty when the caller built the diff by hand or the domain's from-side is
	// unknown; the drop ordering then falls back to declaration order.
	CurrentBaseType string `json:"current_base_type,omitempty"`
}

// RangeDiff represents changes to an existing PostgreSQL range type.
type RangeDiff struct {
	RangeName string            `json:"range_name"`
	Changes   map[string]string `json:"changes"`

	// CurrentSubtype is the subtype the range has in the database being compared
	// against, in that catalog's own spelling. It is the from-side used to order
	// the non-CASCADE DROP, for the same reason DomainDiff.CurrentBaseType is:
	// the DROP runs against the database as it stands, so only the references
	// that database holds now can block it.
	//
	// Empty when the caller built the diff by hand; the drop ordering then falls
	// back to declaration order.
	CurrentSubtype string `json:"current_subtype,omitempty"`
}

// CompositeTypeDiff represents changes to a PostgreSQL composite type.
//
// Changes is the human-readable "old -> new" payload. CurrentFieldTypes is the
// from-side of the same comparison kept as type spellings, for the reason given
// on DomainDiff.CurrentBaseType.
type CompositeTypeDiff struct {
	TypeName string            `json:"type_name"`
	Changes  map[string]string `json:"changes"`

	// CurrentFieldTypes are the field types the composite has in the database
	// being compared against, in that catalog's own spellings and in field
	// order. They order the non-CASCADE DROP the recreate path emits, which
	// runs against the current shape rather than the desired one.
	//
	// Empty when the caller built the diff by hand or the type's from-side is
	// unknown; the drop ordering then falls back to declaration order.
	CurrentFieldTypes []string `json:"current_field_types,omitempty"`
}

// SequenceDiff represents changes to a standalone sequence definition.
type SequenceDiff struct {
	// SequenceName is the (optionally schema-qualified) name of the sequence.
	SequenceName string `json:"sequence_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// ExtensionDiff represents a PostgreSQL extension installation-schema change.
type ExtensionDiff struct {
	Name       string `json:"name"`
	FromSchema string `json:"from_schema"`
	ToSchema   string `json:"to_schema"`
}

// ViewDiff represents changes to a view definition.
type ViewDiff struct {
	ViewName string            `json:"view_name"`
	Changes  map[string]string `json:"changes"`

	// PreviousBody is the view body that is in force before this diff is
	// applied: the database side of a forward comparison, and the post-up side
	// of a reversed one. It is what makes the modification path decidable.
	//
	// PostgreSQL accepts CREATE OR REPLACE VIEW only when the new query yields
	// the old column list with columns appended to the end -- same names, same
	// types, same order. Dropping, renaming or retyping a column is refused at
	// execution time, and a down migration built with CREATE OR REPLACE is
	// therefore un-appliable for every view modification except the one shape
	// the up migration is least likely to have made. A planner that cannot see
	// the prior body cannot tell the legal case from the refused one, so it has
	// to choose between an un-appliable statement and always dropping the view
	// with CASCADE, which takes dependents with it.
	//
	// An empty value means "not known". It does not on its own select a plan:
	// what a planner cannot decide is settled by Rollback below, so a forward
	// plan still attempts the replace -- the engine refuses it if it is illegal,
	// and refusing costs nothing -- while a rollback takes the drop and
	// recreate that always applies.
	PreviousBody string `json:"previous_body,omitempty"`

	// Rollback reports that this entry is being planned in the DOWN direction:
	// the statement rendered for it runs while an operator is undoing a
	// migration.
	//
	// It decides nothing on its own. It settles the cases where a planner cannot
	// prove whether the engine accepts an in-place replace, and the two
	// directions want opposite answers there. Going forward, the replace is
	// worth attempting: it keeps dependent objects and the privileges granted on
	// the view, and if the engine refuses it the migration stops with nothing
	// destroyed. A rollback cannot be stopped that way -- it is already running
	// during the incident -- so it takes the drop-and-recreate that always
	// applies, and pays for it by rebuilding what CASCADE removes.
	//
	// Reverse plan builders set it; a forward comparison leaves it false.
	Rollback bool `json:"rollback,omitempty"`
}

// MaterializedViewDiff represents changes to a materialized view definition.
type MaterializedViewDiff struct {
	ViewName string            `json:"view_name"`
	Changes  map[string]string `json:"changes"`
}

// TriggerRef identifies a trigger by table and trigger name.
type TriggerRef struct {
	TriggerName string `json:"trigger_name"`
	TableName   string `json:"table_name"`
}

// TriggerDiff represents changes to a trigger definition.
type TriggerDiff struct {
	TriggerName string            `json:"trigger_name"`
	TableName   string            `json:"table_name"`
	Changes     map[string]string `json:"changes"`
}

// RLSPolicyRef represents a reference to an RLS policy with its table information.
//
// This structure identifies an RLS policy on both sides of a diff -- policies
// added as well as policies dropped. The table is not decoration: a PostgreSQL
// policy name is scoped to its table, so two tables in one schema may each carry
// a policy called "tenant_isolation", and both CREATE POLICY and DROP POLICY
// need the table to say which one is meant.
//
// # Example Usage
//
//	policyRef := RLSPolicyRef{
//		PolicyName: "user_tenant_isolation",
//		TableName: "users",
//	}
type RLSPolicyRef struct {
	// PolicyName is the name of the RLS policy
	PolicyName string `json:"policy_name"`

	// TableName is the name of the table the policy applies to
	TableName string `json:"table_name"`
}

// RLSPolicyDiff represents changes to Row-Level Security policy definitions.
//
// This structure captures modifications to RLS policies, including changes to
// policy expressions, target roles, and policy types. RLS policy modifications
// typically require dropping and recreating the policy in PostgreSQL.
//
// # Policy Change Types
//
// Common policy changes include:
//   - **PolicyFor**: Changes to policy type (SELECT, INSERT, UPDATE, DELETE, ALL)
//   - **ToRoles**: Changes to target database roles
//   - **UsingExpression**: Changes to USING clause expression
//   - **WithCheckExpression**: Changes to WITH CHECK clause expression
//
// # Example Usage
//
//	policyDiff := RLSPolicyDiff{
//		PolicyName: "user_tenant_isolation",
//		TableName: "users",
//		Changes: map[string]string{
//			"using_expression": "tenant_id = current_user_id() -> tenant_id = get_current_tenant_id()",
//			"to_roles": "app_user -> app_user,admin_user",
//		},
//	}
type RLSPolicyDiff struct {
	// PolicyName is the name of the RLS policy being modified
	PolicyName string `json:"policy_name"`

	// TableName is the name of the table the policy applies to
	TableName string `json:"table_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// RoleDiff represents changes to PostgreSQL role definitions.
//
// This structure captures modifications to role definitions, including changes
// to role attributes such as login capabilities, passwords, privileges, and other
// role properties. Role modifications typically require ALTER ROLE statements in PostgreSQL.
//
// # Role Change Types
//
// Common role changes include:
//   - **Login**: Changes to login capability (true -> false or false -> true)
//   - **Password**: Changes to role password (encrypted)
//   - **Superuser**: Changes to superuser status (true -> false or false -> true)
//   - **CreateDB**: Changes to database creation capability
//   - **CreateRole**: Changes to role creation capability
//   - **Inherit**: Changes to privilege inheritance
//   - **Replication**: Changes to replication capability
//
// # Example Usage
//
//	roleDiff := RoleDiff{
//		RoleName: "app_user",
//		Changes: map[string]string{
//			"login": "false -> true",
//			"password": "old_encrypted_password -> new_encrypted_password",
//			"createdb": "false -> true",
//		},
//	}
type RoleDiff struct {
	// RoleName is the name of the role being modified
	RoleName string `json:"role_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`
}

// GrantRef identifies one PostgreSQL privilege grant.
type GrantRef struct {
	// Role is the role receiving or losing the privilege.
	Role string `json:"role"`

	// Privilege is the individual privilege, e.g. SELECT, INSERT, or USAGE.
	Privilege string `json:"privilege"`

	// ObjectType is the target kind: TABLE, SCHEMA, or SEQUENCE.
	ObjectType string `json:"object_type"`

	// ObjectName is the target table or schema name.
	ObjectName string `json:"object_name"`

	// WithOption records whether the grant has WITH GRANT OPTION.
	WithOption bool `json:"with_option"`
}
