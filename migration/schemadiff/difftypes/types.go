// Package difftypes defines the schema difference model (SchemaDiff and the
// per-object diffs for tables, columns, enums, functions, triggers, RLS
// policies, roles, and grants) produced by schemadiff and consumed by the
// migration planner.
//
// The name is the one its callers already used. Before the package carried it,
// `types` denoted two different packages in adjacent pipeline stages -- this one
// and catalog -- and one package could not keep its own spelling
// straight: schemadiff.go imported catalog bare while database.go beside
// it aliased the same import. Across the tree the alias outnumbered the real
// name 36 to 28, so most call sites were already spelled this way
// (stokaro/ptah#2246 section 2.1).
package difftypes

import (
	"encoding/json"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/deporder"
)

// ViewChanges is a set of views one change applies to, carrying each one's
// body and not only its name.
//
// The seventh family off `[]string` under stokaro/ptah#2315, and the first
// whose name is ALREADY the identity: schemamodel.View has no Schema field
// because the parser folds a declared schema into the name, so the carried
// view's Name is the qualified spelling the name list held.
//
// Its rule is the check option. The catalog reports a word -- NONE, LOCAL,
// CASCADED, or an equivalent a dialect chose -- where the model has a bool, and
// the read is routed by [sqlutil.CheckOptionRequestsCheck] so the comparison
// and the conversion cannot answer it differently.
//
// See [RangeChanges] for why both sides carry the operand and why the wire
// shape does not change.
type ViewChanges []schemamodel.View

// MarshalJSON writes the names alone, the shape `views_added` and
// `views_removed` have always had.
func (v ViewChanges) MarshalJSON() ([]byte, error) {
	if v == nil {
		return []byte("null"), nil
	}
	return json.Marshal(v.Names())
}

// Names is the view names this change applies to.
func (v ViewChanges) Names() []string {
	if v == nil {
		return nil
	}
	names := make([]string, 0, len(v))
	for _, view := range v {
		names = append(names, view.Name)
	}
	return names
}

// MaterializedViewChanges is a set of materialized views one change applies to,
// carrying each one's body and not only its name.
//
// The eighth family off `[]string` under stokaro/ptah#2315, and the plainest
// carry of the eight: catalog.MaterializedView and schemamodel.MaterializedView
// hold the same fields, including the refresh schedule, which is the ast type
// on both sides rather than a copy of it.
//
// See [ViewChanges] for why the name is the identity here, and [RangeChanges]
// for why the wire shape does not change.
type MaterializedViewChanges []schemamodel.MaterializedView

// MarshalJSON writes the names alone, the shape `materialized_views_added` and
// `materialized_views_removed` have always had.
func (m MaterializedViewChanges) MarshalJSON() ([]byte, error) {
	if m == nil {
		return []byte("null"), nil
	}
	return json.Marshal(m.Names())
}

// Names is the materialized view names this change applies to.
func (m MaterializedViewChanges) Names() []string {
	if m == nil {
		return nil
	}
	names := make([]string, 0, len(m))
	for _, view := range m {
		names = append(names, view.Name)
	}
	return names
}

// SynonymChanges is a set of synonyms one change applies to, carrying each
// one's target and not only its name.
//
// The ninth family off `[]string` under stokaro/ptah#2315. A synonym IS its
// target -- there is nothing else to it -- so a change that carried only the
// name carried the half that does not say what the statement should do.
//
// Its rule is that target's spelling. A catalog records base_object_name with
// the server's own bracket quoting and the parsed parts beside it, and a
// declaration writes one to four unquoted dot-separated parts;
// [catalog.Synonym.DeclaredTarget] is the one answer both the conversion and
// this comparison take.
//
// See [RangeChanges] for why both sides carry the operand and why the wire
// shape does not change.
type SynonymChanges []schemamodel.Synonym

// MarshalJSON writes the names alone, the shape `synonyms_added` and
// `synonyms_removed` have always had.
func (s SynonymChanges) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("null"), nil
	}
	return json.Marshal(s.Names())
}

// Names is the synonym names this change applies to.
func (s SynonymChanges) Names() []string {
	if s == nil {
		return nil
	}
	names := make([]string, 0, len(s))
	for _, synonym := range s {
		names = append(names, synonym.QualifiedName())
	}
	return names
}

// HypertableChanges is a set of hypertables one change applies to, carrying
// each one's partitioning and not only its table name.
//
// The tenth family off `[]string` under stokaro/ptah#2315. `create_hypertable`
// takes the column to partition on and the chunk interval, neither of which a
// table name carries.
//
// See [RangeChanges] for why both sides carry the operand and why the wire
// shape does not change.
type HypertableChanges []schemamodel.Hypertable

// MarshalJSON writes the table names alone, the shape `hypertables_added` and
// `hypertables_removed` have always had.
func (h HypertableChanges) MarshalJSON() ([]byte, error) {
	if h == nil {
		return []byte("null"), nil
	}
	return json.Marshal(h.Names())
}

// Names is the tables this change partitions or stops partitioning.
func (h HypertableChanges) Names() []string {
	if h == nil {
		return nil
	}
	names := make([]string, 0, len(h))
	for _, hypertable := range h {
		names = append(names, hypertable.Table)
	}
	return names
}

// ContinuousAggregateChanges is a set of continuous aggregates one change
// applies to, carrying each one's body and not only its name.
//
// The eleventh family off `[]string` under stokaro/ptah#2315, and the plain
// twin of [HypertableChanges]: an aggregate is a materialized view over a
// hypertable, so the body is the statement.
//
// See [RangeChanges] for why both sides carry the operand and why the wire
// shape does not change.
type ContinuousAggregateChanges []schemamodel.ContinuousAggregate

// MarshalJSON writes the names alone, the shape `continuous_aggregates_added`
// and `continuous_aggregates_removed` have always had.
func (a ContinuousAggregateChanges) MarshalJSON() ([]byte, error) {
	if a == nil {
		return []byte("null"), nil
	}
	return json.Marshal(a.Names())
}

// Names is the aggregate names this change applies to.
func (a ContinuousAggregateChanges) Names() []string {
	if a == nil {
		return nil
	}
	names := make([]string, 0, len(a))
	for _, aggregate := range a {
		names = append(names, aggregate.QualifiedName())
	}
	return names
}

// ColumnChanges is a set of columns one table change applies to, carrying each
// one's definition and not only its name.
//
// The twelfth family off `[]string` under stokaro/ptah#2315, and the one a name
// cost the most. `ALTER TABLE ... ADD COLUMN` needs the type, the nullability,
// the default and everything else a column declares, and a planner handed a
// name recovered them by finding the table's Go STRUCT name and then scanning
// every field in the schema for one matching both -- a parser artifact reaching
// into the planner, and a silent no-op when either half of the match failed.
//
// Both sides carry, and they have to: `reverseTableDiffs` swaps the added and
// removed lists to build a down migration, so a rollback that restores a
// dropped column reads its definition out of what the REMOVAL carried. A
// removed column is absent from the desired schema by definition, so the
// comparison describes it from the catalog through
// [go.5x5.cz/ptah/internal/catalogfield.Field].
//
// A removal carries the column and NOT its keys. See [TableDiff.ColumnsRemoved].
//
// See [RangeChanges] for why the wire shape does not change.
type ColumnChanges []schemamodel.Field

// MarshalJSON writes the names alone, the shape `columns_added` and
// `columns_removed` have always had.
func (c ColumnChanges) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte("null"), nil
	}
	return json.Marshal(c.Names())
}

// Names is the column names this change applies to.
func (c ColumnChanges) Names() []string {
	if c == nil {
		return nil
	}
	names := make([]string, 0, len(c))
	for _, column := range c {
		names = append(names, column.Name)
	}
	return names
}

// RoleChanges is a set of roles one change applies to, carrying each one's
// attributes and not only its name.
//
// The thirteenth family off `[]string` under stokaro/ptah#2315. `CREATE ROLE`
// takes LOGIN, SUPERUSER, CREATEDB, CREATEROLE, INHERIT and REPLICATION, none
// of which a name carries, and both planners that emit one recovered them by
// scanning the desired schema for a role of that name.
//
// Removals carry too, because `reverseSchemaDiff` swaps the two lists to build
// a down migration and they have to be one type. The comparison itself never
// fills the removal list -- see [compare.Roles], which declines to plan a
// `DROP ROLE` nobody asked for -- so a removal reaching a planner came from
// that reversal, where it started as an addition and carried its attributes.
//
// The wire is unchanged, and here that is a property worth naming rather than
// a compatibility note: [schemamodel.Role] carries a Password, and the name
// list this marshals to does not. A role's password does not reach a migration
// document.
type RoleChanges []schemamodel.Role

// MarshalJSON writes the names alone, the shape `roles_added` and
// `roles_removed` have always had. A role's attributes -- its password among
// them -- stay in memory.
func (r RoleChanges) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return json.Marshal(r.Names())
}

// Names is the role names this change applies to.
func (r RoleChanges) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r))
	for _, role := range r {
		names = append(names, role.Name)
	}
	return names
}

// RoutineChange is one routine a change applies to: its declaration, and the
// argument list a DROP addresses it by.
//
// The two are separate because they are different facts. `Parameters` is what
// the author wrote -- names, defaults, OUT arguments and all -- and `Signature`
// is what identifies an overload to `DROP FUNCTION`. PostgreSQL answers a bare
// name with `function name "f" is not unique` whenever a schema holds more than
// one, and `IF EXISTS` does not help, because the refusal is about ambiguity
// rather than existence (stokaro/ptah#2296).
//
// A reader that had only the declaration would have to guess between them, and
// [go.5x5.cz/ptah/catalog.Function.IdentityArguments] says why guessing is
// wrong: the two differ exactly where an author wrote a name or a default.
type RoutineChange struct {
	schemamodel.Function

	// Signature is the argument list this routine is addressed by, empty when
	// the routine is not overloaded and the reader supplied none.
	Signature string
}

// FunctionChanges is a set of routines one change applies to, carrying each
// one's declaration and drop identity and not only its name.
//
// The fourteenth family off `[]string` under stokaro/ptah#2315, and the one
// that retires a parallel field rather than only a lookup:
// `FunctionsRemovedWithSignatures` existed because a name could not carry the
// drop identity, which is this issue's problem solved once by hand.
//
// See [RangeChanges] for why both sides carry and why the wire shape does not
// change.
type FunctionChanges []RoutineChange

// MarshalJSON writes the names alone, the shape `functions_added` and
// `functions_removed` have always had.
func (f FunctionChanges) MarshalJSON() ([]byte, error) {
	if f == nil {
		return []byte("null"), nil
	}
	return json.Marshal(f.Names())
}

// Names is the routine names this change applies to.
func (f FunctionChanges) Names() []string {
	if f == nil {
		return nil
	}
	names := make([]string, 0, len(f))
	for _, routine := range f {
		names = append(names, routine.Name)
	}
	return names
}

// Declarations is the routines this change carries, for a consumer that wants
// the declaration rather than the change around it.
func (f FunctionChanges) Declarations() []schemamodel.Function {
	if f == nil {
		return nil
	}
	declarations := make([]schemamodel.Function, 0, len(f))
	for _, routine := range f {
		declarations = append(declarations, routine.Function)
	}
	return declarations
}

// Removals describes these routines the way a planner addresses a DROP.
func (f FunctionChanges) Removals() []RoutineRemoval {
	if f == nil {
		return nil
	}
	removals := make([]RoutineRemoval, 0, len(f))
	for _, routine := range f {
		removals = append(removals, RoutineRemoval{Name: routine.Name, Signature: routine.Signature})
	}
	return removals
}

// DomainChanges is a set of domain types one change applies to, carrying each
// one's definition and not only its name.
//
// The sixth family off `[]string` under stokaro/ptah#2315, and the first whose
// carry needed a RULE rather than a transcription: a catalog reports one
// `Default` string for what the model splits into a literal value and an
// expression. That rule now lives in [sqlutil.DefaultLooksLikeExpression],
// where the column path and this one both reach it, rather than inside
// internal/convert -- the package item 3 retires.
//
// See [RangeChanges] for why both sides carry the operand and why the wire
// shape does not change.
type DomainChanges []schemamodel.Domain

// MarshalJSON writes the names alone, the shape `domains_added` and
// `domains_removed` have always had.
func (d DomainChanges) MarshalJSON() ([]byte, error) {
	if d == nil {
		return []byte("null"), nil
	}
	return json.Marshal(d.Names())
}

// Names is the domain names this change applies to.
func (d DomainChanges) Names() []string {
	if d == nil {
		return nil
	}
	names := make([]string, 0, len(d))
	for _, domain := range d {
		names = append(names, domain.QualifiedName())
	}
	return names
}

// EnumChanges is a set of enum types one change applies to, carrying each one's
// values and not only its name.
//
// The fifth family off `[]string` under stokaro/ptah#2315, and the cleanest
// carry of them: catalog.Enum and schemamodel.Enum declare the same three
// properties -- name, schema and ordered values -- with nothing on either side
// the other cannot hold. See [RangeChanges] for why both sides carry the
// operand and why the wire shape does not change.
type EnumChanges []schemamodel.Enum

// MarshalJSON writes the names alone, the shape `enums_added` and
// `enums_removed` have always had.
func (e EnumChanges) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	return json.Marshal(e.Names())
}

// Names is the enum names this change applies to.
func (e EnumChanges) Names() []string {
	if e == nil {
		return nil
	}
	names := make([]string, 0, len(e))
	for _, enum := range e {
		names = append(names, enum.QualifiedName())
	}
	return names
}

// ExtensionChanges is a set of extensions one change applies to, carrying each
// one's declaration and not only its name.
//
// The fourth family off `[]string` under stokaro/ptah#2315. See [RangeChanges]
// for why both sides carry the operand and why the wire shape does not change.
//
// An extension is named globally rather than per schema, so the name here is
// the bare one, which is what the comparator has always keyed on.
type ExtensionChanges []schemamodel.Extension

// MarshalJSON writes the names alone, the shape `extensions_added` and
// `extensions_removed` have always had.
func (e ExtensionChanges) MarshalJSON() ([]byte, error) {
	if e == nil {
		return []byte("null"), nil
	}
	return json.Marshal(e.Names())
}

// Names is the extension names this change applies to.
func (e ExtensionChanges) Names() []string {
	if e == nil {
		return nil
	}
	names := make([]string, 0, len(e))
	for _, extension := range e {
		names = append(names, extension.Name)
	}
	return names
}

// SequenceChanges is a set of sequences one change applies to, carrying each
// one's definition and not only its name.
//
// The third family off `[]string` under stokaro/ptah#2315. See [RangeChanges]
// for why both sides carry the operand and why the wire shape does not change.
type SequenceChanges []schemamodel.Sequence

// MarshalJSON writes the names alone, the shape `sequences_added` and
// `sequences_removed` have always had.
func (s SequenceChanges) MarshalJSON() ([]byte, error) {
	if s == nil {
		return []byte("null"), nil
	}
	return json.Marshal(s.Names())
}

// Names is the sequence names this change applies to.
func (s SequenceChanges) Names() []string {
	if s == nil {
		return nil
	}
	names := make([]string, 0, len(s))
	for _, sequence := range s {
		names = append(names, sequence.QualifiedName())
	}
	return names
}

// CompositeTypeChanges is a set of composite types one change applies to,
// carrying each one's fields and not only its name.
//
// The second family off `[]string` under stokaro/ptah#2315. See [RangeChanges]
// for why both sides carry the operand and why the wire shape does not change.
type CompositeTypeChanges []schemamodel.CompositeType

// MarshalJSON writes the names alone, the shape `composite_types_added` and
// `composite_types_removed` have always had.
func (c CompositeTypeChanges) MarshalJSON() ([]byte, error) {
	if c == nil {
		return []byte("null"), nil
	}
	return json.Marshal(c.Names())
}

// Names is the composite-type names this change applies to.
func (c CompositeTypeChanges) Names() []string {
	if c == nil {
		return nil
	}
	names := make([]string, 0, len(c))
	for _, composite := range c {
		names = append(names, composite.QualifiedName())
	}
	return names
}

// RangeChanges is a set of range types one change applies to, carrying each
// one's definition and not only its name.
//
// It is the first family to move off `[]string` under stokaro/ptah#2315, whose
// measure of done is `GenerateSchemaDiffAST` no longer taking the desired
// schema: the planner took it to recover, by name, what the diff had thrown
// away. A change that carries its operands needs no such lookup -- and the
// lookup it replaces had an `if found != nil` around it, so a name the planner
// could not resolve silently planned nothing.
//
// ON THE WIRE IT IS STILL A LIST OF NAMES. `ptah schema diff --format json`
// serializes this type as `format_version: 1` has always spelled it, because
// 33 of these families remain and a format that changed shape once per family
// would churn 33 times for one architectural move. The document is written and
// never read back -- nothing in the tree unmarshals a SchemaDiff -- so the
// encoding is a presentation choice, and the version bump belongs to the end of
// the migration rather than to each step of it.
type RangeChanges []schemamodel.Range

// MarshalJSON writes the names alone, preserving the shape `ranges_added` and
// `ranges_removed` have carried since they existed.
//
// A nil list stays null and an empty one stays an empty array, which is the
// distinction every other field of this type keeps: null is a comparison that
// did not run, and [] is one that found nothing.
func (r RangeChanges) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	names := make([]string, 0, len(r))
	for _, rangeType := range r {
		names = append(names, rangeType.QualifiedName())
	}
	return json.Marshal(names)
}

// Names is the range names this change applies to, for the callers that key on
// a name rather than plan from a definition.
func (r RangeChanges) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r))
	for _, rangeType := range r {
		names = append(names, rangeType.QualifiedName())
	}
	return names
}

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

// ConstraintIdentity is what makes two constraint records one object.
//
// The three parts are already FOLDED by the target's rules, and they are kept
// apart rather than joined so that no consumer has to parse them back out. A
// zero value means the producer did not resolve one, which is not the same as a
// constraint in no schema: a consumer that keys on identity has to say which it
// is rather than treat an empty struct as a match.
//
// It exists because the two halves of one modified constraint arrive under two
// spellings -- `widget` from a description and `public.widget` from a catalog --
// and every consumer that paired them by spelling had to fold the names again
// to do it. Folding downstream applies the target's rule twice on one side of
// the pipeline and once on the other, which is how a drop came to be paired
// with a constraint the comparator never removed (stokaro/ptah#1663,
// stokaro/ptah#1987).
type ConstraintIdentity struct {
	// Schema is the folded schema the constraint's table lives in, resolved to
	// the target's default when the source left it unwritten.
	Schema string `json:"schema,omitempty"`

	// Table is the folded name of the table the constraint is on.
	Table string `json:"table,omitempty"`

	// Name is the folded constraint name, under the rule the engine resolves
	// one by -- which is the index rule, not the column rule
	// (stokaro/ptah#2028).
	Name string `json:"name,omitempty"`
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

	// Identity is what pairs this removal with an addition of the same object.
	// Name and TableName stay because they are the spellings a statement and a
	// diagnostic are written with; this is the comparison form.
	Identity ConstraintIdentity `json:"identity,omitzero"`
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
	// Identity is the constraint's, and it is the same value the matching
	// ConstraintRemovalInfo carries: this record is supplemental to that one and
	// is correlated with it. Correlating by the two spellings instead is what
	// makes a foreign key on `widget` fail to find its detail recorded under
	// `public.widget`.
	Identity ConstraintIdentity `json:"identity,omitzero"`
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
	// Deferrable and Initially carry a foreign key's deferral to the planner.
	//
	// Without them a difference the comparator detected arrived with the
	// property already gone, so the ALTER that was supposed to apply a declared
	// DEFERRABLE built a plain key instead -- and where the change is expressed
	// as a drop and an add, the add REMOVED a deferral the database had
	// (stokaro/ptah#2216).
	Deferrable bool   `json:"deferrable,omitempty"`
	Initially  string `json:"initially,omitempty"`
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

	// Identity is what pairs this addition with a removal of the same object.
	// Name and TableName stay because they are the spellings a statement and a
	// diagnostic are written with; this is the comparison form.
	Identity ConstraintIdentity `json:"identity,omitzero"`
}

// RoutineRemoval is one routine to drop, named the way a server can address it.
//
// The signature is not decoration: a routine name is not unique in a schema that
// overloads, and the DROP statement has to say which one. It is the catalog's
// own argument list where the reader fills one, and the declared parameters
// otherwise (stokaro/ptah#2296).
type RoutineRemoval struct {
	// Name is the qualified routine name.
	Name string `json:"name"`
	// Signature is the argument list, empty when the routine is not overloaded
	// and the reader supplied none.
	Signature string `json:"signature,omitempty"`
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
	TablesAdded TableChanges `json:"tables_added"`

	// TablesRemoved contains names of tables that exist in the current database
	// but not in the target schema (potentially dangerous - data loss)
	TablesRemoved []string `json:"tables_removed"`

	// TablesModified contains detailed information about tables that exist in both
	// schemas but have structural differences (columns, constraints, etc.)
	TablesModified []TableDiff `json:"tables_modified"`

	// EnumsAdded contains names of enum types that exist in the target schema
	// but not in the current database schema
	EnumsAdded EnumChanges `json:"enums_added"`

	// EnumsRemoved contains names of enum types that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing data)
	EnumsRemoved EnumChanges `json:"enums_removed"`

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
	//
	// It supplements IndexesRemoved rather than adding to it: every entry here
	// is one of those removals, spelled as the constraint drop the engine
	// needs. See [SupplementLists] for what that means to a reader.
	ConstraintBackedIndexRemovals []IndexRef `json:"constraint_backed_index_removals,omitempty" ptah:"supplement=indexes_removed"`

	// ExtensionsAdded contains names of PostgreSQL extensions that exist in the target schema
	// but not in the current database schema
	ExtensionsAdded ExtensionChanges `json:"extensions_added"`

	// ExtensionsRemoved contains names of PostgreSQL extensions that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	ExtensionsRemoved ExtensionChanges `json:"extensions_removed"`

	// ExtensionsModified contains PostgreSQL extensions whose installation schema differs.
	// PostgreSQL extension names are database-wide identities; schema is placement, not identity.
	ExtensionsModified []ExtensionDiff `json:"extensions_modified"`

	// FunctionsAdded contains names of PostgreSQL functions that exist in the target schema
	// but not in the current database schema
	FunctionsAdded FunctionChanges `json:"functions_added"`

	// FunctionsRemoved contains names of PostgreSQL functions that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	FunctionsRemoved FunctionChanges `json:"functions_removed"`
	// ProceduresRemoved names the procedures the database holds and the desired
	// state does not.
	//
	// It is separate from FunctionsRemoved because the DROP verb has to match the
	// object: a server answers `DROP FUNCTION` aimed at a procedure by name, and
	// the removal is the one operation whose kind cannot be recovered from the
	// declaration -- there is no declaration left. Additions and modifications
	// stay in the function collections, where the planner reads the kind off the
	// declaration it is building from (stokaro/ptah#1722).
	ProceduresRemoved FunctionChanges `json:"procedures_removed,omitempty"`

	// FunctionsModified contains detailed information about functions that exist in both
	// schemas but have different definitions (parameters, body, attributes, etc.)
	FunctionsModified []FunctionDiff `json:"functions_modified"`

	// SequencesAdded contains names of standalone sequences that exist in the target
	// schema but not in the current database schema.
	SequencesAdded SequenceChanges `json:"sequences_added"`

	// SequencesRemoved contains names of standalone sequences that exist in the current
	// database but not in the target schema (potentially dangerous - may break defaults).
	SequencesRemoved SequenceChanges `json:"sequences_removed"`

	// SequencesModified contains detailed information about sequences that exist in both
	// schemas but have different options (increment, cache, cycle, ownership, etc.).
	SequencesModified []SequenceDiff `json:"sequences_modified"`

	// DomainsAdded/Removed/Modified track PostgreSQL domain types.
	//
	// Both lists carry the domain itself, the sixth family to do so under
	// stokaro/ptah#2315.
	DomainsAdded    DomainChanges `json:"domains_added"`
	DomainsRemoved  DomainChanges `json:"domains_removed"`
	DomainsModified []DomainDiff  `json:"domains_modified"`

	// CompositeTypesAdded/Removed/Modified track PostgreSQL composite types.
	//
	// Both lists carry the composite type itself, the second family to do so
	// under stokaro/ptah#2315, for the reason [RangeChanges] gives.
	CompositeTypesAdded    CompositeTypeChanges `json:"composite_types_added"`
	CompositeTypesRemoved  CompositeTypeChanges `json:"composite_types_removed"`
	CompositeTypesModified []CompositeTypeDiff  `json:"composite_types_modified"`

	// RangesAdded/Removed/Modified track PostgreSQL range types. PostgreSQL has
	// no ALTER TYPE ... AS RANGE, so a modification is planned as a non-CASCADE
	// DROP TYPE followed by a CREATE TYPE, the same shape domains and composite
	// types already use.
	//
	// Modified used to be absent, and the comparator built name sets only, so
	// changing the subtype of an existing range type produced an empty plan and
	// `schema apply` reported "Schema is synced" while the database still held
	// the old definition (stokaro/ptah#931 item 2).
	//
	// The two name lists carry the range type itself rather than its name, the
	// first family to do so under stokaro/ptah#2315. Both sides carry it: the
	// reverse of an addition is a removal and the reverse of a removal is an
	// addition, and a planner that had to look one of them up would need a
	// schema again for exactly the direction that swap produces.
	RangesAdded    RangeChanges `json:"ranges_added"`
	RangesRemoved  RangeChanges `json:"ranges_removed"`
	RangesModified []RangeDiff  `json:"ranges_modified"`

	// ViewsAdded contains names of views that exist in the target schema
	// but not in the current database schema.
	ViewsAdded ViewChanges `json:"views_added"`

	// ViewsRemoved contains names of views that exist in the current database
	// but not in the target schema.
	ViewsRemoved ViewChanges `json:"views_removed"`

	// HypertablesAdded names the tables a declaration asks to partition and the
	// database reports as ordinary.
	HypertablesAdded HypertableChanges `json:"hypertables_added"`

	// HypertablesRemoved names the tables the database reports as hypertables
	// and the declaration does not.
	//
	// There is no statement that honors one. TimescaleDB has no
	// `drop_hypertable`: measured on 2.29.2, the call answers
	// `function drop_hypertable(unknown) does not exist`, and the only way back
	// to an ordinary table is dropping this one and its data. So the planner
	// refuses rather than plans (stokaro/ptah#1026).
	HypertablesRemoved HypertableChanges `json:"hypertables_removed"`

	// HypertablesModified names the tables whose partitioning declaration
	// differs from what the catalog reports.
	//
	// It carries the same refusal as a removal, for the same reason: changing a
	// dimension is not a statement either.
	HypertablesModified []HypertableDiff `json:"hypertables_modified"`

	// ContinuousAggregatesAdded names the continuous aggregates a declaration
	// asks for and the database does not report.
	ContinuousAggregatesAdded ContinuousAggregateChanges `json:"continuous_aggregates_added"`

	// ContinuousAggregatesRemoved names the continuous aggregates the database
	// reports and the declaration does not.
	//
	// Unlike a hypertable this one CAN be honored, and the statement is DROP
	// MATERIALIZED VIEW rather than DROP VIEW -- measured on 2.29.2, DROP VIEW
	// answers `cannot drop continuous aggregate using DROP VIEW`.
	ContinuousAggregatesRemoved ContinuousAggregateChanges `json:"continuous_aggregates_removed"`

	// ContinuousAggregatesModified names the aggregates whose declared body or
	// options differ from the ones the catalog reports.
	//
	// It is planned as a drop and a create rather than a replacement, because
	// there is no replacement: measured on 2.29.2, `CREATE OR REPLACE
	// MATERIALIZED VIEW` is `syntax error at or near "MATERIALIZED"`.
	ContinuousAggregatesModified []ContinuousAggregateDiff `json:"continuous_aggregates_modified"`

	// SynonymsAdded contains names of synonyms that exist in the target schema
	// and not in the database.
	SynonymsAdded SynonymChanges `json:"synonyms_added"`

	// SynonymsRemoved contains names of synonyms that exist in the database and
	// not in the target schema.
	SynonymsRemoved SynonymChanges `json:"synonyms_removed"`

	// SynonymsModified contains synonyms whose target changed.
	//
	// A changed target is its own case rather than a removal plus an addition,
	// because T-SQL has no ALTER SYNONYM: the plan has to drop and recreate,
	// and a reader who sees the same name in both the removed and added lists
	// cannot tell a retarget from an unrelated drop that happens to share a
	// name with an unrelated create.
	SynonymsModified []SynonymDiff `json:"synonyms_modified"`

	// ExtendedPropertiesAdded contains the SQL Server extended properties the
	// target schema declares and the database does not have.
	ExtendedPropertiesAdded []ExtendedPropertyRef `json:"extended_properties_added"`

	// ExtendedPropertiesRemoved contains the extended properties the database
	// has and the target schema does not declare.
	ExtendedPropertiesRemoved []ExtendedPropertyRef `json:"extended_properties_removed"`

	// ExtendedPropertiesModified contains the properties whose value differs.
	//
	// A changed value is its own case rather than a removal plus an addition,
	// because SQL Server has a statement for exactly this --
	// sp_updateextendedproperty -- and dropping and re-adding would take the
	// property away for the length of the script.
	ExtendedPropertiesModified []ExtendedPropertyDiff `json:"extended_properties_modified"`

	// ViewsModified contains detailed information about views with changed definitions.
	ViewsModified []ViewDiff `json:"views_modified"`

	// MaterializedViewsAdded contains names of materialized views that exist in the target schema
	// but not in the current database schema.
	MaterializedViewsAdded MaterializedViewChanges `json:"materialized_views_added"`

	// MaterializedViewsRemoved contains names of materialized views that exist in the current database
	// but not in the target schema.
	MaterializedViewsRemoved MaterializedViewChanges `json:"materialized_views_removed"`

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

	// RLSPolicyIdentityConflicts records declared policies that collapse onto
	// one identity, which the lists above cannot show: a colliding pair is
	// already one entry by the time they exist.
	//
	// A planner refuses a diff that carries any, because the alternative is
	// applying one of two policies nobody chose between. It stays off the wire:
	// a refusal is not a plan, so there is nothing for a reader of a stored one
	// to do with it (stokaro/ptah#2440).
	RLSPolicyIdentityConflicts []RLSPolicyConflict `json:"-"`

	// DeclaredUserTypes is the type vocabulary a column may name, carried once
	// for the whole diff. A planner resolves a created table's column types
	// through it; see [UserTypeVocabulary] for why it is not per entry.
	DeclaredUserTypes UserTypeVocabulary `json:"-"`

	// DeclaredTables is every table the declaration holds, carried once for the
	// whole diff and off the wire.
	//
	// A foreign key names the table it references, and that table is usually
	// one this diff does NOT touch -- an existing parent a new child points at.
	// Resolving `parents` to `app.parents` therefore needs the declared table
	// list rather than anything a per-entry operand could carry, for the reason
	// [UserTypeVocabulary] gives about types (stokaro/ptah#2315).
	//
	// It holds the tables, not their columns: what a reference resolution reads
	// is the name and the schema.
	DeclaredTables []schemamodel.Table `json:"-"`

	// RLSEnabledTablesAdded contains names of tables that need RLS enabled
	RLSEnabledTablesAdded RLSEnabledTableChanges `json:"rls_enabled_tables_added"`

	// RLSEnabledTablesRemoved contains names of tables that need RLS disabled
	// (potentially dangerous - removes row-level security)
	RLSEnabledTablesRemoved RLSEnabledTableChanges `json:"rls_enabled_tables_removed"`

	// RolesAdded contains names of PostgreSQL roles that exist in the target schema
	// but not in the current database schema
	RolesAdded RoleChanges `json:"roles_added"`

	// RolesRemoved contains names of PostgreSQL roles that exist in the current database
	// but not in the target schema (potentially dangerous - may break existing functionality)
	RolesRemoved RoleChanges `json:"roles_removed"`

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
	// and does not independently make the diff non-empty. See [SupplementLists]
	// for what that means to a reader.
	ForeignKeysRemovedWithTables []ForeignKeyRemovalInfo `json:"foreign_keys_removed_with_tables" ptah:"supplement=constraints_removed_with_tables"`
}

// EffectiveIdentifierSemantics returns live semantics stored on the diff, or
// conservative offline rules for dialect when the diff has no live metadata.
func (d *SchemaDiff) EffectiveIdentifierSemantics(dialect string) identifier.Semantics {
	if d != nil && d.IdentifierSemantics != nil {
		return d.IdentifierSemantics.Normalize(dialect)
	}
	return identifier.ForDialect(dialect)
}

// HasChanges reports whether the diff holds any change a migration would have
// to carry out.
//
// It answers for every object kind the diff carries, so a caller does not have
// to read the fields itself. It is the check a CI pipeline or an automated
// deployment makes before deciding to generate and apply a migration.
//
// # Return Value
//
// Returns true when any of these carries an entry:
//
//   - tables, and the columns and per-table structure inside them
//   - enum types
//   - indexes
//   - constraints, whether named alone or with their owning table
//   - views and materialized views
//   - functions, procedures, and sequences
//   - triggers
//   - domains, composite types, and range types
//   - synonyms
//   - extensions
//   - roles, grants, and grant options
//   - row-level security policies, and the tables RLS is enabled or disabled on
//   - TimescaleDB hypertables and continuous aggregates
//   - SQL Server extended properties
//
// The method itself is the full set; the list above names the groups rather
// than each individual field.
//
// # Example Usage
//
//	diff := schemadiff.Compare(generated, database)
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
		d.hasSynonymChanges() ||
		d.hasHypertableChanges() ||
		d.hasContinuousAggregateChanges() ||
		d.hasExtendedPropertyChanges() ||
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
		len(d.ProceduresRemoved) > 0 ||
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

func (d *SchemaDiff) hasSynonymChanges() bool {
	return len(d.SynonymsAdded) > 0 ||
		len(d.SynonymsRemoved) > 0 ||
		len(d.SynonymsModified) > 0
}

func (d *SchemaDiff) hasHypertableChanges() bool {
	return len(d.HypertablesAdded) > 0 ||
		len(d.HypertablesRemoved) > 0 ||
		len(d.HypertablesModified) > 0
}

func (d *SchemaDiff) hasContinuousAggregateChanges() bool {
	return len(d.ContinuousAggregatesAdded) > 0 ||
		len(d.ContinuousAggregatesRemoved) > 0 ||
		len(d.ContinuousAggregatesModified) > 0
}

func (d *SchemaDiff) hasExtendedPropertyChanges() bool {
	return len(d.ExtendedPropertiesAdded) > 0 ||
		len(d.ExtendedPropertiesRemoved) > 0 ||
		len(d.ExtendedPropertiesModified) > 0
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
	ColumnsAdded ColumnChanges `json:"columns_added"`

	// ColumnsRemoved contains the columns that need to be removed from the table
	// (potentially dangerous - may cause data loss)
	//
	// It carries the column's definition because the DOWN direction reads it:
	// `reverseTableDiffs` turns a removal into an addition, and the rollback
	// renders `ADD COLUMN` from what this carried.
	//
	// It deliberately carries no PRIMARY KEY and no FOREIGN KEY. Those belong
	// to the constraint comparison, which reports them for a dropped column
	// already; carrying them here as well is what made a rolled-back column
	// emit its foreign key twice, which PostgreSQL answers with `constraint
	// ... already exists` (stokaro/ptah#2404).
	ColumnsRemoved ColumnChanges `json:"columns_removed"`

	// ColumnsModified contains detailed information about columns that exist in both
	// schemas but have different properties (type, constraints, defaults, etc.)
	ColumnsModified []ColumnDiff `json:"columns_modified"`

	// ConstraintsAdded contains names of constraints that need to be added to the table
	ConstraintsAdded []string `json:"constraints_added"`

	// ConstraintsRemoved contains names of constraints that need to be removed from the table
	// (potentially dangerous - may affect data integrity)
	ConstraintsRemoved []string `json:"constraints_removed"`

	// CommentChange carries the table's comment transition, and is nil when the
	// comment is unchanged.
	//
	// Both sides, for the reason [RowTTLChange] gives: a comment the database
	// holds and the declaration does not is a REMOVAL, and only the current
	// state says so. A planner given the desired side alone cannot tell "no
	// comment was declared, and none exists" from "no comment was declared, and
	// one is there to drop" -- and the second is the case that made a changed
	// comment report `Schema is synced` forever (stokaro/ptah#2168).
	CommentChange *CommentChange `json:"comment_change,omitzero"`

	// RowTTLChange carries a CockroachDB row-level TTL transition, and is nil
	// when the table's policy is unchanged.
	//
	// It is a pointer to a pair rather than a list of changed parameters
	// because the planner needs BOTH sides: a parameter present on the target
	// and absent from the declaration is a RESET, and only the current state
	// says which those are. See stokaro/ptah#1027.
	RowTTLChange *RowTTLChange `json:"row_ttl_change,omitzero"`

	// RowDeletionPolicyChange carries a row deletion policy transition, and is
	// nil when the declaration and the database agree. It is a second field
	// beside RowTTLChange because the two are different clauses on different
	// engines and no table carries both (stokaro/ptah#2236).
	RowDeletionPolicyChange *RowDeletionPolicyChange `json:"row_deletion_policy_change,omitzero"`
}

// RowDeletionPolicyChange is one table's row deletion policy transition.
//
// Both sides travel, for the same reason RowTTLChange carries both: adding a
// policy and changing one are different statements, and only the pair says
// which of the two this is.
type RowDeletionPolicyChange struct {
	// Desired is the policy the declaration states, nil for none.
	Desired *ast.RowDeletionPolicySpec `json:"desired,omitzero"`
	// Current is the policy the database carries, nil for none.
	Current *ast.RowDeletionPolicySpec `json:"current,omitzero"`
}

// RowTTLChange is one table's row-level TTL transition.
//
// Either side may be nil: a nil Current is a policy being added, a nil Desired
// is one being removed, and both non-nil is a change. Both nil never reaches
// here, because that is not a change.
type RowTTLChange struct {
	// Desired is the policy the declaration asks for, nil to remove it.
	Desired *ast.RowTTLSpec `json:"desired,omitzero"`
	// Current is the policy the target carries, nil when it has none.
	Current *ast.RowTTLSpec `json:"current,omitzero"`
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

	// CommentChange carries the column's comment transition, and is nil when
	// the comment is unchanged.
	//
	// It is a field of its own rather than an entry in Changes because a
	// comment is prose: it can contain the " -> " the map's format uses as a
	// separator, and it can be empty, which that format cannot tell from
	// absent. Both distinctions are the ones a planner needs.
	CommentChange *CommentChange `json:"comment_change,omitzero"`

	// NotNullConstraintNameChange carries the column's NOT NULL constraint name
	// transition, and is nil when the declaration does not manage the name.
	//
	// Nil is the common case and means "leave it alone", not "they match".
	// PostgreSQL 18 names EVERY NOT NULL and provides no catalog flag
	// separating an author-supplied name from a generated one, so a plain
	// `NOT NULL` declaration sits opposite a server-generated name on every
	// column. Comparing those would report a difference on every table nobody
	// changed (stokaro/ptah#2161).
	NotNullConstraintNameChange *NotNullConstraintNameChange `json:"not_null_constraint_name_change,omitzero"`
}

// NotNullConstraintNameChange is one column's NOT NULL constraint name
// transition.
//
// Desired is never empty: an omitted name leaves the actual one unmanaged and
// produces no change at all. Current may be empty on a target that had no name
// to report.
type NotNullConstraintNameChange struct {
	// Current is the name the database holds.
	Current string `json:"current"`
	// Desired is the name the declaration asks for.
	Desired string `json:"desired"`
}

// CommentChange is one object's comment transition.
//
// An empty string on either side means no comment. The engines agree on that
// much even though they disagree on everything else here: PostgreSQL and Oracle
// store an empty comment as NULL, and MySQL reports an absent one as the empty
// string, so neither has a state between "no comment" and "a comment that says
// nothing".
type CommentChange struct {
	// Current is what the database holds, empty when it holds none.
	Current string `json:"current"`
	// Desired is what the declaration asks for, empty to remove it.
	Desired string `json:"desired"`
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

	// Desired is the function this change asks the database to hold.
	//
	// A modification renders as CREATE OR REPLACE, and the change map records
	// what differs rather than the body and attributes that replacement needs.
	// Carrying the declaration is what lets the planner write it without being
	// handed the schema it came out of (stokaro/ptah#2315).
	//
	// It is the declaration as written, NOT the copy the comparison folds. The
	// comparison canonicalizes case and normalizes MySQL type spellings on both
	// sides so that two spellings of one function converge; rendering from that
	// copy would write Ptah's normalization into the user's DDL.
	//
	// It stays off the wire. The change map is the change; this is the operand.
	Desired schemamodel.Function `json:"-"`
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

	// CurrentCheckConstraints names the CHECK constraints the database holds
	// for this domain, in catalog order.
	//
	// ALTER DOMAIN removes a constraint by name, and a name is the one thing
	// the declaration cannot supply: the author wrote an expression, and the
	// server chose what to call the constraint enforcing it. Without these a
	// changed CHECK has only the drop-and-recreate route, which fails on any
	// domain a column uses -- that is, on every domain worth changing.
	//
	// Empty when nothing needs replacing, and empty as well when the reader
	// could not enumerate them; the planner then leaves the CHECK alone rather
	// than guessing a name (stokaro/ptah#1717).
	CurrentCheckConstraints []string `json:"current_check_constraints,omitempty"`

	// Desired is the domain this change asks the database to hold.
	//
	// A change with no in-place ALTER is a drop and a recreate, and the
	// recreate renders from this. Carrying it is what lets the planner write
	// the pair without being handed the schema the domain came out of
	// (stokaro/ptah#2315), and an empty one is what withholds the drop: a type
	// Ptah cannot rebuild is a type Ptah must not drop.
	//
	// It stays off the wire. The change map is the change; this is the operand.
	Desired schemamodel.Domain `json:"-"`
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

	// Desired is the range type this change asks the database to hold.
	//
	// PostgreSQL has no ALTER TYPE ... AS RANGE, so every change here is a drop
	// and a recreate. It plays the part [DomainDiff.Desired] plays, with the
	// same empty-means-withhold-the-drop rule.
	Desired schemamodel.Range `json:"-"`
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

	// AttributesAdded and AttributesRemoved carry the field-level delta, and
	// they are set only when applying it reaches the declared shape exactly.
	//
	// PostgreSQL appends a new attribute at the end, so a declaration that puts
	// a new field in the middle cannot be reached by ALTER TYPE at all: the
	// catalog order would differ from the declared order and the next
	// comparison would ask for the same change again, forever. The comparator
	// therefore simulates the drops and appends and sets these two only when
	// the result equals the declaration. A field whose TYPE changed also leaves
	// them unset, because ALTER TYPE ... ALTER ATTRIBUTE is refused outright on
	// a composite a column uses -- measured on PostgreSQL 18.4, with CASCADE and
	// without: `cannot alter type "addr" because column "uses_addr.a" uses it`.
	//
	// Both unset means the modification takes the drop-and-recreate path
	// (stokaro/ptah#1717).
	AttributesAdded []CompositeAttribute `json:"attributes_added,omitempty"`
	// AttributesRemoved names the fields to remove. See AttributesAdded.
	AttributesRemoved []string `json:"attributes_removed,omitempty"`

	// Desired is the composite type this change asks the database to hold.
	//
	// It plays the part [DomainDiff.Desired] plays, for the same reason and
	// with the same empty-means-withhold-the-drop rule.
	Desired schemamodel.CompositeType `json:"-"`
}

// CompositeAttribute is one field of a composite type, as a declaration spells
// it.
type CompositeAttribute struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// SequenceDiff represents changes to a standalone sequence definition.
type SequenceDiff struct {
	// SequenceName is the (optionally schema-qualified) name of the sequence.
	SequenceName string `json:"sequence_name"`

	// Changes maps change types to their old->new value transitions
	// Format: "change_type" -> "old_value -> new_value"
	Changes map[string]string `json:"changes"`

	// Desired is the sequence this change asks the database to hold.
	//
	// An ALTER SEQUENCE emits only the options the change map names, and it
	// reads their VALUES off this: the map records the transition as prose.
	// Carrying the declaration is what lets the planner reach them without
	// being handed the schema (stokaro/ptah#2315).
	//
	// It stays off the wire. The change map is the change; this is the operand.
	Desired schemamodel.Sequence `json:"-"`
}

// ExtensionDiff represents a PostgreSQL extension installation-schema change.
type ExtensionDiff struct {
	Name       string `json:"name"`
	FromSchema string `json:"from_schema"`
	ToSchema   string `json:"to_schema"`
	// FromVersion and ToVersion carry a declared version change. The version is
	// the one attribute of an extension that moves over time, and it used to be
	// rendered on the create and never compared: a team that raised the pin in
	// its schema saw "Schema is synced" against a database still running the old
	// one (stokaro/ptah#1718).
	//
	// Both are empty when the declaration names no version, which is the common
	// case and means "whatever the server installs".
	FromVersion string `json:"from_version,omitempty"`
	ToVersion   string `json:"to_version,omitempty"`
	// Relocatable reports whether the installed extension may be moved between
	// schemas at all. It is a fact about the extension rather than the target,
	// read from pg_extension.extrelocatable, and it rides on the diff because
	// the planner is handed the desired schema and this diff -- never the live
	// description that knows it. `ALTER EXTENSION plpgsql SET SCHEMA ext` is
	// `extension "plpgsql" does not support SET SCHEMA`, measured on
	// PostgreSQL 18.
	Relocatable bool `json:"relocatable"`
}

// HypertableDiff describes a hypertable whose declared partitioning differs
// from the one the catalog reports.
type HypertableDiff struct {
	// Table is the table both sides name.
	Table string `json:"table"`
	// OldColumn and NewColumn are the range dimensions, live and declared.
	OldColumn string `json:"old_column"`
	NewColumn string `json:"new_column"`
	// OldChunkInterval and NewChunkInterval are the chunk widths, live and
	// declared. An empty declared interval takes the server's default and is
	// not a difference.
	OldChunkInterval string `json:"old_chunk_interval"`
	NewChunkInterval string `json:"new_chunk_interval"`
}

// ContinuousAggregateDiff describes a continuous aggregate whose declaration
// differs from the one the catalog reports.
//
// Both bodies are carried so a reviewer reading the plan can see what changed;
// the plan itself is a drop followed by a create, and the create uses the
// DECLARED body.
type ContinuousAggregateDiff struct {
	// Name is the aggregate both sides name, schema-qualified when it has one.
	Name string `json:"name"`
	// OldBody and NewBody are the SELECTs, live and declared.
	OldBody string `json:"old_body"`
	NewBody string `json:"new_body"`
	// OldMaterializedOnly and NewMaterializedOnly are the option's two values.
	OldMaterializedOnly bool `json:"old_materialized_only"`
	NewMaterializedOnly bool `json:"new_materialized_only"`

	// Desired is the aggregate this change asks the database to hold.
	//
	// A modification is a drop followed by a create, and the create needs more
	// than the two fields above: the schema and the name separately, and the
	// comment. Carrying the object is what lets the planner render the change
	// without being handed the schema it came out of (stokaro/ptah#2315).
	//
	// It stays off the wire. The bodies and the option are the change; this is
	// the operand the renderer needs to write it, and a reader of a stored plan
	// resolves the object for itself.
	Desired schemamodel.ContinuousAggregate `json:"-"`
}

// ExtendedPropertyRef names one SQL Server extended property, by the address
// that identifies it and the value it should carry.
//
// The address is the identity rather than the name alone: SQL Server stores a
// property under a class and up to two ids, so `ptah_flag` on a schema, on a
// table of it, and on a column of that table are three different properties
// that a plan has to keep apart.
type ExtendedPropertyRef struct {
	Name   string `json:"name"`
	Schema string `json:"schema"`
	Table  string `json:"table,omitempty"`
	Column string `json:"column,omitempty"`
	// Value is the DESIRED value for an addition or a modification, and the
	// live one for a removal, which is the only value a removal has.
	Value string `json:"value"`
}

// ExtendedPropertyDiff describes an extended property whose value changed.
type ExtendedPropertyDiff struct {
	ExtendedPropertyRef
	// OldValue is what the database holds now. Value on the embedded ref is
	// what the declaration asks for.
	OldValue string `json:"old_value"`
}

// SynonymDiff describes a synonym whose target changed.
type SynonymDiff struct {
	SynonymName string `json:"synonym_name"`
	OldTarget   string `json:"old_target"`
	NewTarget   string `json:"new_target"`

	// Desired is the synonym this change asks the database to hold.
	//
	// No dialect has an ALTER SYNONYM, so a retarget is a drop and a create,
	// and the create needs what the two target strings do not carry: the
	// schema, and whether the synonym is public. Carrying the declaration is
	// what lets the planner render it without being handed the schema
	// (stokaro/ptah#2315).
	//
	// It stays off the wire. The two targets are the change; this is the
	// operand.
	Desired schemamodel.Synonym `json:"-"`
}

// ViewDiff represents changes to a view definition.
type ViewDiff struct {
	ViewName string            `json:"view_name"`
	Changes  map[string]string `json:"changes"`

	// Desired is the view this change should leave behind, which is what a
	// `CREATE OR REPLACE VIEW` is written from.
	//
	// It is direction-dependent, and that is the whole subtlety. Going UP it is
	// the declaration; coming DOWN it is the view the database had before, so
	// the rollback restores what it is undoing rather than reapplying it. The
	// comparison sets the first and [reverseViewDiffs] rewrites it to the
	// second -- which is what the planner used to get by being handed a
	// different schema per direction (stokaro/ptah#2315).
	//
	// It is not serialized: the document has always carried a name and a map of
	// what changed, and a consumer reading it is unaffected.
	Desired schemamodel.View `json:"-"`

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

	// RefreshChange carries a ClickHouse refresh-schedule transition, and is
	// nil when the schedule is unchanged.
	//
	// It is a pair rather than a text entry in Changes because the planner
	// needs BOTH sides to choose a statement: a schedule changing to another
	// schedule is an ALTER that keeps the view's rows, while a view gaining or
	// losing one has to be dropped and recreated -- measured, the server
	// answers MODIFY REFRESH on a plain view with `Alter of type
	// 'MODIFY_REFRESH' is not supported by storage MaterializedView`
	// (stokaro/ptah#1802).
	RefreshChange *MatViewRefreshChange `json:"refresh_change,omitzero"`

	// Desired is the materialized view this change asks the database to hold.
	//
	// No engine has an in-place replacement for one that keeps its rows, so a
	// modification other than a schedule change is a drop and a create, and the
	// create needs the whole declaration. Carrying it is what lets the planner
	// render the pair without being handed the schema it came out of
	// (stokaro/ptah#2315).
	//
	// It is the view, where [MatViewRefreshChange.Desired] on the field above
	// is one schedule; the two are named alike because both answer "what is
	// being asked for", at different scales.
	//
	// It stays off the wire. The change map is the change; this is the operand.
	Desired schemamodel.MaterializedView `json:"-"`
}

// MatViewRefreshChange is one materialized view's refresh-schedule transition.
//
// Either side may be nil: a nil Current is a schedule being added, a nil
// Desired is one being removed, and both non-nil is a change of schedule. Both
// nil never reaches here, because that is not a change.
type MatViewRefreshChange struct {
	// Desired is the schedule the declaration asks for, nil to remove it.
	Desired *ast.MatViewRefreshSpec `json:"desired,omitzero"`
	// Current is the schedule the target carries, nil when it has none.
	Current *ast.MatViewRefreshSpec `json:"current,omitzero"`
}

// TriggerRef identifies a trigger by table and trigger name.
type TriggerRef struct {
	TriggerName string `json:"trigger_name"`
	TableName   string `json:"table_name"`

	// Desired is the trigger this entry asks the database to hold.
	//
	// It is populated for an ADDITION, where the planner renders CREATE
	// TRIGGER from it (stokaro/ptah#2315), and empty for a REMOVAL, which needs
	// nothing but the two names above: the DROP is written from them, and so is
	// the PostgreSQL trigger function the drop takes with it.
	//
	// It is the declaration as written, not the copy the comparison folds. That
	// fold -- uppercasing the timing, the event and the FOR EACH clause, and
	// supplying ROW where none was written -- exists to answer "did this
	// change", and carrying it would make the diff the author of the DDL. It is
	// not visible in today's output, because the renderer normalizes the same
	// three the same way; the carry is the faithful one, not a guarantee about
	// the rendered text. [FunctionDiff.Desired] is where the same rule IS
	// visible.
	//
	// It stays off the wire. The names are the reference; this is the operand.
	Desired schemamodel.Trigger `json:"-"`
}

// TriggerDiff represents changes to a trigger definition.
type TriggerDiff struct {
	TriggerName string            `json:"trigger_name"`
	TableName   string            `json:"table_name"`
	Changes     map[string]string `json:"changes"`

	// Desired is the trigger this change asks the database to hold.
	//
	// A modification renders as CREATE OR REPLACE TRIGGER, which needs the whole
	// definition rather than the change map's record of what differs. It carries
	// the declaration as written, for the reason [TriggerRef.Desired] gives, and
	// stays off the wire for the same one.
	Desired schemamodel.Trigger `json:"-"`
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

	// Desired is the policy this entry asks the database to hold.
	//
	// It is populated for an ADDITION and for a MODIFICATION, both of which
	// render CREATE POLICY from a declaration, and empty for a REMOVAL, which
	// `DROP POLICY name ON table` builds from the two names above.
	//
	// An added or modified entry that carries none is refused rather than
	// skipped. The planner's only alternative is to emit no statement, and a
	// plan that silently drops an access-control operation reports success
	// while leaving the database unprotected (stokaro/ptah#1311).
	//
	// It stays off the wire. The names are the reference; this is the operand.
	Desired schemamodel.RLSPolicy `json:"-"`

	// TableSchema is the schema the owning table is declared under, or empty
	// when the declaration does not say.
	//
	// SQL Server needs it: a policy there is addressed as `schema.name` on
	// `schema.table`, and the schema is a property of the TABLE rather than of
	// the policy, so it cannot be read off [RLSPolicyRef.Desired]. Resolving it
	// where the declared tables are in hand is what lets the planner render the
	// policy without being handed the schema (stokaro/ptah#2315).
	//
	// It stays off the wire for the reason Desired does: it is an operand.
	TableSchema string `json:"-"`
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

	// Desired is the policy this entry asks the database to hold.
	//
	// It is populated for an ADDITION and for a MODIFICATION, both of which
	// render CREATE POLICY from a declaration, and empty for a REMOVAL, which
	// `DROP POLICY name ON table` builds from the two names above.
	//
	// An added or modified entry that carries none is refused rather than
	// skipped. The planner's only alternative is to emit no statement, and a
	// plan that silently drops an access-control operation reports success
	// while leaving the database unprotected (stokaro/ptah#1311).
	//
	// It stays off the wire. The names are the reference; this is the operand.
	Desired schemamodel.RLSPolicy `json:"-"`

	// TableSchema is the schema the owning table is declared under, or empty
	// when the declaration does not say.
	//
	// SQL Server needs it: a policy there is addressed as `schema.name` on
	// `schema.table`, and the schema is a property of the TABLE rather than of
	// the policy, so it cannot be read off [RLSPolicyRef.Desired]. Resolving it
	// where the declared tables are in hand is what lets the planner render the
	// policy without being handed the schema (stokaro/ptah#2315).
	//
	// It stays off the wire for the reason Desired does: it is an operand.
	TableSchema string `json:"-"`
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

	// Desired is the role this change asks the database to hold.
	//
	// One entry needs it: a password change records only that one is required,
	// because a comparison cannot read the value it is changing to out of the
	// database. The value comes from the declaration, and carrying the role is
	// what lets the planner reach it without being handed the schema
	// (stokaro/ptah#2315).
	//
	// It stays off the wire, and here that is more than tidiness: this field
	// holds a password.
	Desired schemamodel.Role `json:"-"`
}

// TableCreation is a table a diff creates, together with everything CREATE
// TABLE renders from.
//
// The lists a planner used to reach for are keyed by the Go struct rather than
// owned by the table -- `Database.Fields` holds every column of every table --
// so rendering one table meant being handed the whole desired schema and
// filtering it. The filtering happens once, where the schema is already in
// hand, and the result travels with the change (stokaro/ptah#2315).
type TableCreation struct {
	// Name is the spelling the diff carries for this table, which is the one a
	// plan and a report name it by. It is not derived from Table: the
	// comparison qualifies a name per dialect, and MySQL's answer differs from
	// PostgreSQL's for the same declaration.
	Name string

	// Table is the declaration itself.
	Table schemamodel.Table

	// Fields are this table's columns, embedded fields already resolved into
	// them. The renderer still filters by struct name, so a list holding only
	// this table's columns renders exactly what the whole list did.
	Fields []schemamodel.Field

	// Enums are the enumerated types this table's columns name. A column
	// declared with one renders as that type rather than as its Go spelling,
	// and the renderer resolves it through this list.
	Enums []schemamodel.Enum

	// SelfReferencingForeignKeys are the foreign keys this table declares
	// against itself.
	//
	// They live in a map keyed by qualified table name on the declaration, so
	// they are a property of this table and travel with it. They are planned
	// separately from a column's own reference because a self-reference cannot
	// be part of the CREATE that makes the table (stokaro/ptah#2315).
	SelfReferencingForeignKeys []schemamodel.SelfReferencingFK

	// DependsOn are the tables this one is declared to come after, from the
	// document's own dependency map.
	//
	// It is here because a creation that renders without the schema but cannot
	// be ORDERED without it has moved half a dependency. The other two sources
	// of the ordering graph already travel: a column's foreign reference is in
	// Fields, and a relation-mode embedding is folded into Fields before it
	// gets there. This map is the third, and nothing derives it from a
	// declaration -- an author writes it down.
	DependsOn []string
}

// UserTypeVocabulary is the declaration's type vocabulary: every user type a
// column may name.
//
// It rides on the diff once rather than on each entry, because it is not a
// property of any one change. A column may name a domain nothing in this diff
// touches, so no per-entry operand reproduces it -- and a renderer resolving
// `positive_int` to `app.positive_int` has to know the domain exists at all
// (stokaro/ptah#2315).
//
// It is the four lists [fromschema.QualifyDeclaredUserTypes] reads, and no
// more: what a caller must supply is exactly what the qualification consults.
type UserTypeVocabulary struct {
	Domains        []schemamodel.Domain
	CompositeTypes []schemamodel.CompositeType
	Ranges         []schemamodel.Range
	Enums          []schemamodel.Enum
}

// UserTypeVocabularyOf reads the vocabulary off a declaration.
//
// A comparison fills it in; a caller building a diff by hand has to, and this is
// the one line that does it. A diff without it renders a user-typed column as
// the bare name the author wrote, which is correct for a declaration whose types
// live in the connection's default schema and wrong for one whose do not.
func UserTypeVocabularyOf(desired *schemamodel.Database) UserTypeVocabulary {
	if desired == nil {
		return UserTypeVocabulary{}
	}
	// Each list is nil when it holds nothing, never an empty slice. Two readers
	// of the same document disagree about that -- one finalizes every list and
	// one leaves the absent ones nil -- and a vocabulary that kept the
	// difference would make two diffs of one schema compare unequal for a
	// reason neither document states.
	return UserTypeVocabulary{
		Domains:        nilWhenEmpty(desired.Domains),
		CompositeTypes: nilWhenEmpty(desired.CompositeTypes),
		Ranges:         nilWhenEmpty(desired.Ranges),
		Enums:          nilWhenEmpty(desired.Enums),
	}
}

// nilWhenEmpty answers nil for a list that holds nothing, whatever shape the
// caller's emptiness took.
func nilWhenEmpty[T any](values []T) []T {
	if len(values) == 0 {
		return nil
	}
	return values
}

// declared is the vocabulary in the shape the qualifier reads it, and is the
// whole of what this hands over: the four lists, and no schema description
// built around them.
func (v UserTypeVocabulary) declared() fromschema.DeclaredUserTypes {
	return fromschema.DeclaredUserTypes{
		Domains:        v.Domains,
		CompositeTypes: v.CompositeTypes,
		Ranges:         v.Ranges,
		Enums:          v.Enums,
	}
}

// TableCreationFor assembles what a planner needs to create one declared table.
//
// It is the one place the assembly lives, so a caller building a diff by hand
// gets the same bundle a comparison produces. The three parts each answer a
// question the flat declaration cannot: which columns are this table's, which
// of them exist only after an embedded field is folded in, and which enums
// their types name.
//
// name is the spelling the diff carries, which the caller decides: a comparison
// qualifies it per dialect, and this function has no dialect to do that with.
func TableCreationFor(desired *schemamodel.Database, table schemamodel.Table, name string) TableCreation {
	creation := TableCreation{Name: name, Table: table}
	if desired == nil {
		return creation
	}
	all := fromschema.ProcessEmbeddedFields(desired.EmbeddedFields, desired.Fields)
	owned := make([]schemamodel.Field, 0, len(all))
	for _, field := range all {
		if field.StructName == table.StructName {
			owned = append(owned, field)
		}
	}
	creation.Fields = owned
	creation.Enums = fromschema.EnumsFor(owned, desired.Enums)
	// Derived rather than read out of desired.Dependencies, because that map is
	// filled by [schemamodel.Finalize] and a declaration assembled in memory has
	// not necessarily been through it. The carry's promise is that everything the
	// planner needs travels with the creation; a carry that is complete only when
	// the caller remembered to finalize is the same trap in a new place, and it
	// fails as a table created before the one it references rather than as an
	// error. [deporder.GeneratedTableDependencies] unions the declared map with
	// the three kinds of edge a declaration expresses -- a field's `foreign=`, a
	// relation-mode embedded field, and a table-level FOREIGN KEY constraint --
	// so it is correct for a finalized declaration and for one built by hand.
	creation.DependsOn = deporder.GeneratedTableDependencies(desired)[table.QualifiedName()]
	creation.SelfReferencingForeignKeys = desired.SelfReferencingForeignKeys[table.QualifiedName()]
	return creation
}

// TableCreationsFor assembles the creations for the named tables.
//
// It is the form a caller building a diff by hand wants: the names are what
// such a caller has, and the bundle each one needs is derived here rather than
// spelled out. A name that no declared table answers to is skipped, because a
// diff naming a table the schema does not declare has nothing to create.
//
// It assembles from the declaration exactly as given. A declaration whose
// foreign keys name themselves only by default needs
// [fromschema.AssignDefaultForeignKeyNames] run over it first: that derivation
// reads the whole document -- it truncates an over-long name to a hashed one
// and avoids colliding with an explicit name used anywhere -- so it cannot be
// done per table, and a comparison does it before it assembles these.
//
// A name matches a table's qualified spelling or its bare one, which is the
// same latitude a diff's own names are read with -- `orders` and
// `public.orders` are one table on the engines that resolve an unqualified name
// through a search path.
//
// That latitude is also its limit. A table literally NAMED `tenant.data` and a
// table `data` in schema `tenant` answer to the same string, and this takes the
// first declared one; a caller holding two such tables has to say which it
// means, with [TableCreationFor].
func TableCreationsFor(desired *schemamodel.Database, names ...string) TableChanges {
	if desired == nil || len(names) == 0 {
		return nil
	}
	creations := make(TableChanges, 0, len(names))
	for _, name := range names {
		for _, table := range desired.Tables {
			if table.QualifiedName() != name && table.Name != name {
				continue
			}
			creations = append(creations, TableCreationFor(desired, table, name))
			break
		}
	}
	return creations
}

// TableChanges is a list of tables a diff creates.
type TableChanges []TableCreation

// InDependencyOrder returns these creations ordered so that a table comes after
// everything it references.
//
// The rules are the ones the whole desired schema is ordered by, applied to the
// declaration these creations describe. Every input they read travels with a
// creation, which is what lets a planner order what it is about to create
// without being handed the schema it came out of (stokaro/ptah#2315).
//
// The edges are the ones the declaration finalized -- a field's `foreign=`, a
// relation-mode embedded field, and a table-level FOREIGN KEY constraint all
// reach [go.5x5.cz/ptah/core/schemamodel.Database.Dependencies] before a
// creation is assembled -- so ordering reads one list per creation rather than
// re-deriving three kinds of edge from a schema.
//
// Both the node and the edge are the table's qualified name from the
// DECLARATION, never the creation's Name, which is the spelling the COMPARISON
// produced and is qualified per dialect. The two are different strings for the
// same table on the dialects that rewrite a schema qualifier, and an edge names
// the declaration's spelling.
//
// An edge to a table this diff is not creating is skipped rather than ordered
// against: the referenced table already exists, so nothing here has to come
// after it.
func (t TableChanges) InDependencyOrder() TableChanges {
	if len(t) == 0 {
		return nil
	}
	keys := make([]string, 0, len(t))
	byKey := make(map[string]TableCreation, len(t))
	edges := make(map[string][]string, len(t))
	for _, creation := range t {
		// A creation naming no declared table carries nothing to render, which
		// is what a diff that names a table the declaration does not have
		// assembles to. Ordering is the last step before rendering on every
		// planner, so it is where such an entry stops.
		if creation.Table.Name == "" {
			continue
		}
		key := creation.Table.QualifiedName()
		if _, seen := byKey[key]; seen {
			continue
		}
		keys = append(keys, key)
		byKey[key] = creation
		edges[key] = creation.DependsOn
	}

	ordered := make(TableChanges, 0, len(byKey))
	for _, key := range deporder.StableTopologicalSort(keys, edges) {
		ordered = append(ordered, byKey[key])
	}
	return ordered
}

// Qualified returns these creations with every column type resolved to the
// user type it names, where the vocabulary declares one.
//
// A column carries a type NAME and the declaration carries the schema that type
// lives in, so `positive_int` renders as `app.positive_int` only once the two
// are put together. That is a render step and it stays one: the comparison
// compares a domain column structurally, by the identity the catalog reports,
// and must not be made to normalize a spelling to do it.
//
// A creation keeps its columns as written until this runs, so a caller that
// wants the declaration rather than the rendering has it.
func (t TableChanges) Qualified(vocabulary UserTypeVocabulary, dialect string) TableChanges {
	if len(t) == 0 {
		return nil
	}
	qualified := make(TableChanges, 0, len(t))
	for _, creation := range t {
		creation.Fields = fromschema.QualifyFieldUserTypes(creation.Fields, vocabulary.declared(), dialect)
		qualified = append(qualified, creation)
	}
	return qualified
}

// MarshalJSON writes the table names alone, the shape `tables_added` has always
// had.
func (t TableChanges) MarshalJSON() ([]byte, error) {
	if t == nil {
		return []byte("null"), nil
	}
	return json.Marshal(t.Names())
}

// Names is the table names this change applies to, in the spelling the
// comparison produced.
func (t TableChanges) Names() []string {
	if t == nil {
		return nil
	}
	names := make([]string, 0, len(t))
	for _, creation := range t {
		names = append(names, creation.Name)
	}
	return names
}

// RLSEnabledTableChanges is a list of row-level-security enablements a diff
// adds or removes.
//
// The list used to be table names alone, which meant a planner that renders
// anything beyond the name -- a declared comment, on the targets that carry one
// -- had to find the declaration in a schema handed to it alongside the diff,
// and planned nothing for an enablement it could not find (stokaro/ptah#2315).
type RLSEnabledTableChanges []schemamodel.RLSEnabledTable

// MarshalJSON writes the table names alone, the shape
// `rls_enabled_tables_added` and `rls_enabled_tables_removed` have always had.
func (r RLSEnabledTableChanges) MarshalJSON() ([]byte, error) {
	if r == nil {
		return []byte("null"), nil
	}
	return json.Marshal(r.Names())
}

// Names is the table names this change applies to.
//
// A REMOVED entry carries nothing else: the enablement is one the database
// reports and no declaration describes, so the name is all there is to carry.
func (r RLSEnabledTableChanges) Names() []string {
	if r == nil {
		return nil
	}
	names := make([]string, 0, len(r))
	for _, table := range r {
		names = append(names, table.Table)
	}
	return names
}

// RLSPolicyConflict records two declared row-level security policies that
// resolve to one identity under the target's identifier rules.
//
// A comparison keys declarations by that identity to pair them with what the
// database reports, so a colliding pair is reduced to one entry and the plan
// would depend on which one the map happened to keep. Recording the pair is
// what lets the planner refuse instead: by the time the diff exists the two
// declarations are already one entry, so nothing downstream could otherwise see
// that there were two (stokaro/ptah#2440).
//
// Both sides keep the spelling their declaration supplied, because the refusal
// names them and the author has to recognize what they wrote.
type RLSPolicyConflict struct {
	// First is the declaration the comparison met first.
	First schemamodel.RLSPolicy

	// Second is the one that resolved to the same identity.
	Second schemamodel.RLSPolicy
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
