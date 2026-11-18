package pgpublication

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// The operations a publication can publish, in the order PostgreSQL lists
// them. A declaration that names no publish set publishes all four.
const (
	PublishInsert   = "insert"
	PublishUpdate   = "update"
	PublishDelete   = "delete"
	PublishTruncate = "truncate"
)

// PublishOperations returns every operation a publish set may name, in
// PostgreSQL's order: the set a publication publishes by default.
func PublishOperations() []string {
	return []string{PublishInsert, PublishUpdate, PublishDelete, PublishTruncate}
}

// The values of publish_generated_columns. PostgreSQL 18 has the setting, and
// a server without it publishes no generated column.
const (
	GeneratedColumnsNone   = "none"
	GeneratedColumnsStored = "stored"
)

// Member is one table a publication names. Schema is the table's schema as
// the server stores it; a declaration may leave it empty for the default
// schema, and an observation names it. RowFilter is the member's WHERE
// expression and Columns its column list, as the catalog reports them. Ptah
// reads both and writes neither: no source parses one, and a declaration
// carries one only where it keeps an observation as it is
// ([ObservedPublication.Desired]), so a comparison of the two agrees. A plan
// that would write one is refused with the publication and the table named.
type Member struct {
	Schema    string   `json:"schema,omitempty"`
	Table     string   `json:"table"`
	RowFilter string   `json:"row_filter,omitempty"`
	Columns   []string `json:"columns,omitempty"`
}

// Filtered reports whether the member publishes only some rows or columns of
// its table.
func (m Member) Filtered() bool { return m.RowFilter != "" || len(m.Columns) != 0 }

// Qualified is the member's table qualified by schema, the schema left off
// where the member names none.
func (m Member) Qualified() string {
	if m.Schema == "" {
		return m.Table
	}
	return m.Schema + "." + m.Table
}

func (m Member) copy() Member {
	m.Columns = slices.Clone(m.Columns)
	return m
}

// compareMembers orders members by schema, then table.
func compareMembers(a, b Member) int {
	return cmp.Or(strings.Compare(a.Schema, b.Schema), strings.Compare(a.Table, b.Table))
}

// DesiredPublication is a declared publication.
//
// Its membership is one of four forms. AllTables publishes every table of
// the database, those created later included, and names no table or schema.
// Otherwise Tables are the tables it names and Schemas the schemas whose every
// table it publishes, those created later included; both empty declare a
// publication that publishes nothing until a change adds a member.
//
// Publish is the set of operations it publishes: nil for all four, and an
// empty set, which is not nil, for a publication that publishes no change.
// ViaPartitionRoot publishes a partition's changes as its root table's.
// GeneratedColumns is publish_generated_columns, empty for the server's
// default, which publishes none. Owner is the role that owns it, empty to
// leave the owner unmanaged: Ptah then never changes it.
type DesiredPublication struct {
	AllTables        bool     `json:"all_tables,omitempty"`
	Tables           []Member `json:"tables,omitempty"`
	Schemas          []string `json:"schemas,omitempty"`
	Publish          []string `json:"publish"`
	ViaPartitionRoot bool     `json:"via_partition_root,omitempty"`
	GeneratedColumns string   `json:"generated_columns,omitempty"`
	Owner            string   `json:"owner,omitempty"`
}

// ObservedPublication is a publication as the catalog reports it, or as a
// declaration predicts it. A read names each table's schema; a prediction
// keeps a declared table that names none in the default schema.
// GeneratedColumns is empty where the server has no publish_generated_columns
// setting, and Owner empty where the read did not report the owner.
type ObservedPublication struct {
	AllTables        bool     `json:"all_tables"`
	Tables           []Member `json:"tables,omitempty"`
	Schemas          []string `json:"schemas,omitempty"`
	Publish          []string `json:"publish"`
	ViaPartitionRoot bool     `json:"via_partition_root"`
	GeneratedColumns string   `json:"generated_columns,omitempty"`
	Owner            string   `json:"owner,omitempty"`
}

// Kind returns the model identity.
func (*DesiredPublication) Kind() schemaext.Kind { return PublicationKind }

// Kind returns the model identity.
func (*ObservedPublication) Kind() schemaext.Kind { return PublicationKind }

// ReadsRelation reports whether the publication names the table name, as
// "schema.table" or a bare table of the default schema. A plan that drops the
// table removes it from the publication, which the server does on its own; it
// implements [schemaext.RelationReader].
func (v *ObservedPublication) ReadsRelation(name string) bool {
	if v == nil {
		return false
	}
	return slices.ContainsFunc(v.Tables, func(member Member) bool {
		return member.Qualified() == name || member.Table == name
	})
}

// Clone returns an independent declaration.
func (v *DesiredPublication) Clone() schemaext.Value { return v.Copy() }

// Copy is [DesiredPublication.Clone] without the interface. It shares no
// list with v, and a nil receiver returns nil.
func (v *DesiredPublication) Copy() *DesiredPublication {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Tables = copyMembers(v.Tables)
	copied.Schemas = slices.Clone(v.Schemas)
	copied.Publish = slices.Clone(v.Publish)
	return &copied
}

// Clone returns an independent observation.
func (v *ObservedPublication) Clone() schemaext.Value { return v.Copy() }

// Copy is [ObservedPublication.Clone] without the interface. It shares no
// list with v, and a nil receiver returns nil.
func (v *ObservedPublication) Copy() *ObservedPublication {
	if v == nil {
		return nil
	}
	copied := *v
	copied.Tables = copyMembers(v.Tables)
	copied.Schemas = slices.Clone(v.Schemas)
	copied.Publish = slices.Clone(v.Publish)
	return &copied
}

func copyMembers(members []Member) []Member {
	if members == nil {
		return nil
	}
	copied := make([]Member, len(members))
	for i, member := range members {
		copied[i] = member.copy()
	}
	return copied
}

// EffectivePublish is the publish set the declaration asks for, every
// operation where it names none, in PostgreSQL's order. It is never nil.
func (v *DesiredPublication) EffectivePublish() []string {
	if v == nil || v.Publish == nil {
		return PublishOperations()
	}
	return canonicalPublish(v.Publish)
}

// EffectivePublish is the publish set the publication has, in PostgreSQL's
// order. It is never nil: an observation that names no operation publishes
// none.
func (v *ObservedPublication) EffectivePublish() []string {
	if v == nil || v.Publish == nil {
		return make([]string, 0)
	}
	return canonicalPublish(v.Publish)
}

// EffectiveGeneratedColumns is the publish_generated_columns value the
// declaration asks for, none where it names none.
func (v *DesiredPublication) EffectiveGeneratedColumns() string {
	if v == nil || v.GeneratedColumns == "" {
		return GeneratedColumnsNone
	}
	return v.GeneratedColumns
}

// EffectiveGeneratedColumns is the publish_generated_columns value the
// publication has: none where the server has no such setting.
func (v *ObservedPublication) EffectiveGeneratedColumns() string {
	if v == nil || v.GeneratedColumns == "" {
		return GeneratedColumnsNone
	}
	return v.GeneratedColumns
}

// Equal reports whether other declares the same publication in the same
// words. Members, schemas and operations are sets. Two nil declarations are
// equal.
func (v *DesiredPublication) Equal(other schemaext.Value) bool {
	w, ok := other.(*DesiredPublication)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.AllTables == w.AllTables && equalMembers(v.Tables, w.Tables) && slices.Equal(sortedNames(v.Schemas), sortedNames(w.Schemas)) &&
		(v.Publish == nil) == (w.Publish == nil) && slices.Equal(canonicalPublish(v.Publish), canonicalPublish(w.Publish)) &&
		v.ViaPartitionRoot == w.ViaPartitionRoot && v.GeneratedColumns == w.GeneratedColumns && v.Owner == w.Owner
}

// Equal reports whether other observes the same publication in the same
// state. Members, schemas and operations are sets: their order is the read's.
// Two nil observations are equal.
func (v *ObservedPublication) Equal(other schemaext.Value) bool {
	w, ok := other.(*ObservedPublication)
	if !ok || v == nil || w == nil {
		return ok && v == nil && w == nil
	}
	return v.AllTables == w.AllTables && equalMembers(v.Tables, w.Tables) && slices.Equal(sortedNames(v.Schemas), sortedNames(w.Schemas)) &&
		slices.Equal(canonicalPublish(v.Publish), canonicalPublish(w.Publish)) && v.ViaPartitionRoot == w.ViaPartitionRoot &&
		v.GeneratedColumns == w.GeneratedColumns && v.Owner == w.Owner
}

func equalMembers(a, b []Member) bool {
	return slices.EqualFunc(sortedMembers(a), sortedMembers(b), func(x, y Member) bool {
		return x.Schema == y.Schema && x.Table == y.Table && x.RowFilter == y.RowFilter && slices.Equal(x.Columns, y.Columns)
	})
}

// sortedMembers returns the members ordered by schema and table, without
// changing members.
func sortedMembers(members []Member) []Member {
	sorted := copyMembers(members)
	slices.SortFunc(sorted, compareMembers)
	return sorted
}

// sortedNames returns the names sorted and once each, without changing names.
func sortedNames(names []string) []string {
	sorted := slices.Clone(names)
	slices.Sort(sorted)
	return slices.Compact(sorted)
}

// canonicalPublish returns the operations of publish once each, in
// PostgreSQL's order, nil for nil and empty for empty. An unknown operation
// keeps its place after the known ones, so validation still sees it.
func canonicalPublish(publish []string) []string {
	if publish == nil {
		return nil
	}
	result := make([]string, 0, len(publish))
	for _, operation := range PublishOperations() {
		if slices.Contains(publish, operation) {
			result = append(result, operation)
		}
	}
	for _, operation := range sortedNames(publish) {
		if !slices.Contains(PublishOperations(), operation) {
			result = append(result, operation)
		}
	}
	return result
}

// Desired is the declaration that keeps the observed publication as it is:
// its membership, its publish set and its settings. It names no owner: a read
// reports the owner a publication has, and that declares no role and manages
// no owner. A member's row filter and column list are kept, so the
// declaration still says the publication publishes less than its tables; a
// writer that cannot write them refuses the declaration rather than widen it.
func (v *ObservedPublication) Desired() *DesiredPublication {
	if v == nil {
		return nil
	}
	declared := &DesiredPublication{AllTables: v.AllTables, Tables: copyMembers(v.Tables), Schemas: slices.Clone(v.Schemas),
		ViaPartitionRoot: v.ViaPartitionRoot}
	if publish := v.EffectivePublish(); !slices.Equal(publish, PublishOperations()) {
		declared.Publish = publish
	}
	if v.GeneratedColumns != "" && v.GeneratedColumns != GeneratedColumnsNone {
		declared.GeneratedColumns = v.GeneratedColumns
	}
	return declared
}

// Observed is the observation a declaration predicts: its membership, the
// publish set it publishes, its settings and the owner it names, empty where
// it leaves the owner unmanaged.
func (v *DesiredPublication) Observed() *ObservedPublication {
	if v == nil {
		return nil
	}
	return &ObservedPublication{AllTables: v.AllTables, Tables: copyMembers(v.Tables), Schemas: slices.Clone(v.Schemas),
		Publish: v.EffectivePublish(), ViaPartitionRoot: v.ViaPartitionRoot, GeneratedColumns: v.GeneratedColumns, Owner: v.Owner}
}

// DesiredObject validates a declaration and its identity and returns it as a
// feature object. A refusal names the publication.
func DesiredObject(ref objectidentity.ID, publication DesiredPublication) (schemaext.Object, error) {
	if err := ValidateRef(ref); err != nil {
		return schemaext.Object{}, modelError(PublicationKind, schemaext.Desired, err)
	}
	if err := ValidateDesired(&publication); err != nil {
		return schemaext.Object{}, fmt.Errorf("publication %q: %w", Name(ref), err)
	}
	return schemaext.Object{Ref: ref, Value: publication.Copy()}, nil
}

// ObservedObject validates an observation and its identity and returns it as
// a feature object.
func ObservedObject(ref objectidentity.ID, publication ObservedPublication) (schemaext.Object, error) {
	if err := ValidateRef(ref); err != nil {
		return schemaext.Object{}, modelError(PublicationKind, schemaext.Observed, err)
	}
	if err := ValidateObserved(&publication); err != nil {
		return schemaext.Object{}, fmt.Errorf("publication %q: %w", Name(ref), err)
	}
	return schemaext.Object{Ref: ref, Value: publication.Copy()}, nil
}

// ValidateDesired refuses a nil declaration, a membership no statement can
// write, and text a statement cannot carry. Every refusal is a
// [schemaext.InvalidModelError].
func ValidateDesired(v *DesiredPublication) error {
	if v == nil {
		return modelError(PublicationKind, schemaext.Desired, fmt.Errorf("%w: nil publication declaration", schemaext.ErrInvalidValue))
	}
	if err := validateFilters(v.Tables); err != nil {
		return modelError(PublicationKind, schemaext.Desired, err)
	}
	if err := validateShape(publicationShape{v.AllTables, v.Tables, v.Schemas, v.Publish, v.GeneratedColumns, v.Owner}); err != nil {
		return modelError(PublicationKind, schemaext.Desired, err)
	}
	return nil
}

// RefuseFiltered returns the refusal of a declaration a writer cannot write
// because a table it names has a row filter or a column list, and nil for one
// whose every table is whole. A writer that left the filter out would publish
// more of the table than the publication does.
func RefuseFiltered(name string, v *DesiredPublication) error {
	if v == nil {
		return nil
	}
	for _, member := range v.Tables {
		if member.Filtered() {
			return fmt.Errorf("%w: publication %q publishes table %s with a %s; Ptah does not write a publication's row filters "+
				"or column lists, and leaving one out would publish more of the table than the publication does; manage this "+
				"publication by hand, or leave it to a desired schema that declares no publication",
				schemaext.ErrInvalidValue, name, member.Qualified(), FilterWords(member))
		}
	}
	return nil
}

// ValidateObserved refuses a nil observation, a membership the catalog cannot
// report, and text a catalog cannot carry.
func ValidateObserved(v *ObservedPublication) error {
	if v == nil {
		return modelError(PublicationKind, schemaext.Observed, fmt.Errorf("%w: nil publication observation", schemaext.ErrInvalidValue))
	}
	if err := validateFilters(v.Tables); err != nil {
		return modelError(PublicationKind, schemaext.Observed, err)
	}
	if err := validateShape(publicationShape{v.AllTables, v.Tables, v.Schemas, v.Publish, v.GeneratedColumns, v.Owner}); err != nil {
		return modelError(PublicationKind, schemaext.Observed, err)
	}
	return nil
}

// validateFilters checks each member's row filter and column list as text.
func validateFilters(members []Member) error {
	for _, member := range members {
		if err := validTexts("row filter", member.RowFilter); err != nil {
			return err
		}
		if err := validNames("column", member.Columns); err != nil {
			return err
		}
	}
	return nil
}

// FilterWords names what makes member a filtered one: "row filter", "column
// list", or both.
func FilterWords(member Member) string {
	switch {
	case member.RowFilter != "" && len(member.Columns) != 0:
		return "row filter and a column list"
	case member.RowFilter != "":
		return "row filter"
	default:
		return "column list"
	}
}

// publicationShape is what a declaration, an observation and an operation share.
type publicationShape struct {
	allTables        bool
	tables           []Member
	schemas, publish []string
	generated, owner string
}

// validateShape checks what both representations share: one membership form,
// each table and schema once, known operations and settings, and text.
func validateShape(value publicationShape) error {
	allTables, tables, schemas, publish, generated, owner := value.allTables, value.tables, value.schemas, value.publish, value.generated, value.owner
	if allTables && (len(tables) != 0 || len(schemas) != 0) {
		return fmt.Errorf("%w: FOR ALL TABLES names no table and no schema", schemaext.ErrInvalidValue)
	}
	seen := make(map[[2]string]bool, len(tables))
	for _, member := range tables {
		if member.Table == "" {
			return fmt.Errorf("%w: a publication member names its table", schemaext.ErrInvalidValue)
		}
		if err := validTexts("schema", member.Schema, "table", member.Table); err != nil {
			return err
		}
		key := [2]string{member.Schema, member.Table}
		if seen[key] {
			return fmt.Errorf("%w: table %s is named twice", schemaext.ErrInvalidValue, member.Qualified())
		}
		seen[key] = true
	}
	if err := validNames("schema", schemas); err != nil {
		return err
	}
	if len(sortedNames(schemas)) != len(schemas) {
		return fmt.Errorf("%w: a schema is named twice", schemaext.ErrInvalidValue)
	}
	for _, operation := range publish {
		if !slices.Contains(PublishOperations(), operation) {
			return fmt.Errorf("%w: unknown publish operation %q; the operations are %s", schemaext.ErrInvalidValue, operation,
				strings.Join(PublishOperations(), ", "))
		}
	}
	if len(sortedNames(publish)) != len(publish) {
		return fmt.Errorf("%w: a publish operation is named twice", schemaext.ErrInvalidValue)
	}
	switch generated {
	case "", GeneratedColumnsNone, GeneratedColumnsStored:
	default:
		return fmt.Errorf("%w: unknown publish_generated_columns value %q; the values are none and stored", schemaext.ErrInvalidValue, generated)
	}
	return validTexts("owner", owner)
}

// validNames refuses an empty name and text no statement can carry.
func validNames(field string, names []string) error {
	for _, name := range names {
		if name == "" {
			return fmt.Errorf("%w: empty publication %s", schemaext.ErrInvalidValue, field)
		}
		if err := validTexts(field, name); err != nil {
			return err
		}
	}
	return nil
}

// SortMembers returns the members ordered by schema, then table, without
// changing members.
func SortMembers(members []Member) []Member { return sortedMembers(members) }

// SortNames returns the names sorted and once each, without changing names.
func SortNames(names []string) []string { return sortedNames(names) }
