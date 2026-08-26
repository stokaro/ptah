// Package schemastate is the canonical schema state from ADR 0001, built for
// the one object family stokaro/ptah#1350 selects: foreign-key constraints.
//
// # Why it exists
//
// The tree describes a schema two ways. `core/schemamodel.Database` is what an
// authoring source produces and `catalog.Database` is what a catalog
// read produces; they carry different field sets, spell two families
// differently, and four packages under internal/convert exist to move between
// them. Every conversion is a place a fact can be lost with no gate that
// notices, and the diff those two states produce is a list of NAMES -- which
// is why the planner takes the desired description as a second parameter, to
// recover what the diff dropped.
//
// This package is the shape ADR 0001 decides instead: one state, produced
// directly by every adapter, keyed by the identity model from
// stokaro/ptah#1345.
//
// # Scope
//
// It models tables, their columns, and foreign-key constraints. That is the
// slice, not a limitation to be fixed later by the same code: a prototype whose
// scope is implicit is one whose gaps read as answers. A [State] therefore
// carries [State.Scope], the families the adapter that built it actually
// looked at, and a comparison refuses to plan for a family outside it rather
// than reading its absence as a removal.
//
// Scope is deliberately NOT core/coverage.Set. That type answers a different
// question -- which objects a description declines to describe, for the
// families where absence is ambiguous -- and its kind list is closed and does
// not contain tables or constraints, because their absence is never ambiguous.
// Forcing one concept into the other would widen a closed list built for
// another purpose. See ADR 0001, decision 10, and the revision this prototype
// records against it.
package schemastate

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/objectidentity"
)

// ReferentialAction is what a foreign key does to the referencing row when the
// referenced one changes.
//
// It is a typed value rather than the raw string both sources carry, because
// the two sources spell the same action differently: an authoring document
// writes `CASCADE` and a catalog reports `CASCADE`, but a catalog also reports
// `NO ACTION` where a document wrote nothing at all. A comparison over the raw
// strings plans a modification for a foreign key nobody changed.
type ReferentialAction string

// The actions the SQL standard defines. ActionUnspecified is what a source that
// said nothing carries, and it is NOT the same value as ActionNoAction: the
// difference is what lets normalization decide, per target, whether writing
// nothing and writing NO ACTION are the same foreign key.
const (
	ActionUnspecified ReferentialAction = ""
	ActionNoAction    ReferentialAction = "NO ACTION"
	ActionRestrict    ReferentialAction = "RESTRICT"
	ActionCascade     ReferentialAction = "CASCADE"
	ActionSetNull     ReferentialAction = "SET NULL"
	ActionSetDefault  ReferentialAction = "SET DEFAULT"
)

var referentialActions = []ReferentialAction{
	ActionNoAction, ActionRestrict, ActionCascade, ActionSetNull, ActionSetDefault,
}

// Action is one referential action: what the source wrote, and what the target
// applies.
//
// The two are separate for the reason ADR 0001 invariant 2 gives for identifier
// components, and the prototype found that the invariant is not about
// identifiers -- it is about any value where a comparison folds and a renderer
// emits. A source that wrote no ON DELETE and a catalog that reports NO ACTION
// describe one foreign key, so comparison has to see one value; but rendering
// NO ACTION into DDL the author wrote without it changes the text of their
// schema for nothing.
//
// Carrying one folded value for both jobs is how the differential test against
// the existing path first went red.
type Action struct {
	// Source is the spelling the source wrote, empty when it wrote nothing.
	Source string
	// Normalized is what the target applies, which is what comparison reads.
	Normalized ReferentialAction
}

// Empty reports that the source wrote no action at all.
func (a Action) Empty() bool {
	return strings.TrimSpace(a.Source) == ""
}

// ParseReferentialAction resolves a spelling from either source.
//
// It refuses an unknown action rather than passing it through. A referential
// action Ptah does not understand is one it cannot reason about: planning a
// foreign key with it would render a clause whose behavior on delete is
// unknown, and that is the fail-closed rule of ADR 0001 invariant 10.
func ParseReferentialAction(value string) (ReferentialAction, error) {
	folded := ReferentialAction(strings.ToUpper(strings.Join(strings.Fields(value), " ")))
	if folded == ActionUnspecified {
		return ActionUnspecified, nil
	}
	if slices.Contains(referentialActions, folded) {
		return folded, nil
	}
	return "", fmt.Errorf("unknown referential action %q", value)
}

// Column is one column of a table.
//
// The prototype carries the type and nullability because a foreign key depends
// on them: MySQL and MariaDB refuse ALTER TABLE MODIFY on a column that
// participates in one, and the referencing and referenced types have to match
// for the constraint to be creatable at all.
type Column struct {
	ID objectidentity.ID
	// Type is the type the source wrote. TypeNormalized is the same type after
	// the target's folding rules, filled by [Normalize].
	//
	// The pair mirrors [Action], and for the same reason (ADR 0001 invariant
	// 2): a comparison reads the folded value, because a declared `int` and a
	// catalog `integer` are one type, and a renderer writes the source one,
	// because emitting the folded value would put `integer` into DDL an author
	// spelled `int`.
	Type           string
	TypeNormalized string
	Nullable       bool
	// Unique records that this column alone is a key. PostgreSQL, MySQL and
	// MariaDB all refuse a foreign key whose referenced columns are not, so it
	// is a fact the plan needs rather than an attribute of the column nobody
	// reads.
	//
	// The prototype reads single-column uniqueness only: a column that is
	// unique as part of a composite constraint reads as false here, which is
	// conservative in the safe direction -- it blocks a foreign key the target
	// might have accepted, rather than planning one the target refuses.
	Unique bool
	// PrimaryKey marks a column the table's primary key covers.
	//
	// It is separate from Unique, which answers a different question: Unique is
	// "is this column a key on its own", the fact a foreign key's reference
	// depends on, and a primary key is one way to be that but not the only one.
	// Collapsing them loses the half a CREATE TABLE has to write
	// (stokaro/ptah#1662).
	PrimaryKey bool
	// Default is the default the source wrote, and HasDefault says whether it
	// wrote one. The pair is what a `DEFAULT ''` needs: an empty string is a
	// default, and a model with only the string cannot tell it from a column
	// that has none (stokaro/ptah#1662).
	Default    string
	HasDefault bool
	// DefaultIsExpression says which of the two kinds of default it is. A
	// literal is quoted when it is written back and an expression is not, so a
	// model carrying only the string renders `DEFAULT 'now()'` for a column
	// whose default is a function call.
	DefaultIsExpression bool
	// GeneratedExpression and GeneratedKind are what a generated column
	// computes and whether the result is stored. Both sources report them, so
	// unlike Check they are compared as well as rendered: a column that stops
	// being generated, or starts, is a change either side can ask for.
	GeneratedExpression string
	GeneratedKind       string
	// IdentityGeneration is an identity column's generation mode -- ALWAYS or
	// BY DEFAULT -- empty for a column that is not one.
	//
	// IdentityStart, IdentityIncrement and IdentityOptions are the sequence
	// behind it. They are carried because dropping them builds a column whose
	// numbering starts at 1 and steps by 1 whatever the author asked for, and
	// the table is otherwise identical -- so the loss is invisible until a row
	// is inserted.
	IdentityGeneration string
	IdentityStart      string
	IdentityIncrement  string
	IdentityOptions    string
	// CheckName is the constraint name a column-level CHECK carries, empty when
	// the source let the server derive one. It travels with Check for the
	// reason Check does: a CREATE TABLE that dropped it would name the
	// constraint something the author cannot predict, and every later
	// diagnostic about it would use that name.
	CheckName string
	// DomainName and DomainSchema identify the DOMAIN a column is declared
	// with, empty for every column whose declared type is not one.
	//
	// They are separate from Type because the two are compared differently, and
	// getting that wrong is silent. An array's spelling is a TYPE and folds:
	// `character varying(100)[]` and `varchar(100)[]` are one type. A domain's
	// spelling is an IDENTIFIER its author chose and must never fold, because a
	// domain may be NAMED after a type -- measured, a column whose catalog type
	// is the domain `int8` against a declaration of the base type `bigint` read
	// as unchanged, since both folded to `bigint` (stokaro/ptah#1138,
	// stokaro/ptah#1662).
	DomainName   string
	DomainSchema string
	// Charset and Collate are the column's character set and collation on the
	// MySQL-family targets that put them on a column.
	//
	// Carried for rendering and not compared. A catalog reports a column's
	// EFFECTIVE charset even where the declaration inherited it from the
	// table, so comparing the two spellings would report a modification for
	// every column nobody changed.
	Charset string
	Collate string
	// UpdateExpression is MySQL's `ON UPDATE <expr>` clause. It is carried for
	// rendering only, because no catalog read reports it: dropping it from a
	// CREATE builds a column that silently stops maintaining itself.
	UpdateExpression string
	// Check is the column-level CHECK expression the source wrote, empty for a
	// column with none.
	//
	// It is carried for RENDERING and is not compared. A catalog reports a
	// column-level check as a table-level constraint row, so comparing this
	// against a catalog read would report a modification for every column whose
	// check the server merely spells differently; deciding what a check
	// constraint IS belongs to the constraint family (stokaro/ptah#1663). A
	// CREATE TABLE that dropped it would silently create a table without the
	// guarantee its author declared, which is why it is carried at all.
	Check string
	// AutoIncrement marks a column the engine fills by itself. It answers the
	// same question a default does -- does a row without a value for this
	// column get one -- and it answers it for the columns no DEFAULT clause
	// covers, so a rule that only read HasDefault would block an identity
	// column that never needed blocking.
	AutoIncrement bool
}

// Supplied reports whether a row inserted without a value for this column still
// gets one, from a default the source wrote or from the engine itself.
//
// It is the fact an ADD COLUMN NOT NULL depends on: the statement fails on an
// existing row exactly when nothing supplies the value.
func (c Column) Supplied() bool {
	return c.HasDefault || c.AutoIncrement
}

// Table is a table and the columns a foreign key can reference.
type Table struct {
	Columns []Column
	// EstimatedRows and RowStatsUnknown are the best evidence a plan has about
	// whether the table holds anything, and they are a PAIR for the reason
	// catalog.Table carries them as one: a zero estimate from a server
	// that keeps statistics means "empty at the last analyze", and a zero from
	// a server that keeps none means nothing at all. Reading the number alone
	// turns the second into the first.
	//
	// Only a catalog read fills them. A description says what a table should
	// look like and nothing about what is in it, so [FromDescription] marks
	// them unknown rather than leaving a zero that reads as an empty table
	// (stokaro/ptah#1662).
	EstimatedRows   int64
	RowStatsUnknown bool
	// Strict and WithoutRowID are the SQLite table options. They are typed
	// facts here rather than the untyped option map the AST carries: a map is
	// the RENDERER's contract, and a model that held one would make every
	// consumer parse a string to ask a yes-or-no question.
	//
	// They are carried for rendering and not compared. Changing either on an
	// existing table needs a rebuild -- SQLite has no ALTER for them -- and
	// that is a whole-table operation this family does not plan
	// (stokaro/ptah#1662).
	Strict       bool
	WithoutRowID bool
	// Engine, Charset and Collate are the MySQL-family table options.
	//
	// Carried for rendering and not compared, because no catalog read reports
	// them: a CREATE that dropped the engine builds a table on the server's
	// default, which on a server configured differently is a different storage
	// engine with different transactional behavior.
	Engine  string
	Charset string
	Collate string
	// AutoIncrement is the value a MySQL-family table's counter starts at.
	AutoIncrement string
	// RowTTL is the CockroachDB row-level TTL policy a table declares.
	//
	// It is `*ast.RowTTLSpec` rather than a type of this package, unlike
	// [Partition]. Both sides already carry that exact type -- the authoring
	// model and `catalog.Table` both hold it -- so a mirror here would
	// be a third shape to keep in step for no gain, and the comparison rules
	// its fields carry live with it.
	RowTTL *ast.RowTTLSpec
	// VirtualModule is the SQLite module that owns a virtual table, from the
	// USING clause, and VirtualArguments is the text between its parentheses,
	// verbatim -- module arguments are not SQL, only the module interprets
	// them.
	//
	// A non-empty module makes the table a different STATEMENT: a CREATE that
	// ignored it built an ordinary table of the same name where an FTS5 index
	// was declared, which is a different object that accepts different SQL
	// (stokaro/ptah#1028).
	VirtualModule    string
	VirtualArguments string
	// Partition is the partitioning a table declares, nil for a table with
	// none.
	//
	// It is carried for rendering and not compared. A catalog reports only
	// THAT a table is partitioned -- `catalog.Table.Partitioned` is a boolean and no
	// read returns the key -- so a comparison would hold a declared key against
	// nothing and report a change for every partitioned table. Dropping it from
	// a CREATE builds an ordinary table instead, which accepts every row the
	// partitioned one would and distributes none of them.
	Partition *Partition
	// PrimaryKeyInclude carries PostgreSQL's INCLUDE payload columns for a
	// table-level primary key.
	//
	// It also decides HOW the key is written: a key with payload columns cannot
	// be declared on a column, so a table carrying one renders its key as a
	// table-level constraint however few columns it covers.
	PrimaryKeyInclude []string
}

// Populated reports whether the table is known to hold rows, and whether that
// answer is knowable at all.
//
// Three answers, not two, because the middle one is what an ADD COLUMN NOT NULL
// turns on: a table the plan knows is empty accepts it, a table the plan knows
// has rows refuses it, and a table with no statistics is a measurement nobody
// has taken rather than an empty table.
func (t Table) Populated() (populated, known bool) {
	if t.RowStatsUnknown {
		return false, false
	}
	return t.EstimatedRows > 0, true
}

// Column returns the named column, folding through the identity model so the
// two sources' spellings resolve to one column.
func (t Table) Column(id objectidentity.ID) (Column, bool) {
	for _, column := range t.Columns {
		if column.ID.Key() == id.Key() {
			return column, true
		}
	}
	return Column{}, false
}

// ForeignKey is a foreign-key constraint.
//
// The referenced table is an identity rather than the string each source wrote,
// which is the point of ADR 0001 invariant 3: the reference is resolved once,
// by the adapter that knows its source's quoting and defaulting rules, and no
// later stage re-parses a name.
type ForeignKey struct {
	Columns           []string
	ReferencedTable   objectidentity.ID
	ReferencedColumns []string
	OnDelete          Action
	OnUpdate          Action
}

// Provenance is where an object came from. It never joins an identity: two
// objects that differ only in which file declared them are one object (ADR 0001
// invariant 5).
type Provenance struct {
	// Source is HOW the fact was learned: [coverage.Observed] for a row Ptah
	// read out of a catalog, [coverage.Declared] for something a description
	// stated.
	//
	// It is the closed list a coverage record carries rather than a free
	// string, for the reason the closed list exists: a diagnostic that has to
	// compare "catalog" against "description" is comparing spellings, and a
	// spelling nothing validates drifts. The two halves of "what Ptah knows and
	// how" therefore share one vocabulary (stokaro/ptah#1346).
	Source coverage.Provenance
	// Location is a file position or the catalog relation a row came from. It
	// says WHICH one, where Source says what kind, and it stays free text
	// because a file position and a catalog relation have no closed list
	// between them.
	Location string
}

// Validate reports whether the provenance names a source this build
// understands.
func (p Provenance) Validate() error {
	if !p.Source.Valid() {
		return fmt.Errorf("unknown object provenance source %q", p.Source)
	}
	return nil
}

// Declared reports whether the fact was stated by a description rather than
// read out of a catalog.
//
// It is the question the column type fold turns on: a DECLARED type is asked
// in the renderer's spelling first, because Oracle has no counterpart for most
// declared type names and SQLite stores the declared text verbatim, and an
// OBSERVED one is already what the target holds (stokaro/ptah#1662).
func (p Provenance) Declared() bool { return p.Source == coverage.Declared }

// UniqueKey is a uniqueness guarantee over one or more columns.
//
// A UNIQUE constraint, a primary key and a unique index all answer the one
// question a foreign key asks -- is this column list a key -- and every source
// spells at least two of them differently. Keeping them as separate shapes past
// the adapter means asking that question once per shape, and the prototype
// asked it of single columns only: a column unique as part of a COMPOSITE
// constraint read as not unique, which blocked a foreign key the target
// accepts (stokaro/ptah#1662).
type UniqueKey struct {
	// Columns is the column list the guarantee covers, in the order the source
	// wrote it. Comparison is order-insensitive -- a key on (a, b) is the same
	// guarantee as one on (b, a) -- and the order is kept because a renderer
	// writing the constraint back has to write one.
	Columns []string
	// Standalone marks a guarantee the target holds as an object of its own: a
	// named UNIQUE constraint, added and dropped by its own statement.
	//
	// A column's own flag is not standalone -- it renders beside its column, so
	// planning it as a constraint change would declare the same guarantee twice
	// -- and neither is a primary key, whose statements carry different risk
	// and whose name a target derives. Both still answer the foreign-key
	// question, which is why they are objects at all (stokaro/ptah#1663).
	Standalone bool
}

// Covers reports whether this key guarantees uniqueness for exactly the given
// column list.
//
// Exactly, not "contains". A unique constraint on (a, b) makes the PAIR unique
// and says nothing about either column alone, so a foreign key referencing a
// alone is not made legal by it -- and every engine Ptah targets refuses one.
func (u UniqueKey) Covers(columns []string, fold func(string) string) bool {
	return slices.Equal(foldedSet(u.Columns, fold), foldedSet(columns, fold))
}

// foldedSet is a column list in its comparison form: folded by the target's
// rules and sorted, because a key on (a, b) is the same guarantee as one on
// (b, a) and no engine distinguishes them.
func foldedSet(columns []string, fold func(string) string) []string {
	folded := make([]string, 0, len(columns))
	for _, column := range columns {
		folded = append(folded, fold(column))
	}
	slices.Sort(folded)
	return folded
}

// Partition is a table's partitioning method and key.
//
// It is a type of this package rather than the authoring model's or the AST's,
// because the canonical state is what both sides produce and neither of those
// is available to both. The shapes agree today, and the adapters translate
// rather than alias so a change to either does not reach through.
type Partition struct {
	// Type is the method: RANGE, LIST or HASH.
	Type string
	// Parts are the key's columns or expressions, in the order they were
	// written. Order is part of a partition key, unlike a unique constraint's
	// column set: RANGE (a, b) and RANGE (b, a) partition differently.
	Parts []PartitionPart
}

// PartitionPart is one column or expression of a partition key.
type PartitionPart struct {
	Name string
	Expr string
}

// Index is a table index.
//
// It is its own family rather than a constraint that happens to have columns,
// because its IDENTITY is scoped differently: `identifier.IndexNamespace`
// decides whether an index name must be unique within its table or within its
// schema, and a model that assumed the table scope would merge two indexes on
// SQLite and split one on MySQL (stokaro/ptah#1663).
type Index struct {
	// Table is the table the index is on.
	//
	// It is on the payload rather than read off the identity because the
	// identity does not always carry it: on a target that scopes index names to
	// a SCHEMA the owning table is not part of the identity at all, and a
	// renderer still has to write `ON <table>`.
	Table objectidentity.ID
	// Columns is the key, in order. Order is part of an index: a btree on
	// (a, b) answers a query on `a` alone and one on (b, a) does not.
	Columns []string
	// Unique reports an index that also guarantees uniqueness. Such an index is
	// a [UniqueKey] as well, which is what lets a foreign key reference its
	// columns.
	Unique bool
	// Concurrent records that the source asked for a non-locking index build.
	//
	// Only a description can ask: a catalog reports the index, not how it was
	// built. It is not compared for the same reason -- a request about HOW to
	// apply a change is not a property of the object, so an index built
	// concurrently and one built with a lock are the same index.
	Concurrent bool
	// KeyPartsIncomplete reports that the reader could not name every part of
	// the key -- a MySQL functional key part such as `KEY idx ((b + 1))` has a
	// NULL COLUMN_NAME in information_schema.STATISTICS.
	//
	// A comparison must not read Columns as the whole key when this is set: it
	// would plan a rebuild on every run for a key that never changed
	// (stokaro/ptah#1663, and the same fact catalog.Index records).
	KeyPartsIncomplete bool
	// RequiresExtensions names the extensions this index cannot be built
	// without.
	//
	// It is carried rather than derived because it cannot be read off the
	// index's own text. Measured on PostgreSQL 17.10,
	// `CREATE INDEX t_gin ON t USING gin (n int4_ops)` over an integer column
	// needs btree_gin and is stored, and rendered, as `USING gin (n)`: neither
	// the extension nor the operator class appears anywhere in the statement,
	// because PostgreSQL prints a class exactly when it is NOT the default. The
	// reader resolves it against pg_depend instead, and a state that dropped
	// the answer would leave every later stage to guess it from `gin` -- which
	// pins btree_gin to indexes that do not need it, since tsvector, jsonb and
	// array columns have core GIN classes (stokaro/ptah#1286, stokaro/ptah#1663).
	//
	// It is not compared. A description that never named an extension and a
	// read that resolved one describe the same index, and comparing them would
	// plan a rebuild on every run for an index nobody changed.
	RequiresExtensions []string
}

// TableConstraint is a constraint kind whose whole definition is one clause the
// target holds as an object: CHECK, PRIMARY KEY and EXCLUDE.
//
// They share a type because they share a shape -- a name, a table, and a body
// no engine alters in place -- and differ only in the body. Modelling each as
// its own payload would give the comparison three copies of one rule and the
// planner three places to forget the guard.
//
// UNIQUE is deliberately NOT here: it is a [UniqueKey], because it answers the
// question a foreign key asks and the others do not (stokaro/ptah#1663).
type TableConstraint struct {
	// Kind is the family, spelled the way both sources spell it: CHECK,
	// PRIMARY KEY or EXCLUDE.
	Kind string
	// ConstraintName is the name to write in DDL, which is not always the name
	// in the identity.
	//
	// A PRIMARY KEY is identified by its TABLE, because a table has at most one
	// and a description declares it without a name -- `PrimaryKey []string` has
	// nowhere to put one. Comparing by name would hold an unnamed declaration
	// against a server-derived `parent_pkey` and plan a drop for every primary
	// key on every run. The name is still needed to DROP one, which is why it
	// travels here: empty on the side that declared none, and the server's on
	// the side that read it (stokaro/ptah#1663).
	ConstraintName string
	// Table is the table the constraint is on. It is on the payload for the
	// reason [Index.Table] is: a renderer needs it, and the identity is not
	// obliged to carry it.
	Table objectidentity.ID
	// Expression is a CHECK's condition, empty for the other kinds.
	Expression string
	// Columns is a PRIMARY KEY's column list, empty for the other kinds.
	Columns []string
	// UsingMethod, Elements and Where are an EXCLUDE's index method, its
	// element list and its optional predicate.
	UsingMethod string
	Elements    string
	Where       string
	// RequiresExtensions names the extensions the constraint's backing index
	// needs. It cannot be read off the constraint's text, which is why the
	// catalog reports it as a field of its own (stokaro/ptah#1286).
	RequiresExtensions []string
}

// Object is one schema object in the canonical state.
//
// Exactly one payload pointer is set, decided by ID.Kind. It is a struct with
// typed payloads rather than an interface because ADR 0001 decision 1 rules out
// the type switch: a stage that walks every object must not be able to miss a
// family by forgetting a case.
type Object struct {
	ID         objectidentity.ID
	Table      *Table
	Column     *Column
	ForeignKey *ForeignKey
	UniqueKey  *UniqueKey
	Index      *Index
	Constraint *TableConstraint
	Policy     *Policy
	Grant      *Grant
	Provenance Provenance
}

// State is a schema description: objects keyed by identity, the families the
// reader that built it looked at, and what it declines to describe.
type State struct {
	objects  map[objectidentity.Key]Object
	order    []objectidentity.Key
	scope    []objectidentity.Kind
	coverage coverage.Set
	dialect  string

	// normalized and profile record that the target's rules have been applied
	// and which target's they were. They live on the state rather than beside
	// it so a stage that requires normalization can refuse a state that has not
	// had it, and so applying it twice is detectable.
	normalized bool
	profile    Profile
}

// New returns an empty state for a dialect, declaring the families its builder
// describes.
//
// The scope is required rather than defaulted. An adapter that forgets to say
// what it read is one whose silence about a family reads as "there are none of
// those", and that is the failure mode ADR 0001 invariant 4 exists to stop.
func New(dialect string, scope ...objectidentity.Kind) *State {
	return &State{
		objects: make(map[objectidentity.Key]Object),
		scope:   slices.Clone(scope),
		dialect: dialect,
	}
}

// Dialect returns the target the state was read for or written against.
func (s *State) Dialect() string {
	return s.dialect
}

// Scope returns the families the reader that built this state looked at.
func (s *State) Scope() []objectidentity.Kind {
	return slices.Clone(s.scope)
}

// Describes reports whether this state's reader looked at a family at all.
func (s *State) Describes(kind objectidentity.Kind) bool {
	return slices.Contains(s.scope, kind)
}

// Coverage returns what the description declines to describe, for the families
// where absence is ambiguous.
func (s *State) Coverage() coverage.Set {
	return s.coverage
}

// WithCoverage records what the description declines to describe.
func (s *State) WithCoverage(set coverage.Set) *State {
	s.coverage = set
	return s
}

// Add records an object, reporting the one already present under the same
// identity.
//
// A second Add under one identity is a collision rather than an overwrite:
// silently replacing is how one of two objects the target cannot hold both of
// disappears before anything can report it.
func (s *State) Add(object Object) (existing Object, collided bool) {
	key := object.ID.Key()
	if previous, ok := s.objects[key]; ok {
		return previous, true
	}
	s.objects[key] = object
	s.order = append(s.order, key)
	return Object{}, false
}

// Get returns the object with an identity.
func (s *State) Get(id objectidentity.ID) (Object, bool) {
	object, ok := s.objects[id.Key()]
	return object, ok
}

// Objects returns every object in a deterministic order.
//
// The order is the order objects were added, not map iteration order. Go
// randomizes the latter per run, and a plan whose statement order changes
// between two runs over one input is a plan nobody can review.
func (s *State) Objects() []Object {
	out := make([]Object, 0, len(s.order))
	for _, key := range s.order {
		out = append(out, s.objects[key])
	}
	return out
}

// OfKind returns every object of one family, in the same deterministic order.
func (s *State) OfKind(kind objectidentity.Kind) []Object {
	out := make([]Object, 0)
	for _, object := range s.Objects() {
		if object.ID.Kind == kind {
			out = append(out, object)
		}
	}
	return out
}

// Len returns the number of objects.
func (s *State) Len() int {
	return len(s.objects)
}

// DescribesTable reports whether the absence of a table from a description is
// authoritative.
//
// A table is covered through the SCHEMA that owns it rather than by its own
// name. The schema record is what a reader that skipped a whole namespace can
// produce, and the table names inside it are exactly what such a reader does
// not know -- migration/schemadiff's comparator makes the same choice, and the
// two must not disagree about which silence is a removal
// (stokaro/ptah#1276, stokaro/ptah#1662).
func DescribesTable(state *State, id objectidentity.ID) bool {
	return state.Coverage().DescribesSchema(id.Schema.Source)
}

// IndexRequiredFacts are the capability keys an index change cannot be planned
// without.
//
// A concurrent build is the only one, and it is a fact about the TARGET rather
// than a branch inside a dialect planner: a target that cannot do it must
// refuse the change and name the key, not quietly render a locking build the
// author did not ask for (stokaro/ptah#1663, ADR 0001 decision 5).
func IndexRequiredFacts(index Index) []capability.Capability {
	if !index.Concurrent {
		return nil
	}
	return []capability.Capability{capability.CreateIndexConcurrently}
}
