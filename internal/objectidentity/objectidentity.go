// Package objectidentity is the one identity and reference model schema
// comparison, dependency analysis, planning, diagnostics and rendering share.
//
// # Why it exists
//
// Every object family had grown its own key, and four defects came from the
// same root: a key that was not injective for the domain it modeled.
//
//   - stokaro/ptah#1283 keyed grants by a delimiter-joined string, so two
//     distinct grants collapsed into one;
//   - stokaro/ptah#1276 keyed row-level-security policies by the policy name
//     alone, so one policy name on two tables collapsed;
//   - stokaro/ptah#1311 replaced that with a dotted table-plus-policy string,
//     which collided whenever a valid identifier component contained a dot;
//   - stokaro/ptah#1302 compared domain columns without schema identity, and
//     the comparator planned a destructive drop for a domain that existed.
//
// The shape common to all four is a STRING standing in for a tuple. A joined
// string cannot be injective when its components may contain the joining
// character, and a tuple with a component missing cannot be injective at all.
// So an [ID] here is a struct with typed components, compared as a struct, and
// never rendered into a key (stokaro/ptah#1345).
//
// # Source spelling and comparison identity are different values
//
// [Part] carries both, deliberately. The spelling an author wrote is what a
// diagnostic must quote back and what a renderer must emit; the normalized form
// is what comparison uses, and it is lossy by design -- PostgreSQL folds an
// unquoted identifier to lower case, so `Users` and `users` are one object
// while `"Users"` and `users` are two. Keeping one value for both jobs is how a
// comparator ends up planning a rename that changes nothing, or a renderer ends
// up emitting a name the target cannot resolve.
//
// # Fail closed
//
// Where a source cannot supply a component that safe resolution needs, the
// answer is a refusal naming what is missing, never resolution to a nearby
// object. [Resolve] reports ambiguity, dangling targets and normalized
// collisions as distinct errors, because the three ask the author for different
// things.
//
// # The invariants
//
// docs/object_identity.md is the canonical statement of the eight invariants
// this package enforces, of which consumer keys through it and which still
// hold private ones, and of the deletion criterion for every adapter here.
// Read it before adding a family or changing how one folds.
package objectidentity

import (
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform/identifier"
)

// Kind names the object family an identity belongs to.
//
// It is part of the identity rather than context around it: a table and a view
// may share a name in some catalogs and not others, and a reference that does
// not carry the kind cannot tell "this name is free" from "this name belongs to
// something else".
type Kind string

const (
	KindSchema     Kind = "schema"
	KindTable      Kind = "table"
	KindView       Kind = "view"
	KindColumn     Kind = "column"
	KindIndex      Kind = "index"
	KindConstraint Kind = "constraint"
	KindDomain     Kind = "domain"
	KindEnum       Kind = "enum"
	KindSequence   Kind = "sequence"
	KindFunction   Kind = "function"
	// KindProcedure separates a procedure from a function of the same name.
	// Both engines that model procedures allow one schema to hold both, and a
	// comparison that folded them together would report the procedure as a
	// changed function (stokaro/ptah#1722).
	KindProcedure Kind = "procedure"
	KindTrigger   Kind = "trigger"
	KindPolicy    Kind = "policy"
	KindRole      Kind = "role"
	KindGrant     Kind = "grant"
	KindExtension Kind = "extension"
	KindComposite Kind = "composite type"
	KindRange     Kind = "range type"
	KindMatView   Kind = "materialized view"
)

// Part is one component of a qualified name: what the author wrote, and what
// comparison uses.
//
// Source keeps the spelling including any quoting, because that is what a
// diagnostic quotes and what a renderer emits. Normalized is the folded form
// the target's identifier semantics produce, and two parts are the same
// component exactly when their Normalized values match.
type Part struct {
	// Source is the component as written, quoting included.
	Source string
	// Normalized is the comparison form under the target's semantics.
	Normalized string
	// Quoted records whether the source spelling was quoted. It is a FACT about
	// the spelling rather than part of the key: the quotes are inside
	// Normalized already, which is what keeps `"Users"` and `Users` apart on a
	// folding target, and a renderer asks this instead of parsing the string.
	Quoted bool
}

// Empty reports whether the component was absent, which is different from
// present-and-empty: an absent schema means "the author did not qualify this",
// and a caller deciding what to do about that needs to know.
func (p Part) Empty() bool {
	return p.Source == "" && p.Normalized == ""
}

// ID is the identity of one database object.
//
// It is compared as a struct. Nothing here builds a string key, which is the
// defect class this package exists to remove: every component is its own field,
// so a component containing the character some previous key joined on cannot
// collide with a component boundary.
type ID struct {
	// Kind is the object family.
	Kind Kind
	// Catalog and Schema qualify the object. Catalog is empty on every dialect
	// Ptah targets today and is modeled because a reference that cannot express
	// it cannot refuse an invalid scope transition either.
	Catalog Part
	Schema  Part
	// Parent owns the object for a family that has one: the table a column,
	// index, constraint or policy belongs to. Empty for a schema-level object.
	Parent Part
	// Name is the object's own name.
	Name Part
	// Signature is a routine's overload identity -- the normalized argument
	// type list. Two functions with the same name and different signatures are
	// different objects, and a model without this cannot say so.
	Signature string
}

// Key is the value equality is decided on, and the type a map keys on.
//
// It is a STRUCT, not a string. That is the whole point: a struct key cannot
// have a component boundary forged by a component's own content, which is how
// stokaro/ptah#1311's dotted policy key collided, and it cannot silently omit a
// component the way stokaro/ptah#1276 and #1302 did -- a missing one is the
// zero value of its own field rather than an absent substring.
//
// Its fields are unexported so nothing outside this package can build one
// without going through a [Builder], which is what keeps the folding rule in
// one place. It stays usable as a map key regardless: Go compares unexported
// fields like any other.
type Key struct {
	kind      Kind
	catalog   string
	schema    string
	parent    string
	name      string
	signature string
}

// Kind returns the object family the key belongs to, which a diagnostic needs
// and which is the only component readable back out.
func (k Key) Kind() Kind {
	return k.kind
}

// Key returns the comparison identity: two IDs identify the same object exactly
// when their Keys are equal.
func (id ID) Key() Key {
	return Key{
		kind:      id.Kind,
		catalog:   id.Catalog.Normalized,
		schema:    id.Schema.Normalized,
		parent:    id.Parent.Normalized,
		name:      id.Name.Normalized,
		signature: id.Signature,
	}
}

// Equal reports whether two identities name the same object.
func (id ID) Equal(other ID) bool {
	return id.Key() == other.Key()
}

// String renders the identity for a diagnostic, in the spelling the author
// wrote. It is never used as a key -- Key is -- so a name containing a dot
// renders confusingly at worst rather than colliding.
func (id ID) String() string {
	parts := make([]string, 0, 4)
	for _, part := range []Part{id.Catalog, id.Schema, id.Parent} {
		if !part.Empty() {
			parts = append(parts, part.Source)
		}
	}
	parts = append(parts, id.Name.Source)
	rendered := string(id.Kind) + " " + strings.Join(parts, ".")
	if id.Signature != "" {
		rendered += "(" + id.Signature + ")"
	}
	return rendered
}

// Builder turns source-spelled names into identities under one target's
// identifier semantics.
//
// It exists so the folding rule is applied in one place. Every previous key
// applied its own -- strings.ToLower here, a Semantics call there, nothing at
// all in a third -- and a comparison whose two sides folded differently is the
// #1302 shape: an object that exists reads as absent.
type Builder struct {
	semantics identifier.Semantics
}

// NewBuilder returns a Builder for one target's semantics.
func NewBuilder(semantics identifier.Semantics) Builder {
	return Builder{semantics: semantics}
}

// Semantics returns the identifier rules this builder folds with.
func (b Builder) Semantics() identifier.Semantics {
	return b.semantics
}

// namePart folds one component with the comparison rule that governs it.
//
// The component is folded AS WRITTEN, quotes included, which is what
// [identifier.Semantics] does and therefore what every key in the tree already
// compared. Unquoting first would produce a different normalized string for the
// same object and silently split this model from the keys it is replacing --
// the distinction between `"Users"` and `Users` survives either way, because
// folding `"Users"` yields `"users"` and folding `Users` yields `users`.
//
// Quoted is recorded beside it as the fact rather than as part of the key, so a
// renderer can ask whether the author quoted without parsing the string back.
func (b Builder) namePart(value string, key func(string) string) Part {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return Part{}
	}
	_, quoted := unquote(trimmed)
	return Part{Source: trimmed, Normalized: key(trimmed), Quoted: quoted}
}

// Table builds the identity of a table from a possibly-qualified name.
func (b Builder) Table(qualified string) ID {
	schema, name := splitQualified(qualified)
	return ID{
		Kind:   KindTable,
		Schema: b.schemaPart(schema),
		Name:   b.namePart(name, b.semantics.TableIdentityKey),
	}
}

// TableParts builds a table identity from components the caller already has,
// trimming each and defaulting an absent schema.
//
// It exists beside [Builder.Table] because a caller holding the two components
// separately must not have them rejoined and re-split: a name containing a dot
// would come back as a different pair.
func (b Builder) TableParts(schema, name string) ID {
	return ID{
		Kind:   KindTable,
		Schema: b.schemaPart(strings.TrimSpace(schema)),
		Name:   b.namePart(strings.TrimSpace(name), b.semantics.TableIdentityKey),
	}
}

// TablePartsVerbatim builds a table identity without trimming either component.
//
// SQLite is why: a quoted leading or trailing space is part of a SQLite table
// name, so trimming one there merges two distinct tables. Every other caller
// wants [Builder.TableParts], whose trimming absorbs the incidental whitespace
// a schema file or a catalog row carries around a name.
func (b Builder) TablePartsVerbatim(schema, name string) ID {
	return ID{
		Kind:   KindTable,
		Schema: b.schemaPartVerbatim(schema),
		Name:   Part{Source: name, Normalized: b.semantics.TableIdentityKey(name)},
	}
}

// schemaPartVerbatim defaults an absent schema without trimming a present one.
func (b Builder) schemaPartVerbatim(schema string) Part {
	if schema == "" {
		schema = b.semantics.DefaultSchema
	}
	if schema == "" {
		return Part{}
	}
	return Part{Source: schema, Normalized: b.semantics.TableIdentityKey(schema)}
}

// Column builds the identity of a column owned by a table.
func (b Builder) Column(qualifiedTable, column string) ID {
	table := b.Table(qualifiedTable)
	return ID{
		Kind:   KindColumn,
		Schema: table.Schema,
		Parent: table.Name,
		Name:   b.namePart(column, b.semantics.ColumnIdentityKey),
	}
}

// ColumnParts builds a column identity from components the caller already has.
//
// It exists beside [Builder.Column] for the reason [Builder.TableParts] does: a
// caller holding the table's schema and name separately must not have them
// rejoined into one string and re-split, because a table whose own name
// contains a dot -- written `"tenant.data"` -- comes back as a different pair.
func (b Builder) ColumnParts(schema, table, column string) ID {
	owner := b.TableParts(schema, table)
	return ID{
		Kind:   KindColumn,
		Schema: owner.Schema,
		Parent: owner.Name,
		Name:   b.namePart(column, b.semantics.ColumnIdentityKey),
	}
}

// Domain builds the identity of a domain type.
//
// The schema is part of it because stokaro/ptah#1302 is what happens when it is
// not: a domain compared without schema identity read as absent, and the
// comparator planned a destructive drop for an object that existed.
func (b Builder) Domain(qualified string) ID {
	schema, name := splitQualified(qualified)
	return b.SchemaScopedParts(KindDomain, schema, name)
}

// SchemaScopedParts builds the identity of an object whose name is unique
// within a schema rather than within a table: a sequence, an enum, a view, a
// domain.
//
// The kind is a parameter rather than one method per family because these
// identities differ only in it, and a family that gets its own copy of this
// function is a family whose key can drift from the rest -- which is the drift
// stokaro/ptah#1345 exists to stop. It takes components rather than one
// qualified string for the reason [Builder.TableParts] does.
func (b Builder) SchemaScopedParts(kind Kind, schema, name string) ID {
	return ID{
		Kind:   kind,
		Schema: b.schemaPart(strings.TrimSpace(schema)),
		Name:   b.namePart(strings.TrimSpace(name), b.semantics.TableIdentityKey),
	}
}

// Policy builds the identity of a row-level-security policy.
//
// The owning table is a component rather than a prefix. A PostgreSQL policy
// name is scoped to its table -- the same name succeeds once on each of two
// tables -- so a key without the table collapses them (stokaro/ptah#1276), and
// a key that JOINS the two with a dot collides whenever either contains one
// (stokaro/ptah#1311).
func (b Builder) Policy(qualifiedTable, policy string) ID {
	table := b.Table(qualifiedTable)
	return ID{
		Kind:   KindPolicy,
		Schema: table.Schema,
		Parent: table.Name,
		Name:   b.namePart(policy, b.semantics.TableIdentityKey),
	}
}

// PolicyParts builds a policy identity from components the caller already has.
//
// A catalog reports the three separately and reports each BARE -- a table named
// `orders.2024` arrives as those exact bytes, with no quoting to mark where the
// name begins. That is why this exists beside [Builder.Policy] and why the
// components are never joined: `orders.2024` with policy `p` and `orders` with
// policy `2024.p` render as one string under any separator, and one of two
// distinct policies is then dropped (stokaro/ptah#1311).
func (b Builder) PolicyParts(schema, table, policy string) ID {
	owner := b.TableParts(schema, table)
	return ID{
		Kind:   KindPolicy,
		Schema: owner.Schema,
		Parent: owner.Name,
		Name:   b.namePart(strings.TrimSpace(policy), b.semantics.TableIdentityKey),
	}
}

// GrantParts builds the identity of one privilege grant from components the
// caller already holds separately.
//
// A grant is a triple -- a role, a privilege, and the object they are about --
// and the triple is the identity. Keying it on a delimiter-joined rendering of
// the three collapsed two distinct grants (stokaro/ptah#1283), so each
// component gets its own slot here and none of them can forge a boundary in
// another.
//
// The object goes in the schema and parent slots, and a SCHEMA grant is the
// case that decides their meaning: its target IS a schema, so there is no
// owning schema to resolve, and it takes the schema slot with the parent left
// empty. That is also what keeps `GRANT ... ON SCHEMA app` from colliding with
// `GRANT ... ON TABLE app` -- the first has no parent and the second does.
// Tables and sequences share one namespace on every target Ptah supports, so a
// TABLE and a SEQUENCE grant cannot name the same object and be different
// grants.
func (b Builder) GrantParts(schema, object, role, privilege string) ID {
	owner := b.TableParts(schema, object)
	return ID{
		Kind:      KindGrant,
		Schema:    owner.Schema,
		Parent:    owner.Name,
		Name:      b.namePart(strings.TrimSpace(role), b.semantics.TableIdentityKey),
		Signature: strings.ToUpper(strings.TrimSpace(privilege)),
	}
}

// Index builds the identity of an index.
//
// Whether the name is scoped to the table or to the schema is the target's
// rule, not this package's: on a table-scoped target the owning table is part
// of the identity, and on a schema-scoped one it is not, because two tables in
// one schema cannot then carry the same index name.
func (b Builder) Index(qualifiedTable, index string) ID {
	table := b.Table(qualifiedTable)
	id := ID{
		Kind:   KindIndex,
		Schema: table.Schema,
		Name:   b.namePart(index, b.semantics.IndexIdentityKey),
	}
	if b.semantics.IndexNamespace != identifier.IndexNamespaceSchema {
		id.Parent = table.Name
	}
	return id
}

// Constraint builds the identity of a constraint owned by a table.
func (b Builder) Constraint(qualifiedTable, constraint string) ID {
	table := b.Table(qualifiedTable)
	return ID{
		Kind:   KindConstraint,
		Schema: table.Schema,
		Parent: table.Name,
		Name:   b.namePart(constraint, b.semantics.TableIdentityKey),
	}
}

// ConstraintPartsVerbatim builds a constraint identity from an owning table
// spelling and a constraint name, folding and trimming neither.
//
// It is what a caller holding two raw catalog strings needs when it must not
// change what they mean. The planner is that caller: it pairs a constraint
// removal with an addition, both spellings arrive from the same diff, and a
// fold applied on only one side of the pipeline would pair a drop with a
// different constraint than the comparator intended.
//
// Everything else wants [Builder.Constraint], which parses the qualification
// and folds under the target's rule.
func (b Builder) ConstraintPartsVerbatim(table, constraint string) ID {
	return ID{
		Kind:   KindConstraint,
		Parent: Part{Source: table, Normalized: table},
		Name:   Part{Source: constraint, Normalized: constraint},
	}
}

// ConstraintParts builds a constraint identity from components the caller
// already has, folding each under the target's rule.
//
// It exists beside [Builder.Constraint] for the reason [Builder.TableParts]
// does, and beside [Builder.ConstraintPartsVerbatim] for a different one: this
// is what a source ADAPTER wants, where the spellings arrive unfolded and the
// target's rule has not been applied yet. The verbatim form is for a consumer
// downstream of a comparator that already folded them.
func (b Builder) ConstraintParts(schema, table, constraint string) ID {
	owner := b.TableParts(schema, table)
	return ID{
		Kind:   KindConstraint,
		Schema: owner.Schema,
		Parent: owner.Name,
		Name:   b.namePart(strings.TrimSpace(constraint), b.semantics.IndexIdentityKey),
	}
}

// Role builds the identity of a role, which no dialect Ptah targets qualifies
// by schema.
func (b Builder) Role(role string) ID {
	return ID{Kind: KindRole, Name: b.namePart(role, b.semantics.TableIdentityKey)}
}

// Function builds the identity of a routine, including its overload signature.
//
// Two routines with one name and different argument types are two objects. A
// model without the signature cannot say that, and a comparator built on one
// plans a replacement of whichever it happened to see first.
func (b Builder) Function(qualified, signature string) ID {
	schema, name := splitQualified(qualified)
	return ID{
		Kind:      KindFunction,
		Schema:    b.schemaPart(schema),
		Name:      b.namePart(name, b.semantics.TableIdentityKey),
		Signature: normalizeSignature(signature),
	}
}

// schemaPart folds a schema name, defaulting an unqualified one to the target's
// default schema so `users` and `public.users` are one table.
func (b Builder) schemaPart(schema string) Part {
	if strings.TrimSpace(schema) == "" {
		if b.semantics.DefaultSchema == "" {
			return Part{}
		}
		return b.namePart(b.semantics.DefaultSchema, b.semantics.TableIdentityKey)
	}
	return b.namePart(schema, b.semantics.TableIdentityKey)
}

// normalizeSignature folds a routine argument list to its comparison form:
// lower case, one space after each comma, no surrounding parentheses.
func normalizeSignature(signature string) string {
	trimmed := strings.TrimSpace(signature)
	trimmed = strings.TrimSuffix(strings.TrimPrefix(trimmed, "("), ")")
	if trimmed == "" {
		return ""
	}
	arguments := strings.Split(trimmed, ",")
	for i, argument := range arguments {
		arguments[i] = strings.ToLower(strings.Join(strings.Fields(argument), " "))
	}
	return strings.Join(arguments, ", ")
}

// unquote removes one layer of double quotes and reports whether there was one.
func unquote(value string) (string, bool) {
	if len(value) >= 2 && strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`) {
		return strings.ReplaceAll(value[1:len(value)-1], `""`, `"`), true
	}
	return value, false
}

// splitQualified splits `schema.name` into its two components, honoring quotes
// so a dot INSIDE a quoted component is part of the name rather than a
// separator. That distinction is the whole of stokaro/ptah#1311.
func splitQualified(value string) (schema, name string) {
	trimmed := strings.TrimSpace(value)
	depth := 0
	for i := 0; i < len(trimmed); i++ {
		switch trimmed[i] {
		case '"':
			depth = 1 - depth
		case '.':
			if depth == 0 {
				return trimmed[:i], trimmed[i+1:]
			}
		}
	}
	return "", trimmed
}

// Set is an injective collection of identities, which is what every previous
// per-family key failed to be.
type Set struct {
	members map[Key]ID
}

// NewSet returns an empty Set.
func NewSet() *Set {
	return &Set{members: make(map[Key]ID)}
}

// Add records an identity and reports the one already present under the same
// key, if any. A caller that treats a second Add as a collision gets the
// duplicate detection every one of the four cited defects needed.
func (s *Set) Add(id ID) (existing ID, collided bool) {
	if previous, ok := s.members[id.Key()]; ok {
		return previous, true
	}
	s.members[id.Key()] = id
	return ID{}, false
}

// Contains reports whether an identity is present.
func (s *Set) Contains(id ID) bool {
	_, ok := s.members[id.Key()]
	return ok
}

// Get returns the recorded identity equal to id, which carries the SOURCE
// spelling the schema used rather than the caller's.
func (s *Set) Get(id ID) (ID, bool) {
	found, ok := s.members[id.Key()]
	return found, ok
}

// Len returns the number of distinct identities.
func (s *Set) Len() int {
	return len(s.members)
}

// All returns every identity, ordered by rendered name so a diagnostic reads
// the same on every run.
func (s *Set) All() []ID {
	ids := make([]ID, 0, len(s.members))
	for _, id := range s.members {
		ids = append(ids, id)
	}
	slices.SortFunc(ids, func(a, b ID) int { return strings.Compare(a.String(), b.String()) })
	return ids
}
