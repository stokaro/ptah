// Package coverage records the limits of a schema description, so that a
// comparator can tell an object a description says is gone from one it was
// never asked about.
//
// Ptah has a read path that describes a database and a compare path that
// decides what to change. Both make scope decisions and they make them
// independently, and every time one side moved the other kept its old
// assumption about what silence means: something absent from a description was
// read as something absent from the database (stokaro/ptah#1276).
//
// A description therefore carries a [Set] naming what it does not claim to
// describe. Three states, not two:
//
//   - PRESENT -- the object is in the description.
//   - AUTHORITATIVELY ABSENT -- the object is not in the description, and the
//     description covers it. This is a difference, and a removal or a creation
//     is planned for it.
//   - NOT DESCRIBED -- the object is not in the description and the
//     description never covered it. This is not a difference. Nothing is
//     planned for it in either direction.
//
// The zero [Set] claims everything, so a description that says nothing about
// its own limits is fully authoritative. That is what every hand-authored
// schema file is, and it is why adding coverage changes no existing plan.
//
// # Surviving serialization
//
// Coverage is useless if it lives only in the process that read the database:
//
//	ptah-compat schema inspect > schema.hcl
//	ptah-compat schema apply --to file://schema.hcl
//
// is two processes, and the second one reads the file rather than the first
// one's memory. A [Set] therefore serializes into the leading comment header
// of the document it belongs to, as one directive line per record:
//
//	// ptah:not-described extension
//	// ptah:not-described schema "extra"
//
// A comment is invisible to every reader of the document except Ptah -- the
// pinned Atlas community binary v1.3.0 reads a document carrying these lines at
// exit 0 -- and the same grammar works in HCL (`//` or `#`) and in SQL (`--`),
// so one encoding covers every serialized desired-state format.
//
// Only the leading comment header is read, deliberately. A directive found
// anywhere else in a file would let a table comment or a string literal
// suppress a removal the author asked for.
package coverage

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Kind names one class of schema object a description can decline to describe.
//
// The list is closed. An unknown kind in a serialized directive is refused
// rather than ignored, because ignoring it is the exact failure this package
// exists to prevent: a directive nothing understands reads as no directive at
// all, and the absence it was protecting becomes a removal.
//
// Two declared kinds sit outside that serialized grammar: [Hypertable] and
// [ContinuousAggregate] are built and consulted in process only, and their
// comments carry the consequence for a [Set] that holds one.
type Kind string

// The kinds a description can decline to describe. Each one names a comparator
// that consults coverage before planning an addition or a removal.
const (
	// Composite is a PostgreSQL composite type (CREATE TYPE ... AS (...)).
	Composite Kind = "composite"
	// Domain is a PostgreSQL domain type (CREATE DOMAIN).
	Domain Kind = "domain"
	// Extension is a PostgreSQL extension (CREATE EXTENSION).
	Extension Kind = "extension"
	// ExtendedProperty is a SQL Server extended property
	// (sp_addextendedproperty).
	//
	// Like VirtualTable it is usually declined by construction rather than by
	// choice: the HCL surface has no block for one, so a document rendered from
	// a database that has them carries none of them and its silence is not a
	// request to drop them. Without this kind the round trip was destructive --
	// `schema inspect` followed by `schema apply` of its own output planned
	// sp_dropextendedproperty for every property on the server.
	ExtendedProperty Kind = "extended_property"
	// Policy is a PostgreSQL row-level security policy (CREATE POLICY).
	Policy Kind = "policy"
	// Range is a PostgreSQL range type (CREATE TYPE ... AS RANGE).
	Range Kind = "range"
	// Role is a database role (CREATE ROLE). Roles are cluster-scoped on
	// PostgreSQL, so a reader scoped to one database describes a subset of them
	// by construction.
	//
	// Role is ADDITIVE-ONLY, and deliberately so. The role comparator never
	// plans a removal at all -- roles are created by DBAs and by infrastructure
	// as often as by a schema, so dropping one because a description does not
	// name it is not a decision it makes -- which means a `ptah:not-described
	// role` line in a desired-state document protects nothing that was at risk.
	// It is accepted rather than refused so the closed list stays one list, but
	// a reader meeting it should not conclude that a removal was suppressed.
	Role Kind = "role"
	// Schema is a schema or namespace (CREATE SCHEMA). A schema recorded here
	// also covers everything in it: an object in a schema nobody read is not
	// described either.
	Schema Kind = "schema"
	// Sequence is a standalone sequence (CREATE SEQUENCE).
	Sequence Kind = "sequence"
	// Hypertable is a TimescaleDB hypertable: an ordinary table partitioned on
	// a range dimension.
	//
	// It is declined for the reason [Synonym] is, with one difference that
	// makes it worse: the table IS in the description and only its
	// partitioning is missing, so a format that cannot say a table is
	// partitioned describes a table that looks complete. Reading that silence
	// as intent would plan nothing at all -- there is no statement that undoes
	// create_hypertable -- while a replay of the same description creates an
	// ordinary table and a diff between the two reports no difference
	// (stokaro/ptah#1026).
	//
	// Hypertable is consulted in process rather than serialized: it is not
	// part of the directive grammar this package encodes and decodes, so a
	// [Set] carrying this kind does not survive a round trip through a
	// document. Hold the record in memory and consult it there.
	Hypertable Kind = "hypertable"

	// ContinuousAggregate is a TimescaleDB continuous aggregate: a
	// materialized view over a hypertable the extension keeps up to date.
	//
	// It is declined for the reason [Hypertable] is, and the failure it
	// prevents is louder. A description that cannot express one still describes
	// the hypertable underneath it, so the aggregate reads as an object the
	// document deliberately omits -- and the plan that follows is not a no-op
	// but a DROP. Measured on 2.29.2, that drop cannot even apply: the server
	// refuses DROP VIEW on a continuous aggregate and the run reports the same
	// pending change forever (stokaro/ptah#1026).
	//
	// ContinuousAggregate is consulted in process rather than serialized,
	// exactly as [Hypertable] is; see that constant for what this means for a
	// [Set] carrying it.
	ContinuousAggregate Kind = "continuous_aggregate"

	// ChangeStream is a Spanner change stream (CREATE CHANGE STREAM): a
	// database object with its own lifecycle that publishes row changes to a
	// reader outside the schema.
	//
	// Ptah does not model one. The kind exists so that saying so is possible:
	// a Spanner database's description carries none of its change streams, and
	// that silence is not a statement that it has none. Without the record the
	// silence read as authoritative, which is how an unmodeled construct
	// becomes a DROP (stokaro/ptah#2236).
	//
	// It is recorded whenever the target could have them rather than when this
	// read found some, for the reason [ExtendedProperty] gives: recording only
	// what was found would assert that the absence of every other one is
	// authoritative.
	ChangeStream Kind = "change_stream"
	// Synonym is a SQL Server synonym (CREATE SYNONYM).
	//
	// It is declined for the same reason [ExtendedProperty] is, and the same
	// round trip planned DROP SYNONYM for every synonym the server had.
	Synonym Kind = "synonym"
	// VirtualTable is a SQLite virtual table (CREATE VIRTUAL TABLE ... USING).
	//
	// Unlike the kinds above it, a description usually declines this one by
	// construction rather than by choice: Go annotations, HCL and YAML have no
	// syntax for a virtual table at all, so their silence about one carries no
	// intent and cannot be read as a request to drop it. A native `.sql`
	// document and a database read can both express one and record nothing
	// here, so their silence still means the table is gone
	// (stokaro/ptah#1028).
	VirtualTable Kind = "virtual_table"
)

// kinds is every [Kind] the serialized directive grammar accepts, in the order
// [ParseKind]'s refusal message lists them. [Hypertable] and
// [ContinuousAggregate] are not in it; both constants say what that costs a
// serialized [Set].
var kinds = []Kind{
	ChangeStream, Composite, Domain, Extension, ExtendedProperty, Policy, Range, Role, Schema,
	Sequence, Synonym, VirtualTable,
}

// ParseKind resolves a serialized kind token. It refuses anything not in the
// closed list rather than returning a zero value, so a directive a build does
// not understand fails loudly instead of silently covering nothing.
func ParseKind(token string) (Kind, error) {
	kind := Kind(strings.ToLower(strings.TrimSpace(token)))
	if slices.Contains(kinds, kind) {
		return kind, nil
	}
	return "", fmt.Errorf("unknown coverage kind %q: valid kinds are %s", token, tokenList(kinds))
}

// Object is one thing a description does not describe.
//
// Name is spelled the way the description would have spelled it; both the
// qualified and the unqualified spelling of the same object are accepted by
// [Set.Describes], because the two sides of a comparison do not always agree on
// which one they carry. An EMPTY Name is a record about the whole kind: the
// answer a reader gives when it cannot enumerate what it left out.
//
// Reason and Provenance say why the description declines it and how that limit
// was learned. Both may be unspecified -- a hand-authored directive naming only
// a kind is a complete, valid record -- and neither changes what a comparator
// may do. They change what it can say (stokaro/ptah#1346).
type Object struct {
	Kind       Kind
	Name       string
	Reason     Reason
	Provenance Provenance
}

// Refused is the record a reader makes when the target would not let it look:
// the object family is [NotInspected], and that limit was [Observed] -- Ptah
// watched the server refuse the catalog rather than assuming anything about it.
//
// Four readers make exactly this record, and a surface that explains it wants
// them to be one thing rather than four spellings that can drift apart.
func Refused(kind Kind) Object {
	return Object{Kind: kind, Reason: NotInspected, Provenance: Observed}
}

// WholeKind reports whether this record covers a whole kind rather than one
// named object.
func (o Object) WholeKind() bool { return strings.TrimSpace(o.Name) == "" }

// Validate reports whether every token in the record is one this build
// understands. An unknown one is refused rather than tolerated, for the reason
// [ParseKind] gives.
func (o Object) Validate() error {
	if _, err := ParseKind(string(o.Kind)); err != nil {
		return err
	}
	if !o.Reason.Valid() {
		return fmt.Errorf("unknown coverage reason %q: valid reasons are %s", o.Reason, tokenList(reasons))
	}
	if !o.Provenance.Valid() {
		return fmt.Errorf(
			"unknown coverage provenance %q: valid provenances are %s", o.Provenance, tokenList(provenances))
	}
	return nil
}

// Set is what a description does NOT claim to describe. Its zero value claims
// everything.
//
// Whole-kind and per-object records live in one slice, distinguished by whether
// [Object.WholeKind] holds. Per-object records are preferred wherever the reader
// can enumerate what it left out. A whole-kind record is the honest answer only
// when it cannot: a projection whose rule is "omit this block type unless
// something names it" gives no information about the block types it omits, no
// matter what the database it ran against happened to contain.
type Set struct {
	// Objects names what the description does not claim to describe. A record
	// with an empty name covers its whole kind.
	Objects []Object
}

// IsZero reports whether the description claims to describe everything.
func (s Set) IsZero() bool {
	return len(s.Objects) == 0
}

// Limit returns the record that makes an object's absence uninformative, and
// reports whether there is one.
//
// It is the explanatory half of [Set.Describes], and Describes is written in
// terms of it so the two can never disagree about which records matter. Where
// Describes answers whether a comparator may act on silence, Limit answers what
// to tell the user when it may not.
//
// A whole-kind record wins over a record naming one object, because it is the
// broader statement and it is the one that explains the most.
func (s Set) Limit(kind Kind, names ...string) (Object, bool) {
	var named Object
	var haveNamed bool
	for _, object := range s.Objects {
		if object.Kind != kind {
			continue
		}
		if object.WholeKind() {
			return object, true
		}
		if haveNamed {
			continue
		}
		for _, name := range names {
			if strings.EqualFold(strings.TrimSpace(name), object.Name) {
				named, haveNamed = object, true
				break
			}
		}
	}
	return named, haveNamed
}

// LimitIn is [Set.Limit] for an object owned by a schema. A schema nobody read
// explains everything in it, so its record wins over the object's own kind.
func (s Set) LimitIn(kind Kind, schema string, names ...string) (Object, bool) {
	if strings.TrimSpace(schema) != "" {
		if limit, ok := s.Limit(Schema, schema); ok {
			return limit, true
		}
	}
	return s.Limit(kind, names...)
}

// Describes reports whether the absence of an object from this description is
// authoritative. It is false when the whole kind is undescribed, or when any of
// the supplied spellings names an undescribed object.
//
// A caller passes every spelling the two sides might use -- typically the
// qualified and the unqualified name -- because a false negative here restores
// exactly the defect this package exists to prevent.
func (s Set) Describes(kind Kind, names ...string) bool {
	_, limited := s.Limit(kind, names...)
	return !limited
}

// DescribesSchema reports whether the absence of a schema, or of anything in
// it, is authoritative.
func (s Set) DescribesSchema(schema string) bool {
	if strings.TrimSpace(schema) == "" {
		return true
	}
	return s.Describes(Schema, schema)
}

// DescribesIn reports whether the absence of an object owned by a schema is
// authoritative. An object in a schema nobody read is not described whatever
// its own kind says.
func (s Set) DescribesIn(kind Kind, schema string, names ...string) bool {
	_, limited := s.LimitIn(kind, schema, names...)
	return !limited
}

// WithKind returns the set extended with a whole kind, giving no reason. It is
// the coarse constructor, and it means exactly what a hand-authored directive
// naming only a kind means. A producer that knows why should say so with
// [Set.With].
func (s Set) WithKind(kinds ...Kind) Set {
	out := s.clone()
	for _, kind := range kinds {
		out.Objects = append(out.Objects, Object{Kind: kind})
	}
	return out.Normalize()
}

// WithObject returns the set extended with one object, giving no reason.
func (s Set) WithObject(kind Kind, name string) Set {
	return s.With(Object{Kind: kind, Name: name})
}

// With returns the set extended with records that carry their own reason and
// provenance. This is what a production reader, projection or renderer uses:
// it knows why it is declining the object family, and that sentence is the one
// a user needs (stokaro/ptah#1346).
func (s Set) With(objects ...Object) Set {
	out := s.clone()
	out.Objects = append(out.Objects, objects...)
	return out.Normalize()
}

// Merge unions two descriptions' limits. Loading several schema files into one
// desired state produces one description, and it describes only what all of its
// parts together describe.
//
// Records differing only in reason or provenance are both kept. Two sides can
// decline the same kind for different reasons -- a read was refused it AND a
// policy omitted it -- and both are true; collapsing them would pick one
// explanation to print and silently discard the other.
func (s Set) Merge(other Set) Set {
	out := s.clone()
	out.Objects = append(out.Objects, other.Objects...)
	return out.Normalize()
}

// Validate reports the first record this build does not understand.
func (s Set) Validate() error {
	for _, object := range s.Objects {
		if err := object.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func (s Set) clone() Set {
	return Set{Objects: slices.Clone(s.Objects)}
}

// Normalize sorts and deduplicates the set. Coverage rides in a generated
// document, and a document whose bytes depend on map iteration order is one
// nobody can diff.
//
// It also trims names, which turns a record whose name is blank or whitespace
// into a record about that whole kind. A nameless record names nothing, and
// [Set.Directives] would otherwise write it as an empty quoted string that
// [DecodeHeader] refuses, so the encoder would be producing a document this
// package cannot read. Dropping it instead would widen what the description
// claims to cover, and widening is the destructive direction: the whole-kind
// record is the conservative superset, and it is visible in the document rather
// than silent.
func (s Set) Normalize() Set {
	out := s.clone()
	for i := range out.Objects {
		out.Objects[i].Name = strings.TrimSpace(out.Objects[i].Name)
	}
	slices.SortFunc(out.Objects, compareObjects)
	out.Objects = slices.Compact(out.Objects)
	if len(out.Objects) == 0 {
		out.Objects = nil
	}
	return out
}

// compareObjects orders records by kind, then by name, then by the reason and
// provenance they carry, so a set's serialized bytes depend on nothing but its
// contents. A whole-kind record sorts before the named records of its kind,
// because the empty name sorts first.
func compareObjects(a, b Object) int {
	if a.Kind != b.Kind {
		return strings.Compare(string(a.Kind), string(b.Kind))
	}
	if a.Name != b.Name {
		return strings.Compare(a.Name, b.Name)
	}
	if a.Reason != b.Reason {
		return strings.Compare(string(a.Reason), string(b.Reason))
	}
	return strings.Compare(string(a.Provenance), string(b.Provenance))
}

// DirectiveMarker introduces a serialized coverage record. It carries the
// `ptah:` prefix Ptah's other in-comment directives use, so a reader meeting one
// in a document knows whose it is.
//
// A directive is honored only in a document's leading comment header: see
// [DecodeHeader] for the placement rule and [Set.Directives] for the line
// grammar.
const DirectiveMarker = "ptah:not-described"

// The attribute keys a directive can carry between its kind and its name.
const (
	reasonAttribute     = "reason"
	provenanceAttribute = "provenance"
)

// Directives renders the set as directive bodies, one per record, without a
// comment prefix. The caller adds the prefix its format spells comments with:
// `//` or `#` for HCL, `--` for SQL.
//
// The grammar is a kind, then any attributes the record carries, then an
// optional quoted name:
//
//	ptah:not-described extension reason=not-inspected provenance=observed
//	ptah:not-described schema reason=outside-scope provenance=configured "extra"
//
// Attributes come BEFORE the name because the name is the one field that may
// contain a space, so it has to be last. They are omitted when unspecified, so
// a coarse record still writes the two-token line a hand-authored document uses.
//
// A name is written with [strconv.Quote], so every line is one line whatever
// the identifier contains: a newline, a tab or a control character comes out as
// its escape rather than ending the comment. [DecodeHeader] reverses exactly
// this, and TestDirectivesRoundTripAdversarialNames pins the pair over the name
// shapes a quoted identifier is allowed to have.
func (s Set) Directives() []string {
	normalized := s.Normalize()
	lines := make([]string, 0, len(normalized.Objects))
	for _, object := range normalized.Objects {
		line := fmt.Sprintf("%s %s", DirectiveMarker, object.Kind)
		if object.Reason != ReasonUnspecified {
			line += fmt.Sprintf(" %s=%s", reasonAttribute, object.Reason)
		}
		if object.Provenance != ProvenanceUnspecified {
			line += fmt.Sprintf(" %s=%s", provenanceAttribute, object.Provenance)
		}
		if !object.WholeKind() {
			line += " " + strconv.Quote(object.Name)
		}
		lines = append(lines, line)
	}
	return lines
}

// commentPrefixes are the line-comment spellings a serialized schema document
// can use: HCL accepts the first two, SQL the third.
var commentPrefixes = []string{"//", "#", "--"}

// DecodeHeader reads the coverage a document declares about itself out of its
// leading comment header: the run of comment and blank lines before the first
// line of content.
//
// Stopping at the first content line is what makes the encoding safe. A
// directive recognized anywhere in the file could be smuggled in through a
// table comment or a string literal, and it would suppress a removal the author
// asked for -- a silent, destructive false negative in the one direction this
// package must never fail.
func DecodeHeader(document string) (Set, error) {
	var set Set
	for line := range strings.SplitSeq(document, "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		body, isComment := commentBody(trimmed)
		if !isComment {
			break
		}
		object, ok, err := parseDirective(body)
		if err != nil {
			return Set{}, err
		}
		if !ok {
			continue
		}
		set.Objects = append(set.Objects, object)
	}
	return set.Normalize(), nil
}

func commentBody(trimmed string) (string, bool) {
	for _, prefix := range commentPrefixes {
		if body, ok := strings.CutPrefix(trimmed, prefix); ok {
			return strings.TrimSpace(body), true
		}
	}
	return "", false
}

// parseDirective reads one comment body. It reports whether the body was a
// coverage directive at all, so an ordinary comment is passed over rather than
// refused.
//
// The name is decoded from the WHOLE remainder of the line from its opening
// quote on, rather than from a whitespace-delimited field. A quoted identifier
// may contain a space -- `CREATE SCHEMA "extra reports"` is legal, and so is a
// table or a policy named that way -- and splitting the line on whitespace has
// no idea that the space is inside the quotes, so it counted three tokens where
// [Set.Directives] had written two and refused a document this package had just
// produced itself (stokaro/ptah#1276). [strconv.Unquote] over the remainder
// reverses [strconv.Quote] exactly, and it rejects trailing text after the
// closing quote, so the grammar stays one kind, its attributes, and at most one
// quoted name.
//
// Cutting at the first double quote is what separates the two halves, and it is
// unambiguous because no attribute token can contain one: keys and values are
// both drawn from closed lists of lowercase words.
func parseDirective(body string) (Object, bool, error) {
	rest, ok := strings.CutPrefix(body, DirectiveMarker)
	if !ok {
		return Object{}, false, nil
	}
	head, nameToken := splitAttributesAndName(rest)
	fields := strings.Fields(head)
	if len(fields) == 0 {
		return Object{}, false, malformedDirective(body)
	}
	kind, err := ParseKind(fields[0])
	if err != nil {
		return Object{}, false, err
	}
	object := Object{Kind: kind}
	for _, field := range fields[1:] {
		if err := applyAttribute(&object, field, body); err != nil {
			return Object{}, false, err
		}
	}
	if nameToken == "" {
		return object, true, nil
	}
	name, err := unquoteName(nameToken)
	if err != nil {
		return Object{}, false, fmt.Errorf(
			"malformed %s directive %q: name must be a quoted string",
			DirectiveMarker, body,
		)
	}
	if strings.TrimSpace(name) == "" {
		return Object{}, false, fmt.Errorf(
			"malformed %s directive %q: name must not be empty",
			DirectiveMarker, body,
		)
	}
	object.Name = strings.TrimSpace(name)
	return object, true, nil
}

// applyAttribute reads one `key=value` token into the record. An unknown key,
// an unknown value, or a key given twice is refused: a directive carrying a
// safety claim this build cannot read must not be read as a directive making no
// claim at all.
func applyAttribute(object *Object, field, body string) error {
	key, value, ok := strings.Cut(field, "=")
	if !ok {
		return fmt.Errorf(
			"malformed %s directive %q: %q is neither a %s= or %s= attribute nor a quoted name",
			DirectiveMarker, body, field, reasonAttribute, provenanceAttribute)
	}
	switch key {
	case reasonAttribute:
		if object.Reason != ReasonUnspecified {
			return fmt.Errorf(
				"malformed %s directive %q: %s given twice", DirectiveMarker, body, reasonAttribute)
		}
		reason, err := ParseReason(value)
		if err != nil {
			return err
		}
		object.Reason = reason
	case provenanceAttribute:
		if object.Provenance != ProvenanceUnspecified {
			return fmt.Errorf(
				"malformed %s directive %q: %s given twice", DirectiveMarker, body, provenanceAttribute)
		}
		provenance, err := ParseProvenance(value)
		if err != nil {
			return err
		}
		object.Provenance = provenance
	default:
		return fmt.Errorf(
			"unknown %s attribute %q: valid attributes are %s, %s",
			DirectiveMarker, key, reasonAttribute, provenanceAttribute)
	}
	return nil
}

func malformedDirective(body string) error {
	return fmt.Errorf(
		"malformed %s directive %q: expected a kind, optional %s= and %s= attributes, and an optional quoted name",
		DirectiveMarker, body, reasonAttribute, provenanceAttribute,
	)
}

// splitAttributesAndName cuts a directive body at its first double quote. What
// precedes it is the kind and its attributes; what follows, quote included, is
// the name.
func splitAttributesAndName(rest string) (head, name string) {
	rest = strings.TrimSpace(rest)
	index := strings.IndexByte(rest, '"')
	if index < 0 {
		return rest, ""
	}
	return strings.TrimSpace(rest[:index]), strings.TrimSpace(rest[index:])
}

// unquoteName decodes the quoted form [Set.Directives] writes.
//
// Only the double-quoted spelling is accepted. [strconv.Unquote] also reads
// back-quoted strings and single-quoted runes, and neither is a spelling this
// package ever emits; admitting them would widen a grammar whose whole purpose
// is that a reader can be certain what a line means.
func unquoteName(token string) (string, error) {
	if !strings.HasPrefix(token, `"`) {
		return "", fmt.Errorf("name %q is not a double-quoted string", token)
	}
	return strconv.Unquote(token)
}
