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
)

// kinds is every valid [Kind], in the order directives are written.
var kinds = []Kind{Composite, Domain, Extension, Policy, Range, Role, Schema, Sequence}

// ParseKind resolves a serialized kind token. It refuses anything not in the
// closed list rather than returning a zero value, so a directive a build does
// not understand fails loudly instead of silently covering nothing.
func ParseKind(token string) (Kind, error) {
	kind := Kind(strings.ToLower(strings.TrimSpace(token)))
	if slices.Contains(kinds, kind) {
		return kind, nil
	}
	return "", fmt.Errorf("unknown coverage kind %q: valid kinds are %s", token, kindList())
}

func kindList() string {
	names := make([]string, 0, len(kinds))
	for _, kind := range kinds {
		names = append(names, string(kind))
	}
	return strings.Join(names, ", ")
}

// Object is one object a description does not describe. Name is spelled the way
// the description would have spelled it; both the qualified and the unqualified
// spelling of the same object are accepted by [Set.Describes], because the two
// sides of a comparison do not always agree on which one they carry.
type Object struct {
	Kind Kind
	Name string
}

// Set is what a description does NOT claim to describe. Its zero value claims
// everything.
//
// Both members record the same thing at different resolutions, and per-object
// records are preferred wherever the reader can enumerate what it left out. A
// whole-kind record is the honest answer only when the reader cannot: a
// projection whose rule is "omit this block type unless something names it"
// gives no information about the block types it omits, no matter what the
// database it ran against happened to contain.
type Set struct {
	// Kinds names object kinds whose absence from the description carries no
	// information.
	Kinds []Kind
	// Objects names individual objects whose absence from the description
	// carries no information.
	Objects []Object
}

// IsZero reports whether the description claims to describe everything.
func (s Set) IsZero() bool {
	return len(s.Kinds) == 0 && len(s.Objects) == 0
}

// Describes reports whether the absence of an object from this description is
// authoritative. It is false when the whole kind is undescribed, or when any of
// the supplied spellings names an undescribed object.
//
// A caller passes every spelling the two sides might use -- typically the
// qualified and the unqualified name -- because a false negative here restores
// exactly the defect this package exists to prevent.
func (s Set) Describes(kind Kind, names ...string) bool {
	if slices.Contains(s.Kinds, kind) {
		return false
	}
	for _, object := range s.Objects {
		if object.Kind != kind {
			continue
		}
		for _, name := range names {
			if strings.EqualFold(strings.TrimSpace(name), object.Name) {
				return false
			}
		}
	}
	return true
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
	return s.DescribesSchema(schema) && s.Describes(kind, names...)
}

// WithKind returns the set extended with a whole kind.
func (s Set) WithKind(kinds ...Kind) Set {
	out := s.clone()
	out.Kinds = append(out.Kinds, kinds...)
	return out.Normalize()
}

// WithObject returns the set extended with one object.
func (s Set) WithObject(kind Kind, name string) Set {
	out := s.clone()
	out.Objects = append(out.Objects, Object{Kind: kind, Name: strings.TrimSpace(name)})
	return out.Normalize()
}

// Merge unions two descriptions' limits. Loading several schema files into one
// desired state produces one description, and it describes only what all of its
// parts together describe.
func (s Set) Merge(other Set) Set {
	out := s.clone()
	out.Kinds = append(out.Kinds, other.Kinds...)
	out.Objects = append(out.Objects, other.Objects...)
	return out.Normalize()
}

func (s Set) clone() Set {
	return Set{Kinds: slices.Clone(s.Kinds), Objects: slices.Clone(s.Objects)}
}

// Normalize sorts and deduplicates the set. Coverage rides in a generated
// document, and a document whose bytes depend on map iteration order is one
// nobody can diff.
func (s Set) Normalize() Set {
	out := s.clone()
	slices.Sort(out.Kinds)
	out.Kinds = slices.Compact(out.Kinds)
	slices.SortFunc(out.Objects, func(a, b Object) int {
		if a.Kind != b.Kind {
			return strings.Compare(string(a.Kind), string(b.Kind))
		}
		return strings.Compare(a.Name, b.Name)
	})
	out.Objects = slices.Compact(out.Objects)
	if len(out.Kinds) == 0 {
		out.Kinds = nil
	}
	if len(out.Objects) == 0 {
		out.Objects = nil
	}
	return out
}

// DirectiveMarker introduces a serialized coverage record. It carries the
// `ptah:` prefix Ptah's other in-comment directives use, so a reader meeting one
// in a document knows whose it is.
const DirectiveMarker = "ptah:not-described"

// Directives renders the set as directive bodies, one per record, without a
// comment prefix. The caller adds the prefix its format spells comments with:
// `//` or `#` for HCL, `--` for SQL.
func (s Set) Directives() []string {
	normalized := s.Normalize()
	lines := make([]string, 0, len(normalized.Kinds)+len(normalized.Objects))
	for _, kind := range normalized.Kinds {
		lines = append(lines, fmt.Sprintf("%s %s", DirectiveMarker, kind))
	}
	for _, object := range normalized.Objects {
		lines = append(lines, fmt.Sprintf("%s %s %s", DirectiveMarker, object.Kind, strconv.Quote(object.Name)))
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
		if object.Name == "" {
			set.Kinds = append(set.Kinds, object.Kind)
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
func parseDirective(body string) (Object, bool, error) {
	rest, ok := strings.CutPrefix(body, DirectiveMarker)
	if !ok {
		return Object{}, false, nil
	}
	fields := strings.Fields(rest)
	if len(fields) == 0 || len(fields) > 2 {
		return Object{}, false, fmt.Errorf(
			"malformed %s directive %q: expected a kind and an optional quoted name",
			DirectiveMarker, body,
		)
	}
	kind, err := ParseKind(fields[0])
	if err != nil {
		return Object{}, false, err
	}
	if len(fields) == 1 {
		return Object{Kind: kind}, true, nil
	}
	name, err := strconv.Unquote(fields[1])
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
	return Object{Kind: kind, Name: strings.TrimSpace(name)}, true, nil
}
