package identifiervalidation

import (
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/tableref"
)

// Namesake is the name of an object in a schema as a check that two names
// may be one name compares it: the conflict key of the whole name, and those
// of its schema and of its own name. SchemaKey is empty for a name whose
// schema the rules do not place.
//
// The check of two declarations of one kind ([ValidateTarget]) and the
// planner's refusal of a plan that creates one object and drops another both
// compare names through it, so the two cannot disagree about which names may
// be one. A declaration check that paired a name with no class with every
// name of its kind in any schema would refuse the views sales.Größe and
// dbo.v2 offline, which the planner keeps apart (stokaro/ptah#6747).
type Namesake struct {
	Key       string
	SchemaKey string
	NameKey   string
}

// NamesakeOf is name, which may carry its schema, as [Namesake] compares it
// under semantics, which a caller reads as [indexscope.ConflictSemantics]
// reads them. An unqualified name is in the default schema of semantics.
//
// [indexscope.ConflictSemantics]: ptah.run/internal/indexscope.ConflictSemantics
func NamesakeOf(name string, semantics identifier.Semantics) Namesake {
	namesake := Namesake{Key: semantics.QualifiedTableConflictKey(name), NameKey: semantics.TableConflictKey(name)}
	ref, ok := tableref.Parse(name)
	if !ok {
		return namesake
	}
	namesake.NameKey = semantics.TableConflictKey(ref.Name)
	schema := ref.Schema
	if !ref.Qualified {
		schema = semantics.DefaultSchema
	}
	if schema != "" {
		namesake.SchemaKey = semantics.TableConflictKey(schema)
	}
	return namesake
}

// Unresolved reports whether the schema or the name has no class under the
// rules the keys were made with.
func (n Namesake) Unresolved() bool {
	return unplaced(n.SchemaKey) || unplaced(n.NameKey)
}

// MayBe reports whether the target may read n and other as one name. It may
// where their keys agree, and where one of the two has no class and each part
// of one may be the same part of the other: an accent-insensitive collation
// reads `café` as `cafe`, and only the server says which collation it has.
// A part may be another part when their keys agree, when either has no
// class, or when either is not placed.
//
// Names whose schemas both have a class and differ are never one name,
// however unrelated their own names are: the views sales.Größe and dbo.v2
// are two views under every collation.
func (n Namesake) MayBe(other Namesake) bool {
	if n.Key == other.Key {
		return true
	}
	if !n.Unresolved() && !other.Unresolved() {
		return false
	}
	return partsMayAgree(n.SchemaKey, other.SchemaKey) && partsMayAgree(n.NameKey, other.NameKey)
}

// partsMayAgree reports whether two parts of a name may be one part.
func partsMayAgree(a, b string) bool {
	return a == b || a == "" || b == "" || unplaced(a) || unplaced(b)
}

// unplaced reports whether key, a part's key or a qualified one, holds a name
// the rules give no class.
func unplaced(key string) bool {
	return strings.Contains(key, unresolvedTableKey)
}
