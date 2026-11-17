// Package objectowner holds the rules every source, the comparison and the
// planner share for the owner a desired schema declares for an object: which
// kinds of object a target manages the owner of, which role a declaration may
// name, and how a declared owner meets the one a read reports.
//
// Ownership is explicit. An object whose declaration names no owner is
// unmanaged: nothing here plans a change of its owner, and nothing turns an
// owner a read reports into a declaration or into a declared role. A target
// states the kinds it manages in its descriptor
// ([ptah.run/core/platform/descriptor.StatementForms.ManagedOwners]), so a kind
// joins by a descriptor line and a model field rather than by a comparison of
// dialect names here.
package objectowner

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// Kind is a kind of object a declaration may name the owner of, spelled as
// `ALTER <kind> <name> OWNER TO <role>` spells it.
type Kind string

// The kinds of object whose owner a desired schema can declare.
const (
	// View is a view.
	View Kind = descriptor.OwnerKindView
	// MaterializedView is a materialized view.
	MaterializedView Kind = descriptor.OwnerKindMaterializedView
)

// Kinds returns every kind a declaration may name the owner of, in the order
// a report lists them.
func Kinds() []Kind {
	return []Kind{View, MaterializedView}
}

// Label is the kind in lower case, as a message and a catalog row name it:
// "view" or "materialized view".
func (k Kind) Label() string {
	return strings.ToLower(string(k))
}

// Manages reports whether target reconciles the owner of an object of kind:
// its descriptor lists the kind among its managed owner kinds. A target no
// descriptor describes manages none.
func Manages(target string, kind Kind) bool {
	described, ok := platform.Lookup(target)
	return ok && slices.Contains(described.Grammar.Forms.ManagedOwners, string(kind))
}

// ErrInvalidOwner is the error of a declared owner no role can be: see
// [ValidateRole].
var ErrInvalidOwner = errors.New("invalid owner")

// sessionRoles are the words PostgreSQL reads as the role of the session that
// runs a statement, rather than as the name of a role.
var sessionRoles = []string{"CURRENT_USER", "CURRENT_ROLE", "SESSION_USER"}

// ValidateRole refuses a role a declaration cannot give an object to: an empty
// or blank name, a name with surrounding white space, PUBLIC, which is every
// role rather than one, and CURRENT_USER, CURRENT_ROLE and SESSION_USER, which
// name whichever session runs the statement, so no comparison could say what
// the declaration asks for. The error wraps [ErrInvalidOwner].
func ValidateRole(role string) error {
	switch {
	case strings.TrimSpace(role) == "":
		return fmt.Errorf("%w: the role's name is empty", ErrInvalidOwner)
	case strings.TrimSpace(role) != role:
		return fmt.Errorf("%w %q: white space surrounds the role's name", ErrInvalidOwner, role)
	case strings.EqualFold(role, "PUBLIC"):
		return fmt.Errorf("%w %q: PUBLIC is every role, not one; name the role that owns the object", ErrInvalidOwner, role)
	case slices.ContainsFunc(sessionRoles, func(word string) bool { return strings.EqualFold(word, role) }):
		return fmt.Errorf("%w %s: it names the role of the session that runs the statement; name the role", ErrInvalidOwner, strings.ToUpper(role))
	}
	return nil
}

// ValidateDeclared refuses each owner a view or a materialized view of
// database declares where target does not manage the owner of that kind, and
// a role [ValidateRole] refuses. A declaration that names no owner is not
// checked. The refusal of a target wraps ptaherr.ErrUnsupportedFeature as a
// *ptaherr.CapabilityError, so an owner is never accepted and then left out.
func ValidateDeclared(target string, database *schemamodel.Database) error {
	if database == nil {
		return nil
	}
	for _, view := range database.Views {
		if err := validateDeclared(target, View, view.Name, view.Owner); err != nil {
			return err
		}
	}
	for _, view := range database.MaterializedViews {
		if err := validateDeclared(target, MaterializedView, view.Name, view.Owner); err != nil {
			return err
		}
	}
	return nil
}

func validateDeclared(target string, kind Kind, name, owner string) error {
	if owner == "" {
		return nil
	}
	if err := ValidateRole(owner); err != nil {
		return fmt.Errorf("%s %s: %w", kind.Label(), name, err)
	}
	if !Manages(target, kind) {
		return &ptaherr.CapabilityError{Dialect: target, Feature: kind.Label() + " owner", Err: ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("%s %s declares owner %q, and Ptah does not manage the owner of a %s on %s", kind.Label(), name, owner, kind.Label(), target)}
	}
	return nil
}

// Observed is the owner of each view and materialized view a read reported,
// by kind, schema and name, as the server stores them. The zero value knows
// no owner.
type Observed struct {
	owners map[observedKey]string
}

type observedKey struct {
	kind         Kind
	schema, name string
}

// ObservedOwners indexes the owners a read reported for the kinds this package
// knows, from [catalog.Database.ObjectOwners]. A row the read reported twice
// keeps the first owner, and a row of another kind is left out.
func ObservedOwners(owners []catalog.ObjectOwner) Observed {
	index := Observed{owners: make(map[observedKey]string)}
	for _, row := range owners {
		kind, ok := KindOfCatalog(row.Kind)
		if !ok || row.Owner == "" {
			continue
		}
		key := observedKey{kind: kind, schema: row.Schema, name: row.Name}
		if _, seen := index.owners[key]; !seen {
			index.owners[key] = row.Owner
		}
	}
	return index
}

// KindOfCatalog returns the kind a [catalog.ObjectOwner.Kind] names, and
// whether it is one of [Kinds].
func KindOfCatalog(kind string) (Kind, bool) {
	for _, candidate := range Kinds() {
		if candidate.Label() == kind {
			return candidate, true
		}
	}
	return "", false
}

// Owner returns the owner the read reported for the object of kind in schema
// named name, and whether the read reported one. A read may leave the schema
// off an object of the schema it reads names in, on the object, on its owner
// row or on both, so an empty schema and defaultSchema name one schema here.
// An unknown owner is not an owner that differs, and a caller must not read it
// as the declared one.
func (o Observed) Owner(kind Kind, schema, name, defaultSchema string) (string, bool) {
	if owner, ok := o.owners[observedKey{kind: kind, schema: schema, name: name}]; ok {
		return owner, true
	}
	switch schema {
	case "":
		schema = defaultSchema
	case defaultSchema:
		schema = ""
	default:
		return "", false
	}
	owner, ok := o.owners[observedKey{kind: kind, schema: schema, name: name}]
	return owner, ok
}

// StoredRole is the name the server of dialect stores for written, a role
// name as a statement writes it: a double-quoted name keeps its spelling, an
// unquoted one folds as the dialect folds an unquoted name, and a dialect that
// records every role name under one comparison folds both. A dialect no
// descriptor describes keeps the name as written, quotes removed.
func StoredRole(dialect, written string) string {
	written = strings.TrimSpace(written)
	name, quoted := written, len(written) >= 2 && written[0] == '"' && written[len(written)-1] == '"'
	if quoted {
		name = strings.ReplaceAll(written[1:len(written)-1], `""`, `"`)
	}
	described, ok := platform.Lookup(dialect)
	switch {
	case !ok:
		return name
	case described.Names.Roles != "":
		return identifier.Comparison(described.Names.Roles).IdentityKey(name)
	case !quoted && described.Names.Unquoted != "":
		return identifier.Comparison(described.Names.Unquoted).IdentityKey(name)
	}
	return name
}

// SameRole reports whether a and b, two names as the server of dialect stores
// them, are one role there.
func SameRole(dialect, a, b string) bool {
	if described, ok := platform.Lookup(dialect); ok && described.Names.Roles != "" {
		comparison := identifier.Comparison(described.Names.Roles)
		return comparison.IdentityKey(a) == comparison.IdentityKey(b)
	}
	return a == b
}

// Transfers is a server's answer about which roles one connection may give an
// object to with ALTER ... OWNER TO, asked before a plan sends a statement.
type Transfers struct {
	// Connected is the role the connection runs as.
	Connected string
	// SchemaExempt reports that the server lets Connected give an object to
	// a role that holds no CREATE privilege on the object's schema: a
	// PostgreSQL superuser. CockroachDB checks that privilege of the new
	// owner whoever the connection is, a member of admin included.
	SchemaExempt bool
	// Privilege is the role privilege the server requires of Connected on
	// the new owner: SET on PostgreSQL 16 and later, MEMBER on earlier lines
	// and on CockroachDB. Empty means the server was not asked, and Refused
	// then says nothing.
	Privilege string
	// Refused is the roles Connected does not hold Privilege on, as the
	// server stores their names, sorted. A superuser holds it on every role.
	Refused []string
}

// Refuses reports whether the server answered that the connection may not
// give an object to role, a name as the server of dialect stores it. A role
// the server did not report, such as one a plan creates, is not refused.
func (t Transfers) Refuses(dialect, role string) bool {
	return t.Privilege != "" && slices.ContainsFunc(t.Refused, func(refused string) bool { return SameRole(dialect, refused, role) })
}

// Relation is what a server holds of the relation one ownership statement
// names, asked before a plan runs.
type Relation struct {
	// Owner is the role that owns the relation now, as the server stores its
	// name. It is empty where the server holds no such relation, and the
	// other fields then say nothing.
	Owner string
	// Schema is the schema the relation is in.
	Schema string
	// Creates reports that the role the statement names holds CREATE on
	// Schema, which the server requires of a new owner unless the connection
	// is exempt; see [Transfers.SchemaExempt].
	Creates bool
}
