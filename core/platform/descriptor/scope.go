package descriptor

import (
	"fmt"
	"slices"
	"strings"
)

// Schemas states what a schema is on an engine and how a database URL
// selects one. The zero value is an engine whose schema boundary has not been
// measured: a URL limits a run to nothing, and a run describes the schema the
// connection works in.
type Schemas struct {
	// Selection is how a database URL limits a run to one schema.
	Selection SchemaSelection `json:"selection,omitempty"`
	// Implicit reports that an object a desired state declares without a
	// schema belongs to DefaultSchema when no database URL names one. It is
	// false where only the connection can name that schema, and where the
	// rule has not been measured. It requires DefaultSchema.
	Implicit bool `json:"implicit,omitempty"`
}

// SchemaSelection is how a database URL limits a run to one schema.
type SchemaSelection string

const (
	// SchemaSelectionSearchPath is an engine whose schemas are namespaces
	// inside one database, PostgreSQL's model: the URL's search_path query
	// parameter names the schema a run is limited to, and a URL without one
	// leaves the run at realm scope, every schema of the database. A
	// database is not a schema there, so it is created and listed through
	// the server's database catalog.
	SchemaSelectionSearchPath SchemaSelection = "search-path"
	// SchemaSelectionDatabase is an engine whose schemas are the databases
	// of a server, MySQL's model: the database a URL names is the schema a
	// run is limited to, and a URL naming none is the whole server.
	SchemaSelectionDatabase SchemaSelection = "database"
	// SchemaSelectionSingle is an engine with one namespace, DefaultSchema:
	// every URL limits a run to it, and there is no wider scope to select.
	// It requires DefaultSchema.
	SchemaSelectionSingle SchemaSelection = "single"
)

// SchemaSelections returns every declared schema selection.
func SchemaSelections() []SchemaSelection {
	return []SchemaSelection{SchemaSelectionSearchPath, SchemaSelectionDatabase, SchemaSelectionSingle}
}

// Roles states which of the roles Ptah manages a schema read leaves out of a
// description. The zero value is an engine whose read leaves out the roles
// nothing in the inspected schemas refers to, and offers no way to widen it.
type Roles struct {
	// OutOfScope is which roles a read leaves out of a description.
	OutOfScope RoleScope `json:"out_of_scope,omitempty"`
	// Widenable reports that the read describes every role Ptah manages on
	// the server when the operator asks for it; internal/rolescope names the
	// variable that asks.
	Widenable bool `json:"widenable,omitempty"`
}

// RoleScope is which roles a schema read leaves out of a description.
type RoleScope string

const (
	// RoleScopeReferenced leaves out the roles nothing in the inspected
	// schemas refers to.
	RoleScopeReferenced RoleScope = "referenced"
	// RoleScopeUserMade leaves out the users and groups the server made for
	// itself and the database's owner, and every user and group when the read
	// is of a dev realm, which shares its database's users and groups.
	RoleScopeUserMade RoleScope = "user-made"
)

// RoleScopes returns every declared role scope.
func RoleScopes() []RoleScope {
	return []RoleScope{RoleScopeReferenced, RoleScopeUserMade}
}

// validateScope checks the fields declared in this file.
func (d Dialect) validateScope() error {
	if d.Schemas.Selection != "" && !slices.Contains(SchemaSelections(), d.Schemas.Selection) {
		return fmt.Errorf("%w: dialect %q: schema selection %q is not one of %q",
			ErrInvalidDescriptor, d.Name, d.Schemas.Selection, SchemaSelections())
	}
	if d.DefaultSchema == "" && (d.Schemas.Implicit || d.Schemas.Selection == SchemaSelectionSingle) {
		return fmt.Errorf("%w: dialect %q: an implicit or single schema needs a default schema",
			ErrInvalidDescriptor, d.Name)
	}
	if d.Roles.OutOfScope != "" && !slices.Contains(RoleScopes(), d.Roles.OutOfScope) {
		return fmt.Errorf("%w: dialect %q: role scope %q is not one of %q",
			ErrInvalidDescriptor, d.Name, d.Roles.OutOfScope, RoleScopes())
	}
	seen := make(map[string]bool, len(d.URLValuedParameters))
	for _, parameter := range d.URLValuedParameters {
		folded := strings.ToLower(parameter)
		if strings.TrimSpace(parameter) == "" || seen[folded] {
			return fmt.Errorf("%w: dialect %q: URL-valued parameter %q is empty or repeated",
				ErrInvalidDescriptor, d.Name, parameter)
		}
		seen[folded] = true
	}
	return nil
}
