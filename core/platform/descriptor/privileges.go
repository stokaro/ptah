package descriptor

import (
	"fmt"
	"slices"
	"strings"
)

// AllPrivileges states what the privilege keyword ALL grants on each kind of
// object, spelled as a catalog read of the engine reports the privileges, one
// row each. A grant comparison reads it: a role holds a declared ALL when it
// holds every privilege the list requires, and a declared ALL grants each
// privilege of the list, so none of them is planned for removal from a role
// the schema manages.
//
// The zero value states no list. A comparison then takes the PostgreSQL
// lists, which are also the lists of a comparison that names no dialect. An
// engine whose catalog reports a GRANT ALL as one row of its own, such as
// CockroachDB, needs no list either.
type AllPrivileges struct {
	// Kinds maps an object kind a grant targets, in upper case such as
	// "TABLE" or "SCHEMA", to what ALL grants on it. A kind the map leaves
	// out has no list, so a role holds ALL on it only where the catalog
	// reports ALL.
	Kinds map[string]AllPrivilegeList `json:"kinds,omitempty"`
	// RoutinesByName reports that the engine grants on a function or a
	// procedure with the statement it grants on a table, GRANT ... ON name
	// with no object kind, and that a catalog read reports the grant under
	// TABLE. What ALL grants under a TABLE grant then depends on what the
	// object is: a procedure takes the PROCEDURE list, a function the read
	// reports as returning a table ([ptah.run/catalog.Function.ReturnsTable])
	// keeps the TABLE list, and every other function takes the FUNCTION list.
	// A descriptor that sets it states both routine lists.
	RoutinesByName bool `json:"routines_by_name,omitempty"`
}

// routineKinds are the lists a descriptor that sets
// [AllPrivileges.RoutinesByName] states.
var routineKinds = []string{"FUNCTION", "PROCEDURE"}

// AllPrivilegeList is what ALL grants on one kind of object.
type AllPrivilegeList struct {
	// Privileges is every privilege ALL grants on the kind, in upper case.
	Privileges []string `json:"privileges"`
	// Optional names the privileges of Privileges that ALL does not grant on
	// every object of the kind: some supported release lacks one, or the
	// catalog reads two kinds of object as this one and ALL grants it on only
	// one of them. A role holding the others holds ALL as far as a comparison
	// that reads neither the release nor the object can tell, and a REVOKE of
	// one of these alone is not seen.
	Optional []string `json:"optional,omitempty"`
}

// All returns every privilege ALL grants on objectType, spelled in any case.
// It returns nil for a kind Kinds leaves out, and a new slice each time.
func (a AllPrivileges) All(objectType string) []string {
	return slices.Clone(a.Kinds[strings.ToUpper(strings.TrimSpace(objectType))].Privileges)
}

// Required returns the privileges ALL grants on every object of objectType on
// every supported release: [AllPrivileges.All] without the optional ones. It
// returns nil for a kind Kinds leaves out, and a new slice each time.
func (a AllPrivileges) Required(objectType string) []string {
	optional := a.Kinds[strings.ToUpper(strings.TrimSpace(objectType))].Optional
	return slices.DeleteFunc(a.All(objectType), func(privilege string) bool {
		return slices.Contains(optional, privilege)
	})
}

// validateAllPrivileges refuses a list a comparison could not match against a
// catalog read: a kind or a privilege not in upper case, an empty list, a
// privilege named twice, ALL itself, an optional privilege the list does not
// name, and a routine list left out where a grant names a routine as it names
// a table. Without that list a declared ALL on a routine would never be held.
func (d Dialect) validateAllPrivileges() error {
	for _, kind := range routineKinds {
		if _, stated := d.AllPrivileges.Kinds[kind]; d.AllPrivileges.RoutinesByName && !stated {
			return fmt.Errorf("%w: dialect %q: ALL privileges on routines by name state no %s list", ErrInvalidDescriptor, d.Name, kind)
		}
	}
	for kind, list := range d.AllPrivileges.Kinds {
		if !upperWords(kind) {
			return fmt.Errorf("%w: dialect %q: ALL privilege kind %q must be upper-case words", ErrInvalidDescriptor, d.Name, kind)
		}
		if len(list.Privileges) == 0 {
			return fmt.Errorf("%w: dialect %q: ALL privilege kind %q names no privilege", ErrInvalidDescriptor, d.Name, kind)
		}
		seen := make(map[string]bool, len(list.Privileges))
		for _, privilege := range list.Privileges {
			switch {
			case !upperWords(privilege) || privilege == "ALL":
				return fmt.Errorf("%w: dialect %q: ALL privilege %q on %s must be upper-case words other than ALL", ErrInvalidDescriptor, d.Name, privilege, kind)
			case seen[privilege]:
				return fmt.Errorf("%w: dialect %q: ALL privilege %q is named twice on %s", ErrInvalidDescriptor, d.Name, privilege, kind)
			}
			seen[privilege] = true
		}
		for _, privilege := range list.Optional {
			if !seen[privilege] {
				return fmt.Errorf("%w: dialect %q: optional ALL privilege %q is not one ALL grants on %s", ErrInvalidDescriptor, d.Name, privilege, kind)
			}
		}
	}
	return nil
}

// upperWords reports whether value is upper-case ASCII letters in words one
// space apart, such as "CREATE VIEW".
func upperWords(value string) bool {
	words := strings.Split(value, " ")
	return !slices.ContainsFunc(words, func(word string) bool {
		return word == "" || strings.IndexFunc(word, func(r rune) bool { return r < 'A' || r > 'Z' }) >= 0
	})
}
