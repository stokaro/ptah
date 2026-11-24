package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// listed is a descriptor stating the lists of a table and a database, with
// one privilege of the database's list that ALL does not always grant, and
// a grammar that reads each privilege of more than one word they name.
func listed() descriptor.Dialect {
	described := head("alpha")
	described.AllPrivileges = descriptor.AllPrivileges{Kinds: map[string]descriptor.AllPrivilegeList{
		"TABLE":  {Privileges: []string{"SELECT", "SHOW VIEW"}},
		"SCHEMA": {Privileges: []string{"SELECT", "SHOW CREATE ROUTINE"}, Optional: []string{"SHOW CREATE ROUTINE"}},
	}}
	described.Grammar.Forms.MultiWordPrivileges = []string{"SHOW CREATE ROUTINE", "SHOW VIEW"}
	return described
}

// withMultiWord returns listed with a grammar that reads privileges as its
// privileges of more than one word.
func withMultiWord(privileges ...string) descriptor.Dialect {
	described := listed()
	described.Grammar.Forms.MultiWordPrivileges = privileges
	return described
}

// withList returns listed with list stated for kind, so each row carries the
// one value it is about.
func withList(kind string, list descriptor.AllPrivilegeList) descriptor.Dialect {
	described := listed()
	described.AllPrivileges.Kinds[kind] = list
	return described
}

// withClasses returns listed with a grammar that names classes before `::`.
func withClasses(classes ...string) descriptor.Dialect {
	described := listed()
	described.Grammar.Forms.GrantClasses = classes
	return described
}

// byName returns listed granting on a routine by its name, with a list for
// each of kinds.
func byName(kinds ...string) descriptor.Dialect {
	described := listed()
	described.AllPrivileges.RoutinesByName = true
	for _, kind := range kinds {
		described.AllPrivileges.Kinds[kind] = descriptor.AllPrivilegeList{Privileges: []string{"EXECUTE"}}
	}
	return described
}

func TestNewRegistry_AllPrivileges_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		described descriptor.Dialect
	}{
		{name: "lists", described: listed()},
		{name: "routines by name, with both routine lists", described: byName("FUNCTION", "PROCEDURE")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.described)

			c.Assert(err, qt.IsNil)
			described, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(described, qt.DeepEquals, test.described)
		})
	}
}

func TestAllPrivileges_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		objectType   string
		wantAll      []string
		wantRequired []string
	}{
		{name: "a kind with no optional privilege", objectType: "TABLE", wantAll: []string{"SELECT", "SHOW VIEW"}, wantRequired: []string{"SELECT", "SHOW VIEW"}},
		{name: "a kind spelled in another case", objectType: " table ", wantAll: []string{"SELECT", "SHOW VIEW"}, wantRequired: []string{"SELECT", "SHOW VIEW"}},
		{name: "a kind with one", objectType: "SCHEMA", wantAll: []string{"SELECT", "SHOW CREATE ROUTINE"}, wantRequired: []string{"SELECT"}},
		{name: "a kind with one, spelled in another case", objectType: " schema ", wantAll: []string{"SELECT", "SHOW CREATE ROUTINE"}, wantRequired: []string{"SELECT"}},
		{name: "a kind the lists leave out", objectType: "SEQUENCE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			lists := listed().AllPrivileges

			all := lists.All(test.objectType)
			required := lists.Required(test.objectType)

			c.Assert(all, qt.DeepEquals, test.wantAll)
			c.Assert(required, qt.DeepEquals, test.wantRequired)
		})
	}
}

// TestAllPrivileges_ReturnsACopy pins that a caller changing an answer changes
// nothing a later answer reads.
func TestAllPrivileges_ReturnsACopy(t *testing.T) {
	c := qt.New(t)
	lists := listed().AllPrivileges

	lists.All("TABLE")[0] = "changed"
	lists.Required("TABLE")[0] = "changed"

	c.Assert(lists.All("TABLE"), qt.DeepEquals, []string{"SELECT", "SHOW VIEW"})
}

func TestNewRegistry_AllPrivileges_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		edit    descriptor.Dialect
		wantErr string
	}{
		{
			name:    "a kind in lower case",
			edit:    withList("view", descriptor.AllPrivilegeList{Privileges: []string{"SELECT"}}),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privilege kind "view" must be upper-case words`,
		},
		{
			name:    "a kind with no privilege",
			edit:    withList("VIEW", descriptor.AllPrivilegeList{}),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privilege kind "VIEW" names no privilege`,
		},
		{
			name:    "a privilege in lower case",
			edit:    withList("VIEW", descriptor.AllPrivilegeList{Privileges: []string{"select"}}),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privilege "select" on VIEW must be upper-case words other than ALL`,
		},
		{
			name:    "a privilege with two spaces",
			edit:    withList("VIEW", descriptor.AllPrivilegeList{Privileges: []string{"SHOW  VIEW"}}),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privilege "SHOW  VIEW" on VIEW must be upper-case words other than ALL`,
		},
		{
			name:    "ALL itself",
			edit:    withList("VIEW", descriptor.AllPrivilegeList{Privileges: []string{"ALL"}}),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privilege "ALL" on VIEW must be upper-case words other than ALL`,
		},
		{
			name:    "a privilege named twice",
			edit:    withList("VIEW", descriptor.AllPrivilegeList{Privileges: []string{"SELECT", "SELECT"}}),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privilege "SELECT" is named twice on VIEW`,
		},
		{
			name:    "an optional privilege the list does not name",
			edit:    withList("VIEW", descriptor.AllPrivilegeList{Privileges: []string{"SELECT"}, Optional: []string{"MERGE VIEW"}}),
			wantErr: `invalid dialect descriptor: dialect "alpha": optional ALL privilege "MERGE VIEW" is not one ALL grants on VIEW`,
		},
		{
			name:    "a privilege of more than one word the grammar does not read",
			edit:    withList("VIEW", descriptor.AllPrivilegeList{Privileges: []string{"SELECT", "MERGE VIEW"}}),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privilege "MERGE VIEW" on VIEW is not one of the grammar's multi-word privileges, so a GRANT naming it could not be read`,
		},
		{
			name:    "a multi-word privilege of one word",
			edit:    withMultiWord("SHOW CREATE ROUTINE", "SHOW VIEW", "SELECT"),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: multi-word privilege "SELECT" must be two or more upper-case words one space apart`,
		},
		{
			name:    "a multi-word privilege in lower case",
			edit:    withMultiWord("SHOW CREATE ROUTINE", "show view"),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: multi-word privilege "show view" must be two or more upper-case words one space apart`,
		},
		{
			name:    "a multi-word privilege named twice",
			edit:    withMultiWord("SHOW CREATE ROUTINE", "SHOW VIEW", "SHOW VIEW"),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: multi-word privilege "SHOW VIEW" is named twice`,
		},
		{
			name:    "a grant class in lower case",
			edit:    withClasses("schema"),
			wantErr: `invalid dialect descriptor: dialect "alpha": grammar: grant class "schema" must be upper-case words`,
		},
		{
			name:    "routines by name without a function list",
			edit:    byName("PROCEDURE"),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privileges on routines by name state no FUNCTION list`,
		},
		{
			name:    "routines by name without a procedure list",
			edit:    byName("FUNCTION"),
			wantErr: `invalid dialect descriptor: dialect "alpha": ALL privileges on routines by name state no PROCEDURE list`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.edit)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}
