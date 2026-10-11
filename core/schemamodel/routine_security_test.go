package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// mergeSecurity merges two sources that each declare one routine, the same in
// everything but the security modes given, and returns the modes of the
// merged routines.
func mergeSecurity(c *qt.C, first, second string) ([]string, error) {
	c.Helper()
	routine := func(security string) *schemamodel.Database {
		return &schemamodel.Database{Functions: []schemamodel.Function{
			{Name: "app.f", Parameters: "a int", Returns: "int", Security: security, Body: "BEGIN RETURN a; END;"},
		}}
	}
	merged, err := schemamodel.Merge(routine(first), routine(second))
	if err != nil {
		return nil, err
	}
	modes := make([]string, 0, len(merged.Functions))
	for _, function := range merged.Functions {
		modes = append(modes, function.Security)
	}
	return modes, nil
}

// A routine that states no security mode runs with the rights its target
// gives it, which is not known while sources are merged. It agrees with a
// declaration of the same routine that states INVOKER, in either order, and
// INVOKER is kept: on PostgreSQL and SQL Server the two are one routine.
func TestMerge_AnOmittedSecurityModeAgreesWithInvoker_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  string
		second string
		want   []string
	}{
		{name: "invoker first", first: "INVOKER", second: "", want: []string{"INVOKER"}},
		{name: "invoker second", first: "", second: "INVOKER", want: []string{"INVOKER"}},
		{name: "both definer", first: "DEFINER", second: "definer", want: []string{"DEFINER"}},
		{name: "neither stated", first: "", second: "", want: []string{""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			modes, err := mergeSecurity(c, test.first, test.second)

			c.Assert(err, qt.IsNil)
			c.Assert(modes, qt.DeepEquals, test.want)
		})
	}
}

// An omitted mode does not take a stated DEFINER. On PostgreSQL and SQL Server
// the declaration that says nothing runs with its caller's rights, and keeping
// DEFINER would give the routine its definer's without anything reporting it.
// So the merge refuses the pair, as it refuses two stated modes that differ.
func TestMerge_AnOmittedSecurityModeAgreesWithInvoker_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  string
		second string
	}{
		{name: "definer first", first: "DEFINER", second: ""},
		{name: "definer second", first: "", second: "DEFINER"},
		{name: "definer in another case", first: "", second: "definer"},
		{name: "both stated, differently", first: "INVOKER", second: "DEFINER"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			modes, err := mergeSecurity(c, test.first, test.second)

			c.Assert(err, qt.ErrorMatches, `.*conflicting function "app.f\(integer\)" definitions.*`)
			c.Assert(modes, qt.IsNil)
		})
	}
}

// Deduplicate is exported and runs without the conflict check, so it holds the
// same rule itself: the declaration it keeps takes a stated INVOKER and never
// a stated DEFINER, in whatever case either is spelled.
func TestDeduplicate_AnOmittedSecurityModeTakesOnlyInvoker(t *testing.T) {
	for _, test := range []struct {
		name   string
		second string
		want   string
	}{
		{name: "invoker", second: "invoker", want: "invoker"},
		{name: "definer", second: "DEFINER", want: ""},
		{name: "definer in another case", second: "definer", want: ""},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "app.f", Parameters: "a int", Returns: "int", Body: "BEGIN RETURN a; END;"},
				{Name: "app.f", Parameters: "a int", Returns: "int", Security: test.second, Body: "BEGIN RETURN a; END;"},
			}}

			schemamodel.Deduplicate(database)

			c.Assert(database.Functions, qt.HasLen, 1)
			c.Assert(database.Functions[0].Security, qt.Equals, test.want)
		})
	}
}
