package schemamodel_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// mergeLanguages merges two sources that each declare one routine, the same
// in everything but the languages given, and returns the languages of the
// merged routines.
func mergeLanguages(c *qt.C, first, second string) ([]string, error) {
	c.Helper()
	routine := func(language string) *schemamodel.Database {
		return &schemamodel.Database{Functions: []schemamodel.Function{
			{Name: "app.f", Parameters: "a int", Returns: "int", Language: language, Body: "BEGIN RETURN a; END;"},
		}}
	}
	merged, err := schemamodel.Merge(routine(first), routine(second))
	if err != nil {
		return nil, err
	}
	languages := make([]string, 0, len(merged.Functions))
	for _, function := range merged.Functions {
		languages = append(languages, function.Language)
	}
	return languages, nil
}

// A routine that declares no language is in its target's, which is not known
// while sources are merged. It does not conflict with a declaration of the
// same routine that states one, in either order, and the stated language is
// the one kept: on PostgreSQL the two are one routine.
func TestMerge_AnOmittedLanguageAgreesWithAStatedOne_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		first  string
		second string
		want   []string
	}{
		{name: "stated first", first: "plpgsql", second: "", want: []string{"plpgsql"}},
		{name: "stated second", first: "", second: "plpgsql", want: []string{"plpgsql"}},
		{name: "both stated alike", first: "sql", second: "sql", want: []string{"sql"}},
		{name: "neither stated", first: "", second: "", want: []string{""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			languages, err := mergeLanguages(c, test.first, test.second)

			c.Assert(err, qt.IsNil)
			c.Assert(languages, qt.DeepEquals, test.want)
		})
	}
}

// Two stated languages that differ are two routines no target can hold under
// one signature, and the merge refuses them.
func TestMerge_AnOmittedLanguageAgreesWithAStatedOne_FailurePath(t *testing.T) {
	c := qt.New(t)

	languages, err := mergeLanguages(c, "sql", "plpgsql")

	c.Assert(err, qt.ErrorMatches, `.*conflicting function "app.f\(integer\)" definitions.*`)
	c.Assert(languages, qt.IsNil)
}
