package lintnames_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lintnames"
)

// TestCategoriesAreDeclaredOnce reads the category declaration: each category
// has one entry and a summary, and an engine category is named for its
// engine.
func TestCategoriesAreDeclaredOnce(t *testing.T) {
	c := qt.New(t)
	categories := lintnames.Categories()
	names := make([]string, 0, len(categories))
	for _, category := range categories {
		names = append(names, category.Name)
	}
	unsummarized := slices.DeleteFunc(slices.Clone(categories), func(category lintnames.Category) bool { return category.Summary != "" })
	misnamed := slices.DeleteFunc(slices.Clone(categories), func(category lintnames.Category) bool {
		return category.Engine == "" || category.Engine == category.Name
	})

	c.Assert(slices.IsSorted(names), qt.IsTrue, qt.Commentf("categories are declared in name order: %v", names))
	c.Assert(slices.Compact(slices.Clone(names)), qt.DeepEquals, names)
	c.Assert(unsummarized, qt.HasLen, 0)
	c.Assert(misnamed, qt.HasLen, 0)
}

// TestIsSelector splits selectors into the name space and the rest. A
// lower-case word that is not a category is not a selector, and neither is a
// code, a slash on its own or an upper-case word.
func TestIsSelector(t *testing.T) {
	rows := []struct {
		selector string
		want     bool
	}{
		{selector: "locking/set-not-null", want: true},
		{selector: "locking/", want: true},
		{selector: "locking", want: true},
		{selector: " locking ", want: false},
		{selector: "lockin/", want: true},
		{selector: "lockin/set-not-null", want: true},
		{selector: "lockin", want: false},
		{selector: "LOCKING", want: false},
		{selector: "PG", want: false},
		{selector: "DS101", want: false},
		{selector: "DS101/DS102", want: false},
		{selector: "/", want: false},
		{selector: "", want: false},
	}
	for _, row := range rows {
		t.Run(row.selector, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintnames.IsSelector(row.selector), qt.Equals, row.want)
		})
	}
}

// TestSelects pins exact matching. The rows that select nothing are what a
// prefix match would turn into hits: `locking/set-not` and `lock` both begin
// the name locking/set-not-null.
func TestSelects(t *testing.T) {
	rows := []struct {
		name     string
		selector string
		rule     string
		want     bool
	}{
		{name: "the rule's name", selector: "locking/set-not-null", rule: "locking/set-not-null", want: true},
		{name: "the rule's category", selector: "locking", rule: "locking/set-not-null", want: true},
		{name: "the rule's category with a slash", selector: "locking/", rule: "locking/set-not-null", want: true},
		{name: "a provider's rule", selector: "ydb/add-column-form", rule: "ydb/add-column-form", want: true},
		{name: "the beginning of the name", selector: "locking/set-not", rule: "locking/set-not-null", want: false},
		{name: "the beginning of the category", selector: "lock", rule: "locking/set-not-null", want: false},
		{name: "the beginning of the category with a slash", selector: "lock/", rule: "locking/set-not-null", want: false},
		{name: "another rule's name", selector: "locking/add-foreign-key", rule: "locking/set-not-null", want: false},
		{name: "another category", selector: "destructive", rule: "locking/set-not-null", want: false},
		{name: "a code", selector: "PG303", rule: "locking/set-not-null", want: false},
		{name: "no rule", selector: "locking", rule: "", want: false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintnames.Selects(row.selector, row.rule), qt.Equals, row.want)
		})
	}
}

// migrationSet is a small set of names, two of them in one category.
func migrationSet() lintnames.Set {
	return lintnames.NewSet("migration lint", []string{
		"locking/set-not-null", "locking/add-foreign-key", "destructive/drop-table", "online/unproven",
	})
}

func TestSet_Resolve_HappyPath(t *testing.T) {
	rows := []struct {
		name     string
		selector string
		want     []string
	}{
		{name: "a name", selector: "locking/add-foreign-key", want: []string{"locking/add-foreign-key"}},
		{name: "a category", selector: "locking", want: []string{"locking/add-foreign-key", "locking/set-not-null"}},
		{name: "a category with a slash", selector: "locking/", want: []string{"locking/add-foreign-key", "locking/set-not-null"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationSet().Resolve(row.selector)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}

// TestSet_Resolve_FailurePath refuses every selector that names no rule of
// the set, each with the reason, and refuses a code with no word about the
// rules it once stood for.
func TestSet_Resolve_FailurePath(t *testing.T) {
	rows := []struct {
		name     string
		selector string
		wantErr  string
	}{
		{
			name: "a misspelled name", selector: "locking/set-not-nul",
			wantErr: `"locking/set-not-nul" is not the name of a migration lint rule; ` +
				`a name is a category and a rule joined by a slash, such as locking/add-foreign-key`,
		},
		{
			name: "a category that is not declared", selector: "lockin/",
			wantErr: `"lockin/" is not a rule category; the categories are capability, correctness, .*`,
		},
		{
			name: "a declared category the set has no rule in", selector: "naming",
			wantErr: `"naming" is a category that holds no migration lint rule`,
		},
		{
			name: "a code", selector: "DS102",
			wantErr: `lint rules are named category/name; DS102 is not a rule name`,
		},
		{
			name: "a code prefix", selector: "DS",
			wantErr: `lint rules are named category/name; DS is not a rule name`,
		},
		{
			name: "a lower-case word", selector: "lockin",
			wantErr: `"lockin" is not a rule name or category; lint rules are named category/name, such as locking/add-foreign-key`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := migrationSet().Resolve(row.selector)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestSet_ValidateSelector_HappyPath(t *testing.T) {
	for _, selector := range []string{"", "locking", "locking/", "destructive/drop-table"} {
		t.Run(selector, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationSet().ValidateSelector(selector), qt.IsNil)
		})
	}
}

func TestSet_ValidateSelector_FailurePath(t *testing.T) {
	rows := []struct {
		selector string
		wantErr  string
	}{
		{selector: "PG301", wantErr: `lint rules are named category/name; PG301 is not a rule name`},
		{selector: "DS", wantErr: `lint rules are named category/name; DS is not a rule name`},
		{selector: "locking/typo", wantErr: `"locking/typo" is not the name of a migration lint rule.*`},
		{selector: "lockin", wantErr: `"lockin" is not a rule name or category.*`},
	}
	for _, row := range rows {
		t.Run(row.selector, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(migrationSet().ValidateSelector(row.selector), qt.ErrorMatches, row.wantErr)
		})
	}
}

// TestValidateGrammar judges a selector by form alone, as a configuration file
// is read before the run knows its rules.
func TestValidateGrammar_FailurePath(t *testing.T) {
	rows := []struct {
		selector string
		wantErr  string
	}{
		{selector: "MF101", wantErr: `lint rules are named category/name; MF101 is not a rule name`},
		{selector: "locking/Add", wantErr: `"locking/Add" is not a rule name; a name is a category and a rule joined by a slash.*`},
		{selector: "nosuch/add-foreign-key", wantErr: `"nosuch/add-foreign-key" is not a rule category; the categories are .*`},
		{selector: "nosuch/", wantErr: `"nosuch/" is not a rule category; the categories are .*`},
	}
	for _, row := range rows {
		t.Run(row.selector, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintnames.ValidateGrammar(row.selector), qt.ErrorMatches, row.wantErr)
		})
	}
}

func TestValidateGrammar_HappyPath(t *testing.T) {
	for _, selector := range []string{"", "locking", "locking/", "locking/add-foreign-key", "ydb/add-column-form"} {
		t.Run(selector, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintnames.ValidateGrammar(selector), qt.IsNil)
		})
	}
}

// TestKeysOrderASection fixes the keys a `rules:` section is read with: the
// name names the rule alone, and the category keys name its group.
func TestKeysOrderASection(t *testing.T) {
	c := qt.New(t)

	c.Assert(lintnames.ConfigKeys("destructive/drop-table"), qt.DeepEquals, []string{"destructive/drop-table"})
	c.Assert(lintnames.GroupKeys("destructive/drop-table"), qt.DeepEquals, []string{"destructive", "destructive/"})
	c.Assert(lintnames.GroupKeys("nocategory"), qt.HasLen, 0)
}

func TestSet_ZeroValueHoldsNothing(t *testing.T) {
	c := qt.New(t)
	var set lintnames.Set

	c.Assert(set.Names(), qt.HasLen, 0)
	c.Assert(set.Has("locking/set-not-null"), qt.IsFalse)
}
