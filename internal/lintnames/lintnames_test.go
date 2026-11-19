package lintnames_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/lintnames"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerpublished"
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

// TestIsSelector splits selectors into the name space and the code space. A
// lower-case word that is not a category stays a code, because `ptah sql
// lint` folds the case of a code, and so do upper-case codes joined by a
// slash and a slash alone, which a nolint directive splits at the slash.
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
		{selector: "PG303", want: false},
		{selector: "PG", want: false},
		{selector: "sql001", want: false},
		{selector: "ds101/ds102", want: true},
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

// TestTableSelects pins exact matching. The rows that select nothing are what
// a prefix match would turn into hits: `locking/set-not` and `lock` both
// begin the name of PG303.
func TestTableSelects(t *testing.T) {
	rows := []struct {
		name     string
		selector string
		code     string
		want     bool
	}{
		{name: "the rule's name", selector: "locking/set-not-null", code: "PG303", want: true},
		{name: "the rule's category", selector: "locking", code: "PG303", want: true},
		{name: "the rule's category with a slash", selector: "locking/", code: "PG303", want: true},
		{name: "a name shared on another engine", selector: "locking/add-foreign-key", code: "MY131", want: true},
		{name: "a provider's rule", selector: "ydb/add-column-form", code: "YD103", want: true},
		{name: "the beginning of the name", selector: "locking/set-not", code: "PG303", want: false},
		{name: "the beginning of the category", selector: "lock", code: "PG303", want: false},
		{name: "the beginning of the category with a slash", selector: "lock/", code: "PG303", want: false},
		{name: "another rule's name", selector: "locking/add-foreign-key", code: "PG303", want: false},
		{name: "another category", selector: "destructive", code: "PG303", want: false},
		{name: "the rule's code", selector: "PG303", code: "PG303", want: false},
		{name: "a code the table does not name", selector: "locking", code: "ZZ101", want: false},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lintnames.Migration().Selects(row.selector, row.code), qt.Equals, row.want)
		})
	}
}

// TestTableResolve_HappyPath resolves a selector to every code it selects, in
// each linter's table.
func TestTableResolve_HappyPath(t *testing.T) {
	rows := []struct {
		name     string
		table    lintnames.Table
		selector string
		want     []string
	}{
		{name: "one name on two engines", table: lintnames.Migration(), selector: "locking/add-foreign-key", want: []string{"MY131", "PG306"}},
		{name: "a name in SQL lint", table: lintnames.SQL(), selector: "naming/identifier-too-long", want: []string{"DDL003"}},
		{name: "a category in SQL lint", table: lintnames.SQL(), selector: "coverage/", want: []string{"SQL002", "SQL003", "SQL004"}},
		{name: "a category whose rules share a name", table: lintnames.Migration(), selector: "online", want: []string{"ON101", "ON102", "ON103"}},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := row.table.Resolve(row.selector)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, row.want)
		})
	}
}

// TestTableResolve_FailurePath refuses every selector that names no rule of
// the table, each with the reason.
func TestTableResolve_FailurePath(t *testing.T) {
	rows := []struct {
		name     string
		table    lintnames.Table
		selector string
		wantErr  string
	}{
		{
			name: "a misspelled name", table: lintnames.Migration(), selector: "locking/set-not-nul",
			wantErr: `"locking/set-not-nul" is not the name of a migration lint rule; ` +
				`a name is a category and a rule joined by a slash, such as locking/set-not-null`,
		},
		{
			name: "a name only the other linter has", table: lintnames.SQL(), selector: "locking/set-not-null",
			wantErr: `"locking/set-not-null" is not the name of a SQL lint rule; ` +
				`a name is a category and a rule joined by a slash, such as design/no-primary-key`,
		},
		{
			name: "a category that is not declared", table: lintnames.Migration(), selector: "lockin/",
			wantErr: `"lockin/" is not a rule category; the categories are capability, correctness, .*`,
		},
		{
			name: "a declared category the linter has no rule in", table: lintnames.SQL(), selector: "online",
			wantErr: `"online" is a category that holds no SQL lint rule`,
		},
		{
			name: "a code", table: lintnames.Migration(), selector: "PG303",
			wantErr: `"PG303" is not a rule name or category`,
		},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := row.table.Resolve(row.selector)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(got, qt.IsNil)
		})
	}
}

// TestTableName reads a rule's name, and the empty name of a code the table
// does not hold.
func TestTableName(t *testing.T) {
	c := qt.New(t)
	c.Assert(lintnames.Migration().Name("PG303"), qt.Equals, "locking/set-not-null")
	c.Assert(lintnames.SQL().Name("DDL001"), qt.Equals, "design/no-primary-key")
	c.Assert(lintnames.Migration().Name("DDL001"), qt.Equals, "")
	c.Assert(lintnames.Table{}.Name("PG303"), qt.Equals, "")
}

// TestMigration_NamesEveryRuleAnOfficialProviderPublishes reads the rule
// catalogs the official providers publish, as internal/providerpublished
// records them, and resolves each code a provider reports to a name that
// selects it. A provider reports its findings by code, so a code missing here
// would reach the report with no name and be out of reach of every name and
// category selector.
func TestMigration_NamesEveryRuleAnOfficialProviderPublishes(t *testing.T) {
	c := qt.New(t)
	descriptions, err := providerpublished.Descriptions()
	c.Assert(err, qt.IsNil)
	var codes []string
	for _, description := range descriptions {
		for _, engine := range description.GetEngines() {
			for _, rule := range providerhost.PublishedLintRules(engine) {
				codes = append(codes, rule.Code)
			}
		}
	}
	names := lintnames.Migration()
	unnamed := slices.DeleteFunc(slices.Clone(codes), func(code string) bool { return names.Name(code) != "" })
	unselected := slices.DeleteFunc(slices.Clone(codes), func(code string) bool { return names.Selects(names.Name(code), code) })

	c.Assert(codes, qt.Not(qt.HasLen), 0)
	c.Assert(unnamed, qt.HasLen, 0)
	c.Assert(unselected, qt.HasLen, 0)
}
