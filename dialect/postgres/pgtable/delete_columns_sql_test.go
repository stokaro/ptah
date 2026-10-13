package pgtable_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

func TestPostgresDeleteColumnsFromDefinition(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		expected   []string
	}{
		{
			name:       "SET NULL on one column",
			definition: `FOREIGN KEY (x, a) REFERENCES p(a, b) ON DELETE SET NULL (a)`,
			expected:   []string{"a"},
		},
		{
			name:       "SET DEFAULT",
			definition: `FOREIGN KEY (x, a) REFERENCES p(a, b) ON DELETE SET DEFAULT (a)`,
			expected:   []string{"a"},
		},
		{
			name:       "after ON UPDATE, in the order written",
			definition: `FOREIGN KEY (x, a) REFERENCES p(a, b) ON UPDATE CASCADE ON DELETE SET NULL (a, x)`,
			expected:   []string{"a", "x"},
		},
		{
			name:       "before DEFERRABLE",
			definition: `FOREIGN KEY (x, a) REFERENCES p(a, b) ON DELETE SET NULL (a) DEFERRABLE INITIALLY DEFERRED`,
			expected:   []string{"a"},
		},
		{
			name:       "quoted, with a comma and a parenthesis in the name",
			definition: `FOREIGN KEY ("X", "a,(b)") REFERENCES p(a, b) ON DELETE SET NULL ("a,(b)")`,
			expected:   []string{"a,(b)"},
		},
		{
			// An unbalanced parenthesis inside a quoted name does not close
			// the list.
			name:       "a quoted name holding a closing parenthesis",
			definition: `FOREIGN KEY ("x", "a)b") REFERENCES p(a, b) ON DELETE SET NULL ("a)b")`,
			expected:   []string{"a)b"},
		},
		{
			name:       "no list",
			definition: `FOREIGN KEY (x, a) REFERENCES p(a, b) ON DELETE SET NULL`,
		},
		{
			name:       "SET NULL on update only",
			definition: `FOREIGN KEY (x, a) REFERENCES p(a, b) ON UPDATE SET NULL`,
		},
		{
			name:       "no action at all",
			definition: `FOREIGN KEY (x) REFERENCES p(a)`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgtable.DeleteColumnsFromDefinition(test.definition)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.expected)
		})
	}
}

func TestParseDeleteColumnList_HappyPath(t *testing.T) {
	tests := []struct {
		name, text string
		want       []string
	}{
		{name: "case folding", text: "(Parent_ID, Tenant)", want: []string{"parent_id", "tenant"}},
		{name: "quoted punctuation", text: `("parent,id", "tenant.dot", "a)b")`, want: []string{"parent,id", "tenant.dot", "a)b"}},
		{name: "repeated column", text: "(id, tenant, id)", want: []string{"id", "tenant", "id"}},
		{name: "comments", text: "(/* column */ id -- end\n)", want: []string{"id"}},
		{name: "escaped quote", text: `("a""b")`, want: []string{`a"b`}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgtable.ParseDeleteColumnList(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestParseDeleteColumnList_FailurePath(t *testing.T) {
	for _, text := range []string{"", "()", "(id,)", "(,id)", "(id other)", "(t.id)", "(lower(id))", "(id); DROP TABLE t", "(id) ON UPDATE CASCADE", "(id", `("")`, "('id')"} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgtable.ParseDeleteColumnList(text)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestDeleteColumnsFromDefinition_QuotedClauseWords(t *testing.T) {
	c := qt.New(t)
	got, err := pgtable.DeleteColumnsFromDefinition(`FOREIGN KEY ("ON DELETE SET NULL (x)") REFERENCES parent (id)`)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.IsNil)
}
