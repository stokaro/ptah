package defaultlit_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/renderer/internal/dialects/internal/defaultlit"
)

func TestIsSQLLiteral(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  bool
	}{
		// What the SQL parser stores, having kept the literal as written.
		{name: "a plain literal", value: `'active'`, want: true},
		{name: "an empty literal", value: `''`, want: true},
		{name: "a literal that is one escaped quote", value: `''''`, want: true},
		{name: "a literal with an escaped quote inside", value: `'it''s'`, want: true},

		// A cast makes the literal stop at something other than a quote, which
		// is why the two ends cannot be tested on their own.
		{name: "a literal carrying a cast", value: `'{}'::jsonb`, want: true},
		{name: "a cast to a two-word type", value: `'x'::character varying`, want: true},
		{name: "a cast to a parameterized type", value: `'1.5'::numeric(10,2)`, want: true},
		{name: "a cast to an array type", value: `'{}'::text[]`, want: true},

		// What a struct tag stores, which still needs quoting.
		{name: "a bare word", value: `active`, want: false},
		{name: "a bare number", value: `0`, want: false},
		{name: "a bare keyword", value: `true`, want: false},
		{name: "an empty value", value: ``, want: false},

		// Shapes that only look like literals.
		{name: "a lone quote", value: `'`, want: false},
		{name: "an unterminated literal", value: `'active`, want: false},
		{name: "a literal that never closes because every quote is doubled", value: `'''`, want: false},
		{name: "trailing text that is not a cast", value: `'x' or 1=1`, want: false},
		{name: "a cast marker with no type after it", value: `'x'::`, want: false},
		{name: "two literals", value: `'a','b'`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.IsSQLLiteral(tt.value), qt.Equals, tt.want)
		})
	}
}

// quotePostgres is the escaping the dialect renderers apply, repeated here so
// the test states what Render is choosing between rather than importing a
// renderer and creating a cycle.
func quotePostgres(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func TestRenderQuotesEachValueExactlyOnce(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{
			// The measured fault: quoting this again yields '''x''', which is
			// the three-character value 'x'.
			name: "a literal is passed through", value: `'x'`, want: `'x'`,
		},
		{name: "a bare value is quoted", value: `active`, want: `'active'`},
		{name: "a bare value containing a quote is escaped", value: `it's`, want: `'it''s'`},
		{name: "a literal with a cast is passed through", value: `'{}'::jsonb`, want: `'{}'::jsonb`},
		{name: "surrounding space does not hide a literal", value: `  'x'  `, want: `'x'`},
		{name: "an empty value becomes an empty literal", value: ``, want: `''`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(defaultlit.Render(tt.value, quotePostgres), qt.Equals, tt.want)
		})
	}
}

// TestRenderIsIdempotentOnItsOwnOutput states the property the fault violated:
// rendering a value and rendering the result again have to agree, because a
// default makes a round trip through the parser on every subsequent run.
func TestRenderIsIdempotentOnItsOwnOutput(t *testing.T) {
	for _, value := range []string{`'x'`, `active`, `'{}'::jsonb`, `it's`, ``} {
		t.Run(value, func(t *testing.T) {
			c := qt.New(t)
			once := defaultlit.Render(value, quotePostgres)
			c.Assert(defaultlit.Render(once, quotePostgres), qt.Equals, once)
		})
	}
}
