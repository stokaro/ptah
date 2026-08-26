package compare

// White-box testing required: the per-dialect normalizers these tests pin are
// package-local and have no exported API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
)

// TestNormalizeColumnTypesForDialect_AsksWhatTheRendererWouldWrite holds the
// question the type comparison has to answer.
//
// It is not "are these the same word" but "would rendering this declaration
// produce the type the catalog holds". Oracle has no counterpart for most
// declared type names -- a declared TEXT is a CLOB, an INT is a NUMBER(10), a
// BOOLEAN is a NUMBER(1) -- so comparing them raw reported an ALTER for every
// column of a database Ptah had just built from that declaration
// (stokaro/ptah#1875).
func TestNormalizeColumnTypesForDialect_AsksWhatTheRendererWouldWrite(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		catalog  string
		// wantEqual is whether the two describe one type.
		wantEqual bool
	}{
		{name: "text is a clob", declared: "TEXT", catalog: "CLOB", wantEqual: true},
		{name: "int is a number(10)", declared: "INT", catalog: "NUMBER(10)", wantEqual: true},
		{name: "boolean is a number(1)", declared: "BOOLEAN", catalog: "NUMBER(1)", wantEqual: true},
		{name: "bigint is a number(19)", declared: "BIGINT", catalog: "NUMBER(19)", wantEqual: true},
		{name: "decimal keeps its scale", declared: "DECIMAL(5,2)", catalog: "NUMBER(5,2)", wantEqual: true},
		{name: "varchar is a varchar2", declared: "VARCHAR(200)", catalog: "VARCHAR2(200)", wantEqual: true},
		{name: "jsonb is json", declared: "JSONB", catalog: "JSON", wantEqual: true},
		// The control: the mapping must not fold types that genuinely differ,
		// or a real column change would stop being reported.
		{name: "an integer widening is still a change", declared: "INT", catalog: "NUMBER(19)"},
		{name: "text is not a number", declared: "TEXT", catalog: "NUMBER(10)"},
		{name: "a clob is not a varchar2", declared: "TEXT", catalog: "VARCHAR2(200)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			declared, catalog := normalizeColumnTypesForDialect(schemamodel.Field{Type: test.declared}, test.catalog, platform.Oracle)

			c.Assert(declared == catalog, qt.Equals, test.wantEqual)
		})
	}
}

// TestShouldReportSizedTypeChange_KeepsAWidthChangeVisible is the other half of
// the pairing above.
//
// normalizeColumnTypesForDialect folds a width away deliberately -- that is
// what normalize.Type does for every dialect -- so a declared VARCHAR(200) and
// a catalog VARCHAR2(400) compare equal there. A width change is a real ALTER,
// and this is the function that keeps it visible. Without this test the
// suppression added for Oracle beside SQLite's could hide one and nothing would
// notice.
func TestShouldReportSizedTypeChange_KeepsAWidthChangeVisible(t *testing.T) {
	tests := []struct {
		name     string
		declared string
		catalog  string
		want     bool
	}{
		{name: "a wider varchar2 is reported", declared: "VARCHAR(200)", catalog: "VARCHAR2(400)", want: true},
		{name: "a narrower varchar2 is reported", declared: "VARCHAR(400)", catalog: "VARCHAR2(200)", want: true},
		{name: "an integer widening is reported", declared: "BIGINT", catalog: "NUMBER(10)", want: true},
		// The suppression: a declaration that renders to exactly the catalog
		// type is not a width change at all.
		{name: "the same width is not a change", declared: "VARCHAR(200)", catalog: "VARCHAR2(200)"},
		{name: "int against its own mapping", declared: "INT", catalog: "NUMBER(10)"},
		{name: "decimal against its own mapping", declared: "DECIMAL(5,2)", catalog: "NUMBER(5,2)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := shouldReportSizedTypeChange(test.catalog, test.declared, platform.Oracle)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRenderedDefaultForDialect_FollowsTheTypeTheDefaultLandedOn pins the other
// half of the same question.
//
// BOOLEAN becomes NUMBER(1) on Oracle, so a column declared `default="true"` is
// written as `DEFAULT 1` and read back as `1`. Comparing the declared `true`
// against the catalog's `1` reported a default change on a column that matched.
func TestRenderedDefaultForDialect_FollowsTheTypeTheDefaultLandedOn(t *testing.T) {
	tests := []struct {
		name         string
		declaredType string
		declared     string
		dialect      string
		want         string
	}{
		{name: "true becomes one", declaredType: "BOOLEAN", declared: "true", dialect: platform.Oracle, want: "1"},
		{name: "false becomes zero", declaredType: "BOOLEAN", declared: "false", dialect: platform.Oracle, want: "0"},
		{name: "a quoted spelling too", declaredType: "BOOLEAN", declared: "'true'", dialect: platform.Oracle, want: "1"},
		// A default on any other type is left exactly as declared: Oracle
		// converts a quoted number implicitly, so rewriting those would change
		// defaults that already work.
		{name: "a number is untouched", declaredType: "INT", declared: "0", dialect: platform.Oracle, want: "0"},
		{name: "a string is untouched", declaredType: "TEXT", declared: "'none'", dialect: platform.Oracle, want: "'none'"},
		// And no other dialect is touched at all.
		{name: "postgres keeps true", declaredType: "BOOLEAN", declared: "true", dialect: platform.Postgres, want: "true"},
		{name: "mysql keeps true", declaredType: "BOOLEAN", declared: "true", dialect: platform.MySQL, want: "true"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := renderedDefaultForDialect(test.declared, test.declaredType, test.dialect)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestSameColumnNames_ComparesUnderTheDialectsRule holds the foreign-key half.
//
// Oracle stores an unquoted `author_id` as AUTHOR_ID, so comparing the
// spellings made an untouched foreign key read as changed -- which the diff
// expresses as a drop and an add of the same constraint, on every run.
func TestSameColumnNames_ComparesUnderTheDialectsRule(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		wantEqual bool
	}{
		{name: "oracle folds", dialect: platform.Oracle, wantEqual: true},
		{name: "sqlite folds", dialect: platform.SQLite, wantEqual: true},
		{name: "postgres does not", dialect: platform.Postgres},
		{name: "mysql does not", dialect: platform.MySQL},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			semantics := identifier.ForDialect(test.dialect)

			c.Assert(sameColumnNames(semantics, []string{"author_id"}, []string{"AUTHOR_ID"}),
				qt.Equals, test.wantEqual)
			// The control every row shares: two genuinely different columns
			// must never collide, and neither must two lists of unequal length.
			c.Assert(sameColumnNames(semantics, []string{"author_id"}, []string{"post_id"}), qt.IsFalse)
			c.Assert(sameColumnNames(semantics, []string{"a", "b"}, []string{"a"}), qt.IsFalse)
		})
	}
}
