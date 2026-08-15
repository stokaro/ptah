package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/deporder"
)

// TestUserTypesForCreate_OrdersEachKindAgainstTheOthers pins the property the
// three kinds share: a domain, a composite and a range live in one namespace
// and can name each other in either direction, so no fixed order of kinds
// serves them. Every row here is a pair that a kind-by-kind emitter gets wrong
// in one direction or the other.
func TestUserTypesForCreate_OrdersEachKindAgainstTheOthers(t *testing.T) {

	tests := []struct {
		name  string
		input []deporder.UserType
		want  []string
	}{
		{
			name: "domain over a composite waits for the composite",
			input: []deporder.UserType{
				{Name: "d_comp", References: []string{"addr"}},
				{Name: "addr", References: []string{"text"}},
			},
			want: []string{"addr", "d_comp"},
		},
		{
			name: "composite over a domain waits for the domain",
			input: []deporder.UserType{
				{Name: "addr", References: []string{"d_int", "text"}},
				{Name: "d_int", References: []string{"integer"}},
			},
			want: []string{"d_int", "addr"},
		},
		{
			name: "domain over a range waits for the range",
			input: []deporder.UserType{
				{Name: "d_range", References: []string{"myrange"}},
				{Name: "myrange", References: []string{"integer"}},
			},
			want: []string{"myrange", "d_range"},
		},
		{
			name: "range over a domain waits for the domain",
			input: []deporder.UserType{
				{Name: "myrange", References: []string{"d_int"}},
				{Name: "d_int", References: []string{"integer"}},
			},
			want: []string{"d_int", "myrange"},
		},
		{
			name: "domain over a domain waits for the domain",
			input: []deporder.UserType{
				{Name: "d_outer", References: []string{"d_inner"}},
				{Name: "d_inner", References: []string{"integer"}},
			},
			want: []string{"d_inner", "d_outer"},
		},
		{
			name: "a chain across all three kinds",
			input: []deporder.UserType{
				{Name: "d_comp", References: []string{"addr"}},
				{Name: "addr", References: []string{"d_range", "text"}},
				{Name: "d_range", References: []string{"myrange"}},
				{Name: "myrange", References: []string{"integer"}},
			},
			want: []string{"myrange", "d_range", "addr", "d_comp"},
		},
		{
			name: "types that name nothing in the set keep caller order",
			input: []deporder.UserType{
				{Name: "d_two", References: []string{"integer"}},
				{Name: "d_one", References: []string{"text"}},
			},
			want: []string{"d_two", "d_one"},
		},
		{
			name: "an array of a composite is still a reference to it",
			input: []deporder.UserType{
				{Name: "d_comp", References: []string{"addr[]"}},
				{Name: "addr", References: []string{"text"}},
			},
			want: []string{"addr", "d_comp"},
		},
		{
			name: "a length modifier does not hide the base type",
			input: []deporder.UserType{
				{Name: "d_text", References: []string{"short_text(20)"}},
				{Name: "short_text", References: []string{"character varying(255)"}},
			},
			want: []string{"short_text", "d_text"},
		},
		{
			name: "a qualified reference matches a qualified name",
			input: []deporder.UserType{
				{Name: "app.d_comp", References: []string{`"app"."addr"`}},
				{Name: "app.addr", References: []string{"text"}},
			},
			want: []string{"app.addr", "app.d_comp"},
		},
		{
			name: "an unqualified reference matches the one type carrying that name",
			input: []deporder.UserType{
				{Name: "app.d_comp", References: []string{"addr"}},
				{Name: "app.addr", References: []string{"text"}},
			},
			want: []string{"app.addr", "app.d_comp"},
		},
		{
			name: "a bare name two schemas both offer stays unresolved",
			input: []deporder.UserType{
				{Name: "app.d_comp", References: []string{"addr"}},
				{Name: "other.addr", References: []string{"text"}},
				{Name: "app.addr", References: []string{"text"}},
			},
			want: []string{"app.d_comp", "other.addr", "app.addr"},
		},
		{
			name: "a cycle degrades to caller order rather than dropping a type",
			input: []deporder.UserType{
				{Name: "a", References: []string{"b"}},
				{Name: "b", References: []string{"a"}},
			},
			want: []string{"a", "b"},
		},
		{
			name: "a self reference is not a dependency",
			input: []deporder.UserType{
				{Name: "d_self", References: []string{"d_self"}},
				{Name: "addr", References: []string{"text"}},
			},
			want: []string{"d_self", "addr"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.UserTypesForCreate(test.input), qt.DeepEquals, test.want)
		})
	}
}

// TestUserTypesForDrop_ReversesTheCreateOrder is the other half: a non-CASCADE
// drop of a composite fails while a domain still names it, so the dependent
// goes first.
func TestUserTypesForDrop_ReversesTheCreateOrder(t *testing.T) {

	tests := []struct {
		name  string
		input []deporder.UserType
		want  []string
	}{
		{
			name: "the domain over a composite is dropped before the composite",
			input: []deporder.UserType{
				{Name: "addr", References: []string{"text"}},
				{Name: "d_comp", References: []string{"addr"}},
			},
			want: []string{"d_comp", "addr"},
		},
		{
			name: "the composite over a domain is dropped before the domain",
			input: []deporder.UserType{
				{Name: "d_int", References: []string{"integer"}},
				{Name: "addr", References: []string{"d_int"}},
			},
			want: []string{"addr", "d_int"},
		},
		{
			name: "independent types keep caller order",
			input: []deporder.UserType{
				{Name: "d_two", References: []string{"integer"}},
				{Name: "d_one", References: []string{"text"}},
			},
			want: []string{"d_two", "d_one"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.UserTypesForDrop(test.input), qt.DeepEquals, test.want)
		})
	}
}

func TestNormalizeTypeReference_ReducesASpellingToItsTypeName(t *testing.T) {

	tests := []struct {
		name      string
		reference string
		want      string
	}{
		{name: "a bare name is lowercased", reference: "Addr", want: "addr"},
		{name: "surrounding space is dropped", reference: "  addr  ", want: "addr"},
		{name: "quotes are removed part by part", reference: `"app"."Addr"`, want: "app.addr"},
		{name: "one array marker is dropped", reference: "addr[]", want: "addr"},
		{name: "nested array markers are dropped", reference: "addr[][]", want: "addr"},
		{name: "a length modifier is dropped", reference: "character varying(255)", want: "character varying"},
		{name: "a precision modifier is dropped", reference: "numeric(10,2)", want: "numeric"},
		{name: "a modifier under an array marker is dropped", reference: "varchar(10)[]", want: "varchar"},
		{name: "an empty spelling normalizes to empty", reference: "   ", want: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(deporder.NormalizeTypeReference(test.reference), qt.Equals, test.want)
		})
	}
}
