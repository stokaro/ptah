package statementform_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/statementform"
)

func TestParse_HappyPath(t *testing.T) {
	name := statementform.Term{Kind: statementform.Name}
	word := func(w string) statementform.Term { return statementform.Term{Kind: statementform.Word, Word: w} }
	names := statementform.Term{Kind: statementform.List, Choices: [][]statementform.Term{{name}}}
	tests := []struct {
		name string
		form string
		want []statementform.Term
	}{
		{name: "a keyword and a name", form: "SAVEPOINT <name>", want: []statementform.Term{word("SAVEPOINT"), name}},
		{name: "a list of names", form: "TRUNCATE <names>", want: []statementform.Term{word("TRUNCATE"), names}},
		{
			name: "an optional keyword",
			form: "TRUNCATE [TABLE] <name>",
			want: []statementform.Term{
				word("TRUNCATE"),
				{Kind: statementform.Optional, Choices: [][]statementform.Term{{word("TABLE")}}},
				name,
			},
		},
		{
			name: "a choice of sequences",
			form: "SAVE {TRANSACTION|TRAN} <name>",
			want: []statementform.Term{
				word("SAVE"),
				{Kind: statementform.Choice, Choices: [][]statementform.Term{{word("TRANSACTION")}, {word("TRAN")}}},
				name,
			},
		},
		{
			name: "a list of a sequence",
			form: "LOCK (<name> READ),...",
			want: []statementform.Term{
				word("LOCK"),
				{Kind: statementform.List, Choices: [][]statementform.Term{{name, word("READ")}}},
			},
		},
		{
			name: "a group inside a group",
			form: "T [IN {A {B|C}|D} E]",
			want: []statementform.Term{
				word("T"),
				{Kind: statementform.Optional, Choices: [][]statementform.Term{{
					word("IN"),
					{Kind: statementform.Choice, Choices: [][]statementform.Term{
						{word("A"), {Kind: statementform.Choice, Choices: [][]statementform.Term{{word("B")}, {word("C")}}}},
						{word("D")},
					}},
					word("E"),
				}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := statementform.Parse(test.form)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
			c.Assert(statementform.Opening(test.form), qt.Equals, test.want[0].Word)
		})
	}
}

// TestParse_FailurePath is each form a parser could not read one token at a time,
// which a registry refuses rather than leave for the first document to find.
func TestParse_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		form    string
		wantErr string
	}{
		{name: "empty", form: "", wantErr: `.*a form opens with a keyword`},
		{name: "opening with a name", form: "<name> T", wantErr: `.*a form opens with a keyword`},
		{name: "opening with a group", form: "[T] U", wantErr: `.*a form opens with a keyword`},
		{name: "a lower-case word", form: "T table", wantErr: `.*"table" is neither a keyword in capitals nor a name`},
		{name: "an unknown placeholder", form: "T <NOPE>", wantErr: `.*"<NOPE>" is neither a keyword in capitals nor a name`},
		{name: "an alternative opening with a name", form: "T [<name>|U]", wantErr: `.*an alternative in \] opens with a keyword`},
		{name: "an empty alternative", form: "T {U|}", wantErr: `.*an alternative in } opens with a keyword`},
		{name: "two alternatives with one opening", form: "T {A B|A C}", wantErr: `.*two alternatives open with A`},
		{name: "a bracket left open", form: "T [U", wantErr: `.*a group closes with \]`},
		{name: "a bracket closed by the other kind", form: "T [U}", wantErr: `.*a group closes with \]`},
		{name: "a closing bracket with no opening", form: "T U]", wantErr: `.*unexpected "\]"`},
		{name: "a list left open", form: "T (<name>", wantErr: `.*a list closes with \),\.\.\. and this one does not`},
		{name: "an empty list", form: "T (),...", wantErr: `.*a list repeats something`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := statementform.Parse(test.form)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(got, qt.IsNil)
			c.Assert(statementform.Opening(test.form), qt.Equals, "")
		})
	}
}
