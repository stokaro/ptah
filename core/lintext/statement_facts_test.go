package lintext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/core/platform/capability"
)

// statementFacts are the facts a linter that reads the owner's split judges by.
var statementFacts = []lintext.Fact{
	lintext.FactStatefulDrop, lintext.FactStatementKind, lintext.FactMissingPrimaryKey, lintext.FactRequiredCapability,
}

func kindOf(class lintext.StatementClass, kind, object string) lintext.Effect {
	return lintext.Effect{Fact: lintext.FactStatementKind, Class: class, Kind: kind, Object: object}
}

// A split that judges the statement facts gives every statement one kind, a
// table created without a primary key, and the capabilities a statement needs.
func TestValidateSplit_StatementFacts_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := splitOf("CREATE TABLE a (price DECIMAL(10,2));\nUPSERT INTO a VALUES (1);",
		lintext.Statement{Start: 0, End: 36, Line: 1, SQL: "CREATE TABLE a (price DECIMAL(10,2))", Effects: []lintext.Effect{
			kindOf(lintext.ClassCreateTable, "CREATE TABLE", "a"),
			{Fact: lintext.FactMissingPrimaryKey, Table: "a"},
			{Fact: lintext.FactRequiredCapability, Capability: capability.ParameterizedDecimal, Kind: "COLUMN", Object: "price", Table: "a"},
		}},
		lintext.Statement{Start: 38, End: 62, Line: 2, SQL: "UPSERT INTO a VALUES (1)", Effects: []lintext.Effect{
			kindOf(lintext.ClassOther, "UPSERT", "a"),
		}})
	result.Judged = statementFacts

	c.Assert(lintext.ValidateSplit(request, result), qt.IsNil)
}

func TestValidateSplit_StatementFacts_FailurePath(t *testing.T) {
	dropKind := kindOf(lintext.ClassSchema, "DROP TABLE", "a")
	createKind := kindOf(lintext.ClassCreateTable, "CREATE TABLE", "a")
	for _, test := range []struct {
		name    string
		effects []lintext.Effect
		wantErr string
	}{
		{"a statement without a kind", nil, `.*statement 0 of 1.up.sql has no "statement-kind"`},
		{"two kinds", []lintext.Effect{dropKind, dropKind}, `.*statement 0 of 1.up.sql reports "statement-kind" twice`},
		{"a kind of an unknown class", []lintext.Effect{kindOf("view", "DROP TABLE", "a")},
			`.*statement 0 of 1.up.sql reports a kind of class "view" named "DROP TABLE", which is not a known class and a name`},
		{"a kind without a name", []lintext.Effect{kindOf(lintext.ClassSchema, " ", "a")},
			`.*statement 0 of 1.up.sql reports a kind of class "schema" named " ", which is not a known class and a name`},
		{"a missing primary key on a statement that creates no table", []lintext.Effect{dropKind, {Fact: lintext.FactMissingPrimaryKey, Table: "a"}},
			`.*statement 0 of 1.up.sql reports "missing-primary-key" on a statement of class "schema"`},
		{"a missing primary key naming no table", []lintext.Effect{createKind, {Fact: lintext.FactMissingPrimaryKey, Table: " "}},
			`.*statement 0 of 1.up.sql reports "missing-primary-key" and names no table`},
		{"a class on another fact", []lintext.Effect{dropKind, {Fact: lintext.FactStatefulDrop, Class: lintext.ClassSchema}},
			`.*statement 0 of 1.up.sql carries a class on "stateful-drop", which only "statement-kind" carries`},
		{"a capability on another fact", []lintext.Effect{dropKind, {Fact: lintext.FactStatefulDrop, Capability: capability.ParameterizedDecimal}},
			`.*statement 0 of 1.up.sql carries a capability on "stateful-drop", which only "required-capability" carries`},
		{"a capability the registry does not declare", []lintext.Effect{dropKind, {Fact: lintext.FactRequiredCapability, Capability: "time_travel"}},
			`.*statement 0 of 1.up.sql requires capability "time_travel", which the capability registry does not declare`},
		{"a required capability without a key", []lintext.Effect{dropKind, {Fact: lintext.FactRequiredCapability}},
			`.*statement 0 of 1.up.sql requires capability "", which the capability registry does not declare`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := splitOf("DROP TABLE a;", judging(test.effects...)...)
			result.Judged = statementFacts

			err := lintext.ValidateSplit(request, result)

			c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// A split that does not judge statement kinds owes no statement a kind.
func TestValidateSplit_WithoutStatementKinds_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := splitOf("DROP TABLE a;", judging(lintext.Effect{Fact: lintext.FactMissingPrimaryKey, Table: "a"})...)
	result.Judged = []lintext.Fact{lintext.FactMissingPrimaryKey}

	c.Assert(lintext.ValidateSplit(request, result), qt.IsNil)
}

// statementKindRequest asks for the kind of both statements of one file.
func statementKindRequest() lintext.AnnotateRequest {
	request := annotateRequest()
	request.Facts = []lintext.Fact{lintext.FactStatementKind}
	return request
}

// An annotation asked for kinds gives one to every statement it does not
// decline.
func TestValidateAnnotate_StatementKind_HappyPath(t *testing.T) {
	c := qt.New(t)

	err := lintext.ValidateAnnotate(statementKindRequest(), lintext.AnnotateResult{Files: [][]lintext.Annotation{{
		{Effects: []lintext.Effect{kindOf(lintext.ClassSchema, "DROP TOPIC", "t")}},
		{Declined: true},
	}}})

	c.Assert(err, qt.IsNil)
}

func TestValidateAnnotate_StatementKind_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := lintext.ValidateAnnotate(statementKindRequest(), lintext.AnnotateResult{Files: [][]lintext.Annotation{{
		{Effects: []lintext.Effect{kindOf(lintext.ClassSchema, "DROP TOPIC", "t")}},
		{},
	}}})

	c.Assert(err, qt.ErrorIs, lintext.ErrInvalid)
	c.Assert(err, qt.ErrorMatches, `.*statement 1 of file 0 has no "statement-kind"`)
}
