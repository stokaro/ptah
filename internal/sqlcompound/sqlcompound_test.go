package sqlcompound_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlcompound"
)

// TestNew_ReadsTheGrammarTheDescriptorNames feeds the head of a body and a
// semicolon to the state of a dialect, written in one of its spellings, and
// asks whether the semicolon stays inside the statement. Each body opener is
// one only in the grammar of the dialect's descriptor: in every other grammar,
// and for a dialect nobody described, the semicolon ends the statement.
func TestNew_ReadsTheGrammarTheDescriptorNames(t *testing.T) {
	transactSQL := []string{"CREATE", "PROCEDURE", "p", "AS"}
	plsql := []string{"CREATE", "FUNCTION", "f", "RETURN", "NUMBER", "IS"}
	yql := []string{"DEFINE", "ACTION"}
	tests := []struct {
		name    string
		dialect string
		words   []string
		want    bool
	}{
		{name: "T-SQL opens a procedure body with AS", dialect: "mssql", words: transactSQL, want: true},
		{name: "PL/SQL opens a function body with IS", dialect: "oracledb", words: plsql, want: true},
		{name: "YQL opens an action body", dialect: "ydbs", words: yql, want: true},
		{name: "AS opens nothing on PostgreSQL", dialect: "postgres", words: transactSQL, want: false},
		{name: "IS opens nothing on SQL Server", dialect: "sqlserver", words: plsql, want: false},
		{name: "DEFINE ACTION opens nothing on Oracle", dialect: "oracle", words: yql, want: false},
		{name: "AS opens nothing for an unknown dialect", dialect: "nonesuch", words: transactSQL, want: false},
		{name: "AS opens nothing for no dialect", dialect: "", words: transactSQL, want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			state := sqlcompound.New(test.dialect)
			for _, word := range test.words {
				state.Word(word)
			}
			c.Assert(state.KeepSemicolonInsideStatement(), qt.Equals, test.want)
		})
	}
}

// TestState_ResetKeepsTheGrammar ends one T-SQL routine and reads the next one
// with the same grammar.
func TestState_ResetKeepsTheGrammar(t *testing.T) {
	c := qt.New(t)
	state := sqlcompound.New("mssql")
	state.Word("SELECT")
	state.Reset()
	for _, word := range []string{"CREATE", "PROC", "p", "AS"} {
		state.Word(word)
	}
	c.Assert(state.KeepSemicolonInsideStatement(), qt.IsTrue)
}

// TestState_SymbolReadsLambdaBracesInYQLAlone opens a brace after a YQL
// lambda's arrow and on PostgreSQL, where a brace opens nothing.
func TestState_SymbolReadsLambdaBracesInYQLAlone(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    bool
	}{
		{name: "YQL", dialect: "ydb", want: true},
		{name: "PostgreSQL", dialect: "postgres", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			state := sqlcompound.New(test.dialect)
			state.Symbol("{")
			c.Assert(state.KeepSemicolonInsideStatement(), qt.Equals, test.want)
		})
	}
}
