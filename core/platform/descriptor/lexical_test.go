package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

func TestNewRegistry_LexicalScriptsAndReach_HappyPath(t *testing.T) {
	with := func(change func(*descriptor.Dialect)) descriptor.Dialect {
		described := head("alpha")
		change(&described)
		return described
	}
	tests := []struct {
		name    string
		dialect descriptor.Dialect
	}{
		{name: "every rule of the shared lexer", dialect: with(func(d *descriptor.Dialect) {
			d.Lexical = descriptor.Lexical{
				LineComments: []string{"#", "//"}, BackslashEscapes: true, IdentifierBackslashEscapes: true,
				EscapeStrings: true, ContinuedStrings: true, UnicodeStrings: true, DashCommentNeedsSpace: true,
				ExecutableComments: descriptor.ExecutableCommentsMBang, BracketIdentifiers: true, HashNames: true,
				AlternativeQuoting: true,
			}
		})},
		{name: "a lexer of its own", dialect: with(func(d *descriptor.Dialect) {
			d.Lexical = descriptor.Lexical{Lexer: descriptor.LexerYQL}
		})},
		{name: "a body grammar", dialect: with(func(d *descriptor.Dialect) { d.Scripts.Bodies = descriptor.BodiesPLSQL })},
		{name: "every reach family", dialect: with(func(d *descriptor.Dialect) {
			calls := descriptor.ReachCalls{Construct: "alpha call", Reach: "reaches out", Names: []string{"FETCH_URL"}}
			d.Reach = descriptor.Reach{
				TableFunctions: calls, NetworkPackages: calls, ControlPackages: calls,
				ControlFunctions: calls, ReplicationFunctions: calls,
			}
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.IsNil)
			got, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(got.Equal(test.dialect), qt.IsTrue)
		})
	}
}

func TestNewRegistry_LexicalScriptsAndReach_FailurePath(t *testing.T) {
	with := func(change func(*descriptor.Dialect)) descriptor.Dialect {
		described := head("alpha")
		change(&described)
		return described
	}
	calls := descriptor.ReachCalls{Construct: "alpha call", Reach: "reaches out", Names: []string{"FETCH_URL"}}
	tests := []struct {
		name    string
		dialect descriptor.Dialect
		wantErr string
	}{
		{name: "an undeclared lexer", dialect: with(func(d *descriptor.Dialect) { d.Lexical.Lexer = "antlr" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": lexer "antlr" is not one of .*`},
		{name: "a lexer of its own with a rule of the shared one", dialect: with(func(d *descriptor.Dialect) {
			d.Lexical = descriptor.Lexical{Lexer: descriptor.LexerYQL, BackslashEscapes: true}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": lexer "yql" reads none of the shared lexer's rules, so the descriptor states none`},
		{name: "a lexer of its own with line comments", dialect: with(func(d *descriptor.Dialect) {
			d.Lexical = descriptor.Lexical{Lexer: descriptor.LexerYQL, LineComments: []string{"#"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": lexer "yql" reads none of the shared lexer's rules, so the descriptor states none`},
		{name: "undeclared executable comments", dialect: with(func(d *descriptor.Dialect) { d.Lexical.ExecutableComments = "hash" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": executable comments "hash" is not one of .*`},
		{name: "an empty line comment marker", dialect: with(func(d *descriptor.Dialect) { d.Lexical.LineComments = []string{""} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": line comment marker "" is empty or the standard one`},
		{name: "the standard line comment marker", dialect: with(func(d *descriptor.Dialect) { d.Lexical.LineComments = []string{"--"} }),
			wantErr: `invalid dialect descriptor: dialect "alpha": line comment marker "--" is empty or the standard one`},
		{name: "an undeclared body grammar", dialect: with(func(d *descriptor.Dialect) { d.Scripts.Bodies = "pgsql" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": bodies "pgsql" is not one of .*`},
		{name: "reach names without a construct", dialect: with(func(d *descriptor.Dialect) {
			d.Reach.TableFunctions = descriptor.ReachCalls{Reach: "reaches out", Names: []string{"FETCH_URL"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach table functions states names without a construct and its reach, or the words without names`},
		{name: "reach names without their reach", dialect: with(func(d *descriptor.Dialect) {
			d.Reach.NetworkPackages = descriptor.ReachCalls{Construct: "alpha call", Names: []string{"FETCH_URL"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach network packages states names without a construct and its reach, or the words without names`},
		{name: "reach words without names", dialect: with(func(d *descriptor.Dialect) {
			d.Reach.ControlPackages = descriptor.ReachCalls{Construct: "alpha call"}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach control packages states names without a construct and its reach, or the words without names`},
		{name: "a reach name in lower case", dialect: with(func(d *descriptor.Dialect) {
			d.Reach.ControlFunctions = descriptor.ReachCalls{Construct: "alpha call", Reach: "reaches out", Names: []string{"fetch_url"}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach control functions name "fetch_url" is empty or not in upper case`},
		{name: "an empty reach name", dialect: with(func(d *descriptor.Dialect) {
			d.Reach.TableFunctions = calls
			d.Reach.ReplicationFunctions = descriptor.ReachCalls{Construct: "alpha call", Reach: "reaches out", Names: []string{""}}
		}),
			wantErr: `invalid dialect descriptor: dialect "alpha": reach replication functions name "" is empty or not in upper case`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.dialect)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}
