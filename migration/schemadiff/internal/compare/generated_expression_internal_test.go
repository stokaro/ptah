package compare

// White-box testing required: generatedColumnDiff is package-local, and the
// fact under test is which of four states a column comparison is in. Only two
// of them produce a difference, so a test that asserted on the schema diff
// alone could not tell "compared and equal" from "not compared".

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema/types"
)

// oracleStoredExpression is what a live 23.26.2.0.0 stores for
// `view_count * 2`: every column reference quoted and upper-cased, the spaces
// around the operator gone.
const oracleStoredExpression = `"VIEW_COUNT"*2`

func generatedColumn(expression string) goschema.Field {
	return goschema.Field{
		Name:                "doubled",
		Type:                "INT",
		Nullable:            true,
		GeneratedExpression: expression,
		GeneratedKind:       "VIRTUAL",
	}
}

func storedGeneratedColumn(expression, kind string) types.DBColumn {
	return types.DBColumn{
		Name:                "doubled",
		GeneratedExpression: &expression,
		GeneratedKind:       kind,
	}
}

func resolvedAs(expression string) *config.GeneratedExpression {
	return &config.GeneratedExpression{Expression: expression, Resolved: true}
}

// TestGeneratedColumnDiff_UsesTheServerSpellingWhereItHasOne pins all four
// states a generated column can be compared in.
//
// Oracle does not store the text of a generated expression; it stores a
// rewrite. So a declaration and a catalog disagree textually about an
// expression they agree about semantically, and Ptah planned a MODIFY that
// changed nothing on every run (stokaro/ptah#1915).
//
// The row that makes this a fix rather than a silencer is the third: a real
// change, resolved through the same server, still differs. Measured live,
// `view_count * 3` resolves to `"VIEW_COUNT"*3`.
func TestGeneratedColumnDiff_UsesTheServerSpellingWhereItHasOne(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		declared   string
		stored     string
		storedKind string
		// resolution is what a dev database answered for the declaration, nil
		// when nobody asked one.
		resolution *config.GeneratedExpression
		want       string
	}{
		{
			name:       "a resolved declaration converges with the catalog",
			dialect:    platform.Oracle,
			declared:   "view_count * 2",
			stored:     oracleStoredExpression,
			storedKind: "VIRTUAL",
			resolution: resolvedAs(oracleStoredExpression),
			want:       "",
		},
		{
			name:       "a resolved declaration that really changed still reports",
			dialect:    platform.Oracle,
			declared:   "view_count * 3",
			stored:     oracleStoredExpression,
			storedKind: "VIRTUAL",
			resolution: resolvedAs(`"VIEW_COUNT"*3`),
			want:       `VIRTUAL "VIEW_COUNT"*2 -> VIRTUAL "VIEW_COUNT"*3`,
		},
		{
			// A declaration the server refused has no stored form to be equal
			// or unequal to, so it gets the same answer as no resolution at
			// all: the expression is left alone. The row is here because the
			// two arrive by different routes and must not diverge.
			name:       "a declaration the server refused is not compared",
			dialect:    platform.Oracle,
			declared:   "no_such_column * 2",
			stored:     oracleStoredExpression,
			storedKind: "VIRTUAL",
			resolution: &config.GeneratedExpression{},
			want:       "",
		},
		{
			// No dev database. The textual difference here is the bug, so it is
			// not reported.
			name:       "a rewriting target with nobody to ask leaves it alone",
			dialect:    platform.Oracle,
			declared:   "view_count * 2",
			stored:     oracleStoredExpression,
			storedKind: "VIRTUAL",
			want:       "",
		},
		{
			// And what remains comparable there still is: a column that stops
			// being generated is a change no spelling question can hide.
			name:       "the kind is compared even when the expression cannot be",
			dialect:    platform.Oracle,
			declared:   "view_count * 2",
			stored:     oracleStoredExpression,
			storedKind: "",
			want:       " -> VIRTUAL",
		},
		{
			// The control. PostgreSQL stores the text it was given, so nothing
			// about this change may touch it.
			name:       "a target that stores what it was given is compared as before",
			dialect:    platform.Postgres,
			declared:   "view_count * 3",
			stored:     "view_count * 2",
			storedKind: "VIRTUAL",
			want:       "VIRTUAL view_count*2 -> VIRTUAL view_count*3",
		},
		{
			name:       "and agrees with itself there",
			dialect:    platform.Postgres,
			declared:   "view_count * 2",
			stored:     "view_count * 2",
			storedKind: "VIRTUAL",
			want:       "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := generatedColumnDiff(
				generatedColumn(test.declared),
				storedGeneratedColumn(test.stored, test.storedKind),
				test.dialect,
				test.resolution,
			)

			c.Assert(diff, qt.Equals, test.want)
		})
	}
}

// TestGeneratedColumnKey_MatchesTheResolverSpelling holds the two halves of one
// key to each other.
//
// The map is written by dbschema and read here, and neither package can see the
// other's spelling. A key that folded differently on the two sides would leave
// every entry unfound, which is indistinguishable from nobody having asked a
// server -- the expression would go uncompared and the fix would look like it
// worked.
func TestGeneratedColumnKey_MatchesTheResolverSpelling(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		table  string
		column string
		want   string
	}{
		{name: "unqualified", table: "ora_posts", column: "doubled", want: "ora_posts.doubled"},
		{name: "qualified", schema: "app", table: "ora_posts", column: "doubled", want: "app.ora_posts.doubled"},
		{name: "folded", schema: "APP", table: "ORA_POSTS", column: "DOUBLED", want: "app.ora_posts.doubled"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(generatedColumnKey(test.schema, test.table, test.column), qt.Equals, test.want)
		})
	}
}
