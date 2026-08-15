package graphqlrender_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/graphqlrender"
)

func TestParseOperations_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		want   graphqlrender.Operations
	}{
		{name: "unset is types only", values: nil, want: graphqlrender.Operations{}},
		{name: "explicit none", values: []string{"none"}, want: graphqlrender.Operations{}},
		{name: "list", values: []string{"list"}, want: graphqlrender.Operations{List: true}},
		{name: "by id", values: []string{"by-id"}, want: graphqlrender.Operations{ByID: true}},
		{
			name:   "create input",
			values: []string{"create-input"},
			want:   graphqlrender.Operations{CreateInput: true},
		},
		{
			name:   "update input",
			values: []string{"update-input"},
			want:   graphqlrender.Operations{UpdateInput: true},
		},
		{
			name:   "query shapes are independent",
			values: []string{"by-id", "list"},
			want:   graphqlrender.Operations{List: true, ByID: true},
		},
		{
			name:   "repetition is idempotent",
			values: []string{"list", "list"},
			want:   graphqlrender.Operations{List: true},
		},
		{
			name:   "surrounding space is tolerated",
			values: []string{" list ", "\tby-id"},
			want:   graphqlrender.Operations{List: true, ByID: true},
		},
		{
			name:   "every shape",
			values: []string{"list", "by-id", "create-input", "update-input"},
			want: graphqlrender.Operations{
				List: true, ByID: true, CreateInput: true, UpdateInput: true,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := graphqlrender.ParseOperations(test.values)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

func TestParseOperations_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		values  []string
		wantErr string
	}{
		{
			name:    "unknown shape",
			values:  []string{"mutations"},
			wantErr: `unknown GraphQL operation "mutations": expected none, list, by-id, create-input, or update-input`,
		},
		{
			// A shape that is only misspelled must not be dropped in silence: the
			// export would then be missing exactly what was asked for.
			name:    "near miss spelling",
			values:  []string{"byid"},
			wantErr: `unknown GraphQL operation "byid": expected none, list, by-id, create-input, or update-input`,
		},
		{
			name:    "empty value",
			values:  []string{""},
			wantErr: `unknown GraphQL operation "": expected none, list, by-id, create-input, or update-input`,
		},
		{
			name:    "none with a shape",
			values:  []string{"none", "list"},
			wantErr: `GraphQL operation "none" selects a types-only schema and cannot be combined with list`,
		},
		{
			name:    "shape then none",
			values:  []string{"create-input", "none"},
			wantErr: `GraphQL operation "none" selects a types-only schema and cannot be combined with create-input`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := graphqlrender.ParseOperations(test.values)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
			c.Assert(got, qt.Equals, graphqlrender.Operations{})
		})
	}
}

func TestOperationsPredicates(t *testing.T) {
	tests := []struct {
		name        string
		ops         graphqlrender.Operations
		wantAny     bool
		wantQueries bool
	}{
		{name: "zero value", ops: graphqlrender.Operations{}, wantAny: false, wantQueries: false},
		{name: "list", ops: graphqlrender.Operations{List: true}, wantAny: true, wantQueries: true},
		{name: "by id", ops: graphqlrender.Operations{ByID: true}, wantAny: true, wantQueries: true},
		{
			// Inputs alone add no root operation, so the schema still has no Query.
			name:        "create input only",
			ops:         graphqlrender.Operations{CreateInput: true},
			wantAny:     true,
			wantQueries: false,
		},
		{
			name:        "update input only",
			ops:         graphqlrender.Operations{UpdateInput: true},
			wantAny:     true,
			wantQueries: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.ops.Any(), qt.Equals, test.wantAny)
			c.Assert(test.ops.Queries(), qt.Equals, test.wantQueries)
		})
	}
}
