package pgexclude_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgexclude"
)

func TestParse_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		want       pgexclude.Definition
	}{
		{
			name:       "basic EXCLUDE constraint with GIST",
			definition: "EXCLUDE USING gist (room_id WITH =, during WITH &&)",
			want:       pgexclude.Definition{UsingMethod: "gist", Elements: "room_id WITH =, during WITH &&"},
		},
		{
			name:       "EXCLUDE constraint with WHERE clause",
			definition: "EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true)",
			want:       pgexclude.Definition{UsingMethod: "gist", Elements: "room_id WITH =, during WITH &&", WhereCondition: "is_active = true"},
		},
		{
			name:       "EXCLUDE constraint with BTREE method",
			definition: "EXCLUDE USING btree (user_id WITH =) WHERE (status = 'active')",
			want:       pgexclude.Definition{UsingMethod: "btree", Elements: "user_id WITH =", WhereCondition: "status = 'active'"},
		},
		{
			name:       "EXCLUDE constraint without WHERE parentheses",
			definition: "EXCLUDE USING gist (location WITH &&) WHERE active = true",
			want:       pgexclude.Definition{UsingMethod: "gist", Elements: "location WITH &&", WhereCondition: "active = true"},
		},
		{
			name:       "complex elements with nested parentheses",
			definition: "EXCLUDE USING gist (daterange(start_date, end_date, '[]') WITH &&)",
			want:       pgexclude.Definition{UsingMethod: "gist", Elements: "daterange(start_date, end_date, '[]') WITH &&"},
		},
		{
			name:       "complex WHERE clause with nested parentheses",
			definition: "EXCLUDE USING gist (room_id WITH =) WHERE ((status = 'active') AND (deleted_at IS NULL))",
			want:       pgexclude.Definition{UsingMethod: "gist", Elements: "room_id WITH =", WhereCondition: "(status = 'active') AND (deleted_at IS NULL)"},
		},
		// PostgreSQL 18.6 prints a deferrable EXCLUDE as `EXCLUDE USING btree
		// (r WITH =) WHERE ((r > 0)) DEFERRABLE INITIALLY DEFERRED`; read as it
		// stands, the predicate kept the clauses and never matched the
		// declaration.
		{
			name:       "deferrable",
			definition: "EXCLUDE USING btree (r WITH =) DEFERRABLE",
			want:       pgexclude.Definition{UsingMethod: "btree", Elements: "r WITH ="},
		},
		{
			name:       "deferred, with a predicate",
			definition: "EXCLUDE USING btree (r WITH =) WHERE ((r > 0)) DEFERRABLE INITIALLY DEFERRED",
			want:       pgexclude.Definition{UsingMethod: "btree", Elements: "r WITH =", WhereCondition: "(r > 0)"},
		},
		{
			name:       "a predicate naming a column called deferrable",
			definition: "EXCLUDE USING btree (r WITH =) WHERE ((deferrable > 0))",
			want:       pgexclude.Definition{UsingMethod: "btree", Elements: "r WITH =", WhereCondition: "(deferrable > 0)"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := pgexclude.Parse(test.definition)

			c.Assert(err, qt.IsNil)
			c.Assert(*parsed, qt.Equals, test.want)
		})
	}
}

func TestParse_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		definition string
		wantErr    string
	}{
		{
			name:       "invalid definition without EXCLUDE USING",
			definition: "CHECK (price > 0)",
			wantErr:    `invalid EXCLUDE constraint definition: CHECK \(price > 0\)`,
		},
		{
			name:       "missing using method",
			definition: "EXCLUDE USING",
			wantErr:    `missing using method in EXCLUDE constraint: EXCLUDE USING`,
		},
		{
			name:       "missing opening parenthesis",
			definition: "EXCLUDE USING gist room_id WITH =",
			wantErr:    `missing opening parenthesis in EXCLUDE constraint: EXCLUDE USING gist room_id WITH =`,
		},
		{
			name:       "missing closing parenthesis",
			definition: "EXCLUDE USING gist (room_id WITH =",
			wantErr:    `missing closing parenthesis in EXCLUDE constraint: EXCLUDE USING gist \(room_id WITH =`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			parsed, err := pgexclude.Parse(test.definition)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(parsed, qt.IsNil)
		})
	}
}
