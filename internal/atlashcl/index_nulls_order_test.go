package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
)

// `nulls_first` and `nulls_last` are the spelling the pinned community binary
// v1.3.0 emits for a PostgreSQL index key whose NULLS ordering deviates from
// its direction's default -- measured on PostgreSQL 17.10, its `schema inspect`
// of `CREATE INDEX i ON t (a text_pattern_ops DESC NULLS LAST, b ASC NULLS
// FIRST)` writes `nulls_last = true` on the first key and `nulls_first = true`
// on the second.
//
// Before issue #1272 neither name was modeled here. Under `ptah-compat` the
// unknown-attribute tolerance dropped them, so a file the community binary
// itself produced reached Ptah with the ordering silently gone; under the
// native parser the same file was refused outright. The property is now read,
// which is what lets the comparator treat it as part of the index definition.
func TestParseIndexPartNullsOrder(t *testing.T) {
	tests := []struct {
		name     string
		attr     string
		wantDesc bool
		want     string
	}{
		{
			name: "nulls first on an ascending key",
			attr: "nulls_first = true",
			want: goschema.NullsOrderFirst,
		},
		{
			name: "nulls last on an ascending key",
			attr: "nulls_last = true",
			want: goschema.NullsOrderLast,
		},
		{
			name:     "nulls last on a descending key",
			attr:     "desc = true\n    nulls_last = true",
			wantDesc: true,
			want:     goschema.NullsOrderLast,
		},
		{
			// An absent ordering is the direction's default and stays
			// unrecorded, matching what the live-database reader does with
			// pg_index.indoption.
			name: "no ordering attribute",
			attr: "",
			want: "",
		},
		{
			// `false` is not an assertion of the opposite ordering; it says
			// nothing, which is the default.
			name: "nulls first set to false",
			attr: "nulls_first = false",
			want: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(`
table "t" {
  column "a" {
    type = text
  }
  index "i" {
    on {
      column = column.a
      `+test.attr+`
    }
  }
}
`), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(db.Indexes, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Parts, qt.HasLen, 1)
			c.Assert(db.Indexes[0].Parts[0].NullsOrder, qt.Equals, test.want)
			c.Assert(db.Indexes[0].Parts[0].Desc, qt.Equals, test.wantDesc)
		})
	}
}

// TestParseIndexPartNullsOrderRejectsBoth keeps the parser from inventing an
// answer. A key has one NULLS ordering; a file asking for both is a mistake,
// and picking one of the two would hide it.
func TestParseIndexPartNullsOrderRejectsBoth(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "t" {
  column "a" {
    type = text
  }
  index "i" {
    on {
      column      = column.a
      nulls_first = true
      nulls_last  = true
    }
  }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*index on cannot set both nulls_first and nulls_last.*`)
}

// TestParseIndexPartNullsOrderRejectsNonBool pins the type check, so a string
// spelling is named rather than quietly read as "no ordering".
func TestParseIndexPartNullsOrderRejectsNonBool(t *testing.T) {
	c := qt.New(t)

	_, err := atlashcl.Parse([]byte(`
table "t" {
  column "a" {
    type = text
  }
  index "i" {
    on {
      column     = column.a
      nulls_last = "yes"
    }
  }
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `.*index on attribute "nulls_last" must be a bool.*`)
}
