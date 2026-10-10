package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/atlashcl"
)

// TestParseRowSecurity_FailurePath pins stokaro/ptah#2440 at the HCL frontend:
// two blocks that declare one policy, or one table's switches differently,
// are refused, and the refusal names both blocks. The pinned binary drops the
// second of two row_security blocks unread, so keeping either one would apply
// switches the author may have meant to replace.
func TestParseRowSecurity_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		source  string
		wantErr string
	}{
		{
			name: "one policy named with and without its schema",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
policy "tenant" {
  on    = table.users
  using = "a = 1"
}
policy "tenant" {
  on    = "public.users"
  using = "b = 2"
}
`,
			wantErr: `(?s).*policy "tenant" at schema\.hcl:6 and policy "tenant" at schema\.hcl:10 both declare policy "tenant" on table public\.users; keep one declaration`,
		},
		{
			name: "two row_security blocks that disagree",
			source: `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
  row_security { enabled = true }
  row_security {
    enabled  = true
    enforced = true
  }
}
`,
			wantErr: `(?s).*row_security of table "users" at schema\.hcl:5 and row_security of table "users" at schema\.hcl:6 both declare the row-level security switches of table public\.users; keep one declaration`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.Parse([]byte(test.source), "schema.hcl")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
