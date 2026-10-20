package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
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

			db, err := parseWithOwners(t.Context(), []byte(test.source), "schema.hcl")

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// policyTo renders a table and one policy over it whose `to` is to.
func policyTo(to string) []byte {
	return []byte(`schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
policy "tenant" {
  on = table.users
  to = ` + to + `
}
`)
}

// TestParsePolicyRoles_HappyPath reads the roles a policy applies to: a
// reference names the role it refers to, and a string or an identifier the
// role it writes, so both spellings of one role read alike.
func TestParsePolicyRoles_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		to   string
		want []pgpolicy.RoleSelector
	}{
		{name: "a reference", to: `[role.x]`, want: []pgpolicy.RoleSelector{{Name: "x"}}},
		{name: "a string", to: `["x"]`, want: []pgpolicy.RoleSelector{{Name: "x"}}},
		{name: "a reference with a schema", to: `[role.s.x]`, want: []pgpolicy.RoleSelector{{Name: "s.x"}}},
		{name: "a keyword", to: `[CURRENT_USER, app]`, want: []pgpolicy.RoleSelector{{Keyword: pgpolicy.CurrentUser}, {Name: "app"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := parseWithOwners(t.Context(), policyTo(test.to), "schema.hcl")

			c.Assert(err, qt.IsNil)
			c.Assert(ownerPolicies(c, db)["public.users.tenant"].Roles, qt.DeepEquals, test.want)
		})
	}
}

// TestParsePolicyRoles_FailurePath refuses a role reference that refers to
// another type of object rather than read it as a role's name, and a string
// that spells a role reference, which an earlier reader read as the role it
// refers to: reading it now as the name it writes would grant to another role
// with nothing to tell the author.
func TestParsePolicyRoles_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		to      string
		wantErr string
	}{
		{name: "a view", to: `[view.x]`,
			wantErr: `parse HCL schema at schema\.hcl:8,3-5: policy attribute "to": view\.x is not a role reference`},
		{name: "a string that spells a role reference", to: `["role.x"]`,
			wantErr: `parse HCL schema at schema\.hcl:8,9-17: policy attribute "to": "role\.x" is ambiguous; ` +
				`write role\.x unquoted for the role x, or name the role without the role\. prefix`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := parseWithOwners(t.Context(), policyTo(test.to), "schema.hcl")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParsePolicy_LeavesTheSwitchesOfItsTableToTheDefault is the row-security
// owner's claim over the policies a document declares: a table with policies
// and no row_security block keeps its switches at the owner's default rather
// than turning row-level security off.
func TestParsePolicy_LeavesTheSwitchesOfItsTableToTheDefault(t *testing.T) {
	c := qt.New(t)

	db, err := parseWithOwners(t.Context(), policyTo(`[app]`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureCoverage.Lookup(pgpolicy.TableStateKind, pgpolicysource.TableRef("public", "users")), qt.DeepEquals, pgpolicy.DefaultedSwitches())
	c.Assert(db.FeatureCoverage.Lookup(pgpolicy.TableStateKind, pgpolicysource.TableRef("public", "orders")).State, qt.Equals, schemaext.Complete)
}

// TestParseRowSecurity_ReportsARefusalOfOneBlockFirst reports a policy the
// owner refuses on its own before two policies it refuses together, wherever
// the two stand: the reader refused the one at its block, and checked the
// two together only once every block was read.
func TestParseRowSecurity_ReportsARefusalOfOneBlockFirst(t *testing.T) {
	c := qt.New(t)
	source := `schema "public" {}
table "users" {
  schema = schema.public
  column "id" { type = integer }
}
policy "tenant" {
  on = table.users
}
policy "tenant" {
  on = table.public.users
}
policy "strict" {
  on = table.users
  as = "SOMETIMES"
}
`

	db, err := parseWithOwners(t.Context(), []byte(source), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `parse HCL schema at schema\.hcl:12,1-7: policy "strict" as must be PERMISSIVE or RESTRICTIVE`)
	c.Assert(db, qt.IsNil)
}
