package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasschema"
)

// `ptah-compat schema diff` reaches the comparator through the variant that
// returns no error, so every pre-comparison refusal the native seam makes has
// to be applied here as well or it is applied nowhere on this surface. The
// SQLite virtual-table guard already is; these tests pin the ClickHouse RBAC
// refusals next to it (stokaro/ptah#1025).
//
// The dev URL is never connected to. It names the dialect, which is all the
// refusals need, exactly as the coverage tests in this package use it.

const clickHouseRBACDiffFrom = `
schema "shop" {
}
table "orders" {
  schema = schema.shop
  column "id" {
    type = int
  }
}
`

// TestDiffRefusesAClickHouseGrantToAnUndeclaredRole is the row this file exists
// for. On ClickHouse a GRANT resolves its target across users and roles, so a
// grant naming a role the document does not declare either fails at
// UNKNOWN_ROLE or lands on a USER of that name.
func TestDiffRefusesAClickHouseGrantToAnUndeclaredRole(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.Diff(c.Context(), clickHouseRBACDiffOptions(c,
		clickHouseRBACDiffFrom,
		clickHouseRBACDiffFrom+`
permission {
  to         = role.analyst
  privileges = ["SELECT"]
  for        = schema.shop
}
`))

	c.Assert(err, qt.ErrorMatches, `(?s).*grant names role "analyst", which this schema does not declare.*`)
}

// TestDiffRefusesAClickHousePrivilegeTheServerRewrites covers the other half of
// the same seam with a refusal that has nothing to do with the grantee, so a
// call site wired to only one of the checks turns red here.
func TestDiffRefusesAClickHousePrivilegeTheServerRewrites(t *testing.T) {
	c := qt.New(t)

	_, err := atlasschema.Diff(c.Context(), clickHouseRBACDiffOptions(c,
		clickHouseRBACDiffFrom,
		clickHouseRBACDiffFrom+`
role "analyst" {
}
permission {
  to         = role.analyst
  privileges = ["ALL"]
  for        = schema.shop
}
`))

	c.Assert(err, qt.ErrorMatches, `(?s).*declares privilege "ALL" on shop\.\*.*`)
}

// TestDiffRefusesARefusedDeclarationEvenWithNothingToChange is the row that
// separates this seam from the refusal the renderer already makes.
//
// Rendering the diff's SQL reaches internal/clickhouserbac too, so a diff that
// PLANS something is refused either way. A diff that plans nothing renders
// nothing: with the same refused declaration on both sides, the comparison
// found no change, generated no SQL, and `ptah-compat schema diff` answered
// exit 0 with no changes for a declaration every other surface refuses — a
// report of convergence on a schema Ptah cannot manage.
func TestDiffRefusesARefusedDeclarationEvenWithNothingToChange(t *testing.T) {
	c := qt.New(t)

	refused := clickHouseRBACDiffFrom + `
role "analyst" {
}
permission {
  to         = role.analyst
  privileges = ["ALL"]
  for        = schema.shop
}
`

	_, err := atlasschema.Diff(c.Context(), clickHouseRBACDiffOptions(c, refused, refused))

	c.Assert(err, qt.ErrorMatches, `(?s).*declares privilege "ALL" on shop\.\*.*`)
}

// TestDiffPlansAClickHouseGrantItAccepts is the control. Without it, a seam
// that refused every ClickHouse comparison would satisfy both tests above.
func TestDiffPlansAClickHouseGrantItAccepts(t *testing.T) {
	c := qt.New(t)

	report, err := atlasschema.Diff(c.Context(), clickHouseRBACDiffOptions(c,
		clickHouseRBACDiffFrom,
		clickHouseRBACDiffFrom+`
role "analyst" {
}
permission {
  to         = role.analyst
  privileges = ["SELECT"]
  for        = schema.shop
}
`))

	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()
	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "GRANT SELECT ON `shop`.* TO `analyst`")
}

// TestDiffLeavesTheSameDeclarationAloneOnPostgres is the non-interference
// control: the declaration refused above is ordinary PostgreSQL, and a seam
// that applied ClickHouse's rules to every dialect would break the dialect this
// repository has always supported.
func TestDiffLeavesTheSameDeclarationAloneOnPostgres(t *testing.T) {
	c := qt.New(t)

	options := clickHouseRBACDiffOptions(c,
		clickHouseRBACDiffFrom,
		clickHouseRBACDiffFrom+`
permission {
  to         = role.analyst
  privileges = ["ALL"]
  for        = schema.shop
}
`)
	options.DevURL = "postgres://localhost/dev"

	_, err := atlasschema.Diff(c.Context(), options)

	c.Assert(err, qt.IsNil)
}

func clickHouseRBACDiffOptions(c *qt.C, from, to string) atlasschema.DiffOptions {
	dir := c.TempDir()
	fromPath := filepath.Join(dir, "from.hcl")
	toPath := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(fromPath, []byte(from), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(toPath, []byte(to), 0o600), qt.IsNil)
	return atlasschema.DiffOptions{
		FromURLs: []string{"file://" + fromPath},
		ToURLs:   []string{"file://" + toPath},
		DevURL:   "clickhouse://localhost/dev",
	}
}
