package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
)

// writeRLSStrengthPackage writes a one-file package declaring a table with the
// given RLS annotations and returns its directory.
func writeRLSStrengthPackage(c *qt.C, enableAttrs, policyAttrs string) string {
	dir := c.TempDir()
	source := `package fixture

//ptah:schema:table name="docs"
//ptah:schema:rls:enable table="docs" ` + enableAttrs + `
//ptah:schema:rls:policy name="docs_tenant" table="docs" for="ALL" to="PUBLIC" using="true" ` + policyAttrs + `
type Doc struct {
	//ptah:schema:field name="id" type="INT" primary="true"
	ID int
}
`
	err := os.WriteFile(filepath.Join(dir, "schema.go"), []byte(source), 0o600)
	c.Assert(err, qt.IsNil)
	return dir
}

// TestParseRLSStrengthAnnotations_HappyPath pins that the native annotations
// reach the same two flags the Atlas HCL frontend sets.
//
// A capability the compatibility surface can express and the native one cannot
// is a capability implemented in the adapter, which is the shape this
// repository does not take. Both frontends write into one model, so the
// assertion is on the model rather than on either grammar.
func TestParseRLSStrengthAnnotations_HappyPath(t *testing.T) {
	rows := []struct {
		name        string
		enableAttrs string
		policyAttrs string
		wantForced  bool
		wantStrict  bool
	}{
		{name: "both stated", enableAttrs: `force="true"`, policyAttrs: `as="restrictive"`, wantForced: true, wantStrict: true},
		{name: "uppercase policy", enableAttrs: `force="true"`, policyAttrs: `as="RESTRICTIVE"`, wantForced: true, wantStrict: true},
		{name: "permissive stated", enableAttrs: `force="false"`, policyAttrs: `as="permissive"`, wantForced: false, wantStrict: false},
		{name: "neither stated", enableAttrs: ``, policyAttrs: ``, wantForced: false, wantStrict: false},
	}

	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeRLSStrengthPackage(c, row.enableAttrs, row.policyAttrs)

			db, err := goschema.ParseDir(dir)

			c.Assert(err, qt.IsNil)
			c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
			c.Assert(db.RLSEnabledTables[0].Forced, qt.Equals, row.wantForced)
			c.Assert(db.RLSPolicies, qt.HasLen, 1)
			c.Assert(db.RLSPolicies[0].Restrictive, qt.Equals, row.wantStrict)
		})
	}
}

// TestParseRLSPolicyAsRefusesAnythingElse_FailurePath pins that an
// unrecognized `as` stops the parse.
//
// Folding it into the permissive default would turn a misspelled RESTRICTIVE
// into a policy that grants what its author wrote it to withhold, and nothing
// in the output would say so.
func TestParseRLSPolicyAsRefusesAnythingElse_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := writeRLSStrengthPackage(c, ``, `as="restrictiv"`)

	db, err := goschema.ParseDir(dir)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*must be PERMISSIVE or RESTRICTIVE, got "restrictiv".*`)
	c.Assert(db, qt.IsNil)
}
