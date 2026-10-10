package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

// rowSecurityDocument declares a table, its row-level security and one
// policy, each with the given dialects line.
func rowSecurityDocument(dialects string) []byte {
	return []byte(`tables:
  docs:
    columns:
      id: { type: INTEGER, primary: true }
rls_enabled_tables:
  docs:
    table: docs
    ` + dialects + `
rls_policies:
  docs_read:
    table: docs
    for: SELECT
    using: "true"
    ` + dialects + `
`)
}

// TestParse_RowSecurityScopeSelectsTheModel pins the YAML `dialects` key on
// row-level security: no scope or a PostgreSQL-family scope reaches the
// row-security owner, which keeps the scope, and a scope naming only SQL
// Server keeps the entries in the shared model.
func TestParse_RowSecurityScopeSelectsTheModel(t *testing.T) {
	tests := []struct {
		name        string
		dialects    string
		wantTargets []string
		wantOwned   int
		wantShared  int
	}{
		{name: "no scope", dialects: "", wantOwned: 1},
		{name: "PostgreSQL family", dialects: "dialects: [postgres, yugabytedb]", wantTargets: []string{"postgres", "yugabytedb"}, wantOwned: 1},
		{name: "SQL Server", dialects: "dialects: [mssql]", wantShared: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(rowSecurityOwners, rowSecurityDocument(test.dialects))

			c.Assert(err, qt.IsNil)
			objects := must.Must(db.FeatureObjects.All())
			c.Assert(objects, qt.HasLen, test.wantOwned)
			c.Assert(db.RLSPolicies, qt.HasLen, test.wantShared)
			c.Assert(db.RLSEnabledTables, qt.HasLen, test.wantShared)
			c.Assert(db.Tables[0].Facets.TargetScope(pgpolicy.TableStateKind), qt.DeepEquals, test.wantTargets)
		})
	}
}

// TestParse_RowSecurityScopeSelectsTheModel_FailurePath pins the refusal of a
// scope naming the PostgreSQL family beside another target.
func TestParse_RowSecurityScopeSelectsTheModel_FailurePath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(rowSecurityOwners, rowSecurityDocument("dialects: [postgres, mssql]"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*rls_enabled_tables\.docs: .*mixes PostgreSQL-family targets with others.*`)
	c.Assert(db, qt.IsNil)
}

// TestParse_RowSecurityScopedToClickHouse_FailurePath refuses row-level
// security scoped to ClickHouse: a policy, alone or beside another target, is
// the ClickHouse owner's row policy, declared under its row_policies key, and
// an enablement has no switch to set.
func TestParse_RowSecurityScopedToClickHouse_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "an enablement", document: string(rowSecurityDocument("dialects: [clickhouse]")),
			wantErr: `(?s)rls_enabled_tables\.docs: .*ClickHouse has no row-level security switch.*`},
		{name: "a policy", document: `rls_policies:
  docs_read:
    table: docs
    using: "true"
    dialects: [clickhouse]
`, wantErr: `rls_policies\.docs_read: .*a ClickHouse row policy is not a row-level security policy; declare it under row_policies instead`},
		{name: "a policy beside SQL Server", document: `rls_policies:
  docs_read:
    table: docs
    using: "true"
    dialects: [clickhouse, mssql]
`, wantErr: `rls_policies\.docs_read: .*declare it under row_policies instead`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(rowSecurityOwners, []byte(test.document))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParse_WithoutTheRowSecurityOwnerAPolicyStaysShared is the control on
// the routing: without the owner, the frontend keeps an unscoped policy in the
// shared model and claims no knowledge of the owner's policies, so they stay
// unknown rather than absent.
func TestParse_WithoutTheRowSecurityOwnerAPolicyStaysShared(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse(noOwners, rowSecurityDocument(""))

	c.Assert(err, qt.IsNil)
	c.Assert(db.RLSPolicies, qt.HasLen, 1)
	c.Assert(db.RLSEnabledTables, qt.HasLen, 1)
	c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(db.FeatureCoverage.Lookup(pgpolicy.PolicyKind, pgpolicysource.Ref("", "docs", "other")).State,
		qt.Not(qt.Equals), schemaext.Complete)
}

// TestParse_APolicyComposition_FailurePath refuses an `as` that names neither
// composition, rather than reading it as the weaker PERMISSIVE, on every
// route a policy takes.
func TestParse_APolicyComposition_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialects string
	}{
		{name: "the owner's policy"},
		{name: "a shared policy", dialects: "    dialects: [mysql]\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := yamlschema.Parse(rowSecurityOwners, []byte("rls_policies:\n  docs_read:\n    table: docs\n"+
				"    using: \"true\"\n    as: restrictiv\n"+test.dialects))

			c.Assert(err, qt.ErrorMatches, `rls_policies\.docs_read: .*as must be PERMISSIVE or RESTRICTIVE, got "restrictiv"`)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(db, qt.IsNil)
		})
	}
}
