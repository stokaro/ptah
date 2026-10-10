package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/yamlschema"
	"ptah.run/feature/pgpolicy"
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
// Server or ClickHouse keeps the entries in the shared model.
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

			db, err := yamlschema.Parse(noOwners, rowSecurityDocument(test.dialects))

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

	db, err := yamlschema.Parse(noOwners, rowSecurityDocument("dialects: [postgres, mssql]"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
	c.Assert(err, qt.ErrorMatches, `(?s).*rls_enabled_tables\.docs: .*mixes PostgreSQL-family targets with others.*`)
	c.Assert(db, qt.IsNil)
}
