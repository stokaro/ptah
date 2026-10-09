package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/cockroachdb/crdbdiff"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/cockroachdb/crdbsource"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// declaredTTL is a one-table declaration carrying the given policy, from a
// source that can declare one: a table without a policy requests none.
func declaredTTL(policy *crdbschema.Policy) *schemamodel.Database {
	db := undescribedTTL(policy)
	db.FeatureCoverage = must.Must(crdbsource.Coverage())
	return db
}

// undescribedTTL is the same declaration from a source that cannot declare a
// policy, such as an HCL or SQL document: it says nothing about the TTL.
func undescribedTTL(policy *crdbschema.Policy) *schemamodel.Database {
	table := schemamodel.Table{StructName: "Sessions", Name: "sessions"}
	if policy != nil {
		table.Facets = must.Must(schemaext.NewFacets(&crdbschema.DesiredRowTTL{Policy: *policy}))
	}
	return &schemamodel.Database{
		Tables: []schemamodel.Table{table},
		Fields: []schemamodel.Field{{StructName: "Sessions", Name: "id", Type: "BIGINT", Primary: true}},
	}
}

// liveTTL is the live description of that table with the given policy, read
// with complete row-level TTL knowledge for it.
func liveTTL(policy *crdbschema.Policy) *catalog.Database {
	db := uninspectedTTL(policy)
	subject := objectidentity.NewBuilder(identifier.ForDialect(platform.CockroachDB)).TableParts("", "sessions")
	db.FeatureCoverage = must.Must(crdbschema.RowTTLCoverage(schemaext.Observed,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "only returned tables were read"},
		[]schemaext.SubjectCoverage{{Kind: crdbschema.RowTTLKind, Subject: subject, Knowledge: schemaext.Knowledge{State: schemaext.Complete}}}))
	return db
}

// uninspectedTTL is a live description of that table whose read never asked
// about row-level TTL.
func uninspectedTTL(policy *crdbschema.Policy) *catalog.Database {
	table := catalog.Table{
		Name:    "sessions",
		Type:    "BASE TABLE",
		Columns: []catalog.Column{{Name: "id", DataType: "BIGINT", IsNullable: "NO"}},
	}
	if policy != nil {
		table.Facets = must.Must(must.Must(schemaext.NewFacets(&crdbschema.ObservedRowTTL{Policy: *policy})).
			WithTargetScope(crdbschema.RowTTLKind, platform.CockroachDB))
	}
	return &catalog.Database{Tables: []catalog.Table{table}}
}

// TestCompare_RowTTLTransitions pins what the comparison reports for each
// transition, including the one that has to reach TablesModified with no column
// difference at all.
//
// That last case is the one worth the test: a table whose ONLY difference is its
// retention policy would be dropped by a modified-table condition counting
// columns, and the schema would report as synced while rows expired on a
// schedule nobody declared (stokaro/ptah#1027).
func TestCompare_RowTTLTransitions(t *testing.T) {
	tests := []struct {
		name        string
		desired     *crdbschema.Policy
		current     *crdbschema.Policy
		wantChanged bool
		wantDesired *crdbschema.Policy
		wantCurrent *crdbschema.Policy
	}{
		{
			name:        "neither side has a policy",
			wantChanged: false,
		},
		{
			name:        "an unchanged policy",
			desired:     &crdbschema.Policy{ExpirationExpression: "expires_at"},
			current:     &crdbschema.Policy{ExpirationExpression: "expires_at"},
			wantChanged: false,
		},
		{
			// The server stores its own spelling of an interval; the
			// comparison reads both as the value they denote
			// (stokaro/ptah#1605).
			name:        "an interval in the server's spelling",
			desired:     &crdbschema.Policy{ExpireAfter: "72 hours"},
			current:     &crdbschema.Policy{ExpireAfter: "72:00:00"},
			wantChanged: false,
		},
		{
			// And a duration, which the server truncates and respells
			// (stokaro/ptah#1721).
			name:        "a poll interval in the server's spelling",
			desired:     &crdbschema.Policy{ExpireAfter: "1 day", RowStatsPollInterval: "600s"},
			current:     &crdbschema.Policy{ExpireAfter: "1 day", RowStatsPollInterval: "10m0s"},
			wantChanged: false,
		},
		{
			name:        "a policy being added",
			desired:     &crdbschema.Policy{ExpirationExpression: "expires_at"},
			wantChanged: true,
			wantDesired: &crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
		{
			name:        "a policy being removed",
			current:     &crdbschema.Policy{ExpirationExpression: "expires_at"},
			wantChanged: true,
			wantCurrent: &crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
		{
			name:        "an expression being changed",
			desired:     &crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 hour'"},
			current:     &crdbschema.Policy{ExpirationExpression: "expires_at"},
			wantChanged: true,
			wantDesired: &crdbschema.Policy{ExpirationExpression: "expires_at + INTERVAL '1 hour'"},
			wantCurrent: &crdbschema.Policy{ExpirationExpression: "expires_at"},
		},
		{
			// A month is not thirty days, and the server keeps them apart.
			name:        "two intervals that only look alike",
			desired:     &crdbschema.Policy{ExpireAfter: "30 days"},
			current:     &crdbschema.Policy{ExpireAfter: "1 mon"},
			wantChanged: true,
			wantDesired: &crdbschema.Policy{ExpireAfter: "30 days"},
			wantCurrent: &crdbschema.Policy{ExpireAfter: "1 mon"},
		},
		{
			name:        "a knob being dropped while the policy stays",
			desired:     &crdbschema.Policy{ExpirationExpression: "expires_at"},
			current:     &crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
			wantChanged: true,
			wantDesired: &crdbschema.Policy{ExpirationExpression: "expires_at"},
			wantCurrent: &crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(500))},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := must.Must(schemadiff.CompareWithDialect(
				t.Context(), declaredTTL(test.desired), liveTTL(test.current), platform.CockroachDB, must.Must(builtin.New())))

			change := rowTTLChangeOf(diff)

			c.Assert(change != nil, qt.Equals, test.wantChanged)
			c.Assert(desiredOf(change), qt.DeepEquals, test.wantDesired)
			c.Assert(currentOf(change), qt.DeepEquals, test.wantCurrent)
		})
	}
}

// rowTTLChangeOf returns the TTL transition the comparison attached to a
// table, or nil. These three helpers exist so the assertions above stay data
// comparisons rather than conditionals in a test body.
func rowTTLChangeOf(diff *difftypes.SchemaDiff) *crdbdiff.RowTTL {
	for _, table := range diff.TablesModified {
		for _, change := range table.FeatureChanges {
			if value, ok := change.Value.(*crdbdiff.RowTTL); ok {
				return value
			}
		}
	}
	return nil
}

func desiredOf(change *crdbdiff.RowTTL) *crdbschema.Policy {
	if change == nil || change.After == nil {
		return nil
	}
	return &change.After.Policy
}

func currentOf(change *crdbdiff.RowTTL) *crdbschema.Policy {
	if change == nil || change.Before == nil {
		return nil
	}
	return &change.Before.Policy
}

// TestCompare_ATTLOnlyDifferenceStillReachesTablesModified is the case the
// modified-table condition would drop if it counted columns alone: the two
// sides agree on every column and differ only in the retention policy.
//
// Without it the schema reports as synced while rows expire on a schedule
// nobody declared, which is the failure stokaro/ptah#1027 names.
func TestCompare_ATTLOnlyDifferenceStillReachesTablesModified(t *testing.T) {
	c := qt.New(t)

	desired := declaredTTL(&crdbschema.Policy{ExpirationExpression: "expires_at"})
	current := liveTTL(nil)
	current.Tables[0].Columns = columnsMatching(desired)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.CockroachDB, must.Must(builtin.New())))

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsAdded, qt.HasLen, 0)
	c.Assert(diff.TablesModified[0].ColumnsRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 0)
	c.Assert(rowTTLChangeOf(diff), qt.IsNotNil)
}

// columnsMatching describes the declaration's columns exactly as a read of the
// table it creates would, so a comparison over the two finds no column
// difference at all.
func columnsMatching(_ *schemamodel.Database) []catalog.Column {
	return []catalog.Column{{
		Name: "id", DataType: "bigint", UDTName: "int8", IsNullable: "NO", IsPrimaryKey: true,
	}}
}

// TestCompare_RowTTLChangeCarriesBothSides pins the operands the planner
// depends on. `SET` replaces only the parameters it names, so a plan that has
// to reset a dropped knob can only learn which those are from the CURRENT state
// -- a change carrying the desired policy alone could not produce a correct
// statement.
func TestCompare_RowTTLChangeCarriesBothSides(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithDialect(
		t.Context(), declaredTTL(&crdbschema.Policy{ExpirationExpression: "expires_at"}),
		liveTTL(&crdbschema.Policy{ExpirationExpression: "expires_at", JobCron: "@daily"}),
		platform.CockroachDB, must.Must(builtin.New()),
	))

	change := rowTTLChangeOf(diff)
	c.Assert(change, qt.IsNotNil)
	c.Assert(change.After.Policy.JobCron, qt.Equals, "")
	c.Assert(change.Before.Policy.JobCron, qt.Equals, "@daily")
}

// TestCompare_RowTTLChangeIsIndependentOfTheSchemaItCameFrom pins that the diff
// holds copies. A planner or a caller mutating the change must not reach back
// into the declaration or the live description it was derived from.
func TestCompare_RowTTLChangeIsIndependentOfTheSchemaItCameFrom(t *testing.T) {
	c := qt.New(t)

	desired := declaredTTL(&crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(10))})
	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, liveTTL(nil), platform.CockroachDB, must.Must(builtin.New())))

	change := rowTTLChangeOf(diff)
	c.Assert(change, qt.IsNotNil)

	change.After.Policy.ExpirationExpression = "mutated"
	*change.After.Policy.SelectBatchSize = 99

	declared, _, err := schemaext.FacetAs[*crdbschema.DesiredRowTTL](desired.Tables[0].Facets, crdbschema.RowTTLKind)
	c.Assert(err, qt.IsNil)
	c.Assert(declared.Policy, qt.DeepEquals, crdbschema.Policy{ExpirationExpression: "expires_at", SelectBatchSize: new(int64(10))})
}

// TestCompare_RowTTLASourceThatCannotDeclareOneLeavesItAlone pins that a source
// format without row-level TTL does not remove a live policy. An HCL, SQL or
// hand-built document is silent about TTL, and silence is not a request for
// none: before the owner modeled this, applying such a document back to its own
// database planned `RESET (ttl)`.
func TestCompare_RowTTLASourceThatCannotDeclareOneLeavesItAlone(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), undescribedTTL(nil),
		liveTTL(&crdbschema.Policy{ExpirationExpression: "expires_at"}), platform.CockroachDB, must.Must(builtin.New())))

	c.Assert(rowTTLChangeOf(diff), qt.IsNil)
}

// TestCompare_RowTTLFailurePath pins the refusals: a declared policy whose
// live state was never read is undecided, not an addition, and a declared
// policy on a target with no owner for it is refused rather than dropped.
func TestCompare_RowTTLFailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		current *catalog.Database
		dialect string
		wantErr string
	}{
		{
			name:    "the live policy was never inspected",
			desired: declaredTTL(&crdbschema.Policy{ExpirationExpression: "expires_at"}),
			current: uninspectedTTL(nil),
			dialect: platform.CockroachDB,
			wantErr: `(?s).*row-level TTL was not inspected.*`,
		},
		{
			name:    "PostgreSQL has no owner for the policy",
			desired: declaredTTL(&crdbschema.Policy{ExpirationExpression: "expires_at"}),
			current: uninspectedTTL(nil),
			dialect: platform.Postgres,
			wantErr: `(?s).*no facet comparison for "postgres"/"ptah.run/cockroachdb/row-ttl".*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDialect(t.Context(), test.desired, test.current, test.dialect, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(diff, qt.IsNil)
		})
	}
}
