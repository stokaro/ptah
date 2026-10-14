package shadow

// White-box testing required: mismatch collection turns a schema diff into the
// deterministic list the verification error carries, and the ordering and
// qualification it applies are not observable without calling it directly.

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgtype"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// Every adjustment reaches shadow verification with its role identity and
// privilege names. A mismatch without its rights cannot explain a failed apply.
func TestCollectMismatches_ReportsEveryDefaultPrivilegeDirection(t *testing.T) {
	c := qt.New(t)
	ref := defaultPrivilegeSubject("app_owner")
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ref, Value: &pgdefaultprivilege.Change{
		Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}, Revoked: []pgdefaultprivilege.Grant{{Privilege: "INSERT"}},
		GrantOptionsAdded: []string{"UPDATE"}, GrantOptionsRevoked: []string{"DELETE"},
	}}}}
	detail := "grant SELECT; revoke INSERT; grant option UPDATE; revoke grant option DELETE"
	c.Assert(collectMismatches(diff), qt.DeepEquals, []Mismatch{{
		Kind: "feature_mismatch", Object: ref.String(), Changes: map[string]string{string(pgdefaultprivilege.ChangeKind): detail},
		Message: "feature mismatch " + ref.String() + ": " + string(pgdefaultprivilege.ChangeKind) + " " + detail,
	}})
}

func TestCollectMismatches_OrdersDefaultPrivilegesByTheirIdentity(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{
		{Subject: defaultPrivilegeSubject("beta_owner"), Value: &pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
		{Subject: defaultPrivilegeSubject("alpha_owner"), Value: &pgdefaultprivilege.Change{Granted: []pgdefaultprivilege.Grant{{Privilege: "SELECT"}}}},
	}}
	got := collectMismatches(diff)
	c.Assert(got, qt.HasLen, 2)
	c.Assert(got[0].Object, qt.Equals, defaultPrivilegeSubject("alpha_owner").String())
	c.Assert(got[1].Object, qt.Equals, defaultPrivilegeSubject("beta_owner").String())
}

func defaultPrivilegeSubject(grantor string) objectidentity.ID {
	return must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: grantor, Schema: "app", ObjectType: "TABLES", Grantee: "app_reader"}, identifier.ForDialect("postgres")))
}

func TestCollectMismatches_ReportsQualifiedIndex(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{}
	diff.SetIndexAdditions(difftypes.IndexChanges{
		{Index: schemamodel.Index{Name: "idx_shared", Fields: []string{"code"}}, TableName: "users"},
		{Index: schemamodel.Index{Name: "idx_shared", Fields: []string{"code"}}, TableName: "orders"},
	})

	got := collectMismatches(diff)

	c.Assert(got, qt.DeepEquals, []Mismatch{
		{
			Kind:    "missing_index",
			Table:   "orders",
			Object:  "orders.idx_shared",
			Message: "missing index orders.idx_shared",
		},
		{
			Kind:    "missing_index",
			Table:   "users",
			Object:  "users.idx_shared",
			Message: "missing index users.idx_shared",
		},
	})
}

// TestNewBaselineMismatchError_ReportsWhatTheTargetReadCouldNotCheck holds the
// baseline to the objects the replay created and the target read could not
// see. They are mismatches, one each after the differences, and without a
// difference they are the whole error rather than a pass (stokaro/ptah#3844).
func TestNewBaselineMismatchError_ReportsWhatTheTargetReadCouldNotCheck(t *testing.T) {
	withheld := coverage.Refused(coverage.Role)
	withheld.Name = "reporter"
	undecided := Mismatch{
		Kind:   "undecided_object",
		Object: "reporter",
		Message: "undecided role reporter: the target database does not describe role objects" +
			" because the read was refused the catalog that would have listed them",
	}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want []Mismatch
	}{
		{
			name: "nothing differs",
			diff: &difftypes.SchemaDiff{},
			want: []Mismatch{undecided},
		},
		{
			name: "a missing table beside it",
			diff: &difftypes.SchemaDiff{TablesAdded: difftypes.TableChanges{{Name: "notes"}}},
			want: []Mismatch{
				{Kind: "missing_table", Table: "notes", Object: "notes", Message: "missing table notes"},
				undecided,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := newBaselineMismatchError(test.diff, schemadiff.Diagnostics{Common: []coverage.Object{withheld}})

			c.Assert(err.Result.Stage, qt.Equals, "schema-match")
			c.Assert(err.Result.Mismatches, qt.DeepEquals, test.want)
		})
	}
}

// TestCollectMismatches_DescribesAViewsOwnerChange holds the detail of a
// mismatch that is only an owner change, such as a refresh schedule a replay
// did not reproduce. Without it the mismatch named the view and nothing it
// differs in (stokaro/ptah#4278).
func TestCollectMismatches_DescribesAViewsOwnerChange(t *testing.T) {
	c := qt.New(t)
	refresh := schemaext.ChangeRecord{
		Subject: objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "", "daily"),
		Value: &chdiff.Refresh{
			Before: &chschema.ObservedRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "1 HOUR"}},
			After:  &chschema.DesiredRefresh{Schedule: chschema.Schedule{Mode: chschema.RefreshEvery, Interval: "2 HOUR"}},
		},
	}
	diff := &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{
		{ViewName: "daily", Changes: make(map[string]string), FeatureChanges: []schemaext.ChangeRecord{refresh}},
	}}

	got := collectMismatches(diff)

	c.Assert(got, qt.HasLen, 1)
	c.Assert(got[0].Message, qt.Equals, "materialized view mismatch daily: ptah.run/clickhouse/refresh-change EVERY 1 HOUR -> EVERY 2 HOUR")
	c.Assert(got[0].Changes, qt.DeepEquals, map[string]string{"ptah.run/clickhouse/refresh-change": "EVERY 1 HOUR -> EVERY 2 HOUR"})
}

// describedChange is an owner change that says what changed.
type describedChange struct{}

func (*describedChange) Kind() schemaext.Kind               { return "example.org/described" }
func (*describedChange) CloneChange() schemaext.ChangeValue { return &describedChange{} }
func (*describedChange) String() string                     { return "EVERY 1 HOUR -> EVERY 2 HOUR" }

// effectChange is an owner change that says why it matters.
type effectChange struct{}

func (*effectChange) Kind() schemaext.Kind               { return "example.org/effect" }
func (*effectChange) CloneChange() schemaext.ChangeValue { return &effectChange{} }
func (*effectChange) Effect() schemaext.Effect {
	return schemaext.Effect{Impact: schemaext.Behavioral, Reason: "changes who may read the rows"}
}

// silentChange is an owner change that says nothing about itself.
type silentChange struct{}

func (*silentChange) Kind() schemaext.Kind               { return "example.org/silent" }
func (*silentChange) CloneChange() schemaext.ChangeValue { return &silentChange{} }

func exampleSubject(kind, schema, parent, name string) objectidentity.ID {
	part := func(source string) objectidentity.Part {
		return objectidentity.Part{Source: source, Normalized: source}
	}
	return objectidentity.ID{Kind: objectidentity.Kind(kind), Schema: part(schema), Parent: part(parent), Name: part(name)}
}

// TestCollectMismatches_ReportsEveryOwnerChange pins the row each owner change
// gets, whichever owner made it, standalone or on a table: its subject with the
// owner's kind, and what changed as the owner describes it. Before, a diff
// holding only owner changes reported "schema differs" and nothing more.
func TestCollectMismatches_ReportsEveryOwnerChange(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{
			{Subject: exampleSubject("example.org/schedule", "app", "", "hourly"), Value: &describedChange{}},
			{Subject: exampleSubject("example.org/policy", "app", "users", "tenant"), Value: &effectChange{}},
		},
		TablesModified: []difftypes.TableDiff{{TableName: "app.readings", FeatureChanges: []schemaext.ChangeRecord{
			{Subject: exampleSubject("example.org/setting", "app", "readings", "ttl"), Value: &silentChange{}},
		}}},
	}

	got := collectMismatches(diff)

	c.Assert(got, qt.DeepEquals, []Mismatch{
		{Kind: "feature_mismatch", Object: "example.org/setting app.readings.ttl", Table: "app.readings",
			Changes: map[string]string{"example.org/silent": "changed"},
			Message: "feature mismatch example.org/setting app.readings.ttl: example.org/silent changed"},
		{Kind: "feature_mismatch", Object: "example.org/policy app.users.tenant",
			Changes: map[string]string{"example.org/effect": "changes who may read the rows"},
			Message: "feature mismatch example.org/policy app.users.tenant: example.org/effect changes who may read the rows"},
		{Kind: "feature_mismatch", Object: "example.org/schedule app.hourly",
			Changes: map[string]string{"example.org/described": "EVERY 1 HOUR -> EVERY 2 HOUR"},
			Message: "feature mismatch example.org/schedule app.hourly: example.org/described EVERY 1 HOUR -> EVERY 2 HOUR"},
	})
}

func TestCollectMismatchesReportsRangeChanges(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *pgtype.RangeChange
	}{
		{"addition", &pgtype.RangeChange{After: &pgtype.DesiredRange{Subtype: "numeric"}}},
		{"removal", &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "numeric"}}},
		{"replacement", &pgtype.RangeChange{Before: &pgtype.ObservedRange{Subtype: "numeric"}, After: &pgtype.DesiredRange{Subtype: "int8"}, Differs: []string{pgtype.DiffSubtype}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ref := pgtype.Ref(pgtype.RangeKind, "app", "span")
			got := collectMismatches(&difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: ref, Value: test.value}}})
			c.Assert(got, qt.HasLen, 1)
			c.Assert(got[0].Kind, qt.Equals, "feature_mismatch")
			c.Assert(got[0].Object, qt.Equals, ref.String())
			c.Assert(got[0].Changes[string(pgtype.RangeChangeKind)], qt.Not(qt.Equals), "")
		})
	}
}
