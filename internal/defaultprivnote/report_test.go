package defaultprivnote_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/defaultprivnote"
)

func TestReportUndescribed_NamesIncompleteScopes(t *testing.T) {
	semantics := identifier.ForDialect("postgres")
	class := must.Must(pgdefaultprivilege.ClassRef(pgdefaultprivilege.Class{Schema: "app", ObjectType: "TABLES"}, semantics))
	owner := must.Must(pgdefaultprivilege.Ref(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "FUNCTIONS", Grantee: "owner"}, semantics))
	tests := []struct {
		name      string
		knowledge schemaext.Knowledge
		subjects  []schemaext.SubjectCoverage
		want      string
	}{
		{name: "refused catalog", knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the server refused to read pg_default_acl"},
			want: "note: default privileges are not described: the server refused to read pg_default_acl. A comparison leaves this scope unmanaged.\n"},
		{name: "class and named owner", knowledge: schemaext.Knowledge{State: schemaext.Complete}, subjects: []schemaext.SubjectCoverage{
			{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: owner, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "owner rights are incomplete"}},
			{Kind: pgdefaultprivilege.DefaultPrivilegeKind, Subject: class, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "FOR ALL ROLES has no named grantor"}},
		}, want: "note: default privileges for ALL ROLES on TABLES in schema app are not fully described: FOR ALL ROLES has no named grantor.\n" +
			"note: default privileges for owner on FUNCTIONS globally to owner are not fully described: owner rights are incomplete.\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database := &catalog.Database{FeatureCoverage: must.Must(pgdefaultprivilege.Coverage(schemaext.Observed, test.knowledge, test.subjects))}
			var out bytes.Buffer
			defaultprivnote.ReportUndescribed(&out, database)
			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

func TestReportUndescribed_SilentForCompleteOrMissingKnowledge(t *testing.T) {
	tests := []struct {
		name     string
		database *catalog.Database
	}{
		{name: "no description"},
		{name: "unenrolled owner", database: &catalog.Database{}},
		{name: "complete catalog", database: &catalog.Database{FeatureCoverage: must.Must(pgdefaultprivilege.CompleteCoverage(schemaext.Observed))}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer
			defaultprivnote.ReportUndescribed(&out, test.database)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

func TestReportUndescribed_AcceptsNoDiagnosticsStream(t *testing.T) {
	c := qt.New(t)
	known, err := pgdefaultprivilege.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "catalog read refused"}, nil)
	c.Assert(err, qt.IsNil)
	defaultprivnote.ReportUndescribed(nil, &catalog.Database{FeatureCoverage: known})
}
