package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/difftypes"
	"ptah.run/migration/schemadiff/internal/compare"
)

// readerGetsSelect declares that app_owner's new tables in app give SELECT to
// app_reader, with app_owner a role the declaration manages.
func readerGetsSelect() *schemamodel.Database {
	return &schemamodel.Database{
		Roles: []schemamodel.Role{{Name: "app_owner"}},
		DefaultPrivileges: []schemamodel.DefaultPrivilege{{
			Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader",
			Privileges: []schemamodel.PrivilegeGrant{{Privilege: "SELECT"}},
		}},
	}
}

// TestDefaultPrivilegesWithSemantics_WithholdsWhatARefusedReadCannotConfirm is
// the additive half of coverage for default privileges.
//
// CockroachDB v26.2.7 refuses every read of pg_default_acl while a default
// names a role that needs quoting, and the reader records the kind as not
// inspected instead of failing (stokaro/ptah#3816). The declared default is
// then neither planned nor dropped: it is reported as undecided, with the
// reason the read gave. The row without the record is the inverse: the same
// comparison plans the grant, so the withholding is the record's doing.
func TestDefaultPrivilegesWithSemantics_WithholdsWhatARefusedReadCannotConfirm(t *testing.T) {
	tests := []struct {
		name          string
		notDescribed  coverage.Set
		wantAdded     []string
		wantUndecided []coverage.Object
	}{
		{
			name:         "the read looked",
			notDescribed: coverage.Set{},
			wantAdded:    []string{"SELECT on TABLES in app for app_owner to app_reader"},
		},
		{
			name:         "the server refused the catalog",
			notDescribed: coverage.Set{}.With(coverage.Refused(coverage.DefaultPrivilege)),
			wantAdded:    make([]string, 0),
			wantUndecided: []coverage.Object{{
				Kind:       coverage.DefaultPrivilege,
				Name:       "SELECT on TABLES in app for app_owner to app_reader",
				Reason:     coverage.NotInspected,
				Provenance: coverage.Observed,
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := readerGetsSelect()
			current := &catalog.Database{NotDescribed: test.notDescribed}
			cov := compare.CoverageOf(desired, current)
			diff := &difftypes.SchemaDiff{}

			compare.DefaultPrivilegesWithSemantics(desired, current, diff, identifier.ForDialect(platform.Postgres), cov)

			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(cov.UndecidedAdditions(), qt.DeepEquals, nonNilObjects(test.wantUndecided))
		})
	}
}

// TestDefaultPrivilegesWithSemantics_KeepsWhatADocumentDoesNotDescribe is the
// removal half: a desired document that records the kind as not described does
// not revoke a default it never spoke about, even under a grantor it manages.
// The row without the record is the inverse, and plans the revoke.
func TestDefaultPrivilegesWithSemantics_KeepsWhatADocumentDoesNotDescribe(t *testing.T) {
	tests := []struct {
		name         string
		notDescribed coverage.Set
		wantRemoved  []string
	}{
		{
			name:         "the document describes default privileges",
			notDescribed: coverage.Set{},
			wantRemoved:  []string{"INSERT on TABLES in app for app_owner to app_reader"},
		},
		{
			name:         "the document does not",
			notDescribed: coverage.Set{}.WithKind(coverage.DefaultPrivilege),
			wantRemoved:  make([]string, 0),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := readerGetsSelect()
			desired.NotDescribed = test.notDescribed
			current := &catalog.Database{DefaultPrivileges: []catalog.DefaultPrivilege{
				{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "SELECT"},
				{Grantor: "app_owner", Schema: "app", ObjectType: "TABLES", Grantee: "app_reader", Privilege: "INSERT"},
			}}
			diff := &difftypes.SchemaDiff{}

			compare.DefaultPrivilegesWithSemantics(
				desired, current, diff, identifier.ForDialect(platform.Postgres), compare.CoverageOf(desired, current),
			)

			c.Assert(defaultPrivilegeNames(diff.DefaultPrivilegesRemoved), qt.DeepEquals, test.wantRemoved)
			c.Assert(diff.DefaultPrivilegesAdded, qt.HasLen, 0)
		})
	}
}

// nonNilObjects is want as UndecidedAdditions spells an empty answer: an empty
// slice rather than nil.
func nonNilObjects(want []coverage.Object) []coverage.Object {
	if want == nil {
		return make([]coverage.Object, 0)
	}
	return want
}
