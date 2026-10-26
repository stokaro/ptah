package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/feature/pgextension"
	"ptah.run/feature/pgpolicy"
)

// TestParentKinds_PostgreSQLAttachesRowSecurityOnly checks the vocabulary a
// PostgreSQL rollback reads before it recreates a removed table: policies and
// the row-security switches belong to a table, default privileges and
// extensions belong to a schema or the database, and a model of another
// target belongs to none of its tables.
func TestParentKinds_PostgreSQLAttachesRowSecurityOnly(t *testing.T) {
	c := qt.New(t)

	kinds, err := must.Must(builtin.New()).ParentKinds(platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(kinds, qt.Contains, pgpolicy.PolicyKind)
	c.Assert(kinds, qt.Contains, pgpolicy.TableStateKind)
	c.Assert(kinds, qt.Not(qt.Contains), pgdefaultprivilege.DefaultPrivilegeKind)
	c.Assert(kinds, qt.Not(qt.Contains), pgextension.ExtensionKind)
	c.Assert(kinds, qt.Not(qt.Contains), ydbschema.ChangefeedKind)
}
