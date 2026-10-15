package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
	"ptah.run/internal/ydburl"
)

// scopeAnswers is what the name switches of internal/schemaselection,
// internal/dblock, internal/rolescope and internal/dburldisplay answered for
// one dialect before they read the descriptor. The tables below are written
// out from those switches, so a descriptor that stops agreeing with them
// fails here, by name, rather than as a scope or lock that changed.
type scopeAnswers struct {
	// selection is the arm of schemaselection.Realm and URLScope: the
	// PostgreSQL family read search_path, MySQL and MariaDB the database,
	// SQLite its one namespace.
	selection descriptor.SchemaSelection
	// dialectDefault is what schemaselection.DialectDefault returned.
	dialectDefault string
	// lock is the arm of dblock.acquireOnSession, and unboundedWait what
	// mySQLLockTimeoutSeconds sent for a zero timeout.
	lock          descriptor.LockProtocol
	unboundedWait int64
	// roles is the cause rolescope.outOfScopeCause named, and widenable
	// whether describeAllRemedy offered the variable.
	roles     descriptor.RoleScope
	widenable bool
	// urlValued are the parameters dburldisplay hid a URL inside.
	urlValued []string
}

var scopeAgreement = map[string]scopeAnswers{
	"postgres": {
		selection: descriptor.SchemaSelectionSearchPath, dialectDefault: "public",
		lock:  descriptor.LockPostgresAdvisory,
		roles: descriptor.RoleScopeReferenced, widenable: true,
	},
	"mysql": {
		selection: descriptor.SchemaSelectionDatabase,
		lock:      descriptor.LockGetLock,
	},
	"mariadb": {
		selection: descriptor.SchemaSelectionDatabase,
		lock:      descriptor.LockGetLock, unboundedWait: 31_536_000,
	},
	"clickhouse": {
		roles: descriptor.RoleScopeReferenced,
	},
	"sqlite": {
		selection: descriptor.SchemaSelectionSingle, dialectDefault: "main",
	},
	"sqlserver": {
		lock: descriptor.LockAppLock,
	},
	"cockroachdb": {
		selection: descriptor.SchemaSelectionSearchPath, dialectDefault: "public",
		roles: descriptor.RoleScopeReferenced, widenable: true,
	},
	"yugabytedb": {
		selection: descriptor.SchemaSelectionSearchPath, dialectDefault: "public",
		lock:  descriptor.LockPostgresAdvisory,
		roles: descriptor.RoleScopeReferenced, widenable: true,
	},
	"spanner": {
		selection: descriptor.SchemaSelectionSearchPath,
		roles:     descriptor.RoleScopeReferenced, widenable: true,
	},
	"oracle": {},
	"ydb": {
		lock:      descriptor.LockCoordinationSemaphore,
		roles:     descriptor.RoleScopeUserMade,
		urlValued: []string{ydburl.MonitoringParameter},
	},
}

// TestDescriptors_ScopeAgreesWithTheNameSwitches pins the schema, role, lock
// and URL data of every built-in descriptor against the answers the name
// switches gave.
func TestDescriptors_ScopeAgreesWithTheNameSwitches(t *testing.T) {
	described := builtindialects.Descriptors()
	c := qt.New(t)
	c.Assert(described, qt.HasLen, len(scopeAgreement))
	for _, dialect := range described {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			want, found := scopeAgreement[dialect.Name]
			c.Assert(found, qt.IsTrue)

			// DialectDefault answers DefaultSchema where it is implicit and
			// nothing elsewhere.
			implied := map[bool]string{true: dialect.DefaultSchema}[dialect.Schemas.Implicit]

			c.Assert(dialect.Schemas.Selection, qt.Equals, want.selection)
			c.Assert(dialect.Schemas.Implicit, qt.Equals, want.dialectDefault != "")
			c.Assert(implied, qt.Equals, want.dialectDefault)
			c.Assert(dialect.Lock.Protocol, qt.Equals, want.lock)
			c.Assert(dialect.Lock.UnboundedWaitSeconds, qt.Equals, want.unboundedWait)
			c.Assert(dialect.Roles.OutOfScope, qt.Equals, want.roles)
			c.Assert(dialect.Roles.Widenable, qt.Equals, want.widenable)
			c.Assert(dialect.URLValuedParameters, qt.DeepEquals, want.urlValued)
		})
	}
}
