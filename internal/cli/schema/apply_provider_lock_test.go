package schema_test

import (
	"bytes"
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasurl"
	"ptah.run/internal/cli/schema"
)

// heldLockConnector opens provider sessions on the target sqlite whose lock
// service answers every acquisition busy, as a provider does while another
// apply holds the lock. The target is a dialect Ptah builds in and that has no
// lock of its own, so the decision that reads the dialect name refuses where
// the provider's session would lock. Nothing a session offers but its read is
// used before the lock is taken.
type heldLockConnector struct{ attempts atomic.Int64 }

func (c *heldLockConnector) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return &heldLockSession{connector: c}, nil
}

type heldLockSession struct{ connector *heldLockConnector }

func (*heldLockSession) Info() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: "sqlite", Schema: "main", IdentifierSemantics: identifier.ForDialect("sqlite"),
		Provider: catalog.ProviderIdentity{ID: "example.provider", Version: "test", SemanticIdentity: "test-1", ProtocolRevision: 2,
			ArtifactSHA256: strings.Repeat("a", 64), ContractSHA256: strings.Repeat("b", 64)}}
}
func (*heldLockSession) Reader([]string) catalog.SchemaReader { return heldLockReader{} }
func (*heldLockSession) Writer() catalog.SchemaWriter         { return heldLockWriter{} }
func (s *heldLockSession) OpenSession(context.Context) (dbschema.NativeSession, error) {
	return &heldLockSession{connector: s.connector}, nil
}
func (*heldLockSession) RestrictUntrustedSQL(context.Context) error {
	return ptaherr.ErrUnsupportedFeature
}
func (*heldLockSession) Close() error { return nil }
func (s *heldLockSession) SessionLocker() (dbschema.SessionLocker, error) {
	return heldLocker{connector: s.connector}, nil
}

type heldLocker struct{ connector *heldLockConnector }

func (l heldLocker) TryLock(context.Context, string) (dbschema.SessionLock, error) {
	l.connector.attempts.Add(1)
	return nil, dbschema.ErrSessionLockBusy
}

type heldLockReader struct{}

func (heldLockReader) ReadSchema() (*catalog.Database, error) { return &catalog.Database{}, nil }
func (heldLockReader) ReadSchemaContext(context.Context) (*catalog.Database, error) {
	return &catalog.Database{}, nil
}

type heldLockWriter struct{}

func (heldLockWriter) ExecuteSQL(context.Context, string, ...any) error {
	return ptaherr.ErrUnsupportedFeature
}
func (heldLockWriter) IsDryRun() bool                      { return false }
func (heldLockWriter) SetDryRun(bool)                      {}
func (heldLockWriter) DropAllTables(context.Context) error { return ptaherr.ErrUnsupportedFeature }
func (heldLockWriter) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	return nil, ptaherr.ErrUnsupportedFeature
}

// runSchemaWithProvider executes `ptah schema ...` with connector selected for
// the sqlite:// scheme, as --provider-config selects a provider.
func runSchemaWithProvider(c *qt.C, connector dbschema.NativeConnector, args ...string) (string, error) {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{
		"sqlite": {Target: "sqlite", Connector: connector},
	})
	c.Assert(err, qt.IsNil)
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(""))
	cmd.SetArgs(args)
	err = cmd.ExecuteContext(ctx)
	return out.String(), err
}

// TestSchemaApplyLockTimeoutBoundsAProviderLock_FailurePath drives a target an
// executable provider serves with a lock service, while another holder has
// the lock. A typed --lock-timeout is the provider's to honor: the run asks
// for the lock until the timeout passes and stops there, rather than being
// refused, before it connects, as aimed at a dialect with no lock. The
// database file is never created: the provider's session is what opened.
func TestSchemaApplyLockTimeoutBoundsAProviderLock_FailurePath(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := writeSchemaSQLFile(c, dir, "schema.sql", "CREATE TABLE users (id INTEGER PRIMARY KEY);\n")
	connector := &heldLockConnector{}

	out, err := runSchemaWithProvider(c, connector, "apply",
		"--db-url", atlasurl.SQLiteURLFromPath(filepath.Join(dir, "target.db")),
		"--schema-file", schemaPath,
		"--lock-timeout", "200ms",
		"--auto-approve",
	)

	c.Assert(err, qt.ErrorMatches,
		`acquire schema apply lock: timed out acquiring advisory lock "ptah_schema_apply" on sqlite after 200ms`,
		qt.Commentf("%s", out))
	c.Assert(connector.attempts.Load(), qt.Not(qt.Equals), int64(0))
	c.Assert(out, qt.Not(qt.Contains), "Planned schema changes:")
	_, statErr := os.Stat(filepath.Join(dir, "target.db"))
	c.Assert(statErr, qt.ErrorIs, fs.ErrNotExist)
}
