//go:build integration

package integration_test

import (
	"database/sql"
	"net/url"
	"path/filepath"
	"strings"
	"testing"

	"example.com/ptah-tutorial-provider/backend"
	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	// The test writes a table the provider refuses to describe.
	_ "modernc.org/sqlite"
)

// address returns a file in a fresh directory and its tutorial address.
func address(c *qt.C) (path, location string) {
	c.Helper()
	path = filepath.Join(c.TempDir(), "app.db")
	return path, (&url.URL{Scheme: backend.Engine, Path: "/" + strings.TrimPrefix(filepath.ToSlash(path), "/")}).String()
}

// open opens a session of tutorial on the address and closes it when the test
// ends. The SDK validator is the one both sides of the process boundary run.
func open(c *qt.C, tutorial *backend.Provider, location string) *protocol.Session {
	c.Helper()
	session, err := tutorial.Open(c.Context(), &protocol.OpenRequest{Engine: backend.Engine, Address: location})
	c.Assert(err, qt.IsNil)
	c.Assert(provider.ValidateSession(backend.Engine, session), qt.IsNil)
	c.Cleanup(func() {
		c.Check(tutorial.Close(c.Context(), &protocol.CloseRequest{SessionId: session.GetId()}), qt.IsNil)
	})
	return session
}

// inspect reads the database and checks the snapshot as the SDK does.
func inspect(c *qt.C, tutorial *backend.Provider, session *protocol.Session) *protocol.Schema {
	c.Helper()
	request := &protocol.InspectRequest{SessionId: session.GetId()}
	snapshot, err := tutorial.Inspect(c.Context(), request)
	c.Assert(err, qt.IsNil)
	_, err = provider.InspectionValues(backend.Description(), request, snapshot)
	c.Assert(err, qt.IsNil)
	return snapshot
}

// createNotes is a change, named id, that creates the table notes.
func createNotes(id string) *protocol.Change {
	return &protocol.Change{Id: id, Object: &protocol.Reference{Kind: "table", Name: "notes"}, Operation: &protocol.Change_CreateTable{CreateTable: &protocol.Table{Columns: []*protocol.Column{
		{Name: "id", Type: "INTEGER", PrimaryKey: true},
		{Name: "body", Type: "TEXT", Nullable: true},
	}}}}
}

// render renders changes and checks the batch as the SDK does.
func render(c *qt.C, tutorial *backend.Provider, changes ...*protocol.Change) []*protocol.Unit {
	c.Helper()
	request := &protocol.RenderRequest{Engine: backend.Engine, Changes: changes}
	result, err := tutorial.Render(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(provider.ValidateRenderedBatch(backend.Description(), request, result), qt.IsNil)
	return result.GetUnits()
}

// The read, render, run and read again loop that a plan and an apply make.
func TestReadRenderExecute_HappyPath(t *testing.T) {
	c := qt.New(t)
	tutorial := &backend.Provider{}
	_, location := address(c)
	session := open(c, tutorial, location)
	c.Assert(inspect(c, tutorial, session).GetTables(), qt.HasLen, 0)

	request := &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "apply", Units: render(c, tutorial, createNotes("create"))}
	result, err := tutorial.Execute(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(provider.CheckExecuted(request, result), qt.IsNil)
	c.Assert(result.GetAppliedCount(), qt.Equals, uint32(1))

	tables := inspect(c, tutorial, session).GetTables()
	c.Assert(tables, qt.HasLen, 1)
	c.Assert(tables[0].GetIdentity().GetName(), qt.Equals, "notes")
	c.Assert(tables[0].GetColumns(), qt.HasLen, 2)
	c.Assert(tables[0].GetColumns()[0].GetPrimaryKey(), qt.IsTrue)
	c.Assert(tables[0].GetColumns()[1].GetNullable(), qt.IsTrue)
}

// A unit the database refuses stops the batch. The units before it stay
// applied, and the result says how many with a reason that names the unit.
func TestExecute_FailurePath(t *testing.T) {
	c := qt.New(t)
	tutorial := &backend.Provider{}
	_, location := address(c)
	session := open(c, tutorial, location)
	units := render(c, tutorial, createNotes("create"), createNotes("again"))
	request := &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "apply", Units: units}
	result, err := tutorial.Execute(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_PARTIAL)
	var failed *provider.ExecutionError
	c.Assert(provider.CheckExecuted(request, result), qt.ErrorAs, &failed)
	c.Assert(failed.Applied, qt.Equals, uint32(1))
	c.Assert(failed.Reason, qt.Equals, "the database refused unit 2 of 2")
	c.Assert(inspect(c, tutorial, session).GetTables(), qt.HasLen, 1)
}

// A table the engine cannot describe fails the read instead of disappearing
// from it: an incomplete snapshot would make Ptah plan to create the table.
func TestInspect_FailurePath(t *testing.T) {
	c := qt.New(t)
	tutorial := &backend.Provider{}
	path, location := address(c)
	database, err := sql.Open("sqlite", path)
	c.Assert(err, qt.IsNil)
	_, err = database.ExecContext(c.Context(), `CREATE TABLE "notes" ("id" INTEGER NOT NULL PRIMARY KEY CHECK ("id" > 0))`)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Close(), qt.IsNil)

	session := open(c, tutorial, location)
	snapshot, err := tutorial.Inspect(c.Context(), &protocol.InspectRequest{SessionId: session.GetId()})
	c.Assert(status.Code(err), qt.Equals, codes.FailedPrecondition)
	c.Assert(status.Convert(err).Message(), qt.Equals, "a table definition holds more than the tutorial engine describes")
	c.Assert(snapshot, qt.IsNil)
}

// lockRequest names one lock ID of a session.
func lockRequest(session *protocol.Session, id string) *protocol.LockRequest {
	return &protocol.LockRequest{Scope: &protocol.LockScope{SessionId: session.GetId(), LockId: id, Name: "ptah_schema_apply"}, RequestId: id + "-request"}
}

// tryLock asks for a lock and checks the receipt as the SDK does.
func tryLock(c *qt.C, tutorial *backend.Provider, request *protocol.LockRequest) protocol.LockResult_Outcome {
	c.Helper()
	result, err := tutorial.TryLock(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(provider.ValidateLockResult(provider.LockTry, request, result), qt.IsNil)
	return result.GetOutcome()
}

// releaseLock gives a lock back and checks the receipt as the SDK does.
func releaseLock(c *qt.C, tutorial *backend.Provider, request *protocol.LockRequest) protocol.LockResult_Outcome {
	c.Helper()
	result, err := tutorial.ReleaseLock(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(provider.ValidateLockResult(provider.LockRelease, request, result), qt.IsNil)
	return result.GetOutcome()
}

// Two sessions on one file exclude each other, even through two Provider
// values, as they would through two provider processes.
func TestLocks(t *testing.T) {
	c := qt.New(t)
	first, second := &backend.Provider{}, &backend.Provider{}
	_, location := address(c)
	holder, waiter := open(c, first, location), open(c, second, location)

	c.Assert(tryLock(c, first, lockRequest(holder, "a")), qt.Equals, protocol.LockResult_ACQUIRED)
	c.Assert(tryLock(c, second, lockRequest(waiter, "b")), qt.Equals, protocol.LockResult_BUSY)
	checked, err := first.CheckLock(c.Context(), lockRequest(holder, "a"))
	c.Assert(err, qt.IsNil)
	c.Assert(checked.GetOutcome(), qt.Equals, protocol.LockResult_ACQUIRED)

	c.Assert(releaseLock(c, first, lockRequest(holder, "a")), qt.Equals, protocol.LockResult_RELEASED)
	// A lock ID is used once: the released ID does not acquire again.
	c.Assert(tryLock(c, first, lockRequest(holder, "a")), qt.Equals, protocol.LockResult_RELEASED)
	c.Assert(tryLock(c, second, lockRequest(waiter, "c")), qt.Equals, protocol.LockResult_ACQUIRED)
	// Releasing an ID that never took the lock fences it.
	c.Assert(releaseLock(c, first, lockRequest(holder, "d")), qt.Equals, protocol.LockResult_NOT_ACQUIRED)
	c.Assert(tryLock(c, first, lockRequest(holder, "d")), qt.Equals, protocol.LockResult_NOT_ACQUIRED)
}
