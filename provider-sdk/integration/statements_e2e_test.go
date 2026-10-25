//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

// startStatements starts the dev database fixture, which serves the operator
// statements of contract 25.4 on SQLite.
func startStatements(c *qt.C) *transport.Client {
	c.Helper()
	binary := buildFixture(c, "devdb")
	data, err := os.ReadFile(binary)
	c.Assert(err, qt.IsNil)
	checksum := sha256.Sum256(data)
	return start(c, transport.Options{Executable: binary, SHA256: hex.EncodeToString(checksum[:]), ProviderID: "example.org/fixture",
		RequiredFeatures: []string{provider.StatementsFeature}, HostFeatures: provider.TwoWayFeatures()})
}

func runStatement(c *qt.C, client *transport.Client, session, sql string, access protocol.RunStatementRequest_Access, maxRows uint32) *protocol.StatementResult {
	c.Helper()
	result, err := client.RunStatement(c.Context(), &protocol.RunStatementRequest{SessionId: session, RequestId: sql, Sql: sql, Access: access, MaxRows: maxRows})
	c.Assert(err, qt.IsNil)
	return result
}

// A query's rows come in pages, a write reports the rows it changed, and a
// result is released before its last page.
func TestFixtureStatements_HappyPath(t *testing.T) {
	c := qt.New(t)
	client := startStatements(c)
	session := open(c, client, "").Id
	write := protocol.RunStatementRequest_READ_WRITE
	runStatement(c, client, session, `CREATE TABLE "codes" ("code" TEXT)`, write, 1)
	inserted := runStatement(c, client, session, `INSERT INTO "codes" VALUES ('CZ'), ('SK'), ('PL')`, write, 1)
	c.Assert(inserted.GetRowsAffected(), qt.Equals, uint64(3))

	first := runStatement(c, client, session, `SELECT "code" FROM "codes" ORDER BY "code"`, protocol.RunStatementRequest_READ_ONLY, 2)
	c.Assert(first.Outcome, qt.Equals, protocol.StatementResult_SUCCEEDED)
	c.Assert(first.Columns[0].Name, qt.Equals, "code")
	c.Assert(first.Rows, qt.HasLen, 2)
	page, err := client.ReadStatementRows(c.Context(), &protocol.ReadStatementRowsRequest{SessionId: session, PageToken: first.NextPageToken, MaxRows: 2, ColumnCount: 1})
	c.Assert(err, qt.IsNil)
	c.Assert(page.Rows[0].Values[0].GetStringValue(), qt.Equals, "SK")
	c.Assert(page.NextPageToken, qt.Equals, "")

	open := runStatement(c, client, session, `SELECT "code" FROM "codes"`, write, 1)
	_, err = client.CloseStatement(c.Context(), &protocol.CloseStatementRequest{SessionId: session, PageToken: open.NextPageToken})
	c.Assert(err, qt.IsNil)
	_, err = client.ReadStatementRows(c.Context(), &protocol.ReadStatementRowsRequest{SessionId: session, PageToken: open.NextPageToken, MaxRows: 1, ColumnCount: 1})
	c.Assert(err, qt.ErrorMatches, `provider read statement rows failed \(Unknown\)`)
}

// A statement the engine refuses is FAILED in its words, a read-only write is
// refused before it runs, and an engine that does not measure a count reports
// none.
func TestFixtureStatements_FailurePath(t *testing.T) {
	c := qt.New(t)
	client := startStatements(c)
	session := open(c, client, "").Id
	failed := runStatement(c, client, session, `SELECT * FROM "missing"`, protocol.RunStatementRequest_READ_WRITE, 1)
	c.Assert(failed.Outcome, qt.Equals, protocol.StatementResult_FAILED)
	c.Assert(failed.Error, qt.Contains, "no such table: missing")

	refused := runStatement(c, client, session, `CREATE TABLE "x" ("id" INTEGER)`, protocol.RunStatementRequest_READ_ONLY, 1)
	c.Assert(refused.Outcome, qt.Equals, protocol.StatementResult_REFUSED)
	tables := runStatement(c, client, session, `SELECT count(*) FROM sqlite_schema WHERE name = 'x'`, protocol.RunStatementRequest_READ_ONLY, 1)
	c.Assert(tables.Rows[0].Values[0].GetInt64Value(), qt.Equals, int64(0))

	uncounted := open(c, client, "uncounted-rows").Id
	runStatement(c, client, uncounted, `CREATE TABLE "codes" ("code" TEXT)`, protocol.RunStatementRequest_READ_WRITE, 1)
	written := runStatement(c, client, uncounted, `INSERT INTO "codes" VALUES ('CZ')`, protocol.RunStatementRequest_READ_WRITE, 1)
	c.Assert(written.Outcome, qt.Equals, protocol.StatementResult_SUCCEEDED)
	c.Assert(written.RowsAffected, qt.IsNil)
}
