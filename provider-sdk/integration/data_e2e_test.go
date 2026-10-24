//go:build integration

package integration_test

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	transport "github.com/stokaro/ptah/provider-sdk/runtime"
)

// The ADR 0028 conformance list, against the fixture's SQLite: paging and
// resume, an empty table and a missing one, the rendering of each write mode,
// a refused batch with no prefix, a transaction that rolls back declared rows,
// seeds whose ledger entry exists only with the file, a key conflict, a
// ledger entry replaced when its path runs again, a refusal before anything
// runs, and a lost seed reply reported as unknown and not sent again.

func dataClient(c *qt.C) *transport.Client {
	c.Helper()
	return start(c, newHostOptions(c, buildFixture(c, "data")))
}

func runStatements(c *qt.C, client *transport.Client, session *protocol.Session, transaction string, statements ...string) {
	c.Helper()
	units := make([]*protocol.Unit, len(statements))
	for i, statement := range statements {
		units[i] = &protocol.Unit{ChangeId: string(rune('a' + i)), Body: &protocol.Unit_Sql{Sql: statement}}
	}
	result, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "run", TransactionId: transaction, Units: units})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetOutcome(), qt.Equals, protocol.Execution_APPLIED)
}

func tableRef(name string) *protocol.Reference { return &protocol.Reference{Kind: "table", Name: name} }

func readAll(c *qt.C, client *transport.Client, session *protocol.Session, transaction, name string) []string {
	c.Helper()
	page, err := client.ReadRows(c.Context(), &protocol.ReadRowsRequest{SessionId: session.GetId(), TransactionId: transaction, Table: tableRef(name),
		Columns: []string{"id", "name"}, KeyColumns: []string{"id"}, MaxRows: provider.MaxRowsPerPage})
	c.Assert(err, qt.IsNil)
	c.Assert(page.GetComplete(), qt.IsTrue)
	var rows []string
	for _, row := range page.GetRows() {
		rows = append(rows, row.GetValues()[1].GetStringValue())
	}
	return rows
}

func intValue(v int64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: v}}
}
func textValue(v string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: v}}
}

func TestDataServicesReadPagesInKeyOrderAndResume(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`,
		`INSERT INTO "items" VALUES (3, 'c'), (1, 'a'), (5, 'e'), (2, 'b'), (4, 'd');`)
	request := &protocol.ReadRowsRequest{SessionId: session.GetId(), Table: tableRef("items"), Columns: []string{"id", "name"}, KeyColumns: []string{"id"}, MaxRows: 2}
	var names []string
	var completions []bool
	for range 3 {
		page, err := client.ReadRows(c.Context(), request)
		c.Assert(err, qt.IsNil)
		c.Assert(page.GetExists(), qt.IsTrue)
		c.Assert(page.GetColumns()[0].GetKind(), qt.Equals, protocol.DataKind_DATA_KIND_INT64)
		for _, row := range page.GetRows() {
			names = append(names, row.GetValues()[1].GetStringValue())
		}
		completions = append(completions, page.GetComplete())
		request.PageToken = page.GetNextPageToken()
	}
	c.Assert(names, qt.DeepEquals, []string{"a", "b", "c", "d", "e"})
	c.Assert(completions, qt.DeepEquals, []bool{false, false, true})
}

func TestDataServicesTellAnEmptyTableFromAMissingOne(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`)
	for _, row := range []struct {
		name   string
		exists bool
	}{{"items", true}, {"absent", false}} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			page, err := client.ReadRows(c.Context(), &protocol.ReadRowsRequest{SessionId: session.GetId(), Table: tableRef(row.name),
				Columns: []string{"id"}, KeyColumns: []string{"id"}, MaxRows: 10})
			c.Assert(err, qt.IsNil)
			c.Assert(page.GetExists(), qt.Equals, row.exists)
			c.Assert(page.GetComplete(), qt.IsTrue)
			c.Assert(page.GetRows(), qt.HasLen, 0)
		})
	}
}

func TestDataServicesTypeTwoSpellingsIntoOneForm(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	typed, err := client.TypeRows(c.Context(), &protocol.TypeRowsRequest{Engine: "fixture", Table: tableRef("items"),
		LiveTable: &protocol.LiveTable{Columns: []*protocol.Column{{Name: "id", Type: "INTEGER"}, {Name: "price", Type: "REAL"}}},
		Columns:   []*protocol.RowColumn{{Name: "id", DeclaredType: "INTEGER"}, {Name: "price", DeclaredType: "REAL"}},
		Declared:  []*protocol.Row{{Values: []*protocol.DataValue{textValue("5"), intValue(2)}}},
		Live:      []*protocol.Row{{Values: []*protocol.DataValue{intValue(5), {Value: &protocol.DataValue_DoubleValue{DoubleValue: 2}}}}}})
	c.Assert(err, qt.IsNil)
	c.Assert(typed.GetColumns()[1].GetKind(), qt.Equals, protocol.DataKind_DATA_KIND_DOUBLE)
	c.Assert(typed.GetDeclared()[0].GetValues()[0].GetInt64Value(), qt.Equals, int64(5))
	c.Assert(typed.GetDeclared()[0].GetValues()[1].GetDoubleValue(), qt.Equals, 2.0)
	c.Assert(typed.GetLive()[0].GetValues()[0].GetInt64Value(), qt.Equals, int64(5))
}

func rowWrite(id string, mode protocol.RowWrite_Mode, columns []string, values ...*protocol.DataValue) *protocol.RowWrite {
	return &protocol.RowWrite{ChangeId: id, Mode: mode, Table: tableRef("items"), Columns: columns, KeyColumns: []string{"id"}, Values: values}
}

func TestDataServicesRenderEachWriteMode(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	both := []string{"id", "name"}
	rendered, err := client.RenderRowWrites(c.Context(), &protocol.RenderRowWritesRequest{Engine: "fixture", Writes: []*protocol.RowWrite{
		rowWrite("insert", protocol.RowWrite_INSERT, both, intValue(1), textValue("o'k")),
		rowWrite("upsert", protocol.RowWrite_UPSERT, both, intValue(1), textValue("b")),
		rowWrite("update", protocol.RowWrite_UPDATE_BY_KEY, both, intValue(1), textValue("c")),
		rowWrite("delete", protocol.RowWrite_DELETE_BY_KEY, []string{"id"}, intValue(1)),
	}})
	c.Assert(err, qt.IsNil)
	var statements []string
	for _, unit := range rendered.GetUnits() {
		statements = append(statements, unit.GetSql())
	}
	c.Assert(statements, qt.DeepEquals, []string{
		`INSERT INTO "items" ("id", "name") VALUES (1, 'o''k');`,
		`INSERT INTO "items" ("id", "name") VALUES (1, 'b') ON CONFLICT ("id") DO UPDATE SET "name" = excluded."name";`,
		`UPDATE "items" SET "name" = 'c' WHERE "id" = 1;`,
		`DELETE FROM "items" WHERE "id" = 1;`,
	})
}

func TestDataServicesRefuseABatchWithNoPrefix(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	decimal := &protocol.DataValue{Value: &protocol.DataValue_DecimalValue{DecimalValue: &protocol.DecimalValue{Digits: "150", Scale: 2}}}
	rendered, err := client.RenderRowWrites(c.Context(), &protocol.RenderRowWritesRequest{Engine: "fixture", Writes: []*protocol.RowWrite{
		rowWrite("first", protocol.RowWrite_INSERT, []string{"id", "name"}, intValue(1), textValue("a")),
		rowWrite("second", protocol.RowWrite_INSERT, []string{"id", "name"}, intValue(2), decimal),
	}})
	c.Assert(err, qt.IsNil)
	c.Assert(rendered.GetUnits(), qt.HasLen, 0)
	c.Assert(rendered.GetComplete(), qt.IsFalse)
	c.Assert(rendered.GetRefusal().GetChangeId(), qt.Equals, "second")
	c.Assert(rendered.GetRefusal().GetReason(), qt.Equals, protocol.RenderRefusal_UNSUPPORTED_VALUE)
}

func TestDataServicesRollBackDeclaredRowsWithTheirTransaction(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`)
	rendered, err := client.RenderRowWrites(c.Context(), &protocol.RenderRowWritesRequest{Engine: "fixture", Writes: []*protocol.RowWrite{
		rowWrite("first", protocol.RowWrite_INSERT, []string{"id", "name"}, intValue(1), textValue("a")),
		rowWrite("second", protocol.RowWrite_INSERT, []string{"id", "name"}, intValue(2), textValue("b")),
	}})
	c.Assert(err, qt.IsNil)
	_, err = client.BeginTransaction(c.Context(), &protocol.BeginTransactionRequest{SessionId: session.GetId(), TransactionId: "data", Isolation: protocol.TransactionIsolation_DEFAULT})
	c.Assert(err, qt.IsNil)
	result, err := client.Execute(c.Context(), &protocol.ExecuteRequest{SessionId: session.GetId(), RequestId: "rows", TransactionId: "data", Units: rendered.GetUnits()})
	c.Assert(err, qt.IsNil)
	c.Assert(result.GetAppliedCount(), qt.Equals, uint32(2))
	c.Assert(readAll(c, client, session, "data", "items"), qt.DeepEquals, []string{"a", "b"})
	finished, err := client.FinishTransaction(c.Context(), &protocol.FinishTransactionRequest{SessionId: session.GetId(), TransactionId: "data", Action: protocol.FinishTransactionRequest_ROLLBACK})
	c.Assert(err, qt.IsNil)
	c.Assert(finished.GetOutcome(), qt.Equals, protocol.TransactionResult_ROLLED_BACK)
	c.Assert(readAll(c, client, session, "", "items"), qt.HasLen, 0)
}

func seedFile(text string) *protocol.Seed {
	sum := sha256.Sum256([]byte(text))
	return &protocol.Seed{Path: "dev/1_items.sql", Filename: "1_items.sql", Checksum: hex.EncodeToString(sum[:]), Text: text, Env: "dev"}
}

func seedLedger(c *qt.C, client *transport.Client, session *protocol.Session) *protocol.SeedLedger {
	c.Helper()
	read, err := client.ReadSeedLedger(c.Context(), &protocol.ReadSeedLedgerRequest{SessionId: session.GetId(), MaxEntries: 10})
	c.Assert(err, qt.IsNil)
	return read
}

func TestSeedLedgerEntryExistsOnlyWithItsFile(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`)
	c.Assert(seedLedger(c, client, session).GetExists(), qt.IsFalse)
	failing := seedFile("INSERT INTO \"items\" VALUES (1, 'a');\nINSERT INTO \"absent\" VALUES (2);\n")
	result, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: session.GetId(), RequestId: "failing", Seed: failing})
	c.Assert(err, qt.ErrorAs, new(*provider.SeedError))
	c.Assert(result.GetOutcome(), qt.Equals, protocol.SeedResult_NOT_APPLIED)
	c.Assert(readAll(c, client, session, "", "items"), qt.HasLen, 0)
	c.Assert(seedLedger(c, client, session).GetEntries(), qt.HasLen, 0)
	good := seedFile("INSERT INTO \"items\" VALUES (1, 'a');\n")
	_, err = client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: session.GetId(), RequestId: "good", Seed: good})
	c.Assert(err, qt.IsNil)
	c.Assert(readAll(c, client, session, "", "items"), qt.DeepEquals, []string{"a"})
	entries := seedLedger(c, client, session).GetEntries()
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].GetChecksum(), qt.Equals, good.GetChecksum())
	tables, err := client.ListSeedTables(c.Context(), &protocol.ListSeedTablesRequest{SessionId: session.GetId()})
	c.Assert(err, qt.IsNil)
	c.Assert(tables.GetTables(), qt.HasLen, 1)
	c.Assert(tables.GetTables()[0].GetName(), qt.Equals, "items")
}

func TestSeedKeyConflictLeavesNothingOfTheFile(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`, `INSERT INTO "items" VALUES (1, 'a');`)
	conflicting := seedFile("INSERT INTO \"items\" VALUES (2, 'b');\nINSERT INTO \"items\" VALUES (1, 'x');\n")
	result, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: session.GetId(), RequestId: "conflict", Seed: conflicting})
	c.Assert(err, qt.ErrorAs, new(*provider.SeedError))
	c.Assert(result.GetOutcome(), qt.Equals, protocol.SeedResult_KEY_CONFLICT)
	c.Assert(readAll(c, client, session, "", "items"), qt.DeepEquals, []string{"a"})
	record := &protocol.Seed{Path: conflicting.GetPath(), Filename: conflicting.GetFilename(), Checksum: conflicting.GetChecksum(), Env: conflicting.GetEnv()}
	recorded, err := client.RecordSeed(c.Context(), &protocol.RecordSeedRequest{SessionId: session.GetId(), RequestId: "record", Seed: record})
	c.Assert(err, qt.IsNil)
	c.Assert(recorded.GetOutcome(), qt.Equals, protocol.SeedResult_APPLIED)
	c.Assert(seedLedger(c, client, session).GetEntries()[0].GetChecksum(), qt.Equals, conflicting.GetChecksum())
}

func TestSeedLedgerKeepsOneEntryPerPath(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`)
	first := seedFile("INSERT INTO \"items\" VALUES (1, 'a');\n")
	_, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: session.GetId(), RequestId: "first", Seed: first})
	c.Assert(err, qt.IsNil)
	edited := seedFile("INSERT INTO \"items\" VALUES (2, 'b');\n")
	c.Assert(seedLedger(c, client, session).GetEntries()[0].GetChecksum(), qt.Equals, first.GetChecksum())
	_, err = client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: session.GetId(), RequestId: "edited", Seed: edited})
	c.Assert(err, qt.IsNil)
	entries := seedLedger(c, client, session).GetEntries()
	c.Assert(entries, qt.HasLen, 1)
	c.Assert(entries[0].GetPath(), qt.Equals, edited.GetPath())
	c.Assert(entries[0].GetChecksum(), qt.Equals, edited.GetChecksum())
	c.Assert(entries[0].GetEnv(), qt.Equals, "dev")
	c.Assert(readAll(c, client, session, "", "items"), qt.DeepEquals, []string{"a", "b"})
}

func TestSeedRefusedBeforeItRuns(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`)
	refused := seedFile("INSERT INTO \"items\" VALUES (1, 'a');\nVACUUM;\n")
	result, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: session.GetId(), RequestId: "refused", Seed: refused})
	c.Assert(err, qt.ErrorAs, new(*provider.SeedError))
	c.Assert(result.GetOutcome(), qt.Equals, protocol.SeedResult_REFUSED)
	c.Assert(result.GetPosition().GetLine(), qt.Equals, uint32(2))
	c.Assert(readAll(c, client, session, "", "items"), qt.HasLen, 0)
	c.Assert(seedLedger(c, client, session).GetExists(), qt.IsFalse)
}

func TestSeedLostReplyIsUnknownAndNotSentAgain(t *testing.T) {
	c := qt.New(t)
	client := dataClient(c)
	session := open(c, client, "lost-seed-reply")
	runStatements(c, client, session, "", `CREATE TABLE "items" ("id" INTEGER NOT NULL PRIMARY KEY, "name" TEXT);`, `CREATE TABLE "runs" ("name" TEXT);`)
	lost := seedFile("INSERT INTO \"runs\" VALUES ('once');\n")
	result, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: session.GetId(), RequestId: "lost", Seed: lost})
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(result, qt.IsNil)
	page, err := client.ReadRows(c.Context(), &protocol.ReadRowsRequest{SessionId: session.GetId(), Table: tableRef("runs"),
		Columns: []string{"name"}, KeyColumns: []string{"name"}, MaxRows: 10})
	c.Assert(err, qt.IsNil)
	c.Assert(page.GetRows(), qt.HasLen, 1)
	c.Assert(seedLedger(c, client, session).GetEntries(), qt.HasLen, 1)
}
