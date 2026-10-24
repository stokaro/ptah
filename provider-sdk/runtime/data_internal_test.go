package runtime

// White-box testing required: the loopback sends each client call through the
// server adapter without a process, and malformed peers must reach the client
// without passing the SDK server's own checks.

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func dataDescription() *protocol.Description {
	features := []string{provider.TypeRowsFeature, provider.ReadRowsFeature, provider.RenderRowWritesFeature, provider.SeedsFeature,
		provider.ExecuteFeature, provider.TransactionsFeature}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/data", Version: "1", SemanticIdentity: "data-1", Features: features,
		Engines: []*protocol.Engine{{Id: "rows", Features: slices.Clone(features), Identifiers: &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT,
			ColumnNames: protocol.IdentifierRules_EXACT, IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}}},
	}
}

func ratesTable() *protocol.Reference {
	return &protocol.Reference{Engine: "rows", Schema: "app", Kind: "table", Name: "rates"}
}

func stringValue(value string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: value}}
}

func decimalValue(digits string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_DecimalValue{DecimalValue: &protocol.DecimalValue{Digits: digits, Precision: 10, Scale: 2}}}
}

func rateColumns() []*protocol.DataColumn {
	return []*protocol.DataColumn{
		{Name: "code", Type: "Utf8", Kind: protocol.DataKind_DATA_KIND_STRING},
		{Name: "rate", Type: "Decimal(10,2)", Kind: protocol.DataKind_DATA_KIND_DECIMAL},
	}
}

// memoryData is an owner over one in-memory table and one ledger. It pages
// by key, renders a fixed spelling of each mode, and keeps a seed's entry only
// with the file it ran.
type memoryData struct {
	exists   bool
	rows     [][2]string // code, rate digits, in key order
	noUpsert bool
	ledger   map[string]*protocol.SeedLedgerEntry
	applied  []string
}

func newMemoryData() *memoryData {
	return &memoryData{exists: true, ledger: make(map[string]*protocol.SeedLedgerEntry),
		rows: [][2]string{{"AT", "100"}, {"CZ", "150"}, {"DE", "200"}, {"EE", "250"}, {"FI", "300"}}}
}

func (m *memoryData) ReadRows(_ context.Context, request *protocol.ReadRowsRequest) (*protocol.RowPage, error) {
	page := &protocol.RowPage{Table: request.Table, Exists: m.exists}
	if !m.exists {
		page.Complete = true
		return page, nil
	}
	page.Columns = rateColumns()
	after, _ := strings.CutPrefix(request.PageToken, "after:")
	for _, row := range m.rows {
		if row[0] <= after {
			continue
		}
		if len(page.Rows) == int(request.MaxRows) {
			page.NextPageToken = "after:" + page.Rows[len(page.Rows)-1].Values[0].GetStringValue()
			return page, nil
		}
		page.Rows = append(page.Rows, &protocol.Row{Values: []*protocol.DataValue{stringValue(row[0]), decimalValue(row[1])}})
	}
	page.Complete = true
	return page, nil
}

// TypeRows types a Decimal(10,2) column: a string "1.50" and a decimal with
// scale 1 both become the unscaled digits at scale 2.
func (m *memoryData) TypeRows(_ context.Context, request *protocol.TypeRowsRequest) (*protocol.TypedRows, error) {
	result := &protocol.TypedRows{}
	for _, column := range request.Columns {
		typed := &protocol.DataColumn{Name: column.Name}
		switch column.DeclaredType {
		case "DECIMAL(10,2)":
			typed.Type, typed.Kind = "Decimal(10,2)", protocol.DataKind_DATA_KIND_DECIMAL
		case "VARCHAR(8)":
			typed.Type, typed.Kind = "Utf8", protocol.DataKind_DATA_KIND_STRING
		}
		result.Columns = append(result.Columns, typed)
	}
	canonical := func(rows []*protocol.Row) []*protocol.Row {
		var out []*protocol.Row
		for _, row := range rows {
			copied := proto.CloneOf(row)
			for i, value := range copied.Values {
				if result.Columns[i].Kind != protocol.DataKind_DATA_KIND_DECIMAL {
					continue
				}
				switch v := value.Value.(type) {
				case *protocol.DataValue_StringValue:
					copied.Values[i] = decimalValue(strings.ReplaceAll(v.StringValue, ".", ""))
				case *protocol.DataValue_DecimalValue:
					copied.Values[i] = decimalValue(v.DecimalValue.Digits + strings.Repeat("0", int(2-v.DecimalValue.Scale)))
				}
			}
			out = append(out, copied)
		}
		return out
	}
	result.Declared, result.Live = canonical(request.Declared), canonical(request.Live)
	return result, nil
}

func literal(value *protocol.DataValue) string {
	switch v := value.Value.(type) {
	case *protocol.DataValue_StringValue:
		return "'" + v.StringValue + "'"
	case *protocol.DataValue_DecimalValue:
		digits := v.DecimalValue.Digits
		return digits[:len(digits)-int(v.DecimalValue.Scale)] + "." + digits[len(digits)-int(v.DecimalValue.Scale):]
	default:
		return "NULL"
	}
}

func (m *memoryData) RenderRowWrites(_ context.Context, request *protocol.RenderRowWritesRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, write := range request.Writes {
		values := make([]string, len(write.Values))
		for i, value := range write.Values {
			values[i] = literal(value)
		}
		var sql string
		switch write.Mode {
		case protocol.RowWrite_INSERT:
			sql = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", write.Table.Name, strings.Join(write.Columns, ", "), strings.Join(values, ", "))
		case protocol.RowWrite_UPSERT:
			if m.noUpsert {
				return &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: write.ChangeId,
					Reason: protocol.RenderRefusal_UNSUPPORTED_MODE, Message: "the engine has no upsert"}}, nil
			}
			sql = fmt.Sprintf("UPSERT INTO %s (%s) VALUES (%s)", write.Table.Name, strings.Join(write.Columns, ", "), strings.Join(values, ", "))
		case protocol.RowWrite_UPDATE_BY_KEY:
			sql = fmt.Sprintf("UPDATE %s SET %s = %s WHERE %s = %s", write.Table.Name, write.Columns[1], values[1], write.Columns[0], values[0])
		case protocol.RowWrite_DELETE_BY_KEY:
			sql = fmt.Sprintf("DELETE FROM %s WHERE %s = %s", write.Table.Name, write.Columns[0], values[0])
		}
		result.Units = append(result.Units, &protocol.Unit{ChangeId: write.ChangeId, Body: &protocol.Unit_Sql{Sql: sql}})
	}
	return result, nil
}

func (m *memoryData) ListSeedTables(_ context.Context, request *protocol.ListSeedTablesRequest) (*protocol.SeedTables, error) {
	table := ratesTable()
	table.Schema = request.Schema
	return &protocol.SeedTables{Tables: []*protocol.Reference{table}}, nil
}

// ApplySeed refuses a statement it runs only outside a transaction, reports a
// duplicate key with nothing kept, and otherwise records the file.
func (m *memoryData) ApplySeed(_ context.Context, request *protocol.ApplySeedRequest) (*protocol.SeedResult, error) {
	result := &protocol.SeedResult{RequestId: request.RequestId, Path: request.Seed.Path}
	text := request.Seed.Text
	if at := strings.Index(text, "BATCH UPDATE"); at >= 0 {
		result.Outcome, result.Reason = protocol.SeedResult_REFUSED, "BATCH UPDATE runs only outside a transaction"
		line := uint32(strings.Count(text[:at], "\n")) + 1 // #nosec G115 -- a test file has a handful of lines
		result.Position = &protocol.SourcePosition{ByteOffset: uint64(at), Line: line, Column: 1}
		return result, nil
	}
	if strings.Contains(text, "DUPLICATE") {
		result.Outcome = protocol.SeedResult_KEY_CONFLICT
		return result, nil
	}
	m.applied = append(m.applied, request.Seed.Path)
	m.record(request.Seed)
	result.Outcome = protocol.SeedResult_APPLIED
	return result, nil
}

func (m *memoryData) record(seed *protocol.Seed) {
	m.ledger[seed.Path] = &protocol.SeedLedgerEntry{Path: seed.Path, Checksum: seed.Checksum, Env: seed.Env, AppliedAtSeconds: int64(len(m.ledger))}
}

func (m *memoryData) RecordSeed(_ context.Context, request *protocol.RecordSeedRequest) (*protocol.SeedResult, error) {
	m.record(request.Seed)
	return &protocol.SeedResult{RequestId: request.RequestId, Path: request.Seed.Path, Outcome: protocol.SeedResult_APPLIED}, nil
}

func (m *memoryData) ReadSeedLedger(context.Context, *protocol.ReadSeedLedgerRequest) (*protocol.SeedLedger, error) {
	ledger := &protocol.SeedLedger{Exists: len(m.ledger) > 0, Complete: true}
	for _, path := range slices.Sorted(maps.Keys(m.ledger)) {
		ledger.Entries = append(ledger.Entries, proto.CloneOf(m.ledger[path]))
	}
	return ledger, nil
}

// dataLoopback sends a client call through the server adapter, as a process
// would, without the process.
type dataLoopback struct {
	protocol.ProviderClient
	server *serverAdapter
	calls  int
}

func (l *dataLoopback) TypeRows(ctx context.Context, request *protocol.TypeRowsRequest, _ ...grpc.CallOption) (*protocol.TypedRows, error) {
	l.calls++
	return l.server.TypeRows(ctx, request)
}

func (l *dataLoopback) ReadRows(ctx context.Context, request *protocol.ReadRowsRequest, _ ...grpc.CallOption) (*protocol.RowPage, error) {
	l.calls++
	return l.server.ReadRows(ctx, request)
}

func (l *dataLoopback) RenderRowWrites(ctx context.Context, request *protocol.RenderRowWritesRequest, _ ...grpc.CallOption) (*protocol.RenderResult, error) {
	l.calls++
	return l.server.RenderRowWrites(ctx, request)
}

func (l *dataLoopback) ListSeedTables(ctx context.Context, request *protocol.ListSeedTablesRequest, _ ...grpc.CallOption) (*protocol.SeedTables, error) {
	l.calls++
	return l.server.ListSeedTables(ctx, request)
}

func (l *dataLoopback) ApplySeed(ctx context.Context, request *protocol.ApplySeedRequest, _ ...grpc.CallOption) (*protocol.SeedResult, error) {
	l.calls++
	return l.server.ApplySeed(ctx, request)
}

func (l *dataLoopback) RecordSeed(ctx context.Context, request *protocol.RecordSeedRequest, _ ...grpc.CallOption) (*protocol.SeedResult, error) {
	l.calls++
	return l.server.RecordSeed(ctx, request)
}

func (l *dataLoopback) ReadSeedLedger(ctx context.Context, request *protocol.ReadSeedLedgerRequest, _ ...grpc.CallOption) (*protocol.SeedLedger, error) {
	l.calls++
	return l.server.ReadSeedLedger(ctx, request)
}

func newDataLoopback(owner *memoryData) (*Client, *dataLoopback) {
	description := dataDescription()
	server := Server{Description: description, RowTyper: owner, RowReader: owner, RowWriter: owner, Seeds: owner, Transactions: &idleTransactions{}}
	peer := &dataLoopback{server: &serverAdapter{server: server}}
	return &Client{description: description, rpc: peer}, peer
}

// idleTransactions stands in for the transaction service the server needs
// before it accepts a read inside a transaction.
type idleTransactions struct{}

func (*idleTransactions) BeginTransaction(context.Context, *protocol.BeginTransactionRequest) (*protocol.Transaction, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func (*idleTransactions) FinishTransaction(context.Context, *protocol.FinishTransactionRequest) (*protocol.TransactionResult, error) {
	return nil, status.Error(codes.Unimplemented, "not used")
}

func rowsRequest(token string) *protocol.ReadRowsRequest {
	return &protocol.ReadRowsRequest{SessionId: "session", Table: ratesTable(), Columns: []string{"code", "rate"}, KeyColumns: []string{"code"},
		PageToken: token, MaxRows: 2}
}

func pageKeys(page *protocol.RowPage) []string {
	keys := make([]string, 0, len(page.GetRows()))
	for _, row := range page.GetRows() {
		keys = append(keys, row.GetValues()[0].GetStringValue())
	}
	return keys
}

// Pages come in key order within their bound, and a page token resumes the
// read where the page it came from stopped.
func TestRowsConformancePagesAndResumes(t *testing.T) {
	c := qt.New(t)
	client, peer := newDataLoopback(newMemoryData())
	first, err := client.ReadRows(c.Context(), rowsRequest(""))
	c.Assert(err, qt.IsNil)
	c.Assert(pageKeys(first), qt.DeepEquals, []string{"AT", "CZ"})
	c.Assert(first.Complete, qt.IsFalse)
	second, err := client.ReadRows(c.Context(), rowsRequest(first.NextPageToken))
	c.Assert(err, qt.IsNil)
	c.Assert(pageKeys(second), qt.DeepEquals, []string{"DE", "EE"})
	last, err := client.ReadRows(c.Context(), rowsRequest(second.NextPageToken))
	c.Assert(err, qt.IsNil)
	c.Assert(pageKeys(last), qt.DeepEquals, []string{"FI"})
	c.Assert(last.Complete, qt.IsTrue)
	c.Assert(last.NextPageToken, qt.Equals, "")
	resumed, err := client.ReadRows(c.Context(), rowsRequest(first.NextPageToken))
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(resumed, second), qt.IsTrue)
	c.Assert(peer.calls, qt.Equals, 4)
}

// An empty table and a missing table are both complete pages without rows,
// and only the existence bit tells them apart.
func TestRowsConformanceSeparatesAnEmptyTableFromAMissingOne(t *testing.T) {
	c := qt.New(t)
	empty := newMemoryData()
	empty.rows = nil
	client, _ := newDataLoopback(empty)
	page, err := client.ReadRows(c.Context(), rowsRequest(""))
	c.Assert(err, qt.IsNil)
	c.Assert(page.Exists, qt.IsTrue)
	c.Assert(page.Complete, qt.IsTrue)
	c.Assert(page.Rows, qt.HasLen, 0)
	c.Assert(page.Columns, qt.HasLen, 2)
	missing := newMemoryData()
	missing.exists = false
	client, _ = newDataLoopback(missing)
	page, err = client.ReadRows(c.Context(), rowsRequest(""))
	c.Assert(err, qt.IsNil)
	c.Assert(page.Exists, qt.IsFalse)
	c.Assert(page.Complete, qt.IsTrue)
	c.Assert(page.Rows, qt.HasLen, 0)
	c.Assert(page.Columns, qt.HasLen, 0)
}

// A value read back in another spelling pairs with its declaration once both
// sets are typed: 1.50 declared, and 1.5 read back at scale 1.
func TestRowsConformanceTypesBothSetsInOneForm(t *testing.T) {
	c := qt.New(t)
	client, _ := newDataLoopback(newMemoryData())
	short := &protocol.DataValue{Value: &protocol.DataValue_DecimalValue{DecimalValue: &protocol.DecimalValue{Digits: "15", Precision: 10, Scale: 1}}}
	typed, err := client.TypeRows(c.Context(), &protocol.TypeRowsRequest{
		Engine: "rows", Table: ratesTable(),
		Columns:  []*protocol.RowColumn{{Name: "code", DeclaredType: "VARCHAR(8)"}, {Name: "rate", DeclaredType: "DECIMAL(10,2)"}},
		Declared: []*protocol.Row{{Values: []*protocol.DataValue{stringValue("CZ"), stringValue("1.50")}}},
		Live:     []*protocol.Row{{Values: []*protocol.DataValue{stringValue("CZ"), short}}},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(typed.Declared[0], typed.Live[0]), qt.IsTrue)
	c.Assert(typed.Columns[1].Type, qt.Equals, "Decimal(10,2)")
}

func rateWrites() *protocol.RenderRowWritesRequest {
	key := stringValue("CZ")
	return &protocol.RenderRowWritesRequest{Engine: "rows", Writes: []*protocol.RowWrite{
		{ChangeId: "1", Mode: protocol.RowWrite_INSERT, Table: ratesTable(), Columns: []string{"code", "rate"}, Values: []*protocol.DataValue{key, decimalValue("150")}},
		{ChangeId: "2", Mode: protocol.RowWrite_UPSERT, Table: ratesTable(), Columns: []string{"code", "rate"}, KeyColumns: []string{"code"},
			Values: []*protocol.DataValue{key, decimalValue("175")}},
		{ChangeId: "3", Mode: protocol.RowWrite_UPDATE_BY_KEY, Table: ratesTable(), Columns: []string{"code", "rate"}, KeyColumns: []string{"code"},
			Values: []*protocol.DataValue{key, decimalValue("200")}},
		{ChangeId: "4", Mode: protocol.RowWrite_DELETE_BY_KEY, Table: ratesTable(), Columns: []string{"code"}, KeyColumns: []string{"code"},
			Values: []*protocol.DataValue{key}},
	}}
}

// Each mode renders to one unit in input order, the same bytes every time.
func TestRowWritesConformanceRendersEachMode(t *testing.T) {
	c := qt.New(t)
	client, _ := newDataLoopback(newMemoryData())
	first, err := client.RenderRowWrites(c.Context(), rateWrites())
	c.Assert(err, qt.IsNil)
	var sql []string
	for _, unit := range first.Units {
		sql = append(sql, unit.ChangeId+": "+unit.GetSql())
	}
	c.Assert(sql, qt.DeepEquals, []string{
		"1: INSERT INTO rates (code, rate) VALUES ('CZ', 1.50)",
		"2: UPSERT INTO rates (code, rate) VALUES ('CZ', 1.75)",
		"3: UPDATE rates SET rate = 2.00 WHERE code = 'CZ'",
		"4: DELETE FROM rates WHERE code = 'CZ'",
	})
	again, err := client.RenderRowWrites(c.Context(), rateWrites())
	c.Assert(err, qt.IsNil)
	c.Assert(proto.Equal(first, again), qt.IsTrue)
}

// An engine with no upsert refuses the mode with a typed reason and exposes no
// unit, not even for the writes before it.
func TestRowWritesConformanceRefusalExposesNoPrefix(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryData()
	owner.noUpsert = true
	client, _ := newDataLoopback(owner)
	result, err := client.RenderRowWrites(c.Context(), rateWrites())
	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsFalse)
	c.Assert(result.Units, qt.HasLen, 0)
	c.Assert(result.Refusal.ChangeId, qt.Equals, "2")
	c.Assert(result.Refusal.Reason, qt.Equals, protocol.RenderRefusal_UNSUPPORTED_MODE)
	leaky, _ := newDataLoopback(owner)
	leaky.rpc.(*dataLoopback).server.server.RowWriter = leakingWriter{}
	got, err := leaky.RenderRowWrites(c.Context(), rateWrites())
	c.Assert(err, qt.ErrorMatches, `provider render row writes failed \(Internal\)`)
	c.Assert(got, qt.IsNil)
}

// leakingWriter refuses a write and still returns the units before it.
type leakingWriter struct{}

func (leakingWriter) RenderRowWrites(_ context.Context, request *protocol.RenderRowWritesRequest) (*protocol.RenderResult, error) {
	return &protocol.RenderResult{Units: []*protocol.Unit{{ChangeId: request.Writes[0].ChangeId, Body: &protocol.Unit_Sql{Sql: "INSERT"}}},
		Refusal: &protocol.RenderRefusal{ChangeId: request.Writes[1].ChangeId, Reason: protocol.RenderRefusal_UNSUPPORTED_MODE, Message: "no upsert"}}, nil
}

const ratesChecksum = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

func seedFile(path, text, checksum string) *protocol.Seed {
	return &protocol.Seed{Path: path, Filename: path, Checksum: checksum, Text: text, Env: "dev"}
}

// The ledger holds an entry exactly when the file applied, records a file a
// key conflict stopped only when the caller records it, and keeps the
// checksum a changed file is detected by.
func TestSeedsConformanceLedgerFollowsTheFile(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryData()
	client, _ := newDataLoopback(owner)
	applied, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: "s", RequestId: "1", Seed: seedFile("rates.sql", "INSERT x;", ratesChecksum)})
	c.Assert(err, qt.IsNil)
	c.Assert(applied.Outcome, qt.Equals, protocol.SeedResult_APPLIED)
	conflict, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: "s", RequestId: "2", Seed: seedFile("users.sql", "DUPLICATE;", ratesChecksum)})
	var failed *provider.SeedError
	c.Assert(err, qt.ErrorAs, &failed)
	c.Assert(failed.Outcome, qt.Equals, protocol.SeedResult_KEY_CONFLICT)
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(conflict.Outcome, qt.Equals, protocol.SeedResult_KEY_CONFLICT)
	ledger, err := client.ReadSeedLedger(c.Context(), &protocol.ReadSeedLedgerRequest{SessionId: "s", MaxEntries: 10})
	c.Assert(err, qt.IsNil)
	c.Assert(ledger.Entries, qt.HasLen, 1)
	c.Assert(ledger.Entries[0].Path, qt.Equals, "rates.sql")
	recorded := seedFile("users.sql", "", ratesChecksum)
	_, err = client.RecordSeed(c.Context(), &protocol.RecordSeedRequest{SessionId: "s", RequestId: "3", Seed: recorded})
	c.Assert(err, qt.IsNil)
	changed := strings.Repeat("0", 64)
	_, err = client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: "s", RequestId: "4", Seed: seedFile("rates.sql", "INSERT y;", changed)})
	c.Assert(err, qt.IsNil)
	ledger, err = client.ReadSeedLedger(c.Context(), &protocol.ReadSeedLedgerRequest{SessionId: "s", MaxEntries: 10})
	c.Assert(err, qt.IsNil)
	c.Assert(ledger.Entries, qt.HasLen, 2)
	c.Assert(ledger.Entries[0].Checksum, qt.Equals, changed)
	c.Assert(ledger.Entries[1].Path, qt.Equals, "users.sql")
	c.Assert(owner.applied, qt.DeepEquals, []string{"rates.sql", "rates.sql"})
}

// A file holding a statement the engine runs only outside a transaction is
// refused before it runs, with the position of that statement.
func TestSeedsConformanceRefusesBeforeRunning(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryData()
	client, _ := newDataLoopback(owner)
	text := "INSERT x;\nBATCH UPDATE rates SET rate = 1;\n"
	result, err := client.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: "s", RequestId: "1", Seed: seedFile("rates.sql", text, ratesChecksum)})
	c.Assert(err, qt.ErrorIs, provider.ErrExecution)
	c.Assert(result.Outcome, qt.Equals, protocol.SeedResult_REFUSED)
	c.Assert(proto.Equal(result.Position, &protocol.SourcePosition{ByteOffset: 10, Line: 2, Column: 1}), qt.IsTrue)
	c.Assert(owner.applied, qt.HasLen, 0)
	c.Assert(owner.ledger, qt.HasLen, 0)
}

func TestSeedTablesConformanceListsTheSchema(t *testing.T) {
	c := qt.New(t)
	client, _ := newDataLoopback(newMemoryData())
	tables, err := client.ListSeedTables(c.Context(), &protocol.ListSeedTablesRequest{SessionId: "s", Schema: "app"})
	c.Assert(err, qt.IsNil)
	c.Assert(tables.Tables, qt.HasLen, 1)
	c.Assert(proto.Equal(tables.Tables[0], ratesTable()), qt.IsTrue)
}

// dataPeer answers with fixed replies and counts the calls that reached it.
type dataPeer struct {
	protocol.ProviderClient
	page   *protocol.RowPage
	typed  *protocol.TypedRows
	seed   *protocol.SeedResult
	ledger *protocol.SeedLedger
	err    error
	calls  int
}

func (p *dataPeer) ReadRows(context.Context, *protocol.ReadRowsRequest, ...grpc.CallOption) (*protocol.RowPage, error) {
	p.calls++
	return p.page, p.err
}

func (p *dataPeer) TypeRows(context.Context, *protocol.TypeRowsRequest, ...grpc.CallOption) (*protocol.TypedRows, error) {
	p.calls++
	return p.typed, p.err
}

func (p *dataPeer) ApplySeed(context.Context, *protocol.ApplySeedRequest, ...grpc.CallOption) (*protocol.SeedResult, error) {
	p.calls++
	return p.seed, p.err
}

func (p *dataPeer) RecordSeed(context.Context, *protocol.RecordSeedRequest, ...grpc.CallOption) (*protocol.SeedResult, error) {
	p.calls++
	return p.seed, p.err
}

func (p *dataPeer) ReadSeedLedger(context.Context, *protocol.ReadSeedLedgerRequest, ...grpc.CallOption) (*protocol.SeedLedger, error) {
	p.calls++
	return p.ledger, p.err
}

// A lost seed reply is an unknown outcome. The client sends the file once and
// never again, and exposes no result; it does not say the file did not run.
func TestSeedClientReportsALostReplyAsUnknownWithoutRetry(t *testing.T) {
	c := qt.New(t)
	peer := &dataPeer{err: status.Error(codes.Unavailable, "password=secret")}
	client := &Client{description: dataDescription(), rpc: peer}
	request := &protocol.ApplySeedRequest{SessionId: "s", RequestId: "1", Seed: seedFile("rates.sql", "INSERT x;", ratesChecksum)}
	result, err := client.ApplySeed(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.ErrorMatches, `(?s)provider seed outcome is UNKNOWN.*provider apply seed failed \(Unavailable\)`)
	c.Assert(result, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 1)
	peer.err, peer.seed = nil, &protocol.SeedResult{RequestId: "other", Path: "rates.sql", Outcome: protocol.SeedResult_APPLIED}
	result, err = client.ApplySeed(c.Context(), request)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(result, qt.IsNil)
	recorded := &protocol.RecordSeedRequest{SessionId: "s", RequestId: "2", Seed: seedFile("rates.sql", "", ratesChecksum)}
	_, err = client.RecordSeed(c.Context(), recorded)
	c.Assert(err, qt.ErrorIs, provider.ErrOutcomeUnknown)
	c.Assert(peer.calls, qt.Equals, 3)
}

// A client checks a reply even from a peer that is not the SDK server: a page
// of another table, a typed value of the wrong kind and an incomplete ledger
// expose nothing.
func TestDataClientDiscardsUntrustedReplies(t *testing.T) {
	c := qt.New(t)
	other := ratesTable()
	other.Name = "users"
	peer := &dataPeer{
		page:   &protocol.RowPage{Table: other, Exists: true, Complete: true, Columns: rateColumns()},
		typed:  &protocol.TypedRows{Columns: []*protocol.DataColumn{{Name: "rate", Type: "Decimal(10,2)", Kind: protocol.DataKind_DATA_KIND_DECIMAL}}, Declared: []*protocol.Row{{Values: []*protocol.DataValue{stringValue("1.50")}}}},
		ledger: &protocol.SeedLedger{Exists: true},
	}
	client := &Client{description: dataDescription(), rpc: peer}
	page, err := client.ReadRows(c.Context(), rowsRequest(""))
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(page, qt.IsNil)
	typed, err := client.TypeRows(c.Context(), &protocol.TypeRowsRequest{Engine: "rows", Table: ratesTable(),
		Columns: []*protocol.RowColumn{{Name: "rate"}}, Declared: []*protocol.Row{{Values: []*protocol.DataValue{stringValue("1.50")}}}})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(typed, qt.IsNil)
	ledger, err := client.ReadSeedLedger(c.Context(), &protocol.ReadSeedLedgerRequest{SessionId: "s", MaxEntries: 1})
	c.Assert(err, qt.ErrorIs, provider.ErrIncomplete)
	c.Assert(ledger, qt.IsNil)
	c.Assert(peer.calls, qt.Equals, 3)
}

// Nothing is sent for a canceled call, a request that fails its validator, a
// service the provider did not negotiate, or a read in a transaction without
// the transaction service.
func TestDataClientRefusesBeforeDispatch(t *testing.T) {
	c := qt.New(t)
	peer := &dataPeer{}
	client := &Client{description: dataDescription(), rpc: peer}
	canceled, cancel := context.WithCancel(c.Context())
	cancel()
	_, err := client.ReadRows(canceled, rowsRequest(""))
	c.Assert(err, qt.ErrorIs, context.Canceled)
	invalid := rowsRequest("")
	invalid.KeyColumns = nil
	_, err = client.ReadRows(c.Context(), invalid)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	client.description.Features = []string{provider.ReadRowsFeature}
	inTransaction := rowsRequest("")
	inTransaction.TransactionId = "tx"
	_, err = client.ReadRows(c.Context(), inTransaction)
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	_, err = client.ReadSeedLedger(c.Context(), &protocol.ReadSeedLedgerRequest{SessionId: "s", MaxEntries: 1})
	c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
	c.Assert(peer.calls, qt.Equals, 0)
}

// brokenData answers with replies its own validators refuse.
type brokenData struct{ *memoryData }

func (brokenData) ReadRows(_ context.Context, request *protocol.ReadRowsRequest) (*protocol.RowPage, error) {
	return &protocol.RowPage{Table: request.Table, Exists: true, Complete: true, NextPageToken: "more"}, nil
}

func (brokenData) ApplySeed(_ context.Context, request *protocol.ApplySeedRequest) (*protocol.SeedResult, error) {
	return &protocol.SeedResult{RequestId: request.RequestId, Path: "another.sql", Outcome: protocol.SeedResult_APPLIED}, nil
}

func TestDataServerRefusesInvalidOwnerReplies(t *testing.T) {
	c := qt.New(t)
	owner := brokenData{newMemoryData()}
	server := &serverAdapter{server: Server{Description: dataDescription(), RowReader: owner, Seeds: owner}}
	page, err := server.ReadRows(c.Context(), rowsRequest(""))
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(page, qt.IsNil)
	result, err := server.ApplySeed(c.Context(), &protocol.ApplySeedRequest{SessionId: "s", RequestId: "1", Seed: seedFile("rates.sql", "x", ratesChecksum)})
	c.Assert(status.Code(err), qt.Equals, codes.Internal)
	c.Assert(result, qt.IsNil)
	_, err = server.ReadRows(c.Context(), &protocol.ReadRowsRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.InvalidArgument)
	inTransaction := rowsRequest("")
	inTransaction.TransactionId = "tx"
	_, err = server.ReadRows(c.Context(), inTransaction)
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}

func TestDataServerIsUnimplementedWithoutAnOwner(t *testing.T) {
	c := qt.New(t)
	server := &serverAdapter{server: Server{Description: dataDescription()}}
	_, err := server.TypeRows(c.Context(), &protocol.TypeRowsRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.ReadRows(c.Context(), &protocol.ReadRowsRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.RenderRowWrites(c.Context(), &protocol.RenderRowWritesRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.ListSeedTables(c.Context(), &protocol.ListSeedTablesRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.ApplySeed(c.Context(), &protocol.ApplySeedRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.RecordSeed(c.Context(), &protocol.RecordSeedRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
	_, err = server.ReadSeedLedger(c.Context(), &protocol.ReadSeedLedgerRequest{})
	c.Assert(status.Code(err), qt.Equals, codes.Unimplemented)
}

// Each data service is advertised exactly when it is implemented.
func TestDataServicesMustMatchTheirAdvertisement(t *testing.T) {
	c := qt.New(t)
	owner := newMemoryData()
	server := Server{Description: dataDescription(), Sessions: &sessionOwner{}, Executor: executorOwner{}, Transactions: &idleTransactions{},
		RowTyper: owner, RowReader: owner, RowWriter: owner, Seeds: owner}
	c.Assert(server.Validate(), qt.IsNil)
	for _, remove := range []func(*Server){
		func(s *Server) { s.RowTyper = nil },
		func(s *Server) { s.RowReader = nil },
		func(s *Server) { s.RowWriter = nil },
		func(s *Server) { s.Seeds = (*memoryData)(nil) },
	} {
		missing := server
		remove(&missing)
		c.Assert(missing.Validate(), qt.ErrorIs, provider.ErrProtocol)
	}
}

type executorOwner struct{}

func (executorOwner) Execute(context.Context, *protocol.ExecuteRequest) (*protocol.Execution, error) {
	return &protocol.Execution{Outcome: protocol.Execution_APPLIED}, nil
}
