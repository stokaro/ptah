package embeddings

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strconv"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// encode is the deterministic encoding a record is stored as.
func encode(message proto.Message) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(message)
}

// pointerID is a pointer's key in stateTable: the schema and the table, each
// quoted, so no two pointers share one.
func pointerID(schema, table string) string {
	return strconv.Quote(schema) + "." + strconv.Quote(table)
}

// sortPointers orders pointers as the SDK lists them, by schema and then
// table, which their quoted keys do not.
func sortPointers(pointers []*protocol.EmbeddingPointer) {
	slices.SortFunc(pointers, func(a, b *protocol.EmbeddingPointer) int {
		return cmp.Or(cmp.Compare(a.GetTargetSchema(), b.GetTargetSchema()), cmp.Compare(a.GetTargetTable(), b.GetTargetTable()))
	})
}

// readRecords reads the records of one kind whose keys match where, in key
// order, decoded into fresh messages.
func readRecords[T proto.Message](ctx context.Context, connection *sql.Conn, kind, where string, fresh func() T, args ...any) ([]T, error) {
	// #nosec G202 -- where is one of this package's literal predicates; values are parameters.
	rows, err := connection.QueryContext(ctx, `SELECT "record" FROM "`+stateTable+`" WHERE "kind" = ?`+where+` ORDER BY "key"`, append([]any{kind}, args...)...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var records []T
	for rows.Next() {
		var encoded []byte
		if err := rows.Scan(&encoded); err != nil {
			return nil, err
		}
		record := fresh()
		if err := proto.Unmarshal(encoded, record); err != nil {
			return nil, fmt.Errorf("decode a stored %s record: %w", kind, err)
		}
		records = append(records, record)
	}
	return records, rows.Err()
}

func newGeneration() *protocol.EmbeddingGeneration { return &protocol.EmbeddingGeneration{} }
func newRun() *protocol.EmbeddingRun               { return &protocol.EmbeddingRun{} }
func newPointer() *protocol.EmbeddingPointer       { return &protocol.EmbeddingPointer{} }

// scopeRecords reads what one scope selects, in the SDK's key order.
// Generations and runs sort by their keys in stateTable; pointers are sorted
// after they are read.
func scopeRecords(ctx context.Context, connection *sql.Conn, scope *protocol.EmbeddingStateScope) (*protocol.EmbeddingRecords, error) {
	records := &protocol.EmbeddingRecords{}
	var err error
	switch selected := scope.GetScope().(type) {
	case *protocol.EmbeddingStateScope_Everything:
		if records.Generations, err = readRecords(ctx, connection, generationKind, "", newGeneration); err != nil {
			return nil, err
		}
		if records.Runs, err = readRecords(ctx, connection, runKind, "", newRun); err != nil {
			return nil, err
		}
		records.Pointers, err = readRecords(ctx, connection, pointerKind, "", newPointer)
		sortPointers(records.Pointers)
	case *protocol.EmbeddingStateScope_Run:
		records.Runs, err = readRecords(ctx, connection, runKind, ` AND "key" = ?`, newRun, selected.Run)
	case *protocol.EmbeddingStateScope_Generation:
		records.Generations, err = readRecords(ctx, connection, generationKind, ` AND "key" = ?`, newGeneration, selected.Generation)
	case *protocol.EmbeddingStateScope_RunsOfGeneration:
		var runs []*protocol.EmbeddingRun
		runs, err = readRecords(ctx, connection, runKind, "", newRun)
		for _, run := range runs {
			if run.GetGenerationIdentity() == selected.RunsOfGeneration {
				records.Runs = append(records.Runs, run)
			}
		}
	case *protocol.EmbeddingStateScope_Pointer:
		key := selected.Pointer
		records.Pointers, err = readRecords(ctx, connection, pointerKind, ` AND "key" = ?`, newPointer, pointerID(key.GetTargetSchema(), key.GetTargetTable()))
	default:
		return nil, fmt.Errorf("an embedding state scope the fixture does not know")
	}
	return records, err
}

// stateToken is the digest of a scope's records, in the order they are
// listed.
func stateToken(records *protocol.EmbeddingRecords) (string, error) {
	messages := make([]proto.Message, 0, len(records.GetGenerations())+len(records.GetRuns())+len(records.GetPointers()))
	for _, generation := range records.GetGenerations() {
		messages = append(messages, generation)
	}
	for _, run := range records.GetRuns() {
		messages = append(messages, run)
	}
	for _, pointer := range records.GetPointers() {
		messages = append(messages, pointer)
	}
	return provider.EmbeddingStateToken(messages...)
}

// ReadEmbeddingState reads one scope in one transaction.
func (p *Provider) ReadEmbeddingState(ctx context.Context, request *protocol.ReadEmbeddingStateRequest) (*protocol.EmbeddingState, error) {
	state := &protocol.EmbeddingState{Scope: request.GetScope(), Records: &protocol.EmbeddingRecords{}}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		exists, err := storeExists(ctx, connection)
		if err != nil || !exists {
			return err
		}
		state.Exists = true
		if state.Records, err = scopeRecords(ctx, connection, request.GetScope()); err != nil {
			return err
		}
		count := len(state.Records.GetGenerations()) + len(state.Records.GetRuns()) + len(state.Records.GetPointers())
		if count > int(request.GetMaxRecords()) {
			return fmt.Errorf("the scope holds %d records, more than the %d asked for", count, request.GetMaxRecords())
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	state.Token, err = stateToken(state.Records)
	return state, err
}

// MutateEmbeddingState writes the records when the scope still has the token
// the host read it with.
func (p *Provider) MutateEmbeddingState(ctx context.Context, request *protocol.MutateEmbeddingStateRequest) (*protocol.EmbeddingWriteResult, error) {
	conflict := false
	err := p.withConnection(ctx, request.GetSessionId(), writing, func(connection *sql.Conn, _ string) error {
		exists, err := storeExists(ctx, connection)
		if err != nil {
			return err
		}
		if !exists {
			return errStoreMissing
		}
		current, err := scopeRecords(ctx, connection, request.GetScope())
		if err != nil {
			return err
		}
		token, err := stateToken(current)
		if err != nil {
			return err
		}
		if token != request.GetExpectedToken() {
			conflict = true
			return errRefused
		}
		return writeRecords(ctx, connection, request.GetRecords())
	})
	if conflict {
		return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_CONFLICT}, nil
	}
	result, err := writeResult(request.GetRequestId(), nil, err)
	if result.GetOutcome() == protocol.EmbeddingWriteResult_APPLIED {
		result.Written = request.GetRecords()
	}
	return result, err
}

// writeRecords stores every record whole under its key.
func writeRecords(ctx context.Context, connection *sql.Conn, records *protocol.EmbeddingRecords) error {
	for _, generation := range records.GetGenerations() {
		if err := writeRecord(ctx, connection, generationKind, generation.GetIdentity(), generation); err != nil {
			return err
		}
	}
	for _, run := range records.GetRuns() {
		if err := writeRecord(ctx, connection, runKind, run.GetId(), run); err != nil {
			return err
		}
	}
	for _, pointer := range records.GetPointers() {
		if err := writeRecord(ctx, connection, pointerKind, pointerID(pointer.GetTargetSchema(), pointer.GetTargetTable()), pointer); err != nil {
			return err
		}
	}
	return nil
}

func writeRecord(ctx context.Context, connection *sql.Conn, kind, key string, record proto.Message) error {
	encoded, err := encode(record)
	if err != nil {
		return err
	}
	_, err = connection.ExecContext(ctx, `INSERT OR REPLACE INTO "`+stateTable+`" ("kind", "key", "record") VALUES (?, ?, ?)`, kind, key, encoded)
	return err
}

// readRun reads one run, or nil when it is not there.
func readRun(ctx context.Context, connection *sql.Conn, id string) (*protocol.EmbeddingRun, error) {
	runs, err := readRecords(ctx, connection, runKind, ` AND "key" = ?`, newRun, id)
	if err != nil || len(runs) == 0 {
		return nil, err
	}
	return runs[0], nil
}

// AppendEmbeddingEvent gives an event of an existing run the run's next
// sequence.
func (p *Provider) AppendEmbeddingEvent(ctx context.Context, request *protocol.AppendEmbeddingEventRequest) (*protocol.EmbeddingWriteResult, error) {
	var sequence int64
	var refusal *protocol.EmbeddingRefusal
	event := proto.CloneOf(request.GetEvent())
	err := p.withConnection(ctx, request.GetSessionId(), writing, func(connection *sql.Conn, _ string) error {
		exists, err := storeExists(ctx, connection)
		if err != nil {
			return err
		}
		if !exists {
			return errStoreMissing
		}
		run, err := readRun(ctx, connection, event.GetRunId())
		if err != nil {
			return err
		}
		if run == nil {
			refusal = refused(protocol.EmbeddingRefusal_ABSENT, "run "+event.GetRunId()+" is not recorded")
			return errRefused
		}
		var last sql.NullInt64
		if err := connection.QueryRowContext(ctx, `SELECT max("sequence") FROM "`+eventTable+`" WHERE "run_id" = ?`, event.GetRunId()).Scan(&last); err != nil {
			return err
		}
		sequence = last.Int64 + 1
		event.Sequence = sequence
		encoded, err := encode(event)
		if err != nil {
			return err
		}
		_, err = connection.ExecContext(ctx, `INSERT INTO "`+eventTable+`" ("run_id", "sequence", "record") VALUES (?, ?, ?)`, event.GetRunId(), sequence, encoded)
		return err
	})
	result, err := writeResult(request.GetRequestId(), refusal, err)
	if result.GetOutcome() == protocol.EmbeddingWriteResult_APPLIED {
		result.Sequence = sequence
	}
	return result, err
}

// ReadEmbeddingEvents pages a run's events in sequence order.
func (p *Provider) ReadEmbeddingEvents(ctx context.Context, request *protocol.ReadEmbeddingEventsRequest) (*protocol.EmbeddingEventPage, error) {
	page := &protocol.EmbeddingEventPage{RunId: request.GetRunId(), Complete: true}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		exists, err := storeExists(ctx, connection)
		if err != nil || !exists {
			return err
		}
		run, err := readRun(ctx, connection, request.GetRunId())
		if err != nil || run == nil {
			return err
		}
		page.Exists = true
		rows, err := connection.QueryContext(ctx, `SELECT "record" FROM "`+eventTable+`" WHERE "run_id" = ? AND "sequence" > ? ORDER BY "sequence" LIMIT ?`,
			request.GetRunId(), request.GetAfterSequence(), int64(request.GetMaxEvents())+1)
		if err != nil {
			return err
		}
		defer func() { _ = rows.Close() }()
		for rows.Next() {
			var encoded []byte
			if err := rows.Scan(&encoded); err != nil {
				return err
			}
			event := &protocol.EmbeddingEvent{}
			if err := proto.Unmarshal(encoded, event); err != nil {
				return err
			}
			page.Events = append(page.Events, event)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		if len(page.Events) > int(request.GetMaxEvents()) {
			page.Events, page.Complete = page.Events[:request.GetMaxEvents()], false
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return page, nil
}
