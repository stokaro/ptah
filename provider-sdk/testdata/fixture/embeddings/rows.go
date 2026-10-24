package embeddings

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

// The text the fixture stores for a write's kind.
var kindNames = map[protocol.EmbeddingWriteKind]string{
	protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UPSERT:    "upsert",
	protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_SKIP:      "skip",
	protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE: "tombstone",
}

// kindOf reads a stored kind back. Text the fixture did not write is no
// kind, which verification reports.
func kindOf(name string) protocol.EmbeddingWriteKind {
	for kind, stored := range kindNames {
		if stored == name {
			return kind
		}
	}
	return protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_UNSPECIFIED
}

// asText renders a column read as text, which is how a key, an input field
// and a version travel.
func asText(column string) string { return "CAST(" + quote(column) + " AS TEXT)" }

// keyMatch is the predicate that addresses one key, one parameter per key
// column. SQLite applies the column's affinity to the text it is compared
// with, so a key travels as text and still meets an INTEGER column.
func keyMatch(fields []string) string {
	conditions := make([]string, len(fields))
	for i, field := range fields {
		conditions[i] = quote(field) + " = ?"
	}
	return strings.Join(conditions, " AND ")
}

// keyArguments are a key's components as parameters.
func keyArguments(key *protocol.EmbeddingKey) []any {
	arguments := make([]any, len(key.GetComponents()))
	for i, component := range key.GetComponents() {
		arguments[i] = component
	}
	return arguments
}

// keyOrder compares keys as SQLite orders the columns they come from: a
// column of numeric affinity numerically and every other column by its text.
func keyOrder(columns []columnInfo) func(a, b []string) int {
	numeric := make([]bool, len(columns))
	for i, column := range columns {
		kind := strings.ToUpper(column.kind)
		numeric[i] = strings.Contains(kind, "INT") || strings.Contains(kind, "REAL") || strings.Contains(kind, "FLOA") ||
			strings.Contains(kind, "DOUB") || strings.Contains(kind, "NUM") || strings.Contains(kind, "DEC")
	}
	return func(a, b []string) int {
		for i := range min(len(a), len(b), len(numeric)) {
			if numeric[i] {
				x, xErr := strconv.ParseFloat(a[i], 64)
				y, yErr := strconv.ParseFloat(b[i], 64)
				if xErr == nil && yErr == nil {
					if order := cmp.Compare(x, y); order != 0 {
						return order
					}
					continue
				}
			}
			if order := cmp.Compare(a[i], b[i]); order != 0 {
				return order
			}
		}
		return cmp.Compare(len(a), len(b))
	}
}

// sourceKey describes the source and returns its key columns.
func sourceKey(ctx context.Context, connection *sql.Conn, source *protocol.EmbeddingSource) (string, []columnInfo, error) {
	table, err := tableName(source.GetTable())
	if err != nil {
		return "", nil, err
	}
	columns, exists, err := describeTable(ctx, connection, table)
	if err != nil {
		return "", nil, err
	}
	if !exists {
		return "", nil, fmt.Errorf("source table %s does not exist", table)
	}
	key, refusal := keyColumns(columns, source.GetKeyFields(), table)
	if refusal != nil {
		return "", nil, fmt.Errorf("%s", refusal.GetMessage())
	}
	return table, key, nil
}

// sourceSelect is what a source read selects: the key, the input fields and
// the version, each as text.
func sourceSelect(source *protocol.EmbeddingSource) []string {
	selected := make([]string, 0, len(source.GetKeyFields())+len(source.GetInputFields())+1)
	for _, field := range source.GetKeyFields() {
		selected = append(selected, asText(field))
	}
	for _, field := range source.GetInputFields() {
		selected = append(selected, asText(field))
	}
	if field := source.GetVersionField(); field != "" {
		selected = append(selected, asText(field))
	}
	return selected
}

// sourceRow reads one result of sourceSelect, and refuses a NULL key, which a
// keyset scan could not resume after.
func sourceRow(source *protocol.EmbeddingSource, values []sql.NullString) (*protocol.EmbeddingSourceRow, error) {
	keys, inputs := len(source.GetKeyFields()), len(source.GetInputFields())
	row := &protocol.EmbeddingSourceRow{Key: &protocol.EmbeddingKey{}}
	for i := range keys {
		if !values[i].Valid {
			return nil, fmt.Errorf("key column %s is NULL", source.GetKeyFields()[i])
		}
		row.Key.Components = append(row.Key.Components, values[i].String)
	}
	row.Fields = fieldValues(values[keys : keys+inputs])
	if source.GetVersionField() != "" {
		row.Version = values[keys+inputs].String
	}
	return row, nil
}

// fieldValues are input fields as null or text.
func fieldValues(values []sql.NullString) []*protocol.DataValue {
	fields := make([]*protocol.DataValue, len(values))
	for i, value := range values {
		fields[i] = &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}
		if value.Valid {
			fields[i] = &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: value.String}}
		}
	}
	return fields
}

// scanStrings reads one row of nullable text columns.
func scanStrings(rows *sql.Rows, width int) ([]sql.NullString, error) {
	values := make([]sql.NullString, width)
	destinations := make([]any, width)
	for i := range values {
		destinations[i] = &values[i]
	}
	return values, rows.Scan(destinations...)
}

// querySourceRows runs a source read and collects its rows.
func querySourceRows(ctx context.Context, connection *sql.Conn, source *protocol.EmbeddingSource, query string, arguments ...any) ([]*protocol.EmbeddingSourceRow, error) {
	rows, err := connection.QueryContext(ctx, query, arguments...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	var result []*protocol.EmbeddingSourceRow
	width := len(sourceSelect(source))
	for rows.Next() {
		values, err := scanStrings(rows, width)
		if err != nil {
			return nil, err
		}
		row, err := sourceRow(source, values)
		if err != nil {
			return nil, err
		}
		result = append(result, row)
	}
	return result, rows.Err()
}

// ScanEmbeddingSource reads one keyset page in key order under the filter.
func (p *Provider) ScanEmbeddingSource(ctx context.Context, request *protocol.ScanEmbeddingSourceRequest) (*protocol.EmbeddingSourcePage, error) {
	source := request.GetSource()
	page := &protocol.EmbeddingSourcePage{}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		table, _, err := sourceKey(ctx, connection, source)
		if err != nil {
			return err
		}
		names := make([]string, len(source.GetKeyFields()))
		for i, field := range source.GetKeyFields() {
			names[i] = quote(field)
		}
		var arguments []any
		where := filterClause(source)
		if after := request.GetAfter(); after != nil {
			placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(names)), ", ")
			where += " AND (" + strings.Join(names, ", ") + ") > (" + placeholders + ")"
			arguments = keyArguments(after)
		}
		// #nosec G202 -- the names are quoted, the filter is the specification's own SQL, and the bound is an integer.
		query := "SELECT " + strings.Join(sourceSelect(source), ", ") + " FROM " + quote(table) + " WHERE " + where +
			" ORDER BY " + strings.Join(names, ", ") + " LIMIT " + strconv.FormatUint(uint64(request.GetMaxRows()), 10)
		page.Rows, err = querySourceRows(ctx, connection, source, query, arguments...)
		return err
	})
	if err != nil {
		return nil, err
	}
	page.Done = len(page.GetRows()) < int(request.GetMaxRows())
	if count := len(page.GetRows()); count > 0 {
		page.Cursor = page.GetRows()[count-1].GetKey()
	}
	return page, nil
}

// ReadEmbeddingSourceRows rereads rows by key under the filter, in one read.
func (p *Provider) ReadEmbeddingSourceRows(ctx context.Context, request *protocol.ReadEmbeddingSourceRowsRequest) (*protocol.EmbeddingSourceRows, error) {
	source := request.GetSource()
	result := &protocol.EmbeddingSourceRows{}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		table, _, err := sourceKey(ctx, connection, source)
		if err != nil {
			return err
		}
		// #nosec G202 -- the names are quoted and the filter is the specification's own SQL.
		query := "SELECT " + strings.Join(sourceSelect(source), ", ") + " FROM " + quote(table) + " WHERE " + filterClause(source) + " AND " + keyMatch(source.GetKeyFields())
		for _, key := range request.GetKeys() {
			rows, err := querySourceRows(ctx, connection, source, query, keyArguments(key)...)
			if err != nil {
				return err
			}
			result.Rows = append(result.Rows, rows...)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

// heldTargets reads what the target holds at each key: the first stored row
// of a key that a generation wrote.
func heldTargets(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec, keys []*protocol.EmbeddingKey) ([]*protocol.EmbeddingHeldTarget, error) {
	table, err := tableName(spec.GetTarget().GetTable())
	if err != nil {
		return nil, err
	}
	column := spec.GetTarget().GetColumn()
	where := keyMatch(spec.GetSource().GetKeyFields())
	if spec.GetTarget().GetLayout() == protocol.EmbeddingTarget_OWN_TABLE {
		where += " AND " + quote(column+ordinalSuffix) + " = 0"
	}
	// #nosec G202 -- the names are quoted.
	query := "SELECT " + quote(column+generationSuffix) + ", " + quote(column+inputHashSuffix) + ", " + quote(column+versionSuffix) + ", " + quote(column+stateSuffix) +
		" FROM " + quote(table) + " WHERE " + where
	var held []*protocol.EmbeddingHeldTarget
	for _, key := range keys {
		var generation, inputHash, version, state sql.NullString
		err := connection.QueryRowContext(ctx, query, keyArguments(key)...).Scan(&generation, &inputHash, &version, &state)
		switch {
		case errors.Is(err, sql.ErrNoRows):
			continue
		case err != nil:
			return nil, err
		case !generation.Valid:
			continue
		}
		held = append(held, &protocol.EmbeddingHeldTarget{Key: key, Generation: generation.String, InputHash: inputHash.String, Version: version.String, Kind: kindOf(state.String)})
	}
	return held, nil
}

// batchState reads the run and what the target holds, with their token.
func batchState(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec, runID string, keys []*protocol.EmbeddingKey) (*protocol.EmbeddingBatchState, error) {
	run, err := readRun(ctx, connection, runID)
	if err != nil {
		return nil, err
	}
	held, err := heldTargets(ctx, connection, spec, keys)
	if err != nil {
		return nil, err
	}
	messages := []proto.Message{run}
	for _, target := range held {
		messages = append(messages, target)
	}
	token, err := provider.EmbeddingStateToken(messages...)
	if err != nil {
		return nil, err
	}
	return &protocol.EmbeddingBatchState{RunId: runID, Run: run, Held: held, Token: token}, nil
}

// ReadEmbeddingBatchState reads what a batch commit compares.
func (p *Provider) ReadEmbeddingBatchState(ctx context.Context, request *protocol.ReadEmbeddingBatchStateRequest) (*protocol.EmbeddingBatchState, error) {
	var state *protocol.EmbeddingBatchState
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		exists, err := storeExists(ctx, connection)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("the embedding store is not prepared")
		}
		state, err = batchState(ctx, connection, request.GetSpec(), request.GetRunId(), request.GetKeys())
		return err
	})
	return state, err
}

// CommitEmbeddingBatch makes each written key's rows equal to its set and
// writes the run, when the run and the keys still have the token they were
// read with.
func (p *Provider) CommitEmbeddingBatch(ctx context.Context, request *protocol.CommitEmbeddingBatchRequest) (*protocol.EmbeddingWriteResult, error) {
	conflict, crash := false, false
	err := p.withConnection(ctx, request.GetSessionId(), writing, func(connection *sql.Conn, fault string) error {
		exists, err := storeExists(ctx, connection)
		if err != nil {
			return err
		}
		if !exists {
			return errStoreMissing
		}
		state, err := batchState(ctx, connection, request.GetSpec(), request.GetRunId(), request.GetKeys())
		if err != nil {
			return err
		}
		if state.GetToken() != request.GetExpectedToken() {
			conflict = true
			return errRefused
		}
		for _, set := range sets(request.GetWrites()) {
			if err := applySet(ctx, connection, request.GetSpec(), set); err != nil {
				return err
			}
		}
		crash = fault == CrashAfterCommit
		return writeRecord(ctx, connection, runKind, request.GetRun().GetId(), request.GetRun())
	})
	if crash && err == nil {
		os.Exit(29) //revive:disable-line:deep-exit this fixture tests a provider that ends after a batch committed and before it answers
	}
	if conflict {
		return &protocol.EmbeddingWriteResult{RequestId: request.GetRequestId(), Outcome: protocol.EmbeddingWriteResult_CONFLICT}, nil
	}
	result, err := writeResult(request.GetRequestId(), nil, err)
	if result.GetOutcome() == protocol.EmbeddingWriteResult_APPLIED {
		result.Written = &protocol.EmbeddingRecords{Runs: []*protocol.EmbeddingRun{request.GetRun()}}
	}
	return result, err
}

// sets splits a batch's writes into one set per key. The SDK has checked that
// a key's writes are adjacent and in ordinal order.
func sets(writes []*protocol.EmbeddingTargetWrite) [][]*protocol.EmbeddingTargetWrite {
	var result [][]*protocol.EmbeddingTargetWrite
	for start := 0; start < len(writes); {
		end := start + 1
		for end < len(writes) && slices.Equal(writes[end].GetKey().GetComponents(), writes[start].GetKey().GetComponents()) {
			end++
		}
		result = append(result, writes[start:end])
		start = end
	}
	return result
}

// encodeVector writes a vector as little-endian float32 values, and NULL for
// a write that carries none.
func encodeVector(vector []float32) any {
	if len(vector) == 0 {
		return nil
	}
	encoded := make([]byte, 0, 4*len(vector))
	for _, value := range vector {
		encoded = binary.LittleEndian.AppendUint32(encoded, math.Float32bits(value))
	}
	return encoded
}

// optional stores an empty text as NULL, which reads back as empty.
func optional(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// applySet makes one key's stored rows equal to its set. On the target's
// columns it updates the row the key addresses, so a key the target no longer
// has updates nothing. In a table of the generation's own it writes each
// member for a source row that is still there and removes the rows past the
// set's end; a tombstone there removes the key's rows.
func applySet(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec, set []*protocol.EmbeddingTargetWrite) error {
	table, err := tableName(spec.GetTarget().GetTable())
	if err != nil {
		return err
	}
	column, fields := spec.GetTarget().GetColumn(), spec.GetSource().GetKeyFields()
	key := set[0].GetKey()
	if spec.GetTarget().GetLayout() != protocol.EmbeddingTarget_OWN_TABLE {
		write := set[0]
		arguments := append([]any{encodeVector(write.GetVector()), write.GetGeneration(), write.GetInputHash(), optional(write.GetVersion()), kindNames[write.GetKind()]},
			keyArguments(key)...)
		// #nosec G202 -- the names are quoted.
		_, err := connection.ExecContext(ctx, "UPDATE "+quote(table)+" SET "+quote(column)+" = ?, "+quote(column+generationSuffix)+" = ?, "+quote(column+inputHashSuffix)+" = ?, "+
			quote(column+versionSuffix)+" = ?, "+quote(column+stateSuffix)+" = ? WHERE "+keyMatch(fields), arguments...)
		return err
	}
	keep := len(set)
	if set[0].GetKind() == protocol.EmbeddingWriteKind_EMBEDDING_WRITE_KIND_TOMBSTONE {
		keep = 0
	} else if err := writeOwnedSet(ctx, connection, spec, table, set); err != nil {
		return err
	}
	// #nosec G202 -- the names are quoted.
	_, err = connection.ExecContext(ctx, "DELETE FROM "+quote(table)+" WHERE "+keyMatch(fields)+" AND "+quote(column+ordinalSuffix)+" >= ?", append(keyArguments(key), keep)...)
	return err
}

// writeOwnedSet writes a set's members into an owned table when the source
// row is still there.
func writeOwnedSet(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec, table string, set []*protocol.EmbeddingTargetWrite) error {
	source, err := tableName(spec.GetSource().GetTable())
	if err != nil {
		return err
	}
	fields, column := spec.GetSource().GetKeyFields(), spec.GetTarget().GetColumn()
	var present int
	// #nosec G202 -- the names are quoted.
	if err := connection.QueryRowContext(ctx, "SELECT count(*) FROM "+quote(source)+" WHERE "+keyMatch(fields), keyArguments(set[0].GetKey())...).Scan(&present); err != nil {
		return err
	}
	if present == 0 {
		return nil
	}
	names := make([]string, 0, len(fields)+6)
	for _, field := range fields {
		names = append(names, quote(field))
	}
	names = append(names, quote(column+ordinalSuffix), quote(column), quote(column+generationSuffix), quote(column+inputHashSuffix), quote(column+versionSuffix), quote(column+stateSuffix))
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(names)), ", ")
	for _, write := range set {
		arguments := append(keyArguments(write.GetKey()), int64(write.GetOrdinal()), encodeVector(write.GetVector()), write.GetGeneration(), write.GetInputHash(),
			optional(write.GetVersion()), kindNames[write.GetKind()])
		// #nosec G202 -- the names are quoted.
		if _, err := connection.ExecContext(ctx, "INSERT OR REPLACE INTO "+quote(table)+" ("+strings.Join(names, ", ")+") VALUES ("+placeholders+")", arguments...); err != nil {
			return err
		}
	}
	return nil
}

// walkRow is one position of the verification walk before it is paged.
type walkRow struct {
	key    []string
	source *protocol.EmbeddingVerificationSource
	stored *protocol.EmbeddingStoredRow
}

// readWalk reads both sides of the verification walk in one transaction and
// orders it by key, and by ordinal within a key. A source row is in the walk
// when the filter selects it, and a stored row when a generation wrote it or
// the source row it stands beside is in scope.
func readWalk(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec) ([]walkRow, error) {
	source := spec.GetSource()
	table, key, err := sourceKey(ctx, connection, source)
	if err != nil {
		return nil, err
	}
	sources, err := walkSources(ctx, connection, source, table)
	if err != nil {
		return nil, err
	}
	stored, err := walkStored(ctx, connection, spec)
	if err != nil {
		return nil, err
	}
	keys := make(map[string][]string)
	for id, side := range sources {
		if side.inScope {
			keys[id] = side.key
		}
	}
	for id, rows := range stored {
		if slices.ContainsFunc(rows, func(row storedRow) bool { return row.stored.GetGeneration() != "" }) {
			keys[id] = rows[0].key
		}
	}
	order := keyOrder(key)
	ordered := slices.SortedFunc(func(yield func([]string) bool) {
		for _, components := range keys {
			if !yield(components) {
				return
			}
		}
	}, order)
	var walk []walkRow
	for _, components := range ordered {
		id := keyID(components)
		var side *protocol.EmbeddingVerificationSource
		if found, ok := sources[id]; ok && found.inScope {
			side = found.side
		}
		rows := stored[id]
		if len(rows) == 0 {
			walk = append(walk, walkRow{key: components, source: side})
		}
		for _, row := range rows {
			if side == nil && row.stored.GetGeneration() == "" {
				continue
			}
			walk = append(walk, walkRow{key: components, source: side, stored: row.stored})
		}
	}
	return walk, nil
}

// keyID renders a key as one map key.
func keyID(components []string) string {
	encoded, _ := json.Marshal(components)
	return string(encoded)
}

// sourceSide is a source row with whether the filter selects it.
type sourceSide struct {
	key     []string
	inScope bool
	side    *protocol.EmbeddingVerificationSource
}

// walkSources reads every source row with whether the filter selects it.
func walkSources(ctx context.Context, connection *sql.Conn, source *protocol.EmbeddingSource, table string) (map[string]sourceSide, error) {
	// #nosec G202 -- the names are quoted and the filter is the specification's own SQL.
	rows, err := connection.QueryContext(ctx, "SELECT "+strings.Join(sourceSelect(source), ", ")+", CASE WHEN "+filterClause(source)+" THEN 1 ELSE 0 END FROM "+quote(table))
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	width := len(sourceSelect(source))
	result := make(map[string]sourceSide)
	for rows.Next() {
		values := make([]sql.NullString, width)
		destinations := make([]any, width+1)
		for i := range values {
			destinations[i] = &values[i]
		}
		var inScope bool
		destinations[width] = &inScope
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		row, err := sourceRow(source, values)
		if err != nil {
			return nil, err
		}
		result[keyID(row.GetKey().GetComponents())] = sourceSide{key: row.GetKey().GetComponents(), inScope: inScope,
			side: &protocol.EmbeddingVerificationSource{Fields: row.GetFields(), Version: row.GetVersion()}}
	}
	return result, rows.Err()
}

// storedRow is one row of the target with its key.
type storedRow struct {
	key    []string
	stored *protocol.EmbeddingStoredRow
}

// walkStored reads every row of the target, by key and in ordinal order: the
// generation, input hash, version and kind a write recorded, and the width of
// the stored vector, never the vector.
func walkStored(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec) (map[string][]storedRow, error) {
	table, err := tableName(spec.GetTarget().GetTable())
	if err != nil {
		return nil, err
	}
	column, fields := spec.GetTarget().GetColumn(), spec.GetSource().GetKeyFields()
	if _, exists, err := describeTable(ctx, connection, table); err != nil || !exists {
		return nil, err
	}
	selected := make([]string, 0, len(fields)+6)
	for _, field := range fields {
		selected = append(selected, asText(field))
	}
	ordinal, order := "0", ""
	if spec.GetTarget().GetLayout() == protocol.EmbeddingTarget_OWN_TABLE {
		ordinal = quote(column + ordinalSuffix)
		order = " ORDER BY " + ordinal
	}
	selected = append(selected, quote(column+generationSuffix), quote(column+inputHashSuffix), quote(column+versionSuffix), quote(column+stateSuffix),
		"coalesce(length("+quote(column)+"), 0) / 4", ordinal)
	// #nosec G202 -- the names are quoted.
	rows, err := connection.QueryContext(ctx, "SELECT "+strings.Join(selected, ", ")+" FROM "+quote(table)+order)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	result := make(map[string][]storedRow)
	for rows.Next() {
		values := make([]sql.NullString, len(fields)+4)
		destinations := make([]any, 0, len(values)+2)
		for i := range values {
			destinations = append(destinations, &values[i])
		}
		var width, position uint32
		destinations = append(destinations, &width, &position)
		if err := rows.Scan(destinations...); err != nil {
			return nil, err
		}
		key := make([]string, len(fields))
		for i := range fields {
			key[i] = values[i].String
		}
		meta := values[len(fields):]
		row := storedRow{key: key, stored: &protocol.EmbeddingStoredRow{Generation: meta[0].String, InputHash: meta[1].String, Version: meta[2].String,
			Kind: kindOf(meta[3].String), Dimension: width, Ordinal: position}}
		result[keyID(key)] = append(result[keyID(key)], row)
	}
	return result, rows.Err()
}

// ReadEmbeddingVerification pages the verification walk by key: a page ends
// between two keys, and its token is the last key it holds.
func (p *Provider) ReadEmbeddingVerification(ctx context.Context, request *protocol.ReadEmbeddingVerificationRequest) (*protocol.EmbeddingVerificationPage, error) {
	var after []string
	if token := request.GetPageToken(); token != "" {
		if err := json.Unmarshal([]byte(token), &after); err != nil {
			return nil, fmt.Errorf("invalid verification page token")
		}
	}
	page := &protocol.EmbeddingVerificationPage{}
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		walk, err := readWalk(ctx, connection, request.GetSpec())
		if err != nil {
			return err
		}
		_, key, err := sourceKey(ctx, connection, request.GetSpec().GetSource())
		if err != nil {
			return err
		}
		order := keyOrder(key)
		start := 0
		for after != nil && start < len(walk) && order(walk[start].key, after) <= 0 {
			start++
		}
		end := start
		for end < len(walk) {
			next := end
			for next < len(walk) && slices.Equal(walk[next].key, walk[end].key) {
				next++
			}
			if next-start > int(request.GetMaxRows()) {
				break
			}
			end = next
		}
		if end == start && start < len(walk) {
			return fmt.Errorf("a key holds more rows than one verification page")
		}
		for _, row := range walk[start:end] {
			page.Rows = append(page.Rows, &protocol.EmbeddingVerificationRow{Key: &protocol.EmbeddingKey{Components: row.key}, Source: row.source, Stored: row.stored})
		}
		if end < len(walk) {
			page.NextPageToken = keyID(walk[end-1].key)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	page.Complete = page.GetNextPageToken() == ""
	return page, nil
}

// SearchEmbeddings answers the keys nearest a vector by exhaustive scan. The
// fixture has no index, so the indexed list is the exhaustive one and no index
// served it. A key whose set holds several vectors is as near as its nearest.
func (p *Provider) SearchEmbeddings(ctx context.Context, request *protocol.SearchEmbeddingsRequest) (*protocol.EmbeddingSearchResult, error) {
	spec := request.GetSpec()
	var nearest []*protocol.EmbeddingKey
	err := p.withConnection(ctx, request.GetSessionId(), reading, func(connection *sql.Conn, _ string) error {
		_, key, err := sourceKey(ctx, connection, spec.GetSource())
		if err != nil {
			return err
		}
		scored, err := scoreTarget(ctx, connection, spec, request.GetVector())
		if err != nil {
			return err
		}
		order := keyOrder(key)
		slices.SortFunc(scored, func(a, b scoredKey) int { return cmp.Or(cmp.Compare(a.distance, b.distance), order(a.key, b.key)) })
		for _, candidate := range scored[:min(len(scored), int(request.GetDepth()))] {
			nearest = append(nearest, &protocol.EmbeddingKey{Components: candidate.key})
		}
		return nil
	})
	// A failed search is a reply with a reason, which the host records against
	// its case, and not a failed call.
	if err != nil {
		return &protocol.EmbeddingSearchResult{Outcome: protocol.EmbeddingSearchResult_FAILED, Message: "the fixture could not search the generation"}, nil
	}
	return &protocol.EmbeddingSearchResult{Outcome: protocol.EmbeddingSearchResult_COMPLETED, Indexed: nearest, Exact: nearest}, nil
}

// scoredKey is a key and its distance from the query; a smaller distance is
// nearer under every metric, an inner product counting as its negation.
type scoredKey struct {
	key      []string
	distance float64
}

// scoreTarget measures every stored vector of the generation against the
// query and keeps each key's nearest.
func scoreTarget(ctx context.Context, connection *sql.Conn, spec *protocol.EmbeddingSpec, query []float32) ([]scoredKey, error) {
	table, err := tableName(spec.GetTarget().GetTable())
	if err != nil {
		return nil, err
	}
	column, fields := spec.GetTarget().GetColumn(), spec.GetSource().GetKeyFields()
	selected := make([]string, 0, len(fields)+1)
	for _, field := range fields {
		selected = append(selected, asText(field))
	}
	// #nosec G202 -- the names are quoted.
	rows, err := connection.QueryContext(ctx, "SELECT "+strings.Join(selected, ", ")+", "+quote(column)+" FROM "+quote(table)+" WHERE "+quote(column+generationSuffix)+" = ? AND "+
		quote(column)+" IS NOT NULL", spec.GetGeneration())
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	best := make(map[string]scoredKey)
	for rows.Next() {
		key := make([]string, len(fields))
		destinations := make([]any, 0, len(fields)+1)
		for i := range key {
			destinations = append(destinations, &key[i])
		}
		var encoded []byte
		if err := rows.Scan(append(destinations, &encoded)...); err != nil {
			return nil, err
		}
		distance, ok := measure(spec.GetTarget().GetMetric(), query, encoded)
		if found, seen := best[keyID(key)]; ok && (!seen || distance < found.distance) {
			best[keyID(key)] = scoredKey{key: key, distance: distance}
		}
	}
	scored := make([]scoredKey, 0, len(best))
	for _, candidate := range best {
		scored = append(scored, candidate)
	}
	return scored, rows.Err()
}

// measure is the distance between the query and a stored vector under a
// metric. ok is false for a vector of another width, and under cosine for a
// vector that has no direction.
func measure(metric protocol.EmbeddingTarget_Metric, query []float32, encoded []byte) (float64, bool) {
	if len(encoded) != 4*len(query) {
		return 0, false
	}
	var dot, queryNorm, storedNorm, squares float64
	for i, element := range query {
		stored := float64(math.Float32frombits(binary.LittleEndian.Uint32(encoded[4*i:])))
		dot += float64(element) * stored
		queryNorm += float64(element) * float64(element)
		storedNorm += stored * stored
		squares += (float64(element) - stored) * (float64(element) - stored)
	}
	switch metric {
	case protocol.EmbeddingTarget_L2:
		return math.Sqrt(squares), true
	case protocol.EmbeddingTarget_INNER_PRODUCT:
		return -dot, true
	default:
		if queryNorm == 0 || storedNorm == 0 {
			return 0, false
		}
		return 1 - dot/math.Sqrt(queryNorm*storedNorm), true
	}
}
