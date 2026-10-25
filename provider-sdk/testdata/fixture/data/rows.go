package data

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"

	"example.com/ptah-fixture-provider/backend"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// columnKind is the value case a fixture column type takes. The fixture's
// grammar has four column types, and a value of each is stored in the class
// SQLite gives that type.
func columnKind(columnType string) (protocol.DataKind, bool) {
	switch strings.ToUpper(strings.TrimSpace(columnType)) {
	case "INTEGER":
		return protocol.DataKind_DATA_KIND_INT64, true
	case "TEXT":
		return protocol.DataKind_DATA_KIND_STRING, true
	case "REAL":
		return protocol.DataKind_DATA_KIND_DOUBLE, true
	case "BLOB":
		return protocol.DataKind_DATA_KIND_BYTES, true
	default:
		return protocol.DataKind_DATA_KIND_UNSPECIFIED, false
	}
}

// TypeRows types each column by the live table's type, or by its declared
// type when the table does not hold it, and converts both row sets to that
// type as SQLite stores a value of it: a declared "5" and a live 5 in an
// INTEGER column are one value, and so are a declared 2 and a live 2.0 in a
// REAL column.
func (p *Provider) TypeRows(_ context.Context, request *protocol.TypeRowsRequest) (*protocol.TypedRows, error) {
	live := make(map[string]string)
	for _, column := range request.GetLiveTable().GetColumns() {
		live[strings.ToLower(column.GetName())] = column.GetType()
	}
	result := &protocol.TypedRows{}
	kinds := make([]protocol.DataKind, len(request.GetColumns()))
	for i, column := range request.GetColumns() {
		columnType, found := live[strings.ToLower(column.GetName())]
		if !found {
			columnType = column.GetDeclaredType()
		}
		typed := &protocol.DataColumn{Name: column.GetName()}
		if columnType != "" {
			kind, ok := columnKind(columnType)
			if !ok {
				return nil, fmt.Errorf("the fixture types no column of type %s", columnType)
			}
			typed.Type, typed.Kind, kinds[i] = strings.ToUpper(columnType), kind, kind
		}
		result.Columns = append(result.Columns, typed)
	}
	for _, set := range []struct {
		rows []*protocol.Row
		into *[]*protocol.Row
	}{{request.GetDeclared(), &result.Declared}, {request.GetLive(), &result.Live}} {
		for _, row := range set.rows {
			typed := &protocol.Row{}
			for i, value := range row.GetValues() {
				converted, err := convert(value, kinds[i])
				if err != nil {
					return nil, fmt.Errorf("column %s: %w", request.GetColumns()[i].GetName(), err)
				}
				typed.Values = append(typed.Values, converted)
			}
			*set.into = append(*set.into, typed)
		}
	}
	return result, nil
}

// convert writes value in the class kind stores. An unset value stays unset
// and null stays null; a column with no kind keeps its value.
func convert(value *protocol.DataValue, kind protocol.DataKind) (*protocol.DataValue, error) {
	switch value.GetValue().(type) {
	case nil, *protocol.DataValue_Null:
		return value, nil
	}
	switch kind {
	case protocol.DataKind_DATA_KIND_UNSPECIFIED:
		return value, nil
	case protocol.DataKind_DATA_KIND_INT64:
		return toInteger(value)
	case protocol.DataKind_DATA_KIND_DOUBLE:
		return toReal(value)
	case protocol.DataKind_DATA_KIND_STRING:
		return toText(value)
	default:
		return toBlob(value)
	}
}

func toInteger(value *protocol.DataValue) (*protocol.DataValue, error) {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_Int64Value:
		return value, nil
	case *protocol.DataValue_BoolValue:
		if v.BoolValue {
			return intValue(1), nil
		}
		return intValue(0), nil
	case *protocol.DataValue_Uint64Value:
		if v.Uint64Value <= math.MaxInt64 {
			return intValue(int64(v.Uint64Value)), nil
		}
	case *protocol.DataValue_DoubleValue:
		if v.DoubleValue == math.Trunc(v.DoubleValue) && math.Abs(v.DoubleValue) < 1<<62 {
			return intValue(int64(v.DoubleValue)), nil
		}
	case *protocol.DataValue_StringValue:
		if parsed, err := strconv.ParseInt(strings.TrimSpace(v.StringValue), 10, 64); err == nil {
			return intValue(parsed), nil
		}
	}
	return nil, fmt.Errorf("the value is not an integer")
}

func toReal(value *protocol.DataValue) (*protocol.DataValue, error) {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_DoubleValue:
		return value, nil
	case *protocol.DataValue_Int64Value:
		return realValue(float64(v.Int64Value)), nil
	case *protocol.DataValue_Uint64Value:
		return realValue(float64(v.Uint64Value)), nil
	case *protocol.DataValue_StringValue:
		if parsed, err := strconv.ParseFloat(strings.TrimSpace(v.StringValue), 64); err == nil && !math.IsInf(parsed, 0) && !math.IsNaN(parsed) {
			return realValue(parsed), nil
		}
	}
	return nil, fmt.Errorf("the value is not a real number")
}

func toText(value *protocol.DataValue) (*protocol.DataValue, error) {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_StringValue:
		return value, nil
	case *protocol.DataValue_Int64Value:
		return textValue(strconv.FormatInt(v.Int64Value, 10)), nil
	case *protocol.DataValue_Uint64Value:
		return textValue(strconv.FormatUint(v.Uint64Value, 10)), nil
	}
	return nil, fmt.Errorf("the value is not text")
}

func toBlob(value *protocol.DataValue) (*protocol.DataValue, error) {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_BytesValue:
		return value, nil
	case *protocol.DataValue_StringValue:
		return &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: []byte(v.StringValue)}}, nil
	}
	return nil, fmt.Errorf("the value is not bytes")
}

func intValue(v int64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: v}}
}
func realValue(v float64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: v}}
}
func textValue(v string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: v}}
}
func nullValue() *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}
}

// ReadRows reads one page in key order, resuming at the offset the page
// token holds. A table the database does not hold is a complete page that
// says so.
func (p *Provider) ReadRows(ctx context.Context, request *protocol.ReadRowsRequest) (*protocol.RowPage, error) {
	name, err := tableName(request.GetTable())
	if err != nil {
		return nil, err
	}
	offset := uint64(0)
	if token := request.GetPageToken(); token != "" {
		if offset, err = strconv.ParseUint(token, 10, 63); err != nil {
			return nil, fmt.Errorf("invalid fixture page token")
		}
	}
	page := &protocol.RowPage{Table: request.GetTable()}
	err = p.WithSession(ctx, request.GetSessionId(), request.GetTransactionId(), func(access backend.Access) error {
		types, exists, err := columnTypes(ctx, access.Querier, name)
		if err != nil || !exists {
			page.Complete = true
			return err
		}
		page.Exists = true
		quoted := make([]string, len(request.GetColumns()))
		for i, column := range request.GetColumns() {
			columnType, found := types[strings.ToLower(column)]
			if !found {
				return fmt.Errorf("the fixture table has no column %s", column)
			}
			kind, ok := columnKind(columnType)
			if !ok {
				return fmt.Errorf("the fixture reads no column of type %s", columnType)
			}
			page.Columns = append(page.Columns, &protocol.DataColumn{Name: column, Type: strings.ToUpper(columnType), Kind: kind})
			quoted[i] = quote(column)
		}
		keys := make([]string, len(request.GetKeyColumns()))
		for i, key := range request.GetKeyColumns() {
			keys[i] = quote(key)
		}
		query := "SELECT " + strings.Join(quoted, ", ") + " FROM " + quote(name) + " ORDER BY " + strings.Join(keys, ", ") + " LIMIT ? OFFSET ?"
		rows, err := access.Querier.QueryContext(ctx, query, int64(request.GetMaxRows())+1, int64(offset)) // #nosec G115 -- the SDK bounds max_rows and the token parse bounds the offset
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			if len(page.Rows) == int(request.GetMaxRows()) {
				page.NextPageToken = strconv.FormatUint(offset+uint64(request.GetMaxRows()), 10)
				return rows.Err()
			}
			row, err := scanRow(rows, page.Columns)
			if err != nil {
				return err
			}
			page.Rows = append(page.Rows, row)
		}
		page.Complete = true
		return rows.Err()
	})
	if err != nil {
		return nil, err
	}
	return page, nil
}

func columnTypes(ctx context.Context, queries backend.Querier, table string) (map[string]string, bool, error) {
	var count int
	if err := queries.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_schema WHERE type = 'table' AND name = ? COLLATE NOCASE", table).Scan(&count); err != nil {
		return nil, false, err
	}
	if count == 0 {
		return nil, false, nil
	}
	rows, err := queries.QueryContext(ctx, "SELECT name, type FROM pragma_table_info(?)", table)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()
	types := make(map[string]string)
	for rows.Next() {
		var name, kind string
		if err := rows.Scan(&name, &kind); err != nil {
			return nil, false, err
		}
		types[strings.ToLower(name)] = kind
	}
	return types, true, rows.Err()
}

// scanRow reads one row and refuses a stored value whose class differs from
// its column's type, which a page cannot carry.
func scanRow(rows *sql.Rows, columns []*protocol.DataColumn) (*protocol.Row, error) {
	holders := make([]any, len(columns))
	targets := make([]any, len(columns))
	for i := range holders {
		targets[i] = &holders[i]
	}
	if err := rows.Scan(targets...); err != nil {
		return nil, err
	}
	row := &protocol.Row{}
	for i, held := range holders {
		var value *protocol.DataValue
		switch v := held.(type) {
		case nil:
			value = nullValue()
		case int64:
			value = intValue(v)
		case float64:
			value = realValue(v)
		case string:
			value = textValue(v)
		case []byte:
			value = &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: v}}
		default:
			return nil, fmt.Errorf("the fixture read a value it cannot represent")
		}
		if kind, _ := columnKind(columns[i].GetType()); held != nil && !matches(value, kind) {
			return nil, fmt.Errorf("column %s holds a value whose class differs from its type", columns[i].GetName())
		}
		row.Values = append(row.Values, value)
	}
	return row, nil
}

func matches(value *protocol.DataValue, kind protocol.DataKind) bool {
	switch value.GetValue().(type) {
	case *protocol.DataValue_Int64Value:
		return kind == protocol.DataKind_DATA_KIND_INT64
	case *protocol.DataValue_DoubleValue:
		return kind == protocol.DataKind_DATA_KIND_DOUBLE
	case *protocol.DataValue_StringValue:
		return kind == protocol.DataKind_DATA_KIND_STRING
	case *protocol.DataValue_BytesValue:
		return kind == protocol.DataKind_DATA_KIND_BYTES
	}
	return false
}

// RenderRowWrites renders each write as one SQLite statement. A value the
// fixture has no literal for refuses the batch, which then exposes no unit.
// NativeWrites wraps each statement in a native unit, except in the text form.
func (p *Provider) RenderRowWrites(_ context.Context, request *protocol.RenderRowWritesRequest) (*protocol.RenderResult, error) {
	result := &protocol.RenderResult{Complete: true}
	for _, write := range request.GetWrites() {
		statement, refusal, err := renderWrite(write)
		if err != nil {
			return nil, err
		}
		if refusal != nil {
			return &protocol.RenderResult{Refusal: refusal}, nil
		}
		unit := &protocol.Unit{ChangeId: write.GetChangeId(), Body: &protocol.Unit_Sql{Sql: statement}}
		if p.NativeWrites && request.GetForm() != protocol.RenderRequest_TEXT {
			unit.Body = &protocol.Unit_Native{Native: &protocol.Envelope{ProviderId: "example.org/fixture", Kind: "example.org/fixture/row-write",
				SchemaVersion: 1, Encoding: "sql", Owner: write.GetTable(), Payload: []byte(statement)}}
		}
		result.Units = append(result.Units, unit)
	}
	return result, nil
}

func renderWrite(write *protocol.RowWrite) (string, *protocol.RenderRefusal, error) {
	name, err := tableName(write.GetTable())
	if err != nil {
		return "", nil, err
	}
	literals := make([]string, len(write.GetValues()))
	for i, value := range write.GetValues() {
		literal, ok := literalOf(value)
		if !ok {
			return "", &protocol.RenderRefusal{ChangeId: write.GetChangeId(), Reason: protocol.RenderRefusal_UNSUPPORTED_VALUE,
				Message: "the fixture writes no literal for column " + write.GetColumns()[i]}, nil
		}
		literals[i] = literal
	}
	isKey := func(column string) bool { return slices.Contains(write.GetKeyColumns(), column) }
	var columns, values, assignments, predicates []string
	for i, column := range write.GetColumns() {
		columns, values = append(columns, quote(column)), append(values, literals[i])
		if isKey(column) {
			if literals[i] == "NULL" {
				predicates = append(predicates, quote(column)+" IS NULL")
			} else {
				predicates = append(predicates, quote(column)+" = "+literals[i])
			}
			continue
		}
		assignments = append(assignments, quote(column)+" = "+literals[i])
	}
	table := quote(name)
	switch write.GetMode() {
	case protocol.RowWrite_INSERT:
		return "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(values, ", ") + ");", nil, nil
	case protocol.RowWrite_UPSERT:
		keys := make([]string, len(write.GetKeyColumns()))
		excluded := make([]string, 0, len(assignments))
		for i, key := range write.GetKeyColumns() {
			keys[i] = quote(key)
		}
		for _, column := range write.GetColumns() {
			if !isKey(column) {
				excluded = append(excluded, quote(column)+" = excluded."+quote(column))
			}
		}
		action := "DO NOTHING"
		if len(excluded) > 0 {
			action = "DO UPDATE SET " + strings.Join(excluded, ", ")
		}
		return "INSERT INTO " + table + " (" + strings.Join(columns, ", ") + ") VALUES (" + strings.Join(values, ", ") + ") ON CONFLICT (" +
			strings.Join(keys, ", ") + ") " + action + ";", nil, nil
	case protocol.RowWrite_UPDATE_BY_KEY:
		return "UPDATE " + table + " SET " + strings.Join(assignments, ", ") + " WHERE " + strings.Join(predicates, " AND ") + ";", nil, nil
	case protocol.RowWrite_DELETE_BY_KEY:
		return "DELETE FROM " + table + " WHERE " + strings.Join(predicates, " AND ") + ";", nil, nil
	default:
		return "", &protocol.RenderRefusal{ChangeId: write.GetChangeId(), Reason: protocol.RenderRefusal_UNSUPPORTED_MODE,
			Message: "the fixture has no such row write"}, nil
	}
}

// literalOf writes a value as a SQLite literal: the classes SQLite stores.
// A real number keeps a point so it reads back as REAL.
func literalOf(value *protocol.DataValue) (string, bool) {
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_Null:
		return "NULL", true
	case *protocol.DataValue_BoolValue:
		if v.BoolValue {
			return "1", true
		}
		return "0", true
	case *protocol.DataValue_Int64Value:
		return strconv.FormatInt(v.Int64Value, 10), true
	case *protocol.DataValue_Uint64Value:
		if v.Uint64Value > math.MaxInt64 {
			return "", false
		}
		return strconv.FormatUint(v.Uint64Value, 10), true
	case *protocol.DataValue_DoubleValue:
		if math.IsInf(v.DoubleValue, 0) || math.IsNaN(v.DoubleValue) {
			return "", false
		}
		literal := strconv.FormatFloat(v.DoubleValue, 'g', -1, 64)
		if !strings.ContainsAny(literal, ".eE") {
			literal += ".0"
		}
		return literal, true
	case *protocol.DataValue_StringValue:
		return "'" + strings.ReplaceAll(v.StringValue, "'", "''") + "'", true
	case *protocol.DataValue_BytesValue:
		return "X'" + hex.EncodeToString(v.BytesValue) + "'", true
	default:
		return "", false
	}
}
