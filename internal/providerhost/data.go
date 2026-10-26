package providerhost

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"time"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/core/dataext"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
)

// The data services of ADR 0028: the provider types declared and live rows,
// reads a table's rows and spells every row write. The host keeps the row
// diff, the order across tables and the plan around them. A target whose
// engine lacks a service gets one that refuses, so a declared row set never
// falls back to the host's own INSERT text.

// dataServices returns the managed-data services of the target.
func (t *target) dataServices() dataext.Services {
	return dataext.Services{Rows: rowTyper{target: t}, Writes: rowWriter{target: t}}
}

// offersData reports whether the provider and the engine both declare a data
// feature.
func (t *target) offersData(feature string) bool {
	return slices.Contains(t.host.description.GetFeatures(), feature) && slices.Contains(t.engine.GetFeatures(), feature)
}

func (t *target) requireData(feature, operation string) error {
	if t.offersData(feature) {
		return nil
	}
	return fmt.Errorf("%w: the %s provider does not offer %s, so it cannot %s", ptaherr.ErrUnsupportedFeature, t.engine.GetId(), feature, operation)
}

func capabilityFlags(caps capability.Capabilities) map[string]bool {
	flags := make(map[string]bool, caps.Len())
	for name, value := range caps.All() {
		flags[string(name)] = value
	}
	return flags
}

func tableReference(schema, table string) *protocol.Reference {
	return &protocol.Reference{Kind: "table", Schema: schema, Name: table}
}

type rowTyper struct{ target *target }

// TypeRows sends the rows of one table to the provider, which returns each
// column's engine type and both row sets in one canonical form.
func (r rowTyper) TypeRows(ctx context.Context, request dataext.RowsRequest) (dataext.TypedRows, error) {
	t := r.target
	if err := t.requireData(provider.TypeRowsFeature, "type declared rows"); err != nil {
		return dataext.TypedRows{}, err
	}
	if err := request.Capabilities.Validate(); err != nil {
		return dataext.TypedRows{}, err
	}
	columns := rowColumns(request)
	wire := &protocol.TypeRowsRequest{Engine: t.engine.GetId(), Capabilities: capabilityFlags(request.Capabilities),
		TargetFeatures: featureSet(t.engine.GetFeatures()), Table: tableReference(request.Schema, request.Table)}
	if live := request.LiveTable; live != nil {
		wire.LiveTable = &protocol.LiveTable{}
		for _, column := range live.Columns {
			wire.LiveTable.Columns = append(wire.LiveTable.Columns, &protocol.Column{Name: column.Name, Type: column.DataType,
				Nullable: column.IsNullable != "NO", PrimaryKey: column.IsPrimaryKey})
		}
	}
	for _, column := range columns {
		wire.Columns = append(wire.Columns, &protocol.RowColumn{Name: column, DeclaredType: request.DeclaredTypes[column]})
	}
	var err error
	if wire.Declared, err = wireRows(request.Desired, columns); err != nil {
		return dataext.TypedRows{}, fmt.Errorf("declared %w", err)
	}
	if wire.Live, err = wireRows(request.Live, columns); err != nil {
		return dataext.TypedRows{}, fmt.Errorf("live %w", err)
	}
	typed, err := t.host.client.TypeRows(ctx, wire)
	if err != nil {
		return dataext.TypedRows{}, err
	}
	result := dataext.TypedRows{ColumnTypes: make(map[string]string, len(columns))}
	for _, column := range typed.GetColumns() {
		if column.GetType() != "" {
			result.ColumnTypes[column.GetName()] = column.GetType()
		}
	}
	if result.Desired, err = hostRows(typed.GetDeclared(), columns); err != nil {
		return dataext.TypedRows{}, err
	}
	if result.Live, err = hostRows(typed.GetLive(), columns); err != nil {
		return dataext.TypedRows{}, err
	}
	return result, nil
}

// rowColumns is every column the rows carry or the declaration types, sorted,
// which is the order the rows' values align with on the wire.
func rowColumns(request dataext.RowsRequest) []string {
	seen := make(map[string]bool)
	for column := range request.DeclaredTypes {
		seen[column] = true
	}
	for _, rows := range [][]map[string]any{request.Desired, request.Live} {
		for _, row := range rows {
			for column := range row {
				seen[column] = true
			}
		}
	}
	columns := make([]string, 0, len(seen))
	for column := range seen {
		columns = append(columns, column)
	}
	slices.Sort(columns)
	return columns
}

// wireRows aligns each row with columns. A column a row does not carry is an
// unset value, which differs from null.
func wireRows(rows []map[string]any, columns []string) ([]*protocol.Row, error) {
	wire := make([]*protocol.Row, 0, len(rows))
	for i, row := range rows {
		values := make([]*protocol.DataValue, len(columns))
		for j, column := range columns {
			value, carried := row[column]
			if !carried {
				values[j] = &protocol.DataValue{}
				continue
			}
			converted, err := wireValue(value)
			if err != nil {
				return nil, fmt.Errorf("row %d, column %q: %w", i+1, column, err)
			}
			values[j] = converted
		}
		wire = append(wire, &protocol.Row{Values: values})
	}
	return wire, nil
}

// hostRows turns typed rows back into the maps the row diff compares, leaving
// out the columns a row does not carry.
func hostRows(rows []*protocol.Row, columns []string) ([]map[string]any, error) {
	result := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		values := make(map[string]any, len(columns))
		for i, value := range row.GetValues() {
			if value.GetValue() == nil {
				continue
			}
			converted, err := hostValue(value)
			if err != nil {
				return nil, err
			}
			values[columns[i]] = converted
		}
		result = append(result, values)
	}
	return result, nil
}

type rowWriter struct{ target *target }

// RenderWrites asks the provider to spell each write. The host writes rows as
// statement text, in plans and in migration files alike, so it asks for the
// text form from a provider that negotiated provider.TextUnitsFeature, and a
// write the provider can perform only as a native operation is refused.
func (w rowWriter) RenderWrites(ctx context.Context, caps capability.Capabilities, writes []dataext.RowWrite) ([]string, error) {
	t := w.target
	if err := t.requireData(provider.RenderRowWritesFeature, "write declared rows"); err != nil {
		return nil, err
	}
	if err := caps.Validate(); err != nil {
		return nil, err
	}
	if len(writes) == 0 {
		return nil, nil
	}
	request := &protocol.RenderRowWritesRequest{Engine: t.engine.GetId(), Capabilities: capabilityFlags(caps), TargetFeatures: featureSet(t.engine.GetFeatures())}
	if t.negotiated(provider.TextUnitsFeature) {
		request.Form = protocol.RenderRequest_TEXT
	}
	for i, write := range writes {
		wire, err := wireWrite(strconv.Itoa(i), write)
		if err != nil {
			return nil, err
		}
		request.Writes = append(request.Writes, wire)
	}
	rendered, err := t.host.client.RenderRowWrites(ctx, request)
	if err != nil {
		return nil, err
	}
	if refusal := rendered.GetRefusal(); refusal != nil {
		index, _ := strconv.Atoi(refusal.GetChangeId())
		return nil, fmt.Errorf("%w: the %s provider refuses the row write to %s: %s", ptaherr.ErrUnsupportedFeature,
			t.engine.GetId(), writes[index].Table, refusal.GetMessage())
	}
	statements := make([]string, len(rendered.GetUnits()))
	for i, unit := range rendered.GetUnits() {
		text, ok := unit.GetBody().(*protocol.Unit_Sql)
		if !ok {
			return nil, fmt.Errorf("%w: the %s provider renders a row write as a native operation, and rows are written as statement text",
				ptaherr.ErrUnsupportedFeature, t.engine.GetId())
		}
		statements[i] = text.Sql
	}
	return statements, nil
}

var writeModes = map[dataext.WriteMode]protocol.RowWrite_Mode{
	dataext.WriteInsert:      protocol.RowWrite_INSERT,
	dataext.WriteUpsert:      protocol.RowWrite_UPSERT,
	dataext.WriteUpdateByKey: protocol.RowWrite_UPDATE_BY_KEY,
	dataext.WriteDeleteByKey: protocol.RowWrite_DELETE_BY_KEY,
}

func wireWrite(id string, write dataext.RowWrite) (*protocol.RowWrite, error) {
	mode, known := writeModes[write.Mode]
	if !known {
		return nil, fmt.Errorf("unknown row write mode %d", write.Mode)
	}
	if len(write.Values) != len(write.Columns) {
		return nil, fmt.Errorf("a row write to %s has %d values for %d columns", write.Table, len(write.Values), len(write.Columns))
	}
	wire := &protocol.RowWrite{ChangeId: id, Mode: mode, Table: tableReference(write.Schema, write.Table),
		Columns: slices.Clone(write.Columns), KeyColumns: slices.Clone(write.Keys)}
	for i, value := range write.Values {
		converted, err := wireValue(value)
		if err != nil {
			return nil, fmt.Errorf("column %q of a row write to %s: %w", write.Columns[i], write.Table, err)
		}
		wire.Values = append(wire.Values, converted)
	}
	for column, kind := range write.ColumnTypes {
		if kind != "" && slices.Contains(write.Columns, column) {
			if wire.ColumnTypes == nil {
				wire.ColumnTypes = make(map[string]string)
			}
			wire.ColumnTypes[column] = kind
		}
	}
	return wire, nil
}

// ReadTableRows reads a table's rows through the provider, page by page, in
// key order. A table the database does not hold is an error.
func (s *session) ReadTableRows(ctx context.Context, schema, table string, columns, keys []string) ([]map[string]any, error) {
	if err := s.target.requireData(provider.ReadRowsFeature, "read declared rows"); err != nil {
		return nil, err
	}
	if s.closed.Load() {
		return nil, fmt.Errorf("provider session is closed")
	}
	request := &protocol.ReadRowsRequest{SessionId: s.remote.GetId(), Table: tableReference(schema, table),
		Columns: slices.Clone(columns), KeyColumns: slices.Clone(keys), MaxRows: provider.MaxRowsPerPage}
	var rows []map[string]any
	for {
		page, err := s.target.host.client.ReadRows(ctx, request)
		if err != nil {
			return nil, err
		}
		if !page.GetExists() {
			return nil, fmt.Errorf("the provider reports that table %q does not exist", table)
		}
		read, err := hostRows(page.GetRows(), columns)
		if err != nil {
			return nil, err
		}
		rows = append(rows, read...)
		if page.GetComplete() {
			return rows, nil
		}
		request.PageToken = page.GetNextPageToken()
	}
}

// wireValue writes a value a row holds in the provider's form.
func wireValue(value any) (*protocol.DataValue, error) {
	switch v := value.(type) {
	case nil:
		return &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}, nil
	case providerValue:
		return v.value, nil
	case bool:
		return &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: v}}, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return integerValue(v), nil
	case float32:
		return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: float64(v)}}, nil
	case float64:
		return &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: v}}, nil
	case string:
		return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: v}}, nil
	case []byte:
		return &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: slices.Clone(v)}}, nil
	case time.Time:
		return &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{
			Seconds: v.Unix(), Nanos: int32(v.Nanosecond())}}}, nil // #nosec G115 -- a nanosecond count is below 1e9
	case map[string]any, []any:
		encoded, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}
		return &protocol.DataValue{Value: &protocol.DataValue_JsonValue{JsonValue: string(encoded)}}, nil
	default:
		return nil, fmt.Errorf("a value of type %T has no form a provider reads", value)
	}
}

// integerValue writes a Go integer: a signed type as INT64, an unsigned type
// as UINT64.
func integerValue(value any) *protocol.DataValue {
	signed := reflect.ValueOf(value)
	if signed.CanInt() {
		return &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: signed.Int()}}
	}
	return &protocol.DataValue{Value: &protocol.DataValue_Uint64Value{Uint64Value: signed.Uint()}}
}

// hostValue reads a provider value into the form the row diff compares: the
// Go value of each common case, a UTC time for a timestamp, and a
// providerValue for the cases Go has no plain value for.
func hostValue(value *protocol.DataValue) (any, error) {
	if err := provider.ValidateDataValue(value); err != nil {
		return nil, err
	}
	switch v := value.GetValue().(type) {
	case *protocol.DataValue_Null:
		return nil, nil
	case *protocol.DataValue_BoolValue:
		return v.BoolValue, nil
	case *protocol.DataValue_Int64Value:
		return v.Int64Value, nil
	case *protocol.DataValue_Uint64Value:
		return v.Uint64Value, nil
	case *protocol.DataValue_DoubleValue:
		return v.DoubleValue, nil
	case *protocol.DataValue_StringValue:
		return v.StringValue, nil
	case *protocol.DataValue_BytesValue:
		return slices.Clone(v.BytesValue), nil
	case *protocol.DataValue_TimestampValue:
		return time.Unix(v.TimestampValue.GetSeconds(), int64(v.TimestampValue.GetNanos())).UTC(), nil
	case *protocol.DataValue_DecimalValue, *protocol.DataValue_DateValue, *protocol.DataValue_IntervalValue,
		*protocol.DataValue_JsonValue, *protocol.DataValue_NativeValue:
		return providerValue{value: value}, nil
	default:
		return nil, errors.New("a provider value carries no case")
	}
}

// providerValue holds a provider value Go has no plain value for: a decimal,
// a date, an interval, JSON text and a native value. It compares, prints and
// encodes as its canonical text, so two values the provider returned in one
// canonical form are equal, and it goes back to the provider unchanged.
type providerValue struct{ value *protocol.DataValue }

// String is the canonical text the row diff compares.
func (v providerValue) String() string {
	switch value := v.value.GetValue().(type) {
	case *protocol.DataValue_DecimalValue:
		return decimalText(value.DecimalValue)
	case *protocol.DataValue_DateValue:
		return time.Unix(value.DateValue*86400, 0).UTC().Format(time.DateOnly)
	case *protocol.DataValue_IntervalValue:
		interval := value.IntervalValue
		return fmt.Sprintf("P%dM%dDT%dN", interval.GetMonths(), interval.GetDays(), interval.GetNanoseconds())
	case *protocol.DataValue_JsonValue:
		return value.JsonValue
	case *protocol.DataValue_NativeValue:
		return value.NativeValue.GetType() + ":" + hex.EncodeToString(value.NativeValue.GetPayload())
	default:
		return ""
	}
}

// MarshalJSON encodes the canonical text, so a fingerprint of the rows
// changes when the value does.
func (v providerValue) MarshalJSON() ([]byte, error) { return json.Marshal(v.String()) }

// decimalText places the point scale digits from the right of the unscaled
// digits: 1250 with scale 2 is 12.50.
func decimalText(value *protocol.DecimalValue) string {
	digits, scale := value.GetDigits(), int(value.GetScale())
	negative := strings.HasPrefix(digits, "-")
	digits = strings.TrimPrefix(digits, "-")
	if scale > 0 {
		if len(digits) <= scale {
			digits = strings.Repeat("0", scale-len(digits)+1) + digits
		}
		digits = digits[:len(digits)-scale] + "." + digits[len(digits)-scale:]
	}
	if negative {
		return "-" + digits
	}
	return digits
}
