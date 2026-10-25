package provider

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"
)

// The row services. The host keeps the generic algorithm: which rows are
// declared, the row diff, ordering by foreign keys, protected tables and saved
// plans. The provider owns what depends on the engine: how a value is typed,
// how a row is read back and how a write is spelled. Each service is behind a
// feature of its own, which each supporting engine also declares, and a host
// refuses a row set for an engine whose provider lacks one rather than write
// common text in its place.
const (
	// TypeRowsFeature advertises offline typing of declared and live rows.
	TypeRowsFeature = "data.type-rows"
	// ReadRowsFeature advertises paged reads of table rows on a live session.
	ReadRowsFeature = "data.read-rows"
	// RenderRowWritesFeature advertises offline rendering of row writes into
	// executable units.
	RenderRowWritesFeature = "data.render-writes"
)

// MaxRowsPerPage bounds the rows one ReadRows page may hold. A page also stays
// within the transport's message limit.
const MaxRowsPerPage = 10000

// maxPageToken bounds a page token, which the provider defines.
const maxPageToken = 4096

// RowTyper types the rows of one table offline, as a comparison pairs them.
// Both row sets come back in one canonical form, so a value read back in
// another spelling pairs with its declaration.
type RowTyper interface {
	TypeRows(context.Context, *protocol.TypeRowsRequest) (*protocol.TypedRows, error)
}

// RowReader reads table rows on a live session, in key order and in pages.
// A read that cannot finish is an error and no page.
type RowReader interface {
	ReadRows(context.Context, *protocol.ReadRowsRequest) (*protocol.RowPage, error)
}

// RowWriteRenderer renders typed row writes offline: one executable unit per
// write in input order, or a refusal and no unit. Rendering is deterministic,
// so a saved plan and a migration file cannot disagree about a row.
type RowWriteRenderer interface {
	RenderRowWrites(context.Context, *protocol.RenderRowWritesRequest) (*protocol.RenderResult, error)
}

// validateDataServices checks that an engine names only the data services the
// provider serves.
func validateDataServices(description *protocol.Description, engine *protocol.Engine) error {
	for _, feature := range []string{TypeRowsFeature, ReadRowsFeature, RenderRowWritesFeature, SeedsFeature} {
		if slices.Contains(engine.GetFeatures(), feature) && !slices.Contains(description.GetFeatures(), feature) {
			return fmt.Errorf("%w: engine %s requires the service", ErrProtocol, feature)
		}
	}
	return nil
}

func dataEngine(description *protocol.Description, feature, name string, targetFeatures []string) error {
	if !slices.Contains(description.GetFeatures(), feature) {
		return fmt.Errorf("%w: %s is unavailable", ErrProtocol, feature)
	}
	engine, _, err := valueTarget(description, name, targetFeatures)
	if err != nil {
		return err
	}
	if !slices.Contains(engine.GetFeatures(), feature) {
		return fmt.Errorf("%w: engine %s is unavailable", ErrProtocol, feature)
	}
	return nil
}

// ValidateDataValue refuses a value no reader could pair: text that is not
// UTF-8, JSON text that does not parse, a decimal that is not digits or whose
// scale or digit count exceeds its precision, a timestamp whose nanoseconds
// leave their range, and a native value without a type. An unset value is
// valid here; the message that holds it decides whether a row may omit a
// column.
func ValidateDataValue(value *protocol.DataValue) error {
	switch v := value.GetValue().(type) {
	case nil, *protocol.DataValue_Null, *protocol.DataValue_BoolValue, *protocol.DataValue_Int64Value,
		*protocol.DataValue_Uint64Value, *protocol.DataValue_DoubleValue, *protocol.DataValue_BytesValue,
		*protocol.DataValue_DateValue, *protocol.DataValue_IntervalValue:
		return nil
	case *protocol.DataValue_StringValue:
		if !utf8.ValidString(v.StringValue) {
			return fmt.Errorf("%w: a string value is not UTF-8", ErrProtocol)
		}
	case *protocol.DataValue_JsonValue:
		if !json.Valid([]byte(v.JsonValue)) {
			return fmt.Errorf("%w: a JSON value does not parse", ErrProtocol)
		}
	case *protocol.DataValue_DecimalValue:
		return validateDecimal(v.DecimalValue)
	case *protocol.DataValue_TimestampValue:
		if nanos := v.TimestampValue.GetNanos(); nanos < 0 || nanos >= 1e9 {
			return fmt.Errorf("%w: a timestamp value's nanoseconds are out of range", ErrProtocol)
		}
	case *protocol.DataValue_NativeValue:
		if !comparisonReason(v.NativeValue.GetType()) {
			return fmt.Errorf("%w: a native value needs a type", ErrProtocol)
		}
	default:
		return fmt.Errorf("%w: unknown data value", ErrProtocol)
	}
	return nil
}

func validateDecimal(value *protocol.DecimalValue) error {
	digits := strings.TrimPrefix(value.GetDigits(), "-")
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return fmt.Errorf("%w: a decimal value is not digits", ErrProtocol)
	}
	precision := value.GetPrecision()
	if precision != 0 && (value.GetScale() > precision || uint64(len(strings.TrimLeft(digits, "0"))) > uint64(precision)) {
		return fmt.Errorf("%w: a decimal value exceeds its precision", ErrProtocol)
	}
	return nil
}

// dataKind is the kind a set value takes.
func dataKind(value *protocol.DataValue) protocol.DataKind {
	switch value.GetValue().(type) {
	case *protocol.DataValue_BoolValue:
		return protocol.DataKind_DATA_KIND_BOOL
	case *protocol.DataValue_Int64Value:
		return protocol.DataKind_DATA_KIND_INT64
	case *protocol.DataValue_Uint64Value:
		return protocol.DataKind_DATA_KIND_UINT64
	case *protocol.DataValue_DoubleValue:
		return protocol.DataKind_DATA_KIND_DOUBLE
	case *protocol.DataValue_StringValue:
		return protocol.DataKind_DATA_KIND_STRING
	case *protocol.DataValue_BytesValue:
		return protocol.DataKind_DATA_KIND_BYTES
	case *protocol.DataValue_DecimalValue:
		return protocol.DataKind_DATA_KIND_DECIMAL
	case *protocol.DataValue_TimestampValue:
		return protocol.DataKind_DATA_KIND_TIMESTAMP
	case *protocol.DataValue_DateValue:
		return protocol.DataKind_DATA_KIND_DATE
	case *protocol.DataValue_IntervalValue:
		return protocol.DataKind_DATA_KIND_INTERVAL
	case *protocol.DataValue_JsonValue:
		return protocol.DataKind_DATA_KIND_JSON
	case *protocol.DataValue_NativeValue:
		return protocol.DataKind_DATA_KIND_NATIVE
	default:
		return protocol.DataKind_DATA_KIND_UNSPECIFIED
	}
}

// requireColumns refuses an operation that names no column.
func requireColumns(names []string) error {
	if len(names) == 0 {
		return fmt.Errorf("%w: a row operation names no column", ErrProtocol)
	}
	return validateColumnNames(names)
}

// validateColumnNames refuses an empty name and a name listed twice.
func validateColumnNames(names []string) error {
	for i, name := range names {
		if name == "" || !utf8.ValidString(name) || slices.Contains(names[:i], name) {
			return fmt.Errorf("%w: empty, invalid, or repeated column name", ErrProtocol)
		}
	}
	return nil
}

// requireKeyColumns refuses a keyed operation that names no key column.
func requireKeyColumns(keys, columns []string) error {
	if len(keys) == 0 {
		return fmt.Errorf("%w: a keyed row operation names no key column", ErrProtocol)
	}
	return validateKeyColumns(keys, columns)
}

// validateKeyColumns requires key columns that the column list also names.
func validateKeyColumns(keys, columns []string) error {
	if err := validateColumnNames(keys); err != nil {
		return err
	}
	for _, key := range keys {
		if !slices.Contains(columns, key) {
			return fmt.Errorf("%w: a key column is not one of the row's columns", ErrProtocol)
		}
	}
	return nil
}

// validateRowTable requires a table reference with a name. A row operation
// names its table by reference, never by SQL text.
func validateRowTable(table *protocol.Reference) error {
	if table == nil || table.GetName() == "" || table.GetKind() != "table" {
		return fmt.Errorf("%w: a row operation needs a table reference", ErrProtocol)
	}
	return nil
}

// validateRows checks that each row has one value per column, and that every
// value is valid.
func validateRows(rows []*protocol.Row, width int) error {
	for _, row := range rows {
		if len(row.GetValues()) != width {
			return fmt.Errorf("%w: a row's values do not align with its columns", ErrProtocol)
		}
		for _, value := range row.GetValues() {
			if err := ValidateDataValue(value); err != nil {
				return err
			}
		}
	}
	return nil
}

// requireSetValues refuses a row that leaves a column unset, where every
// column must carry a value or null.
func requireSetValues(rows []*protocol.Row) error {
	for _, row := range rows {
		if slices.ContainsFunc(row.GetValues(), func(value *protocol.DataValue) bool { return value.GetValue() == nil }) {
			return fmt.Errorf("%w: a row leaves a column unset", ErrProtocol)
		}
	}
	return nil
}

// validateTypedColumns checks the typed columns against the names the request
// asked for, in order, and each value against its column's kind. A column
// with no type has no kind, and its values are not checked against one.
func validateTypedColumns(columns []*protocol.DataColumn, names []string) error {
	if len(columns) != len(names) {
		return fmt.Errorf("%w: typed columns do not answer the requested columns", ErrProtocol)
	}
	for i, column := range columns {
		if column.GetName() != names[i] {
			return fmt.Errorf("%w: typed columns do not answer the requested columns", ErrProtocol)
		}
		if err := validateColumnType(column); err != nil {
			return err
		}
	}
	return nil
}

// validateColumnType checks that a column has a type and a kind or neither,
// and a native type exactly when its kind is native.
func validateColumnType(column *protocol.DataColumn) error {
	_, known := protocol.DataKind_name[int32(column.GetKind())]
	typed := column.GetType() != ""
	unspecified := column.GetKind() == protocol.DataKind_DATA_KIND_UNSPECIFIED
	if !known || typed == unspecified || typed && !utf8.ValidString(column.GetType()) {
		return fmt.Errorf("%w: a typed column needs a type and a kind, or neither", ErrProtocol)
	}
	native := column.GetKind() == protocol.DataKind_DATA_KIND_NATIVE
	if native != (column.GetNativeType() != "") || native && !comparisonReason(column.GetNativeType()) {
		return fmt.Errorf("%w: only a native column names a native type", ErrProtocol)
	}
	return nil
}

// validateRowKinds checks every set, non-null value against its column.
func validateRowKinds(rows []*protocol.Row, columns []*protocol.DataColumn) error {
	for _, row := range rows {
		for i, value := range row.GetValues() {
			column := columns[i]
			switch value.GetValue().(type) {
			case nil, *protocol.DataValue_Null:
				continue
			}
			if column.GetKind() == protocol.DataKind_DATA_KIND_UNSPECIFIED {
				continue
			}
			if dataKind(value) != column.GetKind() ||
				column.GetKind() == protocol.DataKind_DATA_KIND_NATIVE && value.GetNativeValue().GetType() != column.GetNativeType() {
				return fmt.Errorf("%w: a value's kind disagrees with its column type", ErrProtocol)
			}
		}
	}
	return nil
}

// ValidateTypeRowsRequest checks the negotiated service, the table, the
// columns the rows align with, and the rows.
func ValidateTypeRowsRequest(description *protocol.Description, request *protocol.TypeRowsRequest) error {
	if request == nil {
		return fmt.Errorf("%w: row typing request is missing", ErrProtocol)
	}
	if err := dataEngine(description, TypeRowsFeature, request.GetEngine(), request.GetTargetFeatures()); err != nil {
		return err
	}
	if err := validateRowTable(request.GetTable()); err != nil {
		return err
	}
	names := make([]string, len(request.GetColumns()))
	for i, column := range request.GetColumns() {
		names[i] = column.GetName()
		if !utf8.ValidString(column.GetDeclaredType()) {
			return fmt.Errorf("%w: a declared column type is not text", ErrProtocol)
		}
	}
	if err := validateColumnNames(names); err != nil {
		return err
	}
	if live := request.GetLiveTable(); live != nil {
		seen := make([]string, 0, len(live.GetColumns()))
		for _, column := range live.GetColumns() {
			if column.GetName() == "" || column.GetType() == "" || slices.Contains(seen, column.GetName()) {
				return fmt.Errorf("%w: malformed or duplicate live column", ErrProtocol)
			}
			seen = append(seen, column.GetName())
		}
	}
	if err := validateRows(request.GetDeclared(), len(names)); err != nil {
		return err
	}
	return validateRows(request.GetLive(), len(names))
}

// ValidateTypedRows refuses a reply whose columns do not answer the request's
// in order, whose row counts differ from the request's, whose rows set a
// column the input row left unset or the reverse, and a value whose kind
// disagrees with its column type.
func ValidateTypedRows(description *protocol.Description, request *protocol.TypeRowsRequest, result *protocol.TypedRows) error {
	if err := ValidateTypeRowsRequest(description, request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: typed rows are missing", ErrProtocol)
	}
	names := make([]string, len(request.GetColumns()))
	for i, column := range request.GetColumns() {
		names[i] = column.GetName()
	}
	if err := validateTypedColumns(result.GetColumns(), names); err != nil {
		return err
	}
	for _, set := range []struct{ input, output []*protocol.Row }{
		{request.GetDeclared(), result.GetDeclared()}, {request.GetLive(), result.GetLive()},
	} {
		if len(set.output) != len(set.input) {
			return fmt.Errorf("%w: typed rows do not answer every input row", ErrProtocol)
		}
		if err := validateRows(set.output, len(names)); err != nil {
			return err
		}
		for i, row := range set.output {
			for j, value := range row.GetValues() {
				if (value.GetValue() == nil) != (set.input[i].GetValues()[j].GetValue() == nil) {
					return fmt.Errorf("%w: typed rows change which columns a row carries", ErrProtocol)
				}
			}
		}
		if err := validateRowKinds(set.output, result.GetColumns()); err != nil {
			return err
		}
	}
	return nil
}

// ValidateReadRowsRequest refuses a read without a session, a table, columns,
// a key among them or a page bound.
func ValidateReadRowsRequest(request *protocol.ReadRowsRequest) error {
	if request.GetSessionId() == "" {
		return fmt.Errorf("%w: a row read requires a session", ErrProtocol)
	}
	if err := validateRowTable(request.GetTable()); err != nil {
		return err
	}
	if err := requireColumns(request.GetColumns()); err != nil {
		return err
	}
	if err := requireKeyColumns(request.GetKeyColumns(), request.GetColumns()); err != nil {
		return err
	}
	if len(request.GetPageToken()) > maxPageToken || !utf8.ValidString(request.GetPageToken()) {
		return fmt.Errorf("%w: invalid page token", ErrProtocol)
	}
	if request.GetMaxRows() == 0 || request.GetMaxRows() > MaxRowsPerPage {
		return fmt.Errorf("%w: a row read requires a page bound of at most %d rows", ErrProtocol, MaxRowsPerPage)
	}
	return nil
}

// ValidateRowPage binds a page to the requested table and columns. It refuses
// more rows than the bound, a row that does not hold a set value for every
// column, a value whose kind disagrees with its column type, a completion
// that disagrees with the next page token, and rows from a missing table.
func ValidateRowPage(request *protocol.ReadRowsRequest, result *protocol.RowPage) error {
	if err := ValidateReadRowsRequest(request); err != nil {
		return err
	}
	if result == nil || !proto.Equal(result.GetTable(), request.GetTable()) {
		return fmt.Errorf("%w: row page answers another table", ErrProtocol)
	}
	if result.GetComplete() != (result.GetNextPageToken() == "") ||
		len(result.GetNextPageToken()) > maxPageToken || !utf8.ValidString(result.GetNextPageToken()) {
		return fmt.Errorf("%w: row page completion disagrees with its next page token", ErrProtocol)
	}
	if len(result.GetRows()) > int(request.GetMaxRows()) {
		return fmt.Errorf("%w: row page holds more rows than its bound", ErrProtocol)
	}
	if !result.GetExists() {
		if !result.GetComplete() || len(result.GetRows()) != 0 || len(result.GetColumns()) != 0 {
			return fmt.Errorf("%w: a missing table returned rows or columns", ErrProtocol)
		}
		return nil
	}
	if err := validateTypedColumns(result.GetColumns(), request.GetColumns()); err != nil {
		return err
	}
	if err := validateRows(result.GetRows(), len(request.GetColumns())); err != nil {
		return err
	}
	if err := requireSetValues(result.GetRows()); err != nil {
		return err
	}
	return validateRowKinds(result.GetRows(), result.GetColumns())
}

// ValidateRenderRowWritesRequest checks the negotiated service and each write:
// a unique change ID, a known mode, a table reference, its columns and key
// columns, one set value per column, and column types only for columns the
// write names. A keyed mode needs a key; a delete writes its key alone, and an
// update writes at least one column outside it.
func ValidateRenderRowWritesRequest(description *protocol.Description, request *protocol.RenderRowWritesRequest) error {
	if request == nil {
		return fmt.Errorf("%w: row write request is missing", ErrProtocol)
	}
	if err := dataEngine(description, RenderRowWritesFeature, request.GetEngine(), request.GetTargetFeatures()); err != nil {
		return err
	}
	// dataEngine resolved the engine.
	engine, _, _ := valueTarget(description, request.GetEngine(), request.GetTargetFeatures())
	if err := validateUnitForm(description, engine, request.GetForm()); err != nil {
		return err
	}
	ids := make(map[string]bool, len(request.GetWrites()))
	for _, write := range request.GetWrites() {
		if write.GetChangeId() == "" || ids[write.GetChangeId()] {
			return fmt.Errorf("%w: a row write needs a unique change ID", ErrProtocol)
		}
		ids[write.GetChangeId()] = true
		if err := validateRowWrite(write); err != nil {
			return err
		}
	}
	return nil
}

func validateRowWrite(write *protocol.RowWrite) error {
	if err := validateRowTable(write.GetTable()); err != nil {
		return err
	}
	if err := requireColumns(write.GetColumns()); err != nil {
		return err
	}
	mode := write.GetMode()
	switch mode {
	case protocol.RowWrite_INSERT, protocol.RowWrite_UPSERT, protocol.RowWrite_UPDATE_BY_KEY, protocol.RowWrite_DELETE_BY_KEY:
	default:
		return fmt.Errorf("%w: unknown row write mode", ErrProtocol)
	}
	keys := validateKeyColumns
	if mode != protocol.RowWrite_INSERT {
		keys = requireKeyColumns
	}
	if err := keys(write.GetKeyColumns(), write.GetColumns()); err != nil {
		return err
	}
	if mode == protocol.RowWrite_DELETE_BY_KEY && len(write.GetColumns()) != len(write.GetKeyColumns()) {
		return fmt.Errorf("%w: a delete by key writes its key columns alone", ErrProtocol)
	}
	if mode == protocol.RowWrite_UPDATE_BY_KEY && len(write.GetColumns()) == len(write.GetKeyColumns()) {
		return fmt.Errorf("%w: an update by key writes no column outside its key", ErrProtocol)
	}
	written := []*protocol.Row{{Values: write.GetValues()}}
	if err := validateRows(written, len(write.GetColumns())); err != nil {
		return err
	}
	if err := requireSetValues(written); err != nil {
		return err
	}
	for column, kind := range write.GetColumnTypes() {
		if !slices.Contains(write.GetColumns(), column) || kind == "" || !utf8.ValidString(kind) {
			return fmt.Errorf("%w: a column type names a column the write does not", ErrProtocol)
		}
	}
	return nil
}

// ValidateRenderedRowWrites accepts either a complete batch with one unit per
// write, in order, each bound to its write's change ID, or a refusal with no
// unit that names a write of the request and why. A native unit must name the
// write's table as its owner: a unit for a table the request did not name is a
// protocol violation. Text units are not parsed, so their target is the
// owner's promise.
func ValidateRenderedRowWrites(description *protocol.Description, request *protocol.RenderRowWritesRequest, result *protocol.RenderResult) error {
	if err := ValidateRenderRowWritesRequest(description, request); err != nil {
		return err
	}
	if result == nil {
		return fmt.Errorf("%w: row write reply is missing", ErrProtocol)
	}
	if refusal := result.GetRefusal(); refusal != nil {
		if result.GetComplete() || len(result.GetUnits()) != 0 {
			return fmt.Errorf("%w: a refused row write batch exposes units", ErrProtocol)
		}
		named := slices.ContainsFunc(request.GetWrites(), func(write *protocol.RowWrite) bool { return write.GetChangeId() == refusal.GetChangeId() })
		reason := refusal.GetReason() == protocol.RenderRefusal_UNSUPPORTED_MODE || refusal.GetReason() == protocol.RenderRefusal_UNSUPPORTED_VALUE ||
			(refusal.GetReason() == protocol.RenderRefusal_NO_TEXT_FORM && request.GetForm() == protocol.RenderRequest_TEXT)
		if !named || !reason || !diagnosticLine(refusal.GetMessage()) {
			return fmt.Errorf("%w: a row write refusal needs a write, a reason, and one line of text", ErrProtocol)
		}
		return nil
	}
	if !result.GetComplete() || len(result.GetUnits()) != len(request.GetWrites()) {
		return fmt.Errorf("%w: incomplete row write render", ErrProtocol)
	}
	if err := validateTextUnits(request.GetForm(), result.GetUnits()); err != nil {
		return err
	}
	for i, write := range request.GetWrites() {
		unit := result.GetUnits()[i]
		if unit.GetChangeId() != write.GetChangeId() {
			return fmt.Errorf("%w: row write unit identity mismatch", ErrProtocol)
		}
		if err := ValidateUnit(unit); err != nil {
			return err
		}
		if native := unit.GetNative(); native != nil && !proto.Equal(native.GetOwner(), write.GetTable()) {
			return fmt.Errorf("%w: a row write unit names a table the write does not", ErrProtocol)
		}
	}
	return nil
}
