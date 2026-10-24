package provider_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

func dataFeatures() []string {
	return []string{provider.TypeRowsFeature, provider.ReadRowsFeature, provider.RenderRowWritesFeature, provider.SeedsFeature}
}

func dataDescription() *protocol.Description {
	rules := &protocol.IdentifierRules{TableNames: protocol.IdentifierRules_EXACT, ColumnNames: protocol.IdentifierRules_EXACT,
		IndexNames: protocol.IdentifierRules_EXACT, IndexNamespace: protocol.IdentifierRules_TABLE}
	return &protocol.Description{
		Revision: provider.Revision, ProviderId: "example.org/data", Version: "1", SemanticIdentity: "data-1",
		Features: dataFeatures(),
		Engines:  []*protocol.Engine{{Id: "rows", Identifiers: rules, Features: dataFeatures()}},
	}
}

func rowsTable() *protocol.Reference {
	return &protocol.Reference{Engine: "rows", Schema: "app", Kind: "table", Name: "rates"}
}

func text(value string) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: value}}
}

func decimal(digits string, precision, scale uint32) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_DecimalValue{DecimalValue: &protocol.DecimalValue{Digits: digits, Precision: precision, Scale: scale}}}
}

func null() *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}
}

func row(values ...*protocol.DataValue) *protocol.Row { return &protocol.Row{Values: values} }

func typeRowsExchange() (*protocol.TypeRowsRequest, *protocol.TypedRows) {
	request := &protocol.TypeRowsRequest{
		Engine: "rows", Table: rowsTable(),
		LiveTable: &protocol.LiveTable{Columns: []*protocol.Column{{Name: "code", Type: "Utf8"}, {Name: "rate", Type: "Decimal(10,2)"}}},
		Columns:   []*protocol.RowColumn{{Name: "code", DeclaredType: "VARCHAR(8)"}, {Name: "rate", DeclaredType: "DECIMAL(10,2)"}, {Name: "note"}},
		Declared:  []*protocol.Row{row(text("CZ"), text("1.50"), &protocol.DataValue{})},
		Live:      []*protocol.Row{row(text("CZ"), decimal("15", 10, 1), null())},
	}
	result := &protocol.TypedRows{
		Columns: []*protocol.DataColumn{
			{Name: "code", Type: "Utf8", Kind: protocol.DataKind_DATA_KIND_STRING},
			{Name: "rate", Type: "Decimal(10,2)", Kind: protocol.DataKind_DATA_KIND_DECIMAL},
			{Name: "note"},
		},
		Declared: []*protocol.Row{row(text("CZ"), decimal("150", 10, 2), &protocol.DataValue{})},
		Live:     []*protocol.Row{row(text("CZ"), decimal("150", 10, 2), null())},
	}
	return request, result
}

func TestDataServicesDescription_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.ValidateDescription(dataDescription(), nil), qt.IsNil)
}

func TestDataServicesDescription_FailurePath(t *testing.T) {
	for _, feature := range dataFeatures() {
		t.Run(feature, func(t *testing.T) {
			c := qt.New(t)
			description := dataDescription()
			description.Features = slices.DeleteFunc(description.Features, func(name string) bool { return name == feature })
			c.Assert(provider.ValidateDescription(description, nil), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateDataValue_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value *protocol.DataValue
	}{
		{"unset", &protocol.DataValue{}},
		{"null", null()},
		{"bool", &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: true}}},
		{"int64", &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: -5}}},
		{"uint64", &protocol.DataValue{Value: &protocol.DataValue_Uint64Value{Uint64Value: 5}}},
		{"double", &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: 1.5}}},
		{"string", text("Praha")},
		{"bytes", &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: []byte{0xff}}}},
		{"decimal", decimal("-1250", 10, 2)},
		{"decimal with leading zeros", decimal("0001250", 4, 2)},
		{"decimal without precision", decimal("12345678901234567890", 0, 0)},
		{"timestamp", &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{Seconds: -1, Nanos: 999999999}}}},
		{"date", &protocol.DataValue{Value: &protocol.DataValue_DateValue{DateValue: 20454}}},
		{"interval", &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{Months: 1, Days: -2, Nanoseconds: 3}}}},
		{"json", &protocol.DataValue{Value: &protocol.DataValue_JsonValue{JsonValue: `{"a":[1,2]}`}}},
		{"native", &protocol.DataValue{Value: &protocol.DataValue_NativeValue{NativeValue: &protocol.NativeValue{Type: "Uuid", Payload: []byte{1}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDataValue(test.value), qt.IsNil)
		})
	}
}

func TestValidateDataValue_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		value *protocol.DataValue
	}{
		{"string that is not UTF-8", text("\xff")},
		{"JSON that does not parse", &protocol.DataValue{Value: &protocol.DataValue_JsonValue{JsonValue: `{"a":`}}},
		{"decimal without digits", decimal("-", 10, 2)},
		{"decimal with a point", decimal("12.50", 10, 2)},
		{"decimal scale above its precision", decimal("1", 2, 3)},
		{"decimal digits above its precision", decimal("123", 2, 0)},
		{"timestamp nanoseconds below range", &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{Nanos: -1}}}},
		{"timestamp nanoseconds above range", &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{Nanos: 1e9}}}},
		{"native value without a type", &protocol.DataValue{Value: &protocol.DataValue_NativeValue{NativeValue: &protocol.NativeValue{Payload: []byte{1}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(provider.ValidateDataValue(test.value), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateTypedRows_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := typeRowsExchange()
	c.Assert(provider.ValidateTypedRows(dataDescription(), request, result), qt.IsNil)
	request.LiveTable, request.Live, result.Live = nil, nil, nil
	c.Assert(provider.ValidateTypedRows(dataDescription(), request, result), qt.IsNil)
}

func TestValidateTypeRowsRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.TypeRowsRequest)
	}{
		{"no service", func(d *protocol.Description, _ *protocol.TypeRowsRequest) { d.Features = d.Features[1:] }},
		{"engine without the service", func(d *protocol.Description, _ *protocol.TypeRowsRequest) {
			d.Engines[0].Features = d.Engines[0].Features[1:]
		}},
		{"unknown engine", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.Engine = "other" }},
		{"no table", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.Table = nil }},
		{"table that is not a table", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.Table.Kind = "view" }},
		{"repeated column", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.Columns[1].Name = "code" }},
		{"declared type that is not text", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.Columns[0].DeclaredType = "\xff" }},
		{"repeated live column", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.LiveTable.Columns[1].Name = "code" }},
		{"live column without a type", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.LiveTable.Columns[0].Type = "" }},
		{"declared row narrower than the columns", func(_ *protocol.Description, r *protocol.TypeRowsRequest) {
			r.Declared[0].Values = r.Declared[0].Values[:2]
		}},
		{"live row wider than the columns", func(_ *protocol.Description, r *protocol.TypeRowsRequest) {
			r.Live[0].Values = append(r.Live[0].Values, null())
		}},
		{"live row with an invalid value", func(_ *protocol.Description, r *protocol.TypeRowsRequest) { r.Live[0].Values[0] = text("\xff") }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request := dataDescription(), firstTypeRows()
			test.mutate(description, request)
			c.Assert(provider.ValidateTypeRowsRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	c.Assert(provider.ValidateTypeRowsRequest(dataDescription(), nil), qt.ErrorIs, provider.ErrProtocol)
}

func firstTypeRows() *protocol.TypeRowsRequest {
	request, _ := typeRowsExchange()
	return request
}

func TestValidateTypedRows_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.TypedRows)
	}{
		{"missing reply", nil},
		{"columns out of order", func(r *protocol.TypedRows) { r.Columns[0], r.Columns[1] = r.Columns[1], r.Columns[0] }},
		{"a column missing", func(r *protocol.TypedRows) { r.Columns = r.Columns[:2] }},
		{"a column added", func(r *protocol.TypedRows) { r.Columns = append(r.Columns, &protocol.DataColumn{Name: "extra"}) }},
		{"a type without a kind", func(r *protocol.TypedRows) { r.Columns[0].Kind = protocol.DataKind_DATA_KIND_UNSPECIFIED }},
		{"a kind without a type", func(r *protocol.TypedRows) { r.Columns[2].Kind = protocol.DataKind_DATA_KIND_STRING }},
		{"an unknown kind", func(r *protocol.TypedRows) { r.Columns[0].Kind = 99 }},
		{"a native type on another kind", func(r *protocol.TypedRows) { r.Columns[0].NativeType = "Uuid" }},
		{"a native column without a native type", func(r *protocol.TypedRows) { r.Columns[0].Kind = protocol.DataKind_DATA_KIND_NATIVE }},
		{"a declared row dropped", func(r *protocol.TypedRows) { r.Declared = nil }},
		{"a live row added", func(r *protocol.TypedRows) { r.Live = append(r.Live, r.Live[0]) }},
		{"a row narrower than the columns", func(r *protocol.TypedRows) { r.Declared[0].Values = r.Declared[0].Values[:2] }},
		{"a column the input left unset is set", func(r *protocol.TypedRows) { r.Declared[0].Values[2] = text("x") }},
		{"a column the input set is unset", func(r *protocol.TypedRows) { r.Live[0].Values[2] = &protocol.DataValue{} }},
		{"a value whose kind disagrees with its column", func(r *protocol.TypedRows) { r.Live[0].Values[1] = text("1.50") }},
		{"a native value of another native type", func(r *protocol.TypedRows) {
			r.Columns[1].Kind, r.Columns[1].NativeType = protocol.DataKind_DATA_KIND_NATIVE, "Decimal"
			r.Live[0].Values[1] = &protocol.DataValue{Value: &protocol.DataValue_NativeValue{NativeValue: &protocol.NativeValue{Type: "Uuid"}}}
			r.Declared[0].Values[1] = &protocol.DataValue{Value: &protocol.DataValue_NativeValue{NativeValue: &protocol.NativeValue{Type: "Decimal"}}}
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := typeRowsExchange()
			result = applyTyped(result, test.mutate)
			c.Assert(provider.ValidateTypedRows(dataDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// applyTyped applies mutate, or drops the reply when there is none.
func applyTyped(result *protocol.TypedRows, mutate func(*protocol.TypedRows)) *protocol.TypedRows {
	if mutate == nil {
		return nil
	}
	mutate(result)
	return result
}

func readRowsExchange() (*protocol.ReadRowsRequest, *protocol.RowPage) {
	request := &protocol.ReadRowsRequest{SessionId: "session", Table: rowsTable(), Columns: []string{"code", "rate"}, KeyColumns: []string{"code"}, MaxRows: 2}
	page := &protocol.RowPage{
		Table: rowsTable(), Exists: true, NextPageToken: "after:EE",
		Columns: []*protocol.DataColumn{{Name: "code", Type: "Utf8", Kind: protocol.DataKind_DATA_KIND_STRING}, {Name: "rate", Type: "Decimal(10,2)", Kind: protocol.DataKind_DATA_KIND_DECIMAL}},
		Rows:    []*protocol.Row{row(text("CZ"), decimal("150", 10, 2)), row(text("EE"), null())},
	}
	return request, page
}

func TestValidateRowPage_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.RowPage)
	}{
		{"a page with more to read", func(*protocol.RowPage) {}},
		{"the last page", func(p *protocol.RowPage) { p.Complete, p.NextPageToken = true, "" }},
		{"an empty table", func(p *protocol.RowPage) { p.Complete, p.NextPageToken, p.Rows = true, "", nil }},
		{"a missing table", func(p *protocol.RowPage) { *p = protocol.RowPage{Table: rowsTable(), Complete: true} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, page := readRowsExchange()
			test.mutate(page)
			c.Assert(provider.ValidateRowPage(request, page), qt.IsNil)
		})
	}
}

func TestValidateReadRowsRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.ReadRowsRequest)
	}{
		{"no session", func(r *protocol.ReadRowsRequest) { r.SessionId = "" }},
		{"no table", func(r *protocol.ReadRowsRequest) { r.Table = nil }},
		{"table without a name", func(r *protocol.ReadRowsRequest) { r.Table.Name = "" }},
		{"no columns", func(r *protocol.ReadRowsRequest) { r.Columns = nil }},
		{"repeated column", func(r *protocol.ReadRowsRequest) { r.Columns = []string{"code", "code"} }},
		{"empty column", func(r *protocol.ReadRowsRequest) { r.Columns = []string{"code", ""} }},
		{"no key", func(r *protocol.ReadRowsRequest) { r.KeyColumns = nil }},
		{"key outside the columns", func(r *protocol.ReadRowsRequest) { r.KeyColumns = []string{"id"} }},
		{"no page bound", func(r *protocol.ReadRowsRequest) { r.MaxRows = 0 }},
		{"page bound above the limit", func(r *protocol.ReadRowsRequest) { r.MaxRows = provider.MaxRowsPerPage + 1 }},
		{"page token too long", func(r *protocol.ReadRowsRequest) { r.PageToken = strings.Repeat("a", 4097) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, _ := readRowsExchange()
			test.mutate(request)
			c.Assert(provider.ValidateReadRowsRequest(request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func TestValidateRowPage_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.RowPage)
	}{
		{"another table", func(p *protocol.RowPage) { p.Table.Name = "other" }},
		{"complete with a next page token", func(p *protocol.RowPage) { p.Complete = true }},
		{"incomplete without a next page token", func(p *protocol.RowPage) { p.NextPageToken = "" }},
		{"more rows than the bound", func(p *protocol.RowPage) { p.Rows = append(p.Rows, p.Rows[0]) }},
		{"a missing table with rows", func(p *protocol.RowPage) { p.Exists, p.Complete, p.NextPageToken, p.Columns = false, true, "", nil }},
		{"a missing table with columns", func(p *protocol.RowPage) { p.Exists, p.Complete, p.NextPageToken, p.Rows = false, true, "", nil }},
		{"a missing table not read to the end", func(p *protocol.RowPage) { *p = protocol.RowPage{Table: rowsTable(), NextPageToken: "x"} }},
		{"columns that do not answer the request", func(p *protocol.RowPage) { p.Columns[1].Name = "note" }},
		{"a row narrower than the columns", func(p *protocol.RowPage) { p.Rows[0].Values = p.Rows[0].Values[:1] }},
		{"a row wider than the columns", func(p *protocol.RowPage) { p.Rows[0].Values = append(p.Rows[0].Values, null()) }},
		{"an unset value", func(p *protocol.RowPage) { p.Rows[1].Values[1] = &protocol.DataValue{} }},
		{"a value whose kind disagrees with its column", func(p *protocol.RowPage) { p.Rows[0].Values[1] = text("1.50") }},
		{"a column of an unknown kind", func(p *protocol.RowPage) {
			p.Complete, p.NextPageToken, p.Rows = true, "", nil
			p.Columns[0].Kind = 99
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, page := readRowsExchange()
			test.mutate(page)
			c.Assert(provider.ValidateRowPage(request, page), qt.ErrorIs, provider.ErrProtocol)
		})
	}
	c := qt.New(t)
	request, _ := readRowsExchange()
	c.Assert(provider.ValidateRowPage(request, nil), qt.ErrorIs, provider.ErrProtocol)
}

func rowWritesExchange() (*protocol.RenderRowWritesRequest, *protocol.RenderResult) {
	key, rate := text("CZ"), decimal("150", 10, 2)
	request := &protocol.RenderRowWritesRequest{Engine: "rows", Writes: []*protocol.RowWrite{
		{ChangeId: "insert", Mode: protocol.RowWrite_INSERT, Table: rowsTable(), Columns: []string{"code", "rate"}, Values: []*protocol.DataValue{key, rate}},
		{ChangeId: "upsert", Mode: protocol.RowWrite_UPSERT, Table: rowsTable(), Columns: []string{"code", "rate"}, KeyColumns: []string{"code"},
			Values: []*protocol.DataValue{key, rate}, ColumnTypes: map[string]string{"rate": "Decimal(10,2)"}},
		{ChangeId: "update", Mode: protocol.RowWrite_UPDATE_BY_KEY, Table: rowsTable(), Columns: []string{"code", "rate"}, KeyColumns: []string{"code"},
			Values: []*protocol.DataValue{key, null()}},
		{ChangeId: "delete", Mode: protocol.RowWrite_DELETE_BY_KEY, Table: rowsTable(), Columns: []string{"code"}, KeyColumns: []string{"code"},
			Values: []*protocol.DataValue{key}},
	}}
	result := &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{
		{ChangeId: "insert", Body: &protocol.Unit_Sql{Sql: "INSERT INTO rates (code, rate) VALUES ('CZ', 1.50)"}},
		{ChangeId: "upsert", Body: &protocol.Unit_Sql{Sql: "UPSERT INTO rates (code, rate) VALUES ('CZ', 1.50)"}},
		{ChangeId: "update", Body: &protocol.Unit_Sql{Sql: "UPDATE rates SET rate = NULL WHERE code = 'CZ'"}},
		{ChangeId: "delete", Body: &protocol.Unit_Native{Native: &protocol.Envelope{ProviderId: "example.org/data", Kind: "delete", SchemaVersion: 1, Encoding: "json", Owner: rowsTable()}}},
	}}
	return request, result
}

func TestValidateRenderedRowWrites_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := rowWritesExchange()
	c.Assert(provider.ValidateRenderedRowWrites(dataDescription(), request, result), qt.IsNil)
	refused := &protocol.RenderResult{Refusal: &protocol.RenderRefusal{ChangeId: "upsert", Reason: protocol.RenderRefusal_UNSUPPORTED_MODE, Message: "the engine has no upsert"}}
	c.Assert(provider.ValidateRenderedRowWrites(dataDescription(), request, refused), qt.IsNil)
}

func TestValidateRenderRowWritesRequest_FailurePath(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*protocol.Description, *protocol.RenderRowWritesRequest)
	}{
		{"engine without the service", func(d *protocol.Description, _ *protocol.RenderRowWritesRequest) { d.Engines[0].Features = nil }},
		{"no change ID", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) { r.Writes[0].ChangeId = "" }},
		{"repeated change ID", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) { r.Writes[1].ChangeId = "insert" }},
		{"unspecified mode on a keyed write", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[1].Mode = protocol.RowWrite_MODE_UNSPECIFIED
		}},
		{"unknown mode on a keyed write", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) { r.Writes[1].Mode = 99 }},
		{"no table", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) { r.Writes[0].Table = nil }},
		{"no columns", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[0].Columns, r.Writes[0].Values = nil, nil
		}},
		{"an upsert without a key", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) { r.Writes[1].KeyColumns = nil }},
		{"a key outside the columns", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[2].KeyColumns = []string{"id"}
		}},
		{"a delete that writes a column outside its key", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[3].Columns, r.Writes[3].Values = []string{"code", "rate"}, []*protocol.DataValue{text("CZ"), null()}
		}},
		{"an update that writes only its key", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[2].Columns, r.Writes[2].Values = []string{"code"}, []*protocol.DataValue{text("CZ")}
		}},
		{"values narrower than the columns", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[0].Values = r.Writes[0].Values[:1]
		}},
		{"an unset value", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[0].Values[1] = &protocol.DataValue{}
		}},
		{"a type for a column the write does not name", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[1].ColumnTypes["note"] = "Utf8"
		}},
		{"an empty column type", func(_ *protocol.Description, r *protocol.RenderRowWritesRequest) {
			r.Writes[1].ColumnTypes["rate"] = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, request := dataDescription(), firstRowWrites()
			test.mutate(description, request)
			c.Assert(provider.ValidateRenderRowWritesRequest(description, request), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

func firstRowWrites() *protocol.RenderRowWritesRequest {
	request, _ := rowWritesExchange()
	return request
}

func TestValidateRenderedRowWrites_FailurePath(t *testing.T) {
	refusal := func() *protocol.RenderRefusal {
		return &protocol.RenderRefusal{ChangeId: "upsert", Reason: protocol.RenderRefusal_UNSUPPORTED_MODE, Message: "the engine has no upsert"}
	}
	tests := []struct {
		name   string
		mutate func(*protocol.RenderResult)
	}{
		{"incomplete without a refusal", func(r *protocol.RenderResult) { r.Complete = false }},
		{"a unit missing", func(r *protocol.RenderResult) { r.Units = r.Units[:3] }},
		{"a unit added", func(r *protocol.RenderResult) {
			r.Units = append(r.Units, &protocol.Unit{ChangeId: "extra", Body: &protocol.Unit_Sql{Sql: "DELETE FROM rates"}})
		}},
		{"units out of order", func(r *protocol.RenderResult) { r.Units[0], r.Units[1] = r.Units[1], r.Units[0] }},
		{"an empty SQL unit", func(r *protocol.RenderResult) { r.Units[0].Body = &protocol.Unit_Sql{Sql: " "} }},
		{"a native unit for another table", func(r *protocol.RenderResult) { r.Units[3].GetNative().Owner.Name = "other" }},
		{"a native unit with no table", func(r *protocol.RenderResult) { r.Units[3].GetNative().Owner = nil }},
		{"a refusal beside units", func(r *protocol.RenderResult) { r.Complete, r.Refusal = false, refusal() }},
		{"a complete batch with a refusal", func(r *protocol.RenderResult) { r.Refusal = refusal() }},
		{"a refusal of a write the request does not hold", func(r *protocol.RenderResult) {
			*r = protocol.RenderResult{Refusal: refusal()}
			r.Refusal.ChangeId = "other"
		}},
		{"a refusal without a reason", func(r *protocol.RenderResult) {
			*r = protocol.RenderResult{Refusal: refusal()}
			r.Refusal.Reason = protocol.RenderRefusal_REASON_UNSPECIFIED
		}},
		{"a refusal of two lines", func(r *protocol.RenderResult) {
			*r = protocol.RenderResult{Refusal: refusal()}
			r.Refusal.Message = "no upsert\nvalue 'CZ'"
		}},
		{"a refusal without a message", func(r *protocol.RenderResult) {
			*r = protocol.RenderResult{Refusal: refusal()}
			r.Refusal.Message = ""
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := rowWritesExchange()
			test.mutate(result)
			c.Assert(provider.ValidateRenderedRowWrites(dataDescription(), request, result), qt.ErrorIs, provider.ErrProtocol)
		})
	}
}

// A schema render never carries a row-write refusal: only RenderRowWrites
// refuses that way.
func TestValidateRenderRefusesARowWriteRefusal(t *testing.T) {
	c := qt.New(t)
	request := &protocol.RenderRequest{Engine: "rows", Changes: []*protocol.Change{{Id: "one"}}}
	result := &protocol.RenderResult{Complete: true, Units: []*protocol.Unit{{ChangeId: "one", Body: &protocol.Unit_Sql{Sql: "SELECT 1"}}}}
	c.Assert(provider.ValidateRender(request, result), qt.IsNil)
	result.Refusal = &protocol.RenderRefusal{ChangeId: "one", Reason: protocol.RenderRefusal_UNSUPPORTED_MODE, Message: "no"}
	c.Assert(provider.ValidateRender(request, result), qt.ErrorIs, provider.ErrProtocol)
}
