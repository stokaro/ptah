package providerhost

// White-box testing required: the conversions between the row diff's values
// and the provider's DataValue decide whether a re-run plans nothing, and the
// end-to-end path reaches only the cases the fixture's SQLite types produce.

import (
	"encoding/json"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"google.golang.org/protobuf/proto"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/query"
)

// A value of each common case goes to the provider and comes back as the
// value the row diff compares.
func TestRowValuesRoundTrip(t *testing.T) {
	moment := time.Date(2026, time.January, 2, 3, 4, 5, 6, time.FixedZone("CET", 3600))
	for _, row := range []struct {
		name      string
		value     any
		wire      *protocol.DataValue
		readsBack any
	}{
		{"null", nil, &protocol.DataValue{Value: &protocol.DataValue_Null{Null: &protocol.Empty{}}}, nil},
		{"bool", true, &protocol.DataValue{Value: &protocol.DataValue_BoolValue{BoolValue: true}}, true},
		{"int", 7, &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: 7}}, int64(7)},
		{"int32", int32(-3), &protocol.DataValue{Value: &protocol.DataValue_Int64Value{Int64Value: -3}}, int64(-3)},
		{"uint64", uint64(1) << 63, &protocol.DataValue{Value: &protocol.DataValue_Uint64Value{Uint64Value: 1 << 63}}, uint64(1) << 63},
		{"float32", float32(1.5), &protocol.DataValue{Value: &protocol.DataValue_DoubleValue{DoubleValue: 1.5}}, 1.5},
		{"string", "Norway", &protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: "Norway"}}, "Norway"},
		{"bytes", []byte{0, 255}, &protocol.DataValue{Value: &protocol.DataValue_BytesValue{BytesValue: []byte{0, 255}}}, []byte{0, 255}},
		{"time", moment, &protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{Seconds: moment.Unix(), Nanos: 6}}}, moment.UTC()},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			wire, err := wireValue(row.value)
			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(wire, row.wire), qt.IsTrue, qt.Commentf("%v", wire))
			back, err := hostValue(wire)
			c.Assert(err, qt.IsNil)
			c.Assert(back, qt.DeepEquals, row.readsBack)
		})
	}
}

// A timestamp reads back in UTC, so a value the provider returns compares and
// prints the same on every host. DeepEquals compares instants, which cannot
// see the location.
func TestRowTimestampReadsBackInUTC(t *testing.T) {
	c := qt.New(t)
	back, err := hostValue(&protocol.DataValue{Value: &protocol.DataValue_TimestampValue{TimestampValue: &protocol.TimestampValue{Seconds: 86400}}})
	c.Assert(err, qt.IsNil)
	c.Assert(back.(time.Time).Location(), qt.Equals, time.UTC)
}

// A map or a slice a declaration holds is JSON text to the provider, and comes
// back as that text.
func TestRowValuesWriteObjectsAsJSON(t *testing.T) {
	c := qt.New(t)
	wire, err := wireValue(map[string]any{"a": 1, "b": []any{true}})
	c.Assert(err, qt.IsNil)
	c.Assert(wire.GetJsonValue(), qt.Equals, `{"a":1,"b":[true]}`)
	back, err := hostValue(wire)
	c.Assert(err, qt.IsNil)
	c.Assert(back.(providerValue).String(), qt.Equals, `{"a":1,"b":[true]}`)
}

// A value Go has no plain value for keeps the provider's value, compares and
// encodes by its canonical text, and goes back unchanged.
func TestProviderValuesKeepTheirCanonicalText(t *testing.T) {
	for _, row := range []struct {
		name string
		wire *protocol.DataValue
		text string
	}{
		{"decimal", &protocol.DataValue{Value: &protocol.DataValue_DecimalValue{DecimalValue: &protocol.DecimalValue{Digits: "1250", Precision: 10, Scale: 2}}}, "12.50"},
		{"small negative decimal", &protocol.DataValue{Value: &protocol.DataValue_DecimalValue{DecimalValue: &protocol.DecimalValue{Digits: "-5", Scale: 3}}}, "-0.005"},
		{"decimal without scale", &protocol.DataValue{Value: &protocol.DataValue_DecimalValue{DecimalValue: &protocol.DecimalValue{Digits: "7"}}}, "7"},
		{"date", &protocol.DataValue{Value: &protocol.DataValue_DateValue{DateValue: 20455}}, "2026-01-02"},
		{"interval", &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{Months: 1, Days: 2, Nanoseconds: 3}}}, "P1M2DT3N"},
		{"native", &protocol.DataValue{Value: &protocol.DataValue_NativeValue{NativeValue: &protocol.NativeValue{Type: "Uuid", Payload: []byte{1, 2}}}}, "Uuid:0102"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			back, err := hostValue(row.wire)
			c.Assert(err, qt.IsNil)
			c.Assert(back.(providerValue).String(), qt.Equals, row.text)
			encoded, err := json.Marshal(back)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, `"`+row.text+`"`)
			again, err := wireValue(back)
			c.Assert(err, qt.IsNil)
			c.Assert(proto.Equal(again, row.wire), qt.IsTrue)
		})
	}
}

func TestRowValues_FailurePath(t *testing.T) {
	c := qt.New(t)
	wire, err := wireValue(struct{}{})
	c.Assert(err, qt.ErrorMatches, `a value of type struct \{\} has no form a provider reads`)
	c.Assert(wire, qt.IsNil)
	back, err := hostValue(&protocol.DataValue{Value: &protocol.DataValue_StringValue{StringValue: "\xff"}})
	c.Assert(err, qt.ErrorMatches, `.*a string value is not UTF-8`)
	c.Assert(back, qt.IsNil)
}

// Every field of a declared query dialect reaches the builder's spelling
// under the descriptor's name.
func TestQuerySpellingReadsEveryField(t *testing.T) {
	c := qt.New(t)
	declared := &protocol.QueryDialect{
		Placeholder: "dollar-named",
		Quoting:     &protocol.QueryQuoting{Style: "yql", Bare: "never", Keywords: []string{"VALUE"}, Paths: true},
		Query: &protocol.QuerySpelling{ChecksCapabilities: true, UnsupportedJoins: []string{"right", "full"}, OffsetFetch: true,
			FetchOrderBy: "(SELECT NULL)", OffsetOnlyLimit: "-1", UnsignedBounds: true, Upsert: "upsert-into",
			UpsertRefusal: "no ON CONFLICT here", UpdateRefusal: "no plain UPDATE here"},
		DefaultCapabilities: map[string]bool{"returning_clause": false},
	}
	c.Assert(querySpelling(declared), qt.DeepEquals, query.Spelling{
		Placeholder: descriptor.PlaceholderDollarNamed,
		Quoting:     descriptor.Quoting{Style: descriptor.QuoteYQL, Bare: descriptor.BareNever, Keywords: []string{"VALUE"}, Paths: true},
		Query: descriptor.Query{ChecksCapabilities: true, UnsupportedJoins: []descriptor.Join{descriptor.JoinRight, descriptor.JoinFull},
			OffsetFetch: true, FetchOrderBy: "(SELECT NULL)", OffsetOnlyLimit: "-1", UnsignedBounds: true, Upsert: descriptor.UpsertInto,
			UpsertRefusal: "no ON CONFLICT here", UpdateRefusal: "no plain UPDATE here"},
		DefaultCapabilities: map[capability.Capability]bool{capability.ReturningClause: false},
	})
}
