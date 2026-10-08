package chschema_test

import (
	"encoding/json"
	"reflect"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

func tableFixture() *chschema.ObservedTable {
	return &chschema.ObservedTable{
		Engine: "ReplacingMergeTree(version)", OrderBy: "(tenant, id)", PrimaryKey: "tenant",
		PartitionBy: "toYYYYMM(created_at)", SampleBy: "cityHash64(tenant)",
		TTL: "created_at + INTERVAL 30 DAY", Settings: "index_granularity = 4096, allow_nullable_key = 1",
	}
}

func TestTableRepresentationAndCodecRoundTrips(t *testing.T) {
	for _, test := range []struct {
		name  string
		value *chschema.ObservedTable
	}{
		{"all properties", tableFixture()},
		{"inspected empty settings", &chschema.ObservedTable{Engine: "Memory"}},
		{"matching sort and primary keys", &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := test.value.Desired()
			projected, err := desired.Observed()
			c.Assert(err, qt.IsNil)
			c.Assert(projected, qt.DeepEquals, test.value)
			for index, value := range []schemaext.Value{desired, test.value} {
				codec := chschema.Codecs()[index]
				encoded, err := codec.Encode(value)
				c.Assert(err, qt.IsNil)
				decoded, err := codec.Decode(encoded)
				c.Assert(err, qt.IsNil)
				c.Assert(value.Equal(decoded.(schemaext.Value)), qt.IsTrue)
				canonical, err := codec.Canonical(value)
				c.Assert(err, qt.IsNil)
				c.Assert(canonical, qt.DeepEquals, encoded)
			}
		})
	}
}

func TestTableIntentStaysDistinct(t *testing.T) {
	c := qt.New(t)
	codec := chschema.Codecs()[0]
	var encodings []string
	for _, state := range []chschema.SettingState{chschema.Unspecified, chschema.Explicit, chschema.Default} {
		value := &chschema.DesiredTable{PrimaryKey: chschema.Setting{State: state}}
		data, err := codec.Encode(value)
		c.Assert(err, qt.IsNil)
		encodings = append(encodings, string(data))
		decoded, err := codec.Decode(data)
		c.Assert(err, qt.IsNil)
		c.Assert(value.Equal(decoded.(schemaext.Value)), qt.IsTrue)
	}
	c.Assert(encodings, qt.DeepEquals, []string{`{}`, `{"primary_key":{"state":"explicit"}}`, `{"primary_key":{"state":"default"}}`})
	value, err := codec.Decode([]byte(`{"primary_key":{"state":""}}`))
	c.Assert(err, qt.IsNil)
	canonical, err := codec.Canonical(value)
	c.Assert(err, qt.IsNil)
	c.Assert(string(canonical), qt.Equals, `{}`)
}

// Derive the field census from the public model so a newly added property must
// join conversion, independent cloning, validation, and the wire definition.
func TestEveryTablePropertyParticipates(t *testing.T) {
	observedType := reflect.TypeFor[chschema.ObservedTable]()
	desiredType := reflect.TypeFor[chschema.DesiredTable]()
	c := qt.New(t)
	c.Assert(observedType.NumField(), qt.Equals, desiredType.NumField())
	c.Assert(observedType.NumField() >= 7, qt.IsTrue)
	for field := range observedType.Fields() {
		t.Run(field.Name, func(t *testing.T) {
			c := qt.New(t)
			original := tableFixture()
			clone := original.Clone().(*chschema.ObservedTable)
			reflect.ValueOf(clone).Elem().FieldByName(field.Name).SetString("changed_" + field.Name)
			c.Assert(original.Equal(clone), qt.IsFalse)
			c.Assert(original, qt.DeepEquals, tableFixture())
			desired := clone.Desired()
			setting := reflect.ValueOf(desired).Elem().FieldByName(field.Name).Interface().(chschema.Setting)
			c.Assert(setting, qt.DeepEquals, chschema.Setting{State: chschema.Explicit, Value: "changed_" + field.Name})
			projected, err := desired.Observed()
			c.Assert(err, qt.IsNil)
			c.Assert(projected, qt.DeepEquals, clone)
			desiredClone := desired.Clone().(*chschema.DesiredTable)
			reflect.ValueOf(desiredClone).Elem().FieldByName(field.Name).Set(reflect.ValueOf(chschema.Setting{State: chschema.Default}))
			c.Assert(desired.Equal(desiredClone), qt.IsFalse)
			unresolved, err := desiredClone.Observed()
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(unresolved, qt.IsNil)
			reflect.ValueOf(clone).Elem().FieldByName(field.Name).SetString("bad\x00value")
			c.Assert(chschema.ValidateObserved(clone), qt.ErrorIs, schemaext.ErrInvalidValue)
			reflect.ValueOf(clone).Elem().FieldByName(field.Name).SetString("bad\xffvalue")
			c.Assert(chschema.ValidateObserved(clone), qt.ErrorIs, schemaext.ErrInvalidValue)
		})
	}
}

func TestWireDefinitionCoversModelFields(t *testing.T) {
	c := qt.New(t)
	definition := must.Must(schemaext.DecodeJSON[map[string]json.RawMessage](chschema.WireDefinition()))
	values := must.Must(schemaext.DecodeJSON[map[string]map[string]map[string]json.RawMessage](definition["values"]))
	for _, codec := range chschema.Codecs() {
		model := values[string(codec.Representation)][string(chschema.TableKind)]
		properties := must.Must(schemaext.DecodeJSON[map[string]json.RawMessage](model["properties"]))
		var actual, expected []string
		for name := range properties {
			actual = append(actual, name)
		}
		modelType := reflect.TypeOf(codec.Prototype).Elem()
		for field := range modelType.Fields() {
			expected = append(expected, strings.Split(field.Tag.Get("json"), ",")[0])
		}
		slices.Sort(actual)
		slices.Sort(expected)
		c.Assert(actual, qt.DeepEquals, expected)
	}
	required := must.Must(schemaext.DecodeJSON[[]string](values["observed"][string(chschema.TableKind)]["required"]))
	var expectedRequired []string
	for _, field := range reflect.VisibleFields(reflect.TypeFor[chschema.ObservedTable]()) {
		expectedRequired = append(expectedRequired, field.Tag.Get("json"))
	}
	c.Assert(required, qt.DeepEquals, expectedRequired)
	copyDefinition := chschema.WireDefinition()
	copyDefinition[0] = '['
	c.Assert(chschema.WireDefinition()[0], qt.Equals, byte('{'))
}

func TestDesiredCodecRefusesMalformedIntent(t *testing.T) {
	for _, data := range []string{
		`null`, `[]`, `{} {}`, `{"unknown":{}}`, `{"Engine":{"state":"explicit","value":"Memory"}}`,
		`{"engine":null}`, `{"engine":{}}`, `{"engine":{"state":null}}`,
		`{"engine":{"state":"explicit","value":null}}`, `{"engine":{"state":"explicit","Value":"Memory"}}`,
		`{"engine":{"state":"other"}}`, `{"engine":{"state":"default","value":"Memory"}}`,
		`{"engine":{"state":"","value":"Memory"}}`, `{"engine":{"state":"explicit"}}`,
		`{"engine":{"state":"explicit","value":"  "}}`, `{"engine":{"state":"explicit","value":true}}`,
		`{"engine":{"state":"default","unknown":true}}`, `{"engine":{"state":"default","state":"explicit"}}`,
		`{"engine":{"state":"default"},"engine":{"state":"explicit","value":"Memory"}}`,
		`{"order_by":{"state":"explicit","value":"bad\u0000value"}}`,
	} {
		t.Run(data, func(t *testing.T) {
			c := qt.New(t)
			value, err := chschema.Codecs()[0].Decode([]byte(data))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}

func TestObservedCodecRequiresEveryInspectedProperty(t *testing.T) {
	for _, field := range reflect.VisibleFields(reflect.TypeFor[chschema.ObservedTable]()) {
		name := field.Tag.Get("json")
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			encoded := must.Must(json.Marshal(tableFixture()))
			fields := must.Must(schemaext.DecodeJSON[map[string]json.RawMessage](encoded))
			delete(fields, name)
			missing, err := chschema.Codecs()[1].Decode(must.Must(json.Marshal(fields)))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(missing, qt.IsNil)
			fields[name] = json.RawMessage(`null`)
			null, err := chschema.Codecs()[1].Decode(must.Must(json.Marshal(fields)))
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(null, qt.IsNil)
		})
	}
}

func TestTableCodecRefusesWrongRepresentationAndInvalidValues(t *testing.T) {
	for _, test := range []struct {
		name  string
		codec int
		value schemaext.Payload
	}{
		{"observation as desired", 0, tableFixture()},
		{"declaration as observed", 1, tableFixture().Desired()},
		{"nil desired", 0, (*chschema.DesiredTable)(nil)},
		{"nil observed", 1, (*chschema.ObservedTable)(nil)},
		{"zero observed", 1, &chschema.ObservedTable{}},
		{"invalid desired", 0, &chschema.DesiredTable{TTL: chschema.Setting{State: chschema.Default, Value: "ignored"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			codec := chschema.Codecs()[test.codec]
			encoded, err := codec.Encode(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(encoded, qt.IsNil)
			canonical, err := codec.Canonical(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(canonical, qt.IsNil)
			clone, err := codec.Clone(test.value)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(clone, qt.IsNil)
		})
	}
}
