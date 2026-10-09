package schemacensus

import (
	"reflect"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbstreaming"
)

// featureCodecs supplies the concrete models to this bundled-provider census.
// The completeness control compares this selection with the runtime's desired
// codecs, so moving a model behind an interface cannot remove it from the gate.
// This is conformance composition, not model dispatch in the schema pipeline.
func featureCodecs() []schemaext.OwnedCodec {
	var models []schemaext.OwnedCodec
	for _, provider := range []struct {
		owner  string
		codecs []schemaext.Codec
	}{
		{owner: "ptah.run/ydb", codecs: ydbschema.Codecs()},
		{owner: "ptah.run/ydb", codecs: ydbcoordination.Codecs()},
		{owner: "ptah.run/ydb", codecs: ydbstreaming.Codecs()},
		{owner: "ptah.run/clickhouse", codecs: chschema.Codecs()},
	} {
		for _, codec := range provider.codecs {
			if codec.Representation == schemaext.Desired {
				models = append(models, schemaext.OwnedCodec{Owner: provider.owner, Codec: codec})
			}
		}
	}
	return models
}

func immutableFeatures(t reflect.Type) bool {
	return t == reflect.TypeFor[schemaext.Objects]() || t == reflect.TypeFor[schemaext.Facets]() || t == reflect.TypeFor[schemaext.Coverage]()
}

// visitFeatures traverses cloned public values, then captures the changed
// collection when the caller is ablating. It never reaches private maps or
// mutates an interface-owned payload in its source container.
func visitFeatures(value reflect.Value, walk func(reflect.Value)) bool {
	switch value.Type() {
	case reflect.TypeFor[schemaext.Objects]():
		objects := must.Must(value.Interface().(schemaext.Objects).All())
		for _, object := range objects {
			walk(reflect.ValueOf(object.Value))
		}
		if value.CanSet() {
			value.Set(reflect.ValueOf(must.Must(schemaext.NewObjects(objects...))))
		}
		return true
	case reflect.TypeFor[schemaext.Facets]():
		facets, _ := reflect.TypeAssert[schemaext.Facets](value) // The type switch above established the concrete type.
		values := must.Must(facets.Values())
		for _, feature := range values {
			walk(reflect.ValueOf(feature))
			if value.CanSet() {
				facets = must.Must(facets.Replace(feature))
			}
		}
		if value.CanSet() {
			value.Set(reflect.ValueOf(facets))
		}
		return true
	case reflect.TypeFor[schemaext.Coverage]():
		return true
	default:
		return false
	}
}
