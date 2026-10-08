package schemacensus

import (
	"reflect"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// featureCodecs supplies the concrete models to this bundled-provider census.
// The completeness control compares this selection with the runtime's desired
// codecs, so moving a model behind an interface cannot remove it from the gate.
// This is conformance composition, not model dispatch in the schema pipeline.
func featureCodecs() []schemaext.OwnedCodec {
	var models []schemaext.OwnedCodec
	for _, codec := range ydbschema.Codecs() {
		if codec.Representation == schemaext.Desired {
			models = append(models, schemaext.OwnedCodec{Owner: "ptah.run/ydb", Codec: codec})
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
		values := must.Must(value.Interface().(schemaext.Facets).Values())
		for _, feature := range values {
			walk(reflect.ValueOf(feature))
		}
		if value.CanSet() {
			value.Set(reflect.ValueOf(must.Must(schemaext.NewFacets(values...))))
		}
		return true
	case reflect.TypeFor[schemaext.Coverage]():
		return true
	default:
		return false
	}
}
