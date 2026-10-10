package schemacensus

import (
	"reflect"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/cockroachdb/crdbschema"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/spanner/spannerschema"
	"ptah.run/dialect/timescaledb/tsschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/feature/pgpolicy"
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
		{owner: "ptah.run/ydb", codecs: ydbworkload.Codecs()},
		{owner: "ptah.run/ydb", codecs: ydbsecret.Codecs()},
		{owner: "ptah.run/ydb", codecs: ydbtopic.Codecs()},
		{owner: "ptah.run/ydb", codecs: ydbexternal.Codecs()},
		{owner: "ptah.run/ydb", codecs: ydbreplication.Codecs()},
		{owner: "ptah.run/clickhouse", codecs: chschema.Codecs()},
		{owner: "ptah.run/clickhouse", codecs: chschema.IndexCodecs()},
		{owner: "ptah.run/clickhouse", codecs: chschema.RefreshCodecs()},
		{owner: "ptah.run/clickhouse", codecs: chschema.RowPolicyCodecs()},
		{owner: crdbschema.Owner, codecs: crdbschema.Codecs()},
		{owner: mysqlschema.Owner, codecs: mysqlschema.TableCodecs()},
		{owner: mysqlschema.Owner, codecs: mysqlschema.ColumnSettingsCodecs()},
		{owner: mssqlschema.Owner, codecs: mssqlschema.Codecs()},
		{owner: pgpolicy.Owner, codecs: pgpolicy.Codecs()},
		{owner: tsschema.Owner, codecs: tsschema.Codecs()},
		{owner: spannerschema.Owner, codecs: spannerschema.Codecs()},
		{owner: ydbschema.Owner, codecs: ydbschema.TTLCodecs()},
		{owner: ydbschema.Owner, codecs: ydbschema.ColumnFamiliesCodecs()},
		{owner: ydbschema.Owner, codecs: ydbschema.TablePartitioningCodecs()},
		{owner: ydbschema.Owner, codecs: ydbschema.ColumnStoreCodecs()},
		{owner: ydbschema.Owner, codecs: ydbschema.VectorIndexCodecs()},
		{owner: ydbschema.Owner, codecs: ydbschema.IndexPartitioningCodecs()},
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
	return t == objectsType || t == facetsType || t == coverageType
}

// The feature container types visitFeatures recognizes, resolved once: the
// walk asks for them at every value it visits.
var (
	objectsType  = reflect.TypeFor[schemaext.Objects]()
	facetsType   = reflect.TypeFor[schemaext.Facets]()
	coverageType = reflect.TypeFor[schemaext.Coverage]()
)

// visitFeatures traverses cloned public values, then captures the changed
// collection when the walk writes. It never reaches private maps or mutates an
// interface-owned payload in its source container.
func visitFeatures(value reflect.Value, mode access, walk func(reflect.Value)) bool {
	write := mode == readWrite && value.CanSet()
	switch value.Type() {
	case objectsType:
		objects := must.Must(value.Interface().(schemaext.Objects).All())
		for _, object := range objects {
			walk(reflect.ValueOf(object.Value))
		}
		if write {
			value.Set(reflect.ValueOf(must.Must(schemaext.NewObjects(objects...))))
		}
		return true
	case facetsType:
		facets, _ := reflect.TypeAssert[schemaext.Facets](value) // The type switch above established the concrete type.
		values := must.Must(facets.Values())
		for _, feature := range values {
			walk(reflect.ValueOf(feature))
			if write {
				facets = must.Must(facets.Replace(feature))
			}
		}
		if write {
			value.Set(reflect.ValueOf(facets))
		}
		return true
	case coverageType:
		return true
	default:
		return false
	}
}
