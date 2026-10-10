package featurejson_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/engine/builtin"
	"ptah.run/internal/featurejson"
	"ptah.run/migration/schemadiff/difftypes"
)

func codecs() schemaext.Registry {
	return must.Must(builtin.New()).Codecs()
}

// ttlChange is a ClickHouse TTL change on table events, as the comparison
// records it.
func ttlChange() schemaext.ChangeRecord {
	before := &chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id", TTL: "at + toIntervalDay(1)"}
	after := before.Desired()
	after.TTL.Value = "at + toIntervalDay(2)"
	subject := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts("", "events")
	return schemaext.ChangeRecord{Subject: subject, Value: &chdiff.Table{Before: before, After: after}}
}

// coordinationChange is a YDB coordination node change, a standalone feature
// object the diff carries at its own scope.
func coordinationChange() schemaext.ChangeRecord {
	return schemaext.ChangeRecord{Subject: ydbcoordination.Ref("", "locks"), Value: &ydbdiff.CoordinationNode{
		Before: &ydbcoordination.Observed{Spec: ydbcoordination.Spec{ReadConsistencyMode: "relaxed"}},
		After:  &ydbcoordination.Desired{Spec: ydbcoordination.Spec{ReadConsistencyMode: "strict"}},
	}}
}

func diffWithChanges() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		FeatureChanges: []schemaext.ChangeRecord{coordinationChange()},
		TablesModified: []difftypes.TableDiff{{TableName: "events", FeatureChanges: []schemaext.ChangeRecord{ttlChange()}}},
	}
}

// changesDocument reads back the parts of a diff document that hold owner
// changes. The name-only lists of a diff do not decode into their Go types,
// so a reader names the fields it wants.
type changesDocument struct {
	FeatureChanges []schemaext.ChangeRecord `json:"feature_changes"`
	TablesModified []struct {
		TableName      string                   `json:"table_name"`
		FeatureChanges []schemaext.ChangeRecord `json:"feature_changes"`
	} `json:"tables_modified"`
}

// Owner changes are written as envelopes naming the owner, the namespaced
// kind and the codec version, and they read back as the records the
// comparison produced.
func TestMarshal_EncodesOwnerChangesThroughTheirCodecs(t *testing.T) {
	c := qt.New(t)
	diff := diffWithChanges()

	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diff)

	c.Assert(err, qt.IsNil)
	var raw struct {
		FeatureChanges []schemaext.EncodedChange `json:"feature_changes"`
		TablesModified []struct {
			FeatureChanges []schemaext.EncodedChange `json:"feature_changes"`
		} `json:"tables_modified"`
	}
	c.Assert(json.Unmarshal(data, &raw), qt.IsNil)
	c.Assert(raw.FeatureChanges[0].Value.Kind, qt.Equals, ydbdiff.CoordinationNodeKind)
	c.Assert(raw.FeatureChanges[0].Value.Owner, qt.Equals, "ptah.run/ydb")
	c.Assert(raw.FeatureChanges[0].Value.Version, qt.Equals, uint32(1))
	c.Assert(raw.FeatureChanges[0].Subject, qt.DeepEquals, coordinationChange().Subject)
	c.Assert(raw.TablesModified[0].FeatureChanges[0].Value.Kind, qt.Equals, chdiff.TableKind)
	c.Assert(raw.TablesModified[0].FeatureChanges[0].Value.Representation, qt.Equals, schemaext.Change)

	var read changesDocument
	c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, data, &read), qt.IsNil)
	c.Assert(read.FeatureChanges, qt.DeepEquals, diff.FeatureChanges)
	c.Assert(read.TablesModified[0].TableName, qt.Equals, "events")
	c.Assert(read.TablesModified[0].FeatureChanges, qt.DeepEquals, diff.TablesModified[0].FeatureChanges)
}

// Everything that is not feature data is written exactly as encoding/json
// writes it: the same keys, the same order, null for a nil list and [] for an
// empty one. Only the feature values differ from what json.Marshal would have
// produced, had it been able to.
func TestMarshal_LeavesEverythingElseAsEncodingJSONWritesIt(t *testing.T) {
	plain := &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{TableName: "events", ColumnsAdded: difftypes.ColumnChanges{{Name: "at", Type: "DateTime"}}, FeatureChanges: make([]schemaext.ChangeRecord, 0)}},
		SchemasAdded:   []schemamodel.Schema{{Name: "analytics", Charset: "utf8mb4"}},
		SchemasRemoved: []string{"legacy"},
	}
	for _, test := range []struct {
		name  string
		value any
	}{
		{"a diff without feature data", plain},
		{"an empty diff", &difftypes.SchemaDiff{}},
		{"a desired schema without feature data", &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events"}}}},
		{"a value with no feature type at all", map[string][]int{"a": {1}}},
		{"nil", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			want, err := json.MarshalIndent(test.value, "", "  ")
			c.Assert(err, qt.IsNil)

			got, err := featurejson.MarshalIndent(t.Context(), codecs(), schemaext.Desired, test.value, "", "  ")

			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, string(want))
		})
	}
}

// A changed field keeps its place: the feature changes of a table are written
// where encoding/json writes the field, between the fields around it.
func TestMarshal_KeepsFieldOrder(t *testing.T) {
	c := qt.New(t)
	diff := diffWithChanges()
	diff.TablesModified[0].ColumnsAdded = difftypes.ColumnChanges{{Name: "at", Type: "DateTime"}}
	plain := &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{TableName: "events", ColumnsAdded: difftypes.ColumnChanges{{Name: "at", Type: "DateTime"}}}}}

	got, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diff)
	c.Assert(err, qt.IsNil)
	want := must.Must(json.Marshal(plain))

	c.Assert(keys(c, got), qt.DeepEquals, append([]string{"feature_changes"}, keys(c, want)...))
}

// A desired schema carries facets, feature objects and coverage; each is
// written through its codec in the representation the caller names, and reads
// back as the same values.
func TestMarshal_RoundTripsSchemaFeatureData(t *testing.T) {
	c := qt.New(t)
	table := must.Must(schemaext.NewFacets(ttlChange().Value.(*chdiff.Table).After))
	objects := must.Must(schemaext.NewObjects(ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{ReadConsistencyMode: "strict"})))
	coverage := must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	database := &schemamodel.Database{
		Tables:          []schemamodel.Table{{Name: "events", Facets: table}},
		FeatureObjects:  objects,
		FeatureCoverage: coverage,
	}

	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, database)
	c.Assert(err, qt.IsNil)
	var read schemamodel.Database
	c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Desired, data, &read), qt.IsNil)

	c.Assert(read.Tables[0].Facets.Equal(table), qt.IsTrue)
	readObjects := must.Must(read.FeatureObjects.All())
	wantObjects := must.Must(objects.All())
	c.Assert(readObjects[0].Ref, qt.DeepEquals, wantObjects[0].Ref)
	c.Assert(readObjects[0].Value.Equal(wantObjects[0].Value), qt.IsTrue)
	c.Assert(read.FeatureCoverage.Lookup(ydbcoordination.Kind, ydbcoordination.Ref("", "locks")).State, qt.Equals, schemaext.Complete)
}

// An observed catalog uses the observed codecs.
func TestMarshal_ObservedRepresentation(t *testing.T) {
	c := qt.New(t)
	observed := &catalog.Database{Tables: []catalog.Table{{Name: "events", Facets: must.Must(schemaext.NewFacets(ttlChange().Value.(*chdiff.Table).Before))}}}

	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Observed, observed)
	c.Assert(err, qt.IsNil)
	var read catalog.Database
	c.Assert(featurejson.Unmarshal(t.Context(), codecs(), schemaext.Observed, data, &read), qt.IsNil)

	c.Assert(read.Tables[0].Facets.Equal(observed.Tables[0].Facets), qt.IsTrue)
}

// A kind the runtime has no codec for is refused with an error that names the
// kind, and no partial document is returned.
func TestMarshal_FailurePath(t *testing.T) {
	empty := must.Must(schemaext.NewRegistry())
	for _, test := range []struct {
		name           string
		registry       schemaext.Registry
		representation schemaext.Representation
		value          any
		want           string
	}{
		{"a change kind without a codec", empty, schemaext.Desired, diffWithChanges(), `.*"ptah.run/ydb/coordination-node-change"/change`},
		{"a facet kind without a codec", empty, schemaext.Desired, &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events",
			Facets: must.Must(schemaext.NewFacets(ttlChange().Value.(*chdiff.Table).After))}}}, `.*"ptah.run/clickhouse/table"/desired`},
		{"a value in the other representation", codecs(), schemaext.Observed, &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events",
			Facets: must.Must(schemaext.NewFacets(ttlChange().Value.(*chdiff.Table).After))}}}, `.*unregistered concrete type.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			data, err := featurejson.Marshal(t.Context(), test.registry, test.representation, test.value)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(data, qt.IsNil)
		})
	}
}

// A unknown-kind failure is the registry's typed error, so a caller can tell
// a missing provider from a malformed value.
func TestMarshal_UnknownKindIsTyped(t *testing.T) {
	c := qt.New(t)
	_, err := featurejson.Marshal(t.Context(), must.Must(schemaext.NewRegistry()), schemaext.Desired, diffWithChanges())
	var unknown *schemaext.UnknownCodecError
	c.Assert(err, qt.ErrorAs, &unknown)
	c.Assert(unknown.Kind, qt.Equals, ydbdiff.CoordinationNodeKind)
}

func TestUnmarshal_FailurePath(t *testing.T) {
	data := must.Must(featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diffWithChanges()))
	for _, test := range []struct {
		name     string
		registry schemaext.Registry
		target   any
		want     error
	}{
		{"a runtime without the owner", must.Must(schemaext.NewRegistry()), &changesDocument{}, schemaext.ErrUnknownCodec},
		{"a nil target", codecs(), (*changesDocument)(nil), schemaext.ErrInvalidValue},
		{"a non-pointer target", codecs(), changesDocument{}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := featurejson.Unmarshal(t.Context(), test.registry, schemaext.Desired, data, test.target)
			c.Assert(err, qt.ErrorIs, test.want)
		})
	}
}

// A refused decode leaves the target as it was.
func TestUnmarshal_LeavesTheTargetOnFailure(t *testing.T) {
	c := qt.New(t)
	data := must.Must(featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, diffWithChanges()))
	read := changesDocument{FeatureChanges: []schemaext.ChangeRecord{ttlChange()}}

	err := featurejson.Unmarshal(t.Context(), must.Must(schemaext.NewRegistry()), schemaext.Desired, data, &read)

	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(read.FeatureChanges, qt.DeepEquals, []schemaext.ChangeRecord{ttlChange()})
}

// embedsFeatureData promotes the fields of an embedded struct beside feature
// data, which the projection refuses rather than encode with other keys.
type embedsFeatureData struct {
	schemamodel.Schema
	Changes []schemaext.ChangeRecord `json:"changes"`
}

func TestMarshal_RefusesEmbeddingBesideFeatureData(t *testing.T) {
	c := qt.New(t)
	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, embedsFeatureData{})
	c.Assert(err, qt.ErrorIs, featurejson.ErrUnsupportedShape)
	c.Assert(data, qt.IsNil)
}

// keys lists an object's top-level keys in document order.
func keys(c *qt.C, data []byte) []string {
	c.Helper()
	decoder := json.NewDecoder(bytes.NewReader(data))
	token, err := decoder.Token()
	c.Assert(err, qt.IsNil)
	c.Assert(token, qt.Equals, json.Delim('{'))
	var result []string
	for decoder.More() {
		key, err := decoder.Token()
		c.Assert(err, qt.IsNil)
		result = append(result, key.(string))
		var skip json.RawMessage
		c.Assert(decoder.Decode(&skip), qt.IsNil)
	}
	return result
}

// reviewed holds feature data and decides for itself when it is empty: a
// review nobody signed is empty whatever it lists.
type reviewed struct {
	Signed  bool                     `json:"signed"`
	Changes []schemaext.ChangeRecord `json:"changes"`
}

func (r reviewed) IsZero() bool { return !r.Signed }

// omitzero asks a field's IsZero method, and the wire type of a field holding
// feature data has none, so the projection asks the original. Without that,
// an unsigned review would be written where encoding/json leaves it out.
func TestMarshal_OmitZeroAsksTheOriginalType(t *testing.T) {
	c := qt.New(t)
	document := struct {
		Review reviewed `json:"review,omitzero"`
	}{Review: reviewed{Changes: []schemaext.ChangeRecord{ttlChange()}}}

	data, err := featurejson.Marshal(t.Context(), codecs(), schemaext.Desired, document)

	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{}`)
}
