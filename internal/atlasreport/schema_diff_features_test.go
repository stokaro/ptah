package atlasreport_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/featurejson"
)

// clickHouseDiff is a diff whose desired side carries a ClickHouse table
// setting, the way a schema read from a ClickHouse source does.
func clickHouseDiff() atlasreport.SchemaDiff {
	settings := &chschema.DesiredTable{Engine: chschema.Setting{State: chschema.Explicit, Value: "MergeTree"}}
	to := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "events", Facets: must.Must(schemaext.NewFacets(settings))}}}
	return atlasreport.NewSchemaDiff(nil, to, nil)
}

// With the helpers on, `json` encodes the feature data of a side through the
// codecs of the runtime that produced the diff, so the document reads back
// with the same settings.
func TestSchemaDiffTemplateJSON_EncodesFeatureData(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasreport.SchemaDiffTemplateHelpersEnvVar, "1")
	codecs := must.Must(builtin.New()).Codecs()
	report := clickHouseDiff()
	var out bytes.Buffer

	err := atlasreport.WriteSchemaDiff(t.Context(), &out, `{{ json .To }}`, report, codecs)

	c.Assert(err, qt.IsNil)
	var read schemamodel.Database
	c.Assert(featurejson.Unmarshal(t.Context(), codecs, schemaext.Desired, out.Bytes(), &read), qt.IsNil)
	c.Assert(read.Tables[0].Facets.Equal(report.To.Tables[0].Facets), qt.IsTrue)
}

// A runtime without the owner refuses the document and names the kind,
// rather than writing a side without its settings.
func TestSchemaDiffTemplateJSON_FailurePath(t *testing.T) {
	c := qt.New(t)
	t.Setenv(atlasreport.SchemaDiffTemplateHelpersEnvVar, "1")
	var out bytes.Buffer

	err := atlasreport.WriteSchemaDiff(t.Context(), &out, `{{ json .To }}`, clickHouseDiff(), schemaext.Registry{})

	c.Assert(err, qt.ErrorIs, schemaext.ErrUnknownCodec)
	c.Assert(err, qt.ErrorMatches, `(?s).*"ptah.run/clickhouse/table"/desired.*`)
	c.Assert(out.String(), qt.Equals, "")
}
