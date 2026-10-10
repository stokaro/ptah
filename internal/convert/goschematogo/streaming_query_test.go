package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// A schema containing only a streaming query still needs a Go holder, and
// newlines and quoted YQL literals must survive both annotation escaping passes.
func TestRender_StreamingQuery_RoundTrip(t *testing.T) {
	c := qt.New(t)
	db, err := yamlschema.Parse([]byte(`streaming_queries:
  copy:
    schema: jobs
    run: false
    resource_pool: batch
    allow_state_reset: true
    text: |
      $tag = "it's a stream";
      INSERT INTO output SELECT * FROM input;
`))
	c.Assert(err, qt.IsNil)
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(builtintest.Annotations(), files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.FeatureObjects.Len(), qt.Equals, 1)
	original, found, err := db.FeatureObjects.Get(ydbstreaming.Ref("jobs", "copy"))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	observed, found, err := parsed.FeatureObjects.Get(original.Ref)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	query := observed.Value.(*ydbstreaming.Desired)
	c.Assert(ydbstreaming.Equal(query.Spec, original.Value.(*ydbstreaming.Desired).Spec), qt.IsTrue)
	c.Assert(query.AllowStateReset, qt.IsTrue)
}
