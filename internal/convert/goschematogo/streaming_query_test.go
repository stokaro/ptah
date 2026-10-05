package goschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/yamlschema"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/ydbstream"
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
	files, err := goschematogo.Render(db, goschematogo.Options{PackageName: "models", SingleFile: true, Dialect: "ydb"})
	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	parsed, err := goschema.ParseSource(files[0].Name, files[0].Data)
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.StreamingQueries, qt.HasLen, 1)
	c.Assert(ydbstream.Equal(parsed.StreamingQueries[0].Spec, db.StreamingQueries[0].Spec), qt.IsTrue)
	c.Assert(parsed.StreamingQueries[0].Schema, qt.Equals, "jobs")
	c.Assert(parsed.StreamingQueries[0].Name, qt.Equals, "copy")
	c.Assert(parsed.StreamingQueries[0].AllowStateReset, qt.IsTrue)
}
