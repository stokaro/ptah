package ydbschema_test

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
)

func TestNamedChangefeeds_YAMLToRenderAndCodec(t *testing.T) {
	c := qt.New(t)
	database, err := yamlschema.Parse([]byte(`tables:
  items:
    columns:
      id:
        type: bigint
        primary: true
    changefeeds:
      updates:
        mode: UPDATES
        format: JSON
        consumers:
          audit:
            supported_codecs: [raw, gzip]
`))
	c.Assert(err, qt.IsNil)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 1)
	ref := ydbschema.ChangefeedRef("", "items", "updates")
	c.Assert(database.FeatureCoverage.Lookup(ydbschema.ChangefeedKind, ref).State, qt.Equals, schemaext.Complete)
	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(database, "ydb", capability.YDB262())
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")
	c.Assert(sql, qt.Contains, "ADD CHANGEFEED `updates`")
	c.Assert(sql, qt.Contains, "ADD CONSUMER `audit`")
	_, err = builtin.GetOrderedCreateStatementsWithCapabilities(database, "postgres", capability.Postgres18())
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)
	wire, err := runtime.Codecs().EncodeObjects(context.Background(), schemaext.Desired, database.FeatureObjects)
	c.Assert(err, qt.IsNil)
	decoded, err := runtime.Codecs().DecodeObjects(context.Background(), schemaext.Desired, wire)
	c.Assert(err, qt.IsNil)
	feeds, err := ydbschema.DesiredChangefeeds(decoded, "", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(feeds, qt.HasLen, 1)
	c.Assert(feeds[0].Consumers[0].SupportedCodecs, qt.DeepEquals, []string{"raw", "gzip"})
	feeds[0].Consumers[0].SupportedCodecs[0] = "changed"
	original, err := ydbschema.DesiredChangefeeds(database.FeatureObjects, "", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(original[0].Consumers[0].SupportedCodecs, qt.DeepEquals, []string{"raw", "gzip"})
}

func TestNamedChangefeeds_GoSourceCapturesParentIdentity(t *testing.T) {
	c := qt.New(t)
	database, err := goschema.ParseSource(builtintest.Annotations(), "entity.go", `package entities
//ptah:schema:table name="items" schema="shop"
//ptah:schema:changefeed name="updates" mode="UPDATES" format="JSON"
//ptah:schema:changefeed:consumer changefeed="updates" name="audit"
type Item struct {
    //ptah:schema:field name="id" type="BIGINT" primary="true"
    ID int64
}
`)
	c.Assert(err, qt.IsNil)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref, qt.Equals, ydbschema.ChangefeedRef("shop", "items", "updates"))
	feeds, err := ydbschema.DesiredChangefeeds(database.FeatureObjects, "shop", "items")
	c.Assert(err, qt.IsNil)
	c.Assert(feeds[0].Consumers[0].Name, qt.Equals, "audit")
}
