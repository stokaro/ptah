package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// changefeedSource is an entity whose struct carries the given annotations
// beside its table directive, and a holder struct that carries others. Each
// line of onStruct ends with a newline, so an empty one leaves the struct's
// doc comment whole.
func changefeedSource(onStruct, onHolder string) string {
	return `package entities

//ptah:schema:table name="items"
` + onStruct + `type Item struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

//ptah:schema:table name="orders" schema="shop"
type Order struct {
	//ptah:schema:field name="id" type="BIGINT" primary="true"
	ID int64
}

type Holder struct {
` + onHolder + `
	_ int
}
`
}

// TestParseSource_Changefeed_HappyPath reads changefeeds and their consumers
// into the table they belong to: the struct's own, or the one named.
func TestParseSource_Changefeed_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		onStruct  string
		onHolder  string
		wantItems []ast.ChangefeedSpec
		wantShop  []ast.ChangefeedSpec
	}{
		{
			name: "one on the struct, its options folded as YDB folds them",
			onStruct: `//ptah:schema:changefeed name="updates" mode="updates" format="json" retention_period="PT12H" initial_scan
`,
			wantItems: []ast.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT12H", InitialScan: true}},
		},
		{
			name: "consumers on the struct, in the order written",
			onStruct: `//ptah:schema:changefeed name="feed" mode="KEYS_ONLY" format="JSON"
//ptah:schema:changefeed:consumer changefeed="feed" name="audit" important
//ptah:schema:changefeed:consumer changefeed="feed" name="late" supported_codecs="raw,gzip" read_from="2026-01-01T00:00:00Z"
`,
			wantItems: []ast.ChangefeedSpec{{Name: "feed", Mode: "KEYS_ONLY", Format: "JSON", Consumers: []ast.TopicConsumerSpec{
				{Name: "audit", Important: true},
				{Name: "late", SupportedCodecs: []string{"raw", "gzip"}, ReadFrom: "2026-01-01T00:00:00Z"},
			}}},
		},
		{
			name: "on a holder field, naming a table in a directory",
			onHolder: `	//ptah:schema:changefeed name="feed" table="shop.orders" mode="NEW_IMAGE" format="DEBEZIUM_JSON"
	//ptah:schema:changefeed:consumer changefeed="feed" table="shop.orders" name="audit"`,
			wantShop: []ast.ChangefeedSpec{{Name: "feed", Mode: "NEW_IMAGE", Format: "DEBEZIUM_JSON",
				Consumers: []ast.TopicConsumerSpec{{Name: "audit"}}}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("items.go", changefeedSource(test.onStruct, test.onHolder))
			c.Assert(err, qt.IsNil)
			c.Assert(db.Tables, qt.HasLen, 2)
			c.Assert(db.Tables[0].Changefeeds, qt.DeepEquals, test.wantItems)
			c.Assert(db.Tables[1].Changefeeds, qt.DeepEquals, test.wantShop)
		})
	}
}

// TestParseSource_Changefeed_FailurePath refuses a declaration with no effect
// or one YDB would refuse, where it was written.
func TestParseSource_Changefeed_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		onStruct string
		onHolder string
		wantErr  string
	}{
		{name: "no format", onStruct: `//ptah:schema:changefeed name="f" mode="UPDATES"
`,
			wantErr: `missing required annotation attribute "format" on //ptah:schema:changefeed at Item`},
		{name: "an unknown mode", onStruct: `//ptah:schema:changefeed name="f" mode="ALL" format="JSON"
`,
			wantErr: `invalid mode "ALL": takes one of .* on //ptah:schema:changefeed at Item`},
		{name: "a holder with no table", onHolder: `	//ptah:schema:changefeed name="f" mode="UPDATES" format="JSON"`,
			wantErr: `struct Holder maps to no table in this file; .*`},
		{name: "a table not in the file", onHolder: `	//ptah:schema:changefeed name="f" table="other" mode="UPDATES" format="JSON"`,
			wantErr: `table "other" is not declared in this file, .*`},
		{name: "one name twice", onStruct: `//ptah:schema:changefeed name="f" mode="UPDATES" format="JSON"
//ptah:schema:changefeed name="f" mode="KEYS_ONLY" format="JSON"
`,
			wantErr: `table "items" declares changefeed "f" twice on //ptah:schema:changefeed at Item`},
		{name: "a consumer of no changefeed", onStruct: `//ptah:schema:changefeed:consumer changefeed="f" name="c"
`,
			wantErr: `table "items" declares no changefeed "f" for consumer "c" on .*`},
		{name: "a consumer read from a fraction of a second", onStruct: `//ptah:schema:changefeed name="f" mode="UPDATES" format="JSON"
//ptah:schema:changefeed:consumer changefeed="f" name="c" read_from="2026-01-01T00:00:00.5Z"
`,
			wantErr: `invalid read_from "2026-01-01T00:00:00.5Z": YDB keeps whole seconds, .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource("items.go", changefeedSource(test.onStruct, test.onHolder))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestParseSource_Changefeed_RefusesAnInvalidValueAsSuch pins the sentinel a
// value the declaration cannot carry is reported with.
func TestParseSource_Changefeed_RefusesAnInvalidValueAsSuch(t *testing.T) {
	c := qt.New(t)
	_, err := goschema.ParseSource("items.go",
		changefeedSource(`//ptah:schema:changefeed name="f" mode="UPDATES" format="JSON" retention_period="1 day"
`, ""))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
}
