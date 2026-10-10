package goschema_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/annotation"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

// errGauge is the refusal the gauge owner pins on a level it does not take.
var errGauge = errors.New("a gauge reads low or high")

// gaugeFile is a file decoder that holds every gauge until the end of the
// file, so a gauge may come before its table, and places each by the
// frontend's own rule.
type gaugeFile struct{ declarations []annotation.Declaration }

func (f *gaugeFile) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	if level := declaration.Attributes["level"]; level != "low" && level != "high" {
		return nil, &annotation.DeclarationError{Attribute: "level", Err: errGauge}
	}
	f.declarations = append(f.declarations, declaration)
	return nil, nil
}

func (f *gaugeFile) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	contributions := make([]annotation.Contribution, 0, len(f.declarations))
	for _, declaration := range f.declarations {
		if _, err := tables.Owning(declaration.Struct, declaration.Attributes["table"], "a gauge"); err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Attribute: "table", Err: err}
		}
		contributions = append(contributions, annotation.Contribution{Table: declaration.Attributes["table"],
			Label: "a gauge", Source: declaration, Facet: &widget{Level: declaration.Attributes["level"]}})
	}
	return contributions, nil
}

// gaugeOwner declares //ptah:schema:gauge through a file decoder, and reads
// not-described declarations of kind "gauge". limits receives the ones a
// parse hands its coverage claim.
func gaugeOwner(c *qt.C, limits *[]coverage.Object) annotation.Set {
	c.Helper()
	set, err := annotation.NewSet(annotation.Extension{
		Owner: "example.org/widget",
		Directives: []annotation.Directive{{Name: "ptah:schema:gauge", Scopes: []annotation.Scope{annotation.ScopeStruct},
			Attributes: []annotation.Attribute{{Name: "table", Value: "string"}, {Name: "level", Value: "string", Required: true}}}},
		Kinds:  []schemaext.Kind{widgetKind},
		File:   func() annotation.FileDecoder { return &gaugeFile{} },
		Limits: []string{"gauge"},
		Coverage: func(written []coverage.Object) (schemaext.Coverage, error) {
			*limits = append(*limits, written...)
			return widgetCoverage()
		},
	})
	c.Assert(err, qt.IsNil)
	return set
}

// TestParseSource_AFileDecoderPlacesAPartBeforeItsTable drives the frontend's
// half of a file decoder: the owner holds a gauge written before its table,
// contributes it once the file is read, and the frontend places it by the
// struct of the declaration it names. A not-described declaration of the
// owner's kind reaches the owner's claim rather than the frontend's set.
func TestParseSource_AFileDecoderPlacesAPartBeforeItsTable(t *testing.T) {
	c := qt.New(t)
	var limits []coverage.Object
	source := "package models\n\n//ptah:schema:notdescribed kind=\"gauge\" name=\"spare\"\n" +
		"//ptah:schema:gauge level=\"high\"\n//ptah:schema:table name=\"items\"\ntype Item struct{}\n"

	db, err := goschema.ParseSource(gaugeOwner(c, &limits), "items.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	gauge, found, err := schemaext.FacetAs[*widget](db.Tables[0].Facets, widgetKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(gauge.Level, qt.Equals, "high")
	c.Assert(limits, qt.DeepEquals, []coverage.Object{{Kind: "gauge", Name: "spare", Provenance: coverage.Declared}})
	c.Assert(db.NotDescribed.IsZero(), qt.IsTrue)
	c.Assert(db.FeatureCoverage.Lookup(widgetKind, objectidentity.ID{}).State, qt.Equals, schemaext.Complete)
}

// TestParseSource_AFileDecoderRefusal_FailurePath pins where an owner's
// refusal is reported: at the declaration being decoded, or at the one a
// refusal from the end of the file names, with the attribute it names and
// the owner's own error under the frontend's sentinel.
func TestParseSource_AFileDecoderRefusal_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		source        string
		wantErr       string
		wantLine      int
		wantAttribute string
	}{
		{
			name:          "a level the owner refuses while decoding",
			source:        "package models\n\n//ptah:schema:table name=\"items\"\n//ptah:schema:gauge level=\"mid\"\ntype Item struct{}\n",
			wantErr:       `a gauge reads low or high on //ptah:schema:gauge at Item`,
			wantLine:      4,
			wantAttribute: "level",
		},
		{
			name:          "a table the file does not declare, refused at the end of the file",
			source:        "package models\n\n//ptah:schema:gauge table=\"other\" level=\"low\"\ntype Gauge struct{}\n",
			wantErr:       `table "other" is not declared in this file, and a gauge is declared beside its table on //ptah:schema:gauge at Gauge`,
			wantLine:      3,
			wantAttribute: "table",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var limits []coverage.Object

			db, err := goschema.ParseSource(gaugeOwner(c, &limits), "items.go", test.source)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			parseErr, ok := errors.AsType[*ptaherr.ParseError](err)
			c.Assert(ok, qt.IsTrue)
			c.Assert(parseErr.Line, qt.Equals, test.wantLine)
			c.Assert(parseErr.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(parseErr.Directive, qt.Equals, "ptah:schema:gauge")
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}

// TestParseSource_AnOwnerRefusalKeepsItsSentinel pins that the frontend wraps
// the owner's error rather than replacing it.
func TestParseSource_AnOwnerRefusalKeepsItsSentinel(t *testing.T) {
	c := qt.New(t)
	var limits []coverage.Object

	_, err := goschema.ParseSource(gaugeOwner(c, &limits), "items.go",
		"package models\n\n//ptah:schema:table name=\"items\"\n//ptah:schema:gauge level=\"mid\"\ntype Item struct{}\n")

	c.Assert(err, qt.ErrorIs, errGauge)
}

// TestParseSource_ANotDescribedKindOfNoSelectedOwnerIsRefused is the control
// on the routing: without the owner, its kind is no kind the frontend knows,
// and the declaration is refused rather than dropped.
func TestParseSource_ANotDescribedKindOfNoSelectedOwnerIsRefused(t *testing.T) {
	c := qt.New(t)

	db, err := goschema.ParseSource(annotation.None(), "items.go",
		"package models\n\n//ptah:schema:notdescribed kind=\"gauge\"\ntype Spare struct{}\n")

	c.Assert(err, qt.ErrorMatches, `unknown coverage kind "gauge".*`)
	c.Assert(db.FeatureCoverage.Representation(), qt.Equals, schemaext.Representation(""))
}
