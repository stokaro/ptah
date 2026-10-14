package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
)

// TestRender_WritesYDBLimitsWhereYDBReadsThem pins that the YDB namespaces a
// schema does not describe are written as limits in an export for YDB and in
// one that names no target, in every file, and left out of an export for
// another target, which never holds YDB objects.
func TestRender_WritesYDBLimitsWhereYDBReadsThem(t *testing.T) {
	for _, test := range []struct {
		dialect string
		files   []int
	}{
		{"ydb", []int{1, 1}},
		{"", []int{1, 1}},
		{"postgres", []int{0}},
		{"clickhouse", []int{0}},
	} {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
				Fields: []schemamodel.Field{{StructName: "Doc", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true}}}

			files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{Annotations: builtintest.Annotations(), Dialect: test.dialect})

			c.Assert(err, qt.IsNil)
			limits := make([]int, 0, len(files))
			for _, file := range files {
				limits = append(limits, strings.Count(string(file.Data), `//ptah:schema:notdescribed kind="topic"`+"\n"))
			}
			c.Assert(limits, qt.DeepEquals, test.files)
		})
	}
}

// TestRender_WithoutTheYDBOwnerRefusesItsModels pins that a YDB object and a
// YDB table setting reach the export only through the YDB owner: a selection
// without it refuses them, since reading the source back with that selection
// would not read them. A table setting goes to the property encoding, which
// refuses it without a runtime.
func TestRender_WithoutTheYDBOwnerRefusesItsModels(t *testing.T) {
	tables := []schemamodel.Table{{StructName: "Event", Name: "events"}}
	for _, test := range []struct {
		name    string
		db      *schemamodel.Database
		wantErr string
	}{
		{"a topic", &schemamodel.Database{Tables: tables,
			FeatureObjects: must.Must(schemaext.NewObjects(ydbtopic.DesiredObject("", "events", "", ydbtopic.Spec{})))},
			`unsupported feature: Go annotations cannot represent feature object .* with value \*ydbtopic.Desired`},
		{"column families", &schemamodel.Database{Tables: []schemamodel.Table{{StructName: "Event", Name: "events",
			Facets: must.Must(schemaext.NewFacets(&ydbschema.DesiredColumnFamilies{Families: []ydbschema.ColumnFamily{{Name: "archive", Data: "hdd"}}}))}}},
			`invalid feature value: a context and selected feature runtime are required`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := goschematogo.Render(c.Context(), test.db, goschematogo.Options{Annotations: annotation.None(), SingleFile: true})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(files, qt.IsNil)
		})
	}
}
