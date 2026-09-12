package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/goschematogo"
)

// readableFamiliesDatabase holds one object of every family a live read can
// produce and the exporter has to write.
//
// The column over the domain is the point. `ptah introspect` keeps a column's
// declared type verbatim, so a table whose column names a domain the export
// leaves out describes a schema that cannot be applied: the type does not
// exist.
func readableFamiliesDatabase() *schemamodel.Database {
	start := int64(100)
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", FieldName: "ID", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", FieldName: "Qty", Name: "qty", Type: "positive_int"},
		},
		Domains: []schemamodel.Domain{{
			Name: "positive_int", BaseType: "integer", Check: "VALUE > 0", NotNull: true,
		}},
		CompositeTypes: []schemamodel.CompositeType{{
			Name: "addr",
			Fields: []schemamodel.CompositeField{
				{Name: "street", Type: "text"},
				{Name: "city", Type: "text"},
			},
		}},
		Ranges: []schemamodel.Range{{
			Name: "tsrange_local", Subtype: "timestamp",
		}},
		Sequences: []schemamodel.Sequence{{
			Name: "order_seq", Start: &start,
		}},
		Functions: []schemamodel.Function{{
			StructName: "P", Name: "do_work", Kind: schemamodel.FunctionKindProcedure,
			Language: "plpgsql", Body: "BEGIN NULL; END;",
		}},
	}
}

// TestRender_EveryReadableFamilyRoundTrips is the test the defect needs.
//
// It compares models rather than asserting on the emitted text: a string
// assertion passes for an annotation the parser cannot read back, and the
// failure this covers is exactly that -- an annotation written where nothing
// reads it, or written with an attribute name the parser does not know.
func TestRender_EveryReadableFamilyRoundTrips(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(readableFamiliesDatabase(), goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)

	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}

	parsed, err := goschema.ParseSource("schema.go", source.String())
	c.Assert(err, qt.IsNil, qt.Commentf("the exported source does not parse:\n%s", source.String()))

	c.Assert(parsed.Domains, qt.HasLen, 1)
	c.Assert(parsed.Domains[0].Name, qt.Equals, "positive_int")
	c.Assert(parsed.Domains[0].BaseType, qt.Equals, "integer")
	c.Assert(parsed.Domains[0].Check, qt.Equals, "VALUE > 0")
	c.Assert(parsed.Domains[0].NotNull, qt.IsTrue)

	c.Assert(parsed.CompositeTypes, qt.HasLen, 1)
	c.Assert(parsed.CompositeTypes[0].Name, qt.Equals, "addr")
	c.Assert(parsed.CompositeTypes[0].Fields, qt.DeepEquals, []schemamodel.CompositeField{
		{Name: "street", Type: "text"},
		{Name: "city", Type: "text"},
	})

	c.Assert(parsed.Ranges, qt.HasLen, 1)
	c.Assert(parsed.Ranges[0].Subtype, qt.Equals, "timestamp")

	c.Assert(parsed.Sequences, qt.HasLen, 1)
	c.Assert(parsed.Sequences[0].Name, qt.Equals, "order_seq")
	c.Assert(parsed.Sequences[0].Start, qt.IsNotNil)
	c.Assert(*parsed.Sequences[0].Start, qt.Equals, int64(100))
}

// TestRender_AProcedureDoesNotComeBackAFunction covers the family that was
// already exported and losing one property.
//
// A procedure and a function are one model separated by Kind, and the grammar
// has no attribute for it -- the parser reads a separate directive. Writing
// every routine as a function is therefore not a missing attribute but a
// changed object.
func TestRender_AProcedureDoesNotComeBackAFunction(t *testing.T) {
	c := qt.New(t)

	files, err := goschematogo.Render(readableFamiliesDatabase(), goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)

	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}

	parsed, err := goschema.ParseSource("schema.go", source.String())
	c.Assert(err, qt.IsNil)

	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(parsed.Functions[0].IsProcedure(), qt.IsTrue,
		qt.Commentf("the routine came back as a function, so CALL became a SELECT"))
}

// TestRender_AFunctionStaysAFunction is the control the test above needs.
//
// Without it, an exporter that wrote every routine under the procedure
// directive would satisfy it, and every function in a schema would become a
// procedure on export.
func TestRender_AFunctionStaysAFunction(t *testing.T) {
	c := qt.New(t)

	db := readableFamiliesDatabase()
	db.Functions[0].Kind = ""
	db.Functions[0].Returns = "integer"

	files, err := goschematogo.Render(db, goschematogo.Options{SingleFile: true})
	c.Assert(err, qt.IsNil)

	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}

	parsed, err := goschema.ParseSource("schema.go", source.String())
	c.Assert(err, qt.IsNil)

	c.Assert(parsed.Functions, qt.HasLen, 1)
	c.Assert(parsed.Functions[0].IsProcedure(), qt.IsFalse)
	c.Assert(parsed.Functions[0].Returns, qt.Equals, "integer")
}

// TestRender_AUserTypeAloneStillGetsTheStructItHangsOff is the regression the
// predicate guards.
//
// The global annotations are the doc comment of one generated struct, and that
// struct is written only when the predicate says something needs it. A family
// emitted without being counted there produces a comment attached to nothing,
// which the parser does not refuse -- it does not read it at all.
func TestRender_AUserTypeAloneStillGetsTheStructItHangsOff(t *testing.T) {
	for _, db := range []struct {
		name  string
		build func() *schemamodel.Database
	}{
		{name: "a domain alone", build: func() *schemamodel.Database {
			return &schemamodel.Database{Domains: []schemamodel.Domain{{Name: "d", BaseType: "integer"}}}
		}},
		{name: "a composite alone", build: func() *schemamodel.Database {
			return &schemamodel.Database{CompositeTypes: []schemamodel.CompositeType{{
				Name: "c", Fields: []schemamodel.CompositeField{{Name: "f", Type: "text"}},
			}}}
		}},
		{name: "a range alone", build: func() *schemamodel.Database {
			return &schemamodel.Database{Ranges: []schemamodel.Range{{Name: "r", Subtype: "integer"}}}
		}},
		{name: "a sequence alone", build: func() *schemamodel.Database {
			return &schemamodel.Database{Sequences: []schemamodel.Sequence{{Name: "s"}}}
		}},
	} {
		t.Run(db.name, func(t *testing.T) {
			c := qt.New(t)

			files, err := goschematogo.Render(db.build(), goschematogo.Options{SingleFile: true})
			c.Assert(err, qt.IsNil)

			var source strings.Builder
			for _, file := range files {
				source.Write(file.Data)
			}

			parsed, err := goschema.ParseSource("schema.go", source.String())
			c.Assert(err, qt.IsNil)

			total := len(parsed.Domains) + len(parsed.CompositeTypes) + len(parsed.Ranges) + len(parsed.Sequences)
			c.Assert(total, qt.Equals, 1,
				qt.Commentf("the annotation hangs off no struct, so nothing read it back:\n%s", source.String()))
		})
	}
}
