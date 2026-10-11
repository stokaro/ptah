package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// A declaration converted to a catalog is the current side of a comparison
// with a schema file as its source, as in `schema diff --from file --to
// file`. A range keeps every attribute it declares through that conversion:
// one that lost them read as a different range, and a file compared with
// itself planned a DROP TYPE and a CREATE TYPE for every range naming one.
func TestToDBSchema_KeepsRangeAttributes(t *testing.T) {
	c := qt.New(t)
	db := &schemamodel.Database{
		Ranges: []schemamodel.Range{{
			Name:           "floatrange",
			Schema:         "app",
			Subtype:        "float8",
			SubtypeOpClass: "float8_ops",
			Collation:      "C",
			Canonical:      "app.floatrange_canonical",
			SubtypeDiff:    "float8mi",
		}},
	}
	schemamodel.Finalize(db)
	runtime := must.Must(builtin.New())

	got, err := goschematodb.ToDBSchema(t.Context(), db, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)

	c.Assert(got.Ranges, qt.DeepEquals, []catalog.Range{{
		Name:           "floatrange",
		Schema:         "app",
		Subtype:        "float8",
		SubtypeOpClass: "float8_ops",
		Collation:      "C",
		Canonical:      "app.floatrange_canonical",
		SubtypeDiff:    "float8mi",
	}})
	diff, err := schemadiff.CompareWithDialect(t.Context(), db, got, platform.Postgres, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("a range compared with its own conversion changed: %#v", diff.RangesModified))
}
