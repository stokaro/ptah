package schemafile_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemafile"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// postgresIndexDatabase is the desired state behind both tests: one index
// carrying every property issue #1272 made the PostgreSQL comparator read.
func postgresIndexDatabase() *goschema.Database {
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "T", Name: "t"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "a", Type: "text"},
			{StructName: "T", Name: "b", Type: "text"},
			{StructName: "T", Name: "c", Type: "text"},
		},
		Indexes: []goschema.Index{
			{
				StructName:     "T",
				Name:           "i",
				Fields:         []string{"a", "b"},
				Type:           "btree",
				IncludeColumns: []string{"c"},
				Parts: []goschema.IndexPart{
					{Name: "a", Operator: "text_pattern_ops", Desc: true, NullsOrder: goschema.NullsOrderLast},
					{Name: "b", NullsOrder: goschema.NullsOrderFirst},
				},
			},
		},
	}
	goschema.Finalize(db)
	return db
}

// TestToDBSchema_CarriesPostgresIndexSemantics pins the conversion used for the
// --from side of `schema diff` when that side is a local file.
//
// Everything the comparator reads has to survive this hop. The access method,
// the structured key parts and the INCLUDE payload were dropped here, which was
// invisible only while the PostgreSQL comparator ignored all three: once it
// started reading them, a file diffed against the database it was inspected
// from would have planned a rebuild for every index carrying any of them.
func TestToDBSchema_CarriesPostgresIndexSemantics(t *testing.T) {
	c := qt.New(t)

	got := schemafile.ToDBSchema(postgresIndexDatabase(), platform.Postgres)

	c.Assert(got.Indexes, qt.HasLen, 1)
	index := got.Indexes[0]
	c.Assert(index.Method, qt.Equals, "btree")
	c.Assert(index.IncludeColumns, qt.DeepEquals, []string{"c"})
	c.Assert(index.Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
		{Name: "a", Operator: "text_pattern_ops", Desc: true, NullsOrder: dbschematypes.NullsOrderLast},
		{Name: "b", NullsOrder: dbschematypes.NullsOrderFirst},
	})
}

// TestToDBSchema_PostgresIndexSemanticsAreIdempotent is the churn control for
// the same hop: a desired state converted to the DB shape and compared against
// itself must report nothing. It fails the moment any index property is lost on
// the way through, which is what makes it worth having next to the field-level
// assertions above.
func TestToDBSchema_PostgresIndexSemanticsAreIdempotent(t *testing.T) {
	c := qt.New(t)
	db := postgresIndexDatabase()

	current := schemafile.ToDBSchema(db, platform.Postgres)
	diff := schemadiff.CompareWithDialect(db, current, platform.Postgres)

	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

// TestToDBSchema_ClickHouseSkippingIndexTypeIsNotAnAccessMethod keeps the two
// concepts goschema.Index.Type carries apart. On ClickHouse the field is the
// data-skipping-index type, which the DB shape keeps in DBIndex.Type; reporting
// it as a PostgreSQL access method would make a ClickHouse "bloom_filter" and a
// PostgreSQL "gin" indistinguishable at the comparison layer.
func TestToDBSchema_ClickHouseSkippingIndexTypeIsNotAnAccessMethod(t *testing.T) {
	c := qt.New(t)
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "E", Name: "events"}},
		Fields: []goschema.Field{{StructName: "E", Name: "payload", Type: "String"}},
		Indexes: []goschema.Index{
			{
				StructName:  "E",
				Name:        "idx_events_payload",
				Fields:      []string{"payload"},
				Type:        "bloom_filter",
				Granularity: 64,
			},
		},
	}
	goschema.Finalize(db)

	got := schemafile.ToDBSchema(db, platform.ClickHouse)

	c.Assert(got.Indexes, qt.HasLen, 1)
	c.Assert(got.Indexes[0].Method, qt.Equals, "")
	c.Assert(got.Indexes[0].Type, qt.Equals, "bloom_filter")
	c.Assert(got.Indexes[0].Granularity, qt.Equals, 64)
}
