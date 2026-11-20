package oraclecollation_test

import (
	"fmt"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/oraclecollation"
)

// sorts is a short V$NLS_VALID_VALUES list.
var sorts = []string{"BINARY", "FRENCH", "GERMAN"}

// declaration is table tm with the fields and indexes given.
func declaration(fields []schemamodel.Field, indexes ...schemamodel.Index) *schemamodel.Database {
	for i := range fields {
		fields[i].StructName = "Tm"
	}
	for i := range indexes {
		indexes[i].StructName = "Tm"
	}
	return &schemamodel.Database{
		Tables:  []schemamodel.Table{{StructName: "Tm", Name: "tm"}},
		Fields:  fields,
		Indexes: indexes,
	}
}

func desc(name string) schemamodel.Index {
	return schemamodel.Index{Name: name, Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}}}
}

func asc(name string) schemamodel.Index { return schemamodel.Index{Name: name, Fields: []string{"c"}} }

func collatedC() []schemamodel.Field {
	return []schemamodel.Field{{Name: "c", Collate: "BINARY_CI"}, {Name: "a"}}
}

// existing is the database read of table tm holding columns c, d and id, with
// the indexes given.
func existing(indexes ...catalog.Index) *catalog.Database {
	for i := range indexes {
		indexes[i].TableName = "TM"
	}
	return &catalog.Database{
		Tables:  []catalog.Table{{Name: "TM", Columns: []catalog.Column{{Name: "ID"}, {Name: "C"}, {Name: "D"}}}},
		Indexes: indexes,
	}
}

// descDeclaration is a collated c with the indexes given, declared in that order.
func descDeclaration(indexes ...schemamodel.Index) *schemamodel.Database {
	return declaration(collatedC(), indexes...)
}

func named(index schemamodel.Index, name string) schemamodel.Index {
	index.Name = name
	return index
}

func TestRefuseDeclared_HappyPath(t *testing.T) {
	descD := schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "a"}, {Name: "c", Desc: true}}}
	descC := schemamodel.Index{Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}, {Name: "a"}}}
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		observed *catalog.Database
		sorts    []string
	}{
		{name: "no declaration"},
		{name: "known names", sorts: sorts, desired: declaration([]schemamodel.Field{
			{Name: "a", Collate: "BINARY_CI"}, {Name: "b", Collate: "french_ai"}, {Name: "c", Collate: "GERMAN"}, {Name: "d", Collate: "USING_NLS_COMP"},
		})},
		{name: "an unknown name where the list was not read", desired: declaration([]schemamodel.Field{
			{Name: "a", Collate: "utf8mb4_bin"},
		})},
		{name: "a first index over a collated column takes DESC", sorts: sorts, desired: descDeclaration(desc("i"))},
		{name: "DESC first by name, then an ascending index", sorts: sorts, desired: descDeclaration(asc("tm_b"), desc("tm_a"))},
		{name: "DESC after DESC in every shape", sorts: sorts, desired: descDeclaration(
			named(descD, "tm_b"), named(descC, "tm_c"), desc("tm_d"),
			schemamodel.Index{Name: "tm_e", Fields: []string{"a", "c desc"}},
		)},
		{name: "DESC after a function-based index", sorts: sorts, desired: descDeclaration(
			schemamodel.Index{Name: "tm_a", Parts: []schemamodel.IndexPart{{Expr: `UPPER("C")`}}}, desc("tm_b"),
		)},
		{name: "DESC over a column with no collation beside a collated one", sorts: sorts, desired: descDeclaration(
			schemamodel.Index{Name: "i", Parts: []schemamodel.IndexPart{{Name: "c"}, {Name: "a", Desc: true}}},
		)},
		{name: "DESC over USING_NLS_COMP", sorts: sorts, desired: declaration(
			[]schemamodel.Field{{Name: "c", Collate: "USING_NLS_COMP"}}, desc("tm_b"), asc("tm_a"),
		)},
		{name: "ascending indexes over a collated column", sorts: sorts, desired: descDeclaration(asc("i"), asc("j"))},
		{name: "DESC over an expression", sorts: sorts, desired: descDeclaration(
			schemamodel.Index{Name: "tm_b", Parts: []schemamodel.IndexPart{{Expr: "UPPER(c)", Desc: true}}}, asc("tm_a"),
		)},
		{name: "an index the database already holds is not created again", sorts: sorts,
			observed: existing(catalog.Index{Name: "I", Columns: []string{"C"}}, catalog.Index{Name: "J", Columns: []string{"C"}}),
			desired:  descDeclaration(desc("i"), asc("j"))},
		{name: "DESC after an index the database holds only as DESC", sorts: sorts,
			observed: existing(catalog.Index{Name: "TM_OLD", Parts: []catalog.IndexPart{{Name: "C", Desc: true}}}),
			desired:  descDeclaration(named(descD, "tm_new"))},
		{name: "DESC after a function-based index the database holds", sorts: sorts,
			observed: existing(catalog.Index{Name: "TM_UP", Parts: []catalog.IndexPart{{Expr: `UPPER("C")`}}}),
			desired:  descDeclaration(desc("tm_new"))},
		{name: "a UNIQUE constraint the plan adds to an existing table comes after the indexes", sorts: sorts,
			observed: existing(),
			desired: func() *schemamodel.Database {
				db := descDeclaration(desc("tm_c_desc"))
				db.Constraints = []schemamodel.Constraint{{StructName: "Tm", Name: "tm_uq", Type: "UNIQUE", Columns: []string{"c"}}}
				return db
			}()},
		{name: "a UNIQUE flag on a column the database already holds without one is not planned before the index", sorts: sorts,
			observed: existing(),
			desired:  declaration([]schemamodel.Field{{Name: "c", Collate: "BINARY_CI", Unique: true}}, desc("tm_c_desc"))},
		{name: "DESC over a column the database holds with no collation", sorts: sorts,
			observed: func() *catalog.Database {
				db := existing(catalog.Index{Name: "OTHER", Columns: []string{"C"}})
				return db
			}(),
			desired: declaration([]schemamodel.Field{{Name: "c"}}, desc("i"))},
		{name: "an index of another table over the column", sorts: sorts,
			observed: &catalog.Database{Indexes: []catalog.Index{{Name: "OTHER", TableName: "ELSEWHERE", Columns: []string{"C"}}}},
			desired:  descDeclaration(desc("i"))},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(oraclecollation.RefuseDeclared(platform.Oracle, tc.desired, tc.observed, tc.sorts), qt.IsNil)
		})
	}
}

// TestRefuseDeclared_FailurePathNamesAnUnknownCollation is the name Oracle
// answers ORA-12746 to, after the tables created before it have committed.
func TestRefuseDeclared_FailurePathNamesAnUnknownCollation(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{name: "a MySQL collation", desired: declaration([]schemamodel.Field{{Name: "v", Collate: "utf8mb4_bin"}}),
			want: `.*column "v" of table "tm" declares collation "utf8mb4_bin", which this database does not know \(ORA-12746\).*`},
		{name: "a name the platform override gives", desired: declaration([]schemamodel.Field{{
			Name: "v", Collate: "BINARY", Overrides: map[string]map[string]string{"oracle": {"collate": "latin1_swedish_ci"}},
		}}),
			want: `.*column "v" of table "tm" declares collation "latin1_swedish_ci".*`},
		{name: "a suffix Oracle does not take", desired: declaration([]schemamodel.Field{{Name: "v", Collate: "FRENCH_CS"}}),
			want: `.*declares collation "FRENCH_CS".*`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			err := oraclecollation.RefuseDeclared(platform.Oracle, tc.desired, nil, sorts)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, tc.want)
		})
	}
}

// TestRefuseDeclared_FailurePathNamesADescendingKeyAnAscendingKeyPrecedes is the
// statement Oracle answers ORA-54034 to when an index or key created before it
// holds the collated column in an ascending part. The plan creates new indexes in
// name order, so "before" is the name order, and the keys of a CREATE TABLE or an
// ADD COLUMN come ahead of every index.
func TestRefuseDeclared_FailurePathNamesADescendingKeyAnAscendingKeyPrecedes(t *testing.T) {
	const want = `.*index "tm_m_desc" of table "tm" orders column "c" DESC, and the column holds collation BINARY_CI while %s already holds it in an ascending key.*ORA-54034.*`
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		observed *catalog.Database
		holder   string
	}{
		{name: "an ascending index whose name sorts first", desired: descDeclaration(desc("tm_m_desc"), asc("tm_a")),
			holder: `index "tm_a"`},
		{name: "an index over two columns", desired: descDeclaration(
			schemamodel.Index{Name: "tm_a", Fields: []string{"a", "c"}}, desc("tm_m_desc")),
			holder: `index "tm_a"`},
		{name: "an ascending index then DESC as the second part", desired: descDeclaration(
			asc("tm_a"), schemamodel.Index{Name: "tm_m_desc", Parts: []schemamodel.IndexPart{{Name: "a"}, {Name: "c", Desc: true}}}),
			holder: `index "tm_a"`},
		{name: "DESC, then ascending, then DESC", desired: descDeclaration(
			schemamodel.Index{Name: "tm_a", Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}}}, asc("tm_b"),
			schemamodel.Index{Name: "tm_m_desc", Parts: []schemamodel.IndexPart{{Name: "a"}, {Name: "c", Desc: true}}}),
			holder: `index "tm_b"`},
		{name: "a UNIQUE column of a new table", desired: declaration([]schemamodel.Field{{Name: "c", Collate: "BINARY_CI", Unique: true}}, desc("tm_m_desc")),
			holder: `its own key`},
		{name: "a PRIMARY KEY column of a new table", desired: declaration([]schemamodel.Field{{Name: "c", Collate: "BINARY_CI", Primary: true}}, desc("tm_m_desc")),
			holder: `its own key`},
		{name: "a UNIQUE column the plan adds", observed: &catalog.Database{Tables: []catalog.Table{{Name: "TM", Columns: []catalog.Column{{Name: "ID"}}}}},
			desired: declaration([]schemamodel.Field{{Name: "id"}, {Name: "c", Collate: "BINARY_CI", Unique: true}}, desc("tm_m_desc")),
			holder:  `its own key`},
		{name: "a table UNIQUE constraint of a new table", desired: func() *schemamodel.Database {
			db := descDeclaration(desc("tm_m_desc"))
			db.Constraints = []schemamodel.Constraint{{StructName: "Tm", Name: "tm_uq", Type: "UNIQUE", Columns: []string{"a", "C"}}}
			return db
		}(), holder: `the unique constraint "tm_uq"`},
		{name: "an index the database holds, whatever its name", desired: descDeclaration(desc("tm_m_desc")),
			observed: existing(catalog.Index{Name: "TM_Z", Columns: []string{"C"}}),
			holder:   `index "TM_Z"`},
		{name: "an index the database holds as one of several parts", desired: descDeclaration(desc("tm_m_desc")),
			observed: existing(catalog.Index{Name: "TM_DC", Parts: []catalog.IndexPart{{Name: "D"}, {Name: "C"}}}),
			holder:   `index "TM_DC"`},
		{name: "a key Oracle named itself", desired: descDeclaration(desc("tm_m_desc")),
			observed: func() *catalog.Database {
				db := existing()
				db.Tables[0].Columns[1].IsUnique = true
				return db
			}(),
			holder: `a key of the table`},
		{name: "a collation the database holds and the declaration does not name",
			desired: declaration([]schemamodel.Field{{Name: "c"}}, desc("tm_m_desc")),
			observed: func() *catalog.Database {
				db := existing(catalog.Index{Name: "TM_OLD", Columns: []string{"C"}})
				db.Tables[0].Columns[1].EffectiveCollate = "BINARY_CI"
				return db
			}(),
			holder: `index "TM_OLD"`},
		{name: "DESC written after the column", desired: descDeclaration(
			schemamodel.Index{Name: "tm_m_desc", Fields: []string{"c desc"}}, asc("tm_a")),
			holder: `index "tm_a"`},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			err := oraclecollation.RefuseDeclared(platform.Oracle, tc.desired, tc.observed, sorts)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, fmt.Sprintf(want, regexp.QuoteMeta(tc.holder)))
		})
	}
}

// TestRefuseDeclared_HappyPathLeavesOtherDialectsAlone is the control that the
// check is Oracle's: the declaration it refuses on Oracle is a valid MySQL one.
func TestRefuseDeclared_HappyPathLeavesOtherDialectsAlone(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.Postgres, ""} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			desired := declaration(collatedC(), asc("tm_c"), desc("tm_c_desc"))
			desired.Fields[1].Collate = "utf8mb4_bin"

			c.Assert(oraclecollation.RefuseDeclared(dialect, desired, nil, sorts), qt.IsNil)
		})
	}
}
