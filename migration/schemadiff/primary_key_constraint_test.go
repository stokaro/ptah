package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// declaredKeyDesired is table hk whose primary key over id is a PRIMARY KEY
// constraint named name, the spelling a Go annotation, a YAML document and an
// HCL constraint block produce.
func declaredKeyDesired(name, comment string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "HK", Name: "hk"}},
		Fields: []schemamodel.Field{{StructName: "HK", Name: "id", Type: "INT"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "HK", Table: "hk", Name: name, Type: "PRIMARY KEY", Columns: []string{"id"}, Comment: comment,
		}},
	}
}

// liveKeyCurrent is table hk as a reader reports it, its primary key over id
// named name.
func liveKeyCurrent(name string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "hk", Columns: []catalog.Column{
			{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO", IsPrimaryKey: true},
		}}},
		Constraints: []catalog.Constraint{{
			Name: name, TableName: "hk", Type: "PRIMARY KEY", ColumnNames: []string{"id"}, ColumnName: "id",
		}},
	}
}

// TestCompare_DeclaredPrimaryKey_Synced plans nothing for a declared key the
// database holds under the name its server gave it. MySQL and MariaDB call
// every primary key PRIMARY, and PostgreSQL calls an unnamed one
// `<table>_pkey`. The key column reads as a key column without the field
// declaring it one (stokaro/ptah#3959).
func TestCompare_DeclaredPrimaryKey_Synced(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared string
		live     string
	}{
		{name: "named on MySQL", dialect: platform.MySQL, declared: "hk_pk", live: "PRIMARY"},
		{name: "unnamed on MariaDB", dialect: platform.MariaDB, live: "PRIMARY"},
		{name: "unnamed on PostgreSQL", dialect: platform.Postgres, live: "hk_pkey"},
		{name: "named on PostgreSQL", dialect: platform.Postgres, declared: "hk_pk", live: "hk_pk"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(declaredKeyDesired(test.declared, ""), liveKeyCurrent(test.live), test.dialect)

			c.Assert(diff.HasChanges(), qt.IsFalse, qt.Commentf("%#v", diff))
		})
	}
}

// TestCompare_DeclaredPrimaryKey_Renamed plans a PostgreSQL key declared under
// another name than the database's as a replacement: the old key is dropped,
// once, before the new one is added under its declared name. A table holds one
// primary key, so the other order fails.
func TestCompare_DeclaredPrimaryKey_Renamed(t *testing.T) {
	c := qt.New(t)
	diff := schemadiff.CompareWithDialect(declaredKeyDesired("hk_pk", ""), liveKeyCurrent("hk_pkey"), platform.Postgres)

	plan, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)

	c.Assert(err, qt.IsNil)
	joined := strings.Join(plan, "\n")
	c.Assert(strings.Count(joined, `DROP CONSTRAINT IF EXISTS "hk_pkey"`), qt.Equals, 1, qt.Commentf("%s", joined))
	c.Assert(strings.Index(joined, `DROP CONSTRAINT IF EXISTS "hk_pkey"`) <
		strings.Index(joined, `ADD CONSTRAINT "hk_pk" PRIMARY KEY ("id")`), qt.IsTrue, qt.Commentf("%s", joined))
}

// TestCompare_DeclaredPrimaryKey_DefaultNameWritesNone adds a PostgreSQL key
// whose name is the one the server gives an unnamed key without the name, so
// the statement reads as it does for the table's own key.
func TestCompare_DeclaredPrimaryKey_DefaultNameWritesNone(t *testing.T) {
	c := qt.New(t)
	current := liveKeyCurrent("hk_pkey")
	current.Constraints = nil
	diff := schemadiff.CompareWithDialect(declaredKeyDesired("hk_pkey", ""), current, platform.Postgres)

	plan, err := planner.GenerateSchemaDiffSQLStatements(diff, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(plan, "\n"), qt.Contains, `ADD PRIMARY KEY ("id")`)
	c.Assert(strings.Join(plan, "\n"), qt.Not(qt.Contains), "CONSTRAINT")
}

// TestCompare_DeclaredPrimaryKey_CommentIsCompared compares the comment of an
// unnamed declared key with the comment of the key PostgreSQL named for it.
func TestCompare_DeclaredPrimaryKey_CommentIsCompared(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(declaredKeyDesired("", "the key"), liveKeyCurrent("hk_pkey"), platform.Postgres)

	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.ConstraintCommentsChanged, qt.HasLen, 1)
	c.Assert(diff.ConstraintCommentsChanged[0].Name, qt.Equals, "hk_pkey")
}
