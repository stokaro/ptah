package pgtablesource_test

import (
	"strconv"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/hclext"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/parser"
	"ptah.run/internal/pgtablesource"
)

func TestPersistenceProperties_RoundTrip(t *testing.T) {
	for _, unlogged := range []bool{true, false} {
		t.Run(strconv.FormatBool(unlogged), func(t *testing.T) {
			c := qt.New(t)
			fragment := schemaext.PropertyFragment{Kind: pgtable.PersistenceKind, Properties: map[string]string{"unlogged": strconv.FormatBool(unlogged)}}
			service := pgtablesource.Service{}
			values, err := service.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: "postgres", Format: schemaext.TablePlatformProperties, Fragments: []schemaext.PropertyFragment{fragment}})
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Value{&pgtable.DesiredPersistence{Unlogged: unlogged}})
			encoded, err := service.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: "postgres", Format: schemaext.TablePlatformProperties, Values: values})
			c.Assert(err, qt.IsNil)
			c.Assert(encoded, qt.DeepEquals, []schemaext.PropertyFragment{fragment})
			encoded[0].Properties["unlogged"] = "changed"
			c.Assert(fragment.Properties["unlogged"], qt.Equals, strconv.FormatBool(unlogged))
		})
	}
}

func TestPersistenceProperties_InvalidBatch(t *testing.T) {
	for _, properties := range []map[string]string{nil, {"unlogged": ""}, {"unlogged": "TRUE"}, {"unlogged": "1"}, {"other": "true"}, {"unlogged": "true", "other": "false"}} {
		c := qt.New(t)
		values, err := (pgtablesource.Service{}).DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: "postgres", Format: schemaext.TablePlatformProperties,
			Fragments: []schemaext.PropertyFragment{{Kind: pgtable.PersistenceKind, Properties: map[string]string{"unlogged": "true"}}, {Kind: pgtable.PersistenceKind, Properties: properties}}})
		c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
		c.Assert(values, qt.IsNil)
	}
}

func TestPersistenceSQL_TablePrefix(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(sqlext.NewSet(pgtablesource.SQL()...))
	parsed, err := parser.NewParser(`CREATE /* prefix */ UNLOGGED /* end */ TABLE IF NOT EXISTS "app"."cache" (id int PRIMARY KEY);
CREATE TABLE app.plain (id int);
CREATE UNLOGGED TABLE other.cache (id int);`, parser.WithDialect("postgres"), parser.WithSQL(owners)).Parse()
	c.Assert(err, qt.IsNil)
	c.Assert(parsed.Statements, qt.HasLen, 3)
	first := parsed.Statements[0].(*ast.CreateTableNode)
	c.Assert(first.Name, qt.Equals, `"app"."cache"`)
	c.Assert(first.IfNotExists, qt.IsTrue)
	c.Assert(first.Columns, qt.HasLen, 1)
	c.Assert(first.Columns[0].Primary, qt.IsTrue)
	for _, row := range []struct {
		index    int
		unlogged bool
		present  bool
	}{{0, true, true}, {1, false, false}, {2, true, true}} {
		value, found, err := pgtable.PersistenceOf(parsed.Statements[row.index].(*ast.CreateTableNode).Facets)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.Equals, row.present)
		c.Assert(value.Unlogged, qt.Equals, row.unlogged)
	}
}

func TestPersistenceSQL_RefusesMalformedPrefix(t *testing.T) {
	for _, sql := range []string{`CREATE UNLOGGED VIEW v AS SELECT 1`, `CREATE UNLOGGED OTHER TABLE t (id int)`, `CREATE UNLOGGED UNLOGGED TABLE t (id int)`} {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			owners := must.Must(sqlext.NewSet(pgtablesource.SQL()...))
			_, err := parser.NewParser(sql, parser.WithDialect("postgres"), parser.WithSQL(owners)).Parse()
			c.Assert(err, qt.IsNotNil)
		})
	}
}

func TestPersistenceHCL_TableAttributes(t *testing.T) {
	c := qt.New(t)
	owners := must.Must(hclext.NewSet(pgtablesource.HCL()))
	db, err := atlashcl.ParseWithOptions(t.Context(), []byte(`schema "app" {}
schema "other" {}
table "app" "cache" { unlogged = true }
table "other" "cache" { unlogged = false }
table "app" "plain" {}`), "schema.hcl", atlashcl.Options{HCL: owners})
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 3)
	tables := make(map[string]schemamodel.Table)
	for _, table := range db.Tables {
		tables[table.Schema+"."+table.Name] = table
	}
	for _, row := range []struct {
		name     string
		unlogged bool
		present  bool
	}{
		{"app.cache", true, true}, {"other.cache", false, true}, {"app.plain", false, false},
	} {
		value, found, err := pgtable.PersistenceOf(tables[row.name].Facets)
		c.Assert(err, qt.IsNil)
		c.Assert(found, qt.Equals, row.present, qt.Commentf("table %s", row.name))
		c.Assert(value.Unlogged, qt.Equals, row.unlogged)
	}
}
