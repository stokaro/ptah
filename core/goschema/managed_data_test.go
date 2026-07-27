package goschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/ptaherr"
)

func TestParseManagedDataAnnotation(t *testing.T) {
	tests := []struct {
		name     string
		key      string
		wantKeys []string
	}{
		{name: "single key", key: "code", wantKeys: []string{"code"}},
		{name: "composite key", key: "tenant_id,code", wantKeys: []string{"tenant_id", "code"}},
		{name: "composite key with spaces", key: "tenant_id, code", wantKeys: []string{"tenant_id", "code"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			db := mustParseSource(c, "schema.go", `
package fixture

//migrator:schema:data table="countries" key="`+tt.key+`" file="countries.yaml"
type Country struct {
	//migrator:schema:field name="code" type="VARCHAR(2)" primary="true"
	Code string

	//migrator:schema:field name="name" type="VARCHAR(255)" not_null="true"
	Name string
}
`)

			c.Assert(db.ManagedData, qt.HasLen, 1)
			md := db.ManagedData[0]
			c.Assert(md.StructName, qt.Equals, "Country")
			c.Assert(md.Table, qt.Equals, "countries")
			c.Assert(md.Keys, qt.DeepEquals, tt.wantKeys)
			c.Assert(md.File, qt.Equals, "countries.yaml")
		})
	}
}

func TestParseManagedDataAnnotation_MissingRequiredAttributeRejected(t *testing.T) {
	tests := []struct {
		name          string
		annotation    string
		wantAttribute string
	}{
		{
			name:          "missing file",
			annotation:    `//migrator:schema:data table="countries" key="code"`,
			wantAttribute: "file",
		},
		{
			name:          "missing key",
			annotation:    `//migrator:schema:data table="countries" file="countries.yaml"`,
			wantAttribute: "key",
		},
		{
			name:          "missing table",
			annotation:    `//migrator:schema:data key="code" file="countries.yaml"`,
			wantAttribute: "table",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := goschema.ParseSource("schema.go", `
package fixture

`+tt.annotation+`
type Country struct{}
`)

			var parseErr *ptaherr.ParseError
			c.Assert(err, qt.ErrorAs, &parseErr)
			c.Assert(parseErr.Attribute, qt.Equals, tt.wantAttribute)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrMissingRequiredAttribute)
		})
	}
}

func TestParseManagedDataAnnotation_AggregatesAcrossFiles(t *testing.T) {
	c := qt.New(t)

	root := t.TempDir()
	writeGoFile(c, root, "countries.go", `
package fixture

//migrator:schema:data table="countries" key="code" file="countries.yaml"
type Country struct {
	//migrator:schema:field name="code" type="VARCHAR(2)" primary="true"
	Code string
}
`)
	writeGoFile(c, root, "currencies.go", `
package fixture

//migrator:schema:data table="currencies" key="tenant_id,code" file="currencies.yaml"
type Currency struct {
	//migrator:schema:field name="code" type="VARCHAR(3)" primary="true"
	Code string
}
`)

	db, err := goschema.ParseDir(root)
	c.Assert(err, qt.IsNil)
	c.Assert(db.ManagedData, qt.HasLen, 2)

	byTable := map[string]goschema.ManagedData{}
	for _, md := range db.ManagedData {
		byTable[md.Table] = md
	}
	c.Assert(byTable["countries"].Keys, qt.DeepEquals, []string{"code"})
	c.Assert(byTable["countries"].File, qt.Equals, "countries.yaml")
	c.Assert(byTable["currencies"].Keys, qt.DeepEquals, []string{"tenant_id", "code"})
	c.Assert(byTable["currencies"].File, qt.Equals, "currencies.yaml")
}

func TestLoadManagedRows(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "countries.yaml"), []byte(`
- code: US
  name: United States
  rank: 1
- code: CZ
  name: Czechia
  rank: 2
`), 0o600), qt.IsNil)

	rows, err := goschema.LoadManagedRows(dir, goschema.ManagedData{
		Table: "countries",
		Keys:  []string{"code"},
		File:  "countries.yaml",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.DeepEquals, []map[string]any{
		{"code": "US", "name": "United States", "rank": 1},
		{"code": "CZ", "name": "Czechia", "rank": 2},
	})
}

func TestLoadManagedRows_MissingFileReturnsError(t *testing.T) {
	c := qt.New(t)

	_, err := goschema.LoadManagedRows(t.TempDir(), goschema.ManagedData{
		Table: "countries",
		Keys:  []string{"code"},
		File:  "does-not-exist.yaml",
	})
	c.Assert(err, qt.ErrorMatches, `read managed data file ".*does-not-exist.yaml" for table "countries": .*`)
}

func TestLoadManagedRows_MalformedYAMLReturnsError(t *testing.T) {
	c := qt.New(t)

	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "bad.yaml"), []byte("this: is: not a list of rows"), 0o600), qt.IsNil)

	_, err := goschema.LoadManagedRows(dir, goschema.ManagedData{
		Table: "countries",
		Keys:  []string{"code"},
		File:  "bad.yaml",
	})
	c.Assert(err, qt.ErrorMatches, `parse managed data file ".*bad.yaml" for table "countries": .*`)
}
