//go:build integration

package providercli_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
	"ptah.run/internal/atlashclrender"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/testutils"
)

func indexKindsProvider(c *qt.C, environment ...string) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderEnvironment(c, "index-kinds", environment)
	return runtime
}

// treeSets are the tree index sets the fixture decoded, by table, and the
// common indexes the frontend built, by name.
func treeSets(c *qt.C, runtime *engine.Runtime, db schemamodel.Database) (sets, common []string) {
	c.Helper()
	for _, index := range db.Indexes {
		common = append(common, index.Name)
	}
	return scopedRetentions(c, runtime, db), common
}

// indexKindsGoSource declares a table with two tree indexes, one spelled
// "global tree" and one with a fanout, and a btree index the frontend reads
// itself.
const indexKindsGoSource = `package models

//ptah:schema:table name="docs"
type Doc struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
	//ptah:schema:field name="gen" type="INTEGER"
	Gen int
	//ptah:schema:field name="body" type="TEXT"
	//ptah:schema:index name="by_body" fields="body" type="btree"
	Body string
	//ptah:schema:index name="by_gen" fields="gen, body" include="id" type="Global  Tree" comment="generations" fanout="4"
	_ struct{}
	//ptah:schema:index name="by_id" fields="id" type="tree" unique="true" ops="int_ops"
	_ struct{}
}
`

// wantTreeSet is the tree index set the sources of these tests declare.
const wantTreeSet = `table docs [{"name":"by_gen","type":"Global  Tree","columns":["gen","body"],"include":["id"],"comment":"generations","fanout":4},` +
	`{"name":"by_id","type":"tree","columns":["id"],"unique":true,"ops":"int_ops"}]`

// TestProviderIndexKindsGo_HappyPath reads the tree index directives of a Go
// file as one fragment each, which the provider reads into the table's tree
// index set, and builds no common index of them; the btree index stays the
// frontend's.
func TestProviderIndexKindsGo_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := indexKindsProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "docs.go", indexKindsGoSource)

	c.Assert(err, qt.IsNil)
	sets, common := treeSets(c, runtime, db)
	c.Assert(sets, qt.DeepEquals, []string{wantTreeSet})
	c.Assert(common, qt.DeepEquals, []string{"by_body"})
}

// indexKindsYAMLSource declares the same table in YAML, the second tree index
// as a top-level entry that names its table.
const indexKindsYAMLSource = `tables:
  docs:
    columns:
      id: {type: INTEGER, primary: true}
      gen: {type: INTEGER}
      body: {type: TEXT}
    indexes:
      by_body: {fields: [body], type: btree}
      by_gen:
        fields: [gen, body]
        include: [id]
        type: Global  Tree
        comment: generations
        fanout: 4
indexes:
  by_id: {table: docs, fields: [id], type: tree, unique: true, ops: int_ops}
`

// TestProviderIndexKindsYAML_HappyPath reads the tree index entries of a YAML
// document, a table's and a top-level one, as the Go source declares them.
func TestProviderIndexKindsYAML_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := indexKindsProvider(c)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte(indexKindsYAMLSource))

	c.Assert(err, qt.IsNil)
	sets, common := treeSets(c, runtime, *db)
	c.Assert(sets, qt.DeepEquals, []string{wantTreeSet})
	c.Assert(common, qt.DeepEquals, []string{"by_body"})
}

// indexKindsHCLSource declares the same table in HCL.
const indexKindsHCLSource = `table "docs" {
  column "id" {
    type = INTEGER
  }
  column "gen" {
    type = INTEGER
  }
  column "body" {
    type = TEXT
  }
  primary_key {
    columns = [column.id]
  }
  index "by_body" {
    type    = "btree"
    columns = [column.body]
  }
  index "by_gen" {
    type    = "Global  Tree"
    columns = [column.gen, column.body]
    include = [column.id]
    comment = "generations"
    fanout  = 4
  }
  index "by_id" {
    unique  = true
    type    = "tree"
    ops     = "int_ops"
    columns = [column.id]
  }
}
`

// TestProviderIndexKindsHCL_HappyPath reads the tree index blocks of an HCL
// document as the Go source declares them.
func TestProviderIndexKindsHCL_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := indexKindsProvider(c)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(indexKindsHCLSource), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

	c.Assert(err, qt.IsNil)
	sets, common := treeSets(c, runtime, *db)
	c.Assert(sets, qt.DeepEquals, []string{wantTreeSet})
	c.Assert(common, qt.DeepEquals, []string{"by_body"})
}

// TestProviderIndexKindsGo_FailurePath refuses a tree index directive that
// writes an attribute the provider would never receive, one without a name,
// and the provider's refusal of a fanout on an index of another type.
func TestProviderIndexKindsGo_FailurePath(t *testing.T) {
	runtime := indexKindsProvider(qt.New(t))
	declare := func(directive string) string {
		return "package models\n\n//ptah:schema:table name=\"docs\"\ntype Doc struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n" +
			"\t//" + directive + "\n\tID int\n}\n"
	}
	for _, row := range []struct {
		name   string
		source string
		want   string
	}{
		{"a condition", declare(`ptah:schema:index name="by_id" fields="id" type="tree" where="id > 0"`),
			`.*index "by_id" is a "tree" index, which engine "fixture" reads in place of the frontend; the provider receives no "where" attribute of it on //ptah:schema:index at Doc`},
		{"no name", declare(`ptah:schema:index fields="id" type="tree"`),
			`.*//ptah:schema:index at Doc of type "tree" needs a name attribute: its owner reads the index by its name`},
		{"a fanout of a btree index", declare(`ptah:schema:index name="by_id" fields="id" type="btree" fanout="4"`),
			`.*provider decode source failed \(\w+\).*`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := goschema.ParseSource(runtime.Annotations(), "docs.go", row.source)

			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}

// TestProviderIndexKindsYAMLAndHCL_FailurePath refuses a tree index entry and
// block that write what the provider would never receive.
func TestProviderIndexKindsYAMLAndHCL_FailurePath(t *testing.T) {
	runtime := indexKindsProvider(qt.New(t))
	t.Run("a YAML condition", func(t *testing.T) {
		c := qt.New(t)

		db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte("tables:\n  docs:\n    columns:\n      id: {type: INTEGER}\n"+
			"    indexes:\n      by_id: {fields: [id], type: tree, where: id > 0}\n"))

		c.Assert(err, qt.ErrorMatches, `.*index "by_id" is a "tree" index, which example.org/fixture reads in place of the frontend, and example.org/fixture receives no where of it`)
		c.Assert(db, qt.IsNil)
	})
	t.Run("an HCL condition", func(t *testing.T) {
		c := qt.New(t)

		db, err := atlashcl.ParseWithOptions(c.Context(), []byte("table \"docs\" {\n  column \"id\" {\n    type = INTEGER\n  }\n"+
			"  index \"by_id\" {\n    type = \"tree\"\n    where = \"id > 0\"\n    columns = [column.id]\n  }\n}\n"), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

		c.Assert(err, qt.ErrorMatches, `.*index "by_id" is a "tree" index, which its owner reads in place of the frontend, and its owner receives no where of it`)
		c.Assert(db, qt.IsNil)
	})
}

// A run that does not select the provider builds the tree indexes as common
// indexes, as any index of a type no owner reads.
func TestProviderIndexKindsWithoutTheProvider_KeepsTheCommonIndexes(t *testing.T) {
	c := qt.New(t)
	plain, err := engine.New()
	c.Assert(err, qt.IsNil)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(strings.Replace(indexKindsHCLSource, "    fanout  = 4\n", "", 1)), "schema.hcl",
		atlashcl.Options{HCL: plain.HCL()})

	c.Assert(err, qt.IsNil)
	sets, common := treeSets(c, plain, *db)
	c.Assert(sets, qt.HasLen, 0)
	c.Assert(common, qt.DeepEquals, []string{"by_body", "by_gen", "by_id"})
}

// TestProviderIndexKindsGoExport_HappyPath writes a table's tree index set
// back as the frontend's index directives, after the table's own index, and
// reading the source back gives the same set.
func TestProviderIndexKindsGoExport_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := indexKindsProvider(c)
	db, err := goschema.ParseSource(runtime.Annotations(), "docs.go", indexKindsGoSource)
	c.Assert(err, qt.IsNil)

	files, err := goschematogo.Render(c.Context(), &db, goschematogo.Options{Annotations: runtime.Annotations(), Runtime: runtime, PackageName: "models",
		SingleFile: true, Dialect: "fixture"})

	c.Assert(err, qt.IsNil)
	c.Assert(files, qt.HasLen, 1)
	source := string(files[0].Data)
	c.Assert(source, qt.Contains, "\t//ptah:schema:index name=\"by_gen\" fields=\"gen,body\" include=\"id\" type=\"Global  Tree\" comment=\"generations\" fanout=\"4\"\n\t_ struct{}\n")
	c.Assert(source, qt.Contains, "\t//ptah:schema:index name=\"by_id\" fields=\"id\" unique=\"true\" type=\"tree\" ops=\"int_ops\"\n\t_ struct{}\n")
	again, err := goschema.ParseSource(runtime.Annotations(), files[0].Name, source)
	c.Assert(err, qt.IsNil)
	sets, common := treeSets(c, runtime, again)
	c.Assert(sets, qt.DeepEquals, []string{wantTreeSet})
	c.Assert(common, qt.DeepEquals, []string{"by_body"})
}

// TestProviderIndexKindsHCLExport_HappyPath writes the set as the table's
// index blocks, among the table's own by name, through `ptah schema export
// --to hcl`, and the document reads back to the same set.
func TestProviderIndexKindsHCLExport_HappyPath(t *testing.T) {
	c := qt.New(t)
	f := newProviderCLI(c, fixtureReleaseSpec(c, "index-kinds", testutils.FixtureRelease{}))
	models := c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(models, "docs.go"), []byte(indexKindsGoSource), 0o600), qt.IsNil)
	out := filepath.Join(c.TempDir(), "schema.hcl")

	_, stderr, code := executeProviderCLI(c.Context(), []string{"schema", "export", "--provider-config", f.manifestPath, "--provider-engine", "fixture",
		"--to", "hcl", "--root-dir", models, "--out", out})

	c.Assert(code, qt.Equals, 0, qt.Commentf("%s", stderr))
	written, err := os.ReadFile(out)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Contains, "  index \"by_body\" {\n    type = \"btree\"\n    columns = [column.body]\n  }\n"+
		"  index \"by_gen\" {\n    type = \"Global  Tree\"\n    comment = \"generations\"\n    include = [column.id]\n    fanout = 4\n    columns = [column.gen, column.body]\n  }\n"+
		"  index \"by_id\" {\n    unique = true\n    type = \"tree\"\n    ops = \"int_ops\"\n    columns = [column.id]\n  }\n")
	runtime := indexKindsProvider(c)
	db, err := atlashcl.ParseWithOptions(c.Context(), written, "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)
	sets, common := treeSets(c, runtime, *db)
	c.Assert(sets, qt.DeepEquals, []string{wantTreeSet})
	c.Assert(common, qt.DeepEquals, []string{"by_body"})
}

// TestProviderIndexKindsHCLPartialLoss writes a set its provider writes in
// part: the index blocks are written, and what they do not hold is a warning
// at the table.
func TestProviderIndexKindsHCLPartialLoss(t *testing.T) {
	c := qt.New(t)
	runtime := indexKindsProvider(c, "FIXTURE_TREE_PARTIAL=1")
	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(indexKindsHCLSource), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})
	c.Assert(err, qt.IsNil)

	result, err := atlashclrender.Render(db, atlashclrender.Options{HCL: runtime.HCL(), Target: "fixture"})

	c.Assert(err, qt.IsNil)
	c.Assert(string(result.Data), qt.Contains, "  index \"by_id\" {\n")
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{{Severity: atlashclrender.SeverityWarning, Path: "table.docs",
		Message: `the tree depth of table "docs" is not represented in the source`}})
}
