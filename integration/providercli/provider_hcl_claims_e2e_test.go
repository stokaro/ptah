//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/engine"
	"ptah.run/internal/atlashcl"
)

func hclClaimsProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderCommand(c, "hcl-claims")
	return runtime
}

// hclClaimsDocument declares a retention in a table block, one in a
// top-level block that names another table, and one as an attribute of an
// index with its type. Its header leaves the retention of a third table
// undescribed, which the provider reads.
const hclClaimsDocument = `// ptah:not-described retention "audits"
schema "public" {}
table "records" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
  column "body" {
    type = text
  }
  index "by_body" {
    columns           = [column.body]
    type              = BTREE
    retention_seconds = "9"
  }
  retention {
    seconds = "5"
  }
}
table "events" {
  schema = schema.public
  column "id" {
    type = integer
    null = false
  }
}
retention "public" "events" {
  seconds = "30"
}
`

// TestProviderHCLClaims_HappyPath reads a provider's blocks and index
// attributes, attaches each value to what its block declares a setting of,
// and states the provider's account of the document.
func TestProviderHCLClaims_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := hclClaimsProvider(c)

	db, err := atlashcl.ParseWithOptions(c.Context(), []byte(hclClaimsDocument), "schema.hcl", atlashcl.Options{HCL: runtime.HCL()})

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{
		`table events {"seconds":30}`,
		`table records {"seconds":5}`,
		`index by_body {"seconds":9}`,
	})
	var subjects []string
	for _, record := range db.FeatureCoverage.SubjectRecords() {
		subjects = append(subjects, string(record.Subject.Kind)+" "+record.Subject.Name.Source+" "+string(record.Knowledge.State))
	}
	c.Assert(subjects, qt.ContentEquals, []string{"table records complete", "table events complete", "index by_body complete"})
}

// TestProviderHCLClaims_FailurePath refuses an index whose inherited type the
// provider refuses a retention on, a document whose header leaves undescribed
// a table it declares a retention of, which shows the header reaches the
// provider, and, in a run that did not select the provider, the provider's
// block as an unknown name.
func TestProviderHCLClaims_FailurePath(t *testing.T) {
	runtime := hclClaimsProvider(qt.New(t))
	plain, err := engine.New()
	qt.New(t).Assert(err, qt.IsNil)
	for _, row := range []struct {
		name    string
		runtime *engine.Runtime
		source  string
		want    string
	}{
		{"an index whose inherited type the provider refuses", runtime,
			"table \"records\" {\n  column \"id\" {\n    type = integer\n  }\n  index \"by_id\" {\n    columns = [column.id]\n    type = HASH\n    retention_seconds = \"9\"\n  }\n}\n",
			`(?s).*provider decode source failed \(Unknown\).*`},
		{"a table the header leaves undescribed", runtime, "// ptah:not-described retention \"events\"\nschema \"public\" {}\ntable \"events\" {\n  schema = schema.public\n  column \"id\" {\n    type = integer\n  }\n}\n" +
			"retention \"public\" \"events\" {\n  seconds = \"30\"\n}\n", `(?s).*provider decode source failed \(Unknown\).*`},
		{"the provider's block without the provider", plain, "retention \"public\" \"events\" {\n  seconds = \"30\"\n}\n",
			`parse HCL schema at schema.hcl:1,1-10: unsupported top-level block "retention"`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := atlashcl.ParseWithOptions(c.Context(), []byte(row.source), "schema.hcl", atlashcl.Options{HCL: row.runtime.HCL()})

			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(db, qt.IsNil)
		})
	}
}
