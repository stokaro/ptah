//go:build integration

package providercli_test

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/providerhost"
)

const retentionKind = schemaext.Kind("example.org/fixture/retention")

func parseGoSource(c *qt.C, runtime *engine.Runtime, source string) (retentions []string, err error) {
	c.Helper()
	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", source)
	if err != nil {
		return nil, err
	}
	for _, table := range db.Tables {
		if len(table.Facets.DeclaredKinds()) == 0 {
			continue
		}
		envelopes, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, table.Facets)
		c.Assert(err, qt.IsNil)
		for _, envelope := range envelopes {
			var wire struct {
				Payload []byte `json:"payload"`
			}
			c.Assert(json.Unmarshal(envelope.Value.Payload, &wire), qt.IsNil)
			retentions = append(retentions, table.Name+" "+string(wire.Payload))
		}
	}
	return retentions, nil
}

func TestProviderInterpretsGoAnnotations_HappyPath(t *testing.T) {
	_, runtime := yamlProvider(qt.New(t))
	for _, row := range []struct{ name, source, want string }{
		{"attributes", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention seconds=\"9007199254740993\"\ntype Record struct{}\n", `records {"seconds":9007199254740993}`},
		{"directive before the table", "package p\n\n//ptah:fixture:retention minutes=5\n//ptah:schema:table name=\"records\"\ntype Record struct{}\n", `records {"seconds":300}`},
		{"no attributes", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention\ntype Record struct{}\n", `records {"seconds":60}`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			retentions, err := parseGoSource(c, runtime, row.source)
			c.Assert(err, qt.IsNil)
			c.Assert(retentions, qt.DeepEquals, []string{row.want})
		})
	}
}

func TestProviderGoAnnotationsCoverage(t *testing.T) {
	_, runtime := yamlProvider(qt.New(t))
	t.Run("declaration", func(t *testing.T) {
		c := qt.New(t)
		db, err := goschema.ParseSource(runtime.Annotations(), "records.go", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention seconds=60\ntype Record struct{}\n")
		c.Assert(err, qt.IsNil)
		c.Assert(db.FeatureCoverage.KindRecords(), qt.HasLen, 1)
		c.Assert(db.FeatureCoverage.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Complete)
		c.Assert(db.FeatureCoverage.SubjectRecords(), qt.HasLen, 1)
		c.Assert(db.FeatureCoverage.SubjectRecords()[0].Knowledge.State, qt.Equals, schemaext.Complete)
		c.Assert(db.FeatureCoverage.SubjectRecords()[0].Targets, qt.DeepEquals, []string{"fixture"})
	})
	t.Run("defaulted", func(t *testing.T) {
		c := qt.New(t)
		db, err := goschema.ParseSource(runtime.Annotations(), "records.go", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention\ntype Record struct{}\n")
		c.Assert(err, qt.IsNil)
		c.Assert(db.FeatureCoverage.SubjectRecords(), qt.HasLen, 1)
		c.Assert(db.FeatureCoverage.SubjectRecords()[0].Knowledge.State, qt.Equals, schemaext.Defaulted)
	})
	t.Run("no declaration", func(t *testing.T) {
		c := qt.New(t)
		db, err := goschema.ParseSource(runtime.Annotations(), "records.go", "package p\n\n//ptah:schema:table name=\"records\"\ntype Record struct{}\n")
		c.Assert(err, qt.IsNil)
		c.Assert(db.Tables[0].Facets.DeclaredKinds(), qt.HasLen, 0)
		c.Assert(db.FeatureCoverage.KindRecords(), qt.HasLen, 1)
		c.Assert(db.FeatureCoverage.KindRecords()[0].Knowledge.Unscoped().State, qt.Equals, schemaext.Complete)
		c.Assert(db.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
	})
}

func TestProviderGoAnnotationsDeclareTheClaimedKind(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProvider(c)
	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention seconds=60\ntype Record struct{}\n")
	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].Facets.DeclaredKinds(), qt.DeepEquals, []schemaext.Kind{retentionKind})
}

func TestProviderGoAnnotations_FailurePath(t *testing.T) {
	_, runtime := yamlProvider(qt.New(t))
	for _, row := range []struct {
		name, source, want string
		is                 error
	}{
		{"attribute outside the claim", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention typo=1\ntype Record struct{}\n",
			`unknown annotation attribute "typo" on //ptah:fixture:retention at Record`, ptaherr.ErrUnknownAttribute},
		{"value the owner refuses", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention seconds=soon\ntype Record struct{}\n",
			`decode Go directives of engine "fixture": provider decode source failed .* on //ptah:fixture:retention at Record`, ptaherr.ErrInvalidAttributeValue},
		{"struct without a table", "package p\n\n//ptah:fixture:retention seconds=60\ntype Record struct{}\n",
			`struct Record maps to no table in this file.* on //ptah:fixture:retention at Record`, ptaherr.ErrInvalidAttributeValue},
		{"declared twice", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention seconds=60\n//ptah:fixture:retention seconds=61\ntype Record struct{}\n",
			`decode Go directives of engine "fixture": provider decode source failed .*\(reported at the first of 2 Go directives sent together\) on //ptah:fixture:retention at Record`, ptaherr.ErrInvalidAttributeValue},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			retentions, err := parseGoSource(c, runtime, row.source)
			c.Assert(err, qt.ErrorIs, row.is)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want+`.*`)
			c.Assert(retentions, qt.HasLen, 0)
		})
	}
}

func TestProviderGoDirectiveOnAnUnhostedFrontendDirective_FailurePath(t *testing.T) {
	c := qt.New(t)
	executable := independentCLIProviderCommand(c, "go-unhosted-directive")
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	host, err := providerhost.Start(c.Context(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"})
	c.Assert(err, qt.ErrorMatches, `.*Go directive "ptah:schema:constraint" of engine "fixture" adds attributes to a frontend directive the host does not read for providers.*`)
	c.Assert(host, qt.IsNil)
}

// scopedGoSource declares a settings value on every part of a table the
// provider can own: the table, two columns and three indexes. Each is written
// either as an attribute of the frontend's own directive or as a directive of
// the provider's own, and one index belongs to a table another struct declares.
const scopedGoSource = `package p

//ptah:schema:table name="records" seconds=30
type Record struct {
	//ptah:schema:field name="id" type="INTEGER" minutes=2
	ID int
	//ptah:schema:field name="body" type="TEXT"
	//ptah:fixture:column-retention seconds=7
	Body string

	//ptah:schema:index name="by_body" fields="body" seconds=9
	_ int
	//ptah:schema:index name="by_id" fields="id"
	//ptah:fixture:index-retention minutes=1
	_ int
}

type Extra struct {
	//ptah:schema:index name="elsewhere" fields="id" table="records"
	//ptah:fixture:index-retention seconds=11
	_ int
}
`

func scopedRetentions(c *qt.C, runtime *engine.Runtime, db schemamodel.Database) []string {
	c.Helper()
	var retentions []string
	collect := func(label string, facets schemaext.Facets) {
		c.Helper()
		envelopes, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, facets)
		c.Assert(err, qt.IsNil)
		for _, envelope := range envelopes {
			var wire struct {
				Payload []byte `json:"payload"`
			}
			c.Assert(json.Unmarshal(envelope.Value.Payload, &wire), qt.IsNil)
			retentions = append(retentions, label+" "+string(wire.Payload))
		}
	}
	for _, table := range db.Tables {
		collect("table "+table.Name, table.Facets)
	}
	for _, field := range db.Fields {
		collect("column "+field.Name, field.Facets)
	}
	for _, index := range db.Indexes {
		collect("index "+index.Name, index.Facets)
	}
	return retentions
}

func scopedGoProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderCommand(c, "go-scopes")
	return runtime
}

func TestProviderGoDirectivesAtColumnAndIndexScope_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := scopedGoProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", scopedGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, db), qt.DeepEquals, []string{
		`table records {"seconds":30}`,
		`column id {"seconds":120}`,
		`column body {"seconds":7}`,
		`index by_body {"seconds":9}`,
		`index by_id {"seconds":60}`,
		`index elsewhere {"seconds":11}`,
	})
}

// holderGoSource declares a table directive on a struct that maps to no table.
// The directive names the table it describes with the table attribute, as an
// index directive does, and the same attribute moves a directive of a table's
// own struct to another table.
const holderGoSource = `package p

//ptah:schema:table name="records" schema="app"
type Record struct{}

//ptah:schema:table name="events"
type Event struct{}

//ptah:fixture:table-retention table="records" seconds=5
type Holder struct{}

//ptah:schema:table name="extra"
//ptah:fixture:table-retention table="events" minutes=2
type Extra struct{}
`

func TestProviderGoTableDirectiveNamesItsTable_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := scopedGoProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", holderGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, db), qt.DeepEquals, []string{
		`table records {"seconds":5}`,
		`table events {"seconds":120}`,
	})
}

func TestProviderGoTableDirectiveNamesItsTable_FailurePath(t *testing.T) {
	runtime := scopedGoProvider(qt.New(t))
	const head = "package p\n\n//ptah:schema:table name=\"records\" schema=\"app\"\ntype Record struct{}\n\n" +
		"//ptah:schema:table name=\"records\" schema=\"archive\"\ntype Archived struct{}\n\n"
	for _, row := range []struct{ name, source, want string }{
		{"a struct without a table that names none", head + "//ptah:fixture:table-retention seconds=1\ntype Holder struct{}\n",
			`struct Holder maps to no table in this file.* on //ptah:fixture:table-retention at Holder`},
		{"a table the file does not declare", head + "//ptah:fixture:table-retention table=\"absent\" seconds=1\ntype Holder struct{}\n",
			`table "absent" is not declared in this file, and the ptah:fixture:table-retention directive is declared beside its table on //ptah:fixture:table-retention at Holder`},
		{"a table declared in two schemas", head + "//ptah:fixture:table-retention table=\"records\" seconds=1\ntype Holder struct{}\n",
			`table "records" is declared in schemas "app" and "archive"; name the schema in the table attribute on //ptah:fixture:table-retention at Holder`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := goschema.ParseSource(runtime.Annotations(), "records.go", row.source)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, row.want)
		})
	}
}

func TestProviderGoDirectivesAtColumnAndIndexScope_Coverage(t *testing.T) {
	c := qt.New(t)
	runtime := scopedGoProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", scopedGoSource)

	c.Assert(err, qt.IsNil)
	var subjects []string
	for _, record := range db.FeatureCoverage.SubjectRecords() {
		subjects = append(subjects, string(record.Subject.Kind)+" "+record.Subject.Parent.Source+" "+record.Subject.Name.Source+" "+string(record.Knowledge.State))
	}
	// A column is identified by the table it belongs to, as the planner
	// identifies it, or the comparator would never find its coverage. The
	// fixture names its indexes in the schema, so an index has no parent.
	c.Assert(subjects, qt.DeepEquals, []string{
		"column records body complete",
		"column records id complete",
		"index  by_body complete",
		"index  by_id complete",
		"index  elsewhere complete",
		"table  records complete",
	})
	c.Assert(db.FeatureCoverage.SubjectRecords()[0].Targets, qt.DeepEquals, []string{"fixture"})
}

func TestProviderGoDirectivesAtColumnAndIndexScope_FailurePath(t *testing.T) {
	runtime := scopedGoProvider(qt.New(t))
	const head = "package p\n\n//ptah:schema:table name=\"records\"\ntype Record struct {\n"
	for _, row := range []struct{ name, source, want string }{
		{"a column directive beside no field directive", head + "\t//ptah:fixture:column-retention seconds=1\n\tBody string\n}\n",
			`a column directive is written beside the ptah:schema:field directive of its column on //ptah:fixture:column-retention at Record`},
		{"an index directive beside no index directive", head + "\t//ptah:fixture:index-retention seconds=1\n\t_ int\n}\n",
			`an index directive is written beside the ptah:schema:index directive of its index on //ptah:fixture:index-retention at Record`},
		{"a column value the provider refuses", head + "\t//ptah:schema:field name=\"id\" type=\"INTEGER\"\n\t//ptah:fixture:column-retention seconds=soon\n\tID int\n}\n",
			`decode Go directives of engine "fixture": provider decode source failed .* on //ptah:fixture:column-retention at Record`},
		{"an attribute value the provider refuses", head + "\t//ptah:schema:field name=\"id\" type=\"INTEGER\" seconds=soon\n\tID int\n}\n",
			`decode Go directives of engine "fixture": provider decode source failed .* on //ptah:schema:field at Record`},
		{"a column that declares a value twice", head + "\t//ptah:schema:field name=\"id\" type=\"INTEGER\" seconds=1\n\t//ptah:fixture:column-retention seconds=2\n\tID int\n}\n",
			`decode Go directives of engine "fixture": provider decode source failed .*\(reported at the first of 2 Go directives sent together\) on //ptah:schema:field at Record`},
		{"a table that declares a value twice", "package p\n\n//ptah:schema:table name=\"records\" seconds=1\n//ptah:fixture:retention seconds=2\ntype Record struct{}\n",
			`decode Go directives of engine "fixture": provider decode source failed .*\(reported at the first of 2 Go directives sent together\) on //ptah:schema:table at Record`},
		{"an index of a table the file does not declare", "package p\n\ntype Extra struct {\n\t//ptah:schema:index name=\"x\" fields=\"id\" table=\"absent\" seconds=1\n\t_ int\n}\n",
			`table "absent" is not declared in this file.* on //ptah:schema:index at Extra`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(runtime.Annotations(), "records.go", row.source)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+row.want)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}

// TestProviderGoAttributesWithoutTheProvider_FailurePath pins the control: a
// run that selected the plain fixture does not know the attributes the
// go-scopes fixture claims, so they are refused as unknown and never dropped.
func TestProviderGoAttributesWithoutTheProvider_FailurePath(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProvider(c)

	_, err := goschema.ParseSource(runtime.Annotations(), "records.go", scopedGoSource)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnknownAttribute)
}

// TestProviderGoFacetOfAnotherPart_FailurePath pins that a value the provider
// attaches to a part the directive does not describe is refused. The provider
// is the one that decides the meaning of a directive, and it is not the one that
// decides which column or index the author wrote it beside.
func TestProviderGoFacetOfAnotherPart_FailurePath(t *testing.T) {
	_, runtime := yamlProviderCommand(qt.New(t), "go-misplaced-subject")
	for _, row := range []struct{ name, source, want string }{
		{"column", "package p\n\n//ptah:schema:table name=\"records\"\ntype Record struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" seconds=1\n\tID int\n}\n",
			`decoded Go facet is owned by column "misplaced", not the column "id" the directive describes on //ptah:schema:field at Record`},
		{"index", "package p\n\n//ptah:schema:table name=\"records\"\ntype Record struct {\n\t//ptah:schema:index name=\"by_id\" fields=\"id\" seconds=1\n\t_ int\n}\n",
			`decoded Go facet is owned by index "misplaced", not the index "by_id" the directive describes on //ptah:schema:index at Record`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(runtime.Annotations(), "records.go", row.source)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(db.Tables, qt.HasLen, 0)
		})
	}
}
