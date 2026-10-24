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
	"ptah.run/core/yamlschema"
	"ptah.run/engine"
	"ptah.run/internal/providerhost"
)

func platformProvider(c *qt.C) *engine.Runtime {
	c.Helper()
	_, runtime := yamlProviderCommand(c, "platform-claims")
	return runtime
}

// platformGoSource writes the retention of a table, a column and an index as
// platform properties the provider claimed, beside a platform property of
// another engine that nobody claimed.
const platformGoSource = `package p

//ptah:schema:table name="records" platform.fixture.seconds=30 platform.mysql.engine="InnoDB"
type Record struct {
	//ptah:schema:field name="id" type="INTEGER" platform.fixture.minutes=2
	ID int
	//ptah:schema:field name="body" type="TEXT" platform.mysql.type="TEXT"
	Body string

	//ptah:schema:index name="by_body" fields="body" platform.fixture.seconds=9
	_ int
}
`

func TestProviderClaimedPlatformAttributes_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", platformGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, db), qt.DeepEquals, []string{
		`table records {"seconds":30}`,
		`column id {"seconds":120}`,
		`index by_body {"seconds":9}`,
	})
}

// TestProviderClaimedPlatformAttributes_KeepsUnclaimed pins that only the
// claimed properties leave the common model: the provider holds them, and a
// property nobody claimed is still the common model's.
func TestProviderClaimedPlatformAttributes_KeepsUnclaimed(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", platformGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"engine": "InnoDB"}})
	c.Assert(db.Fields[0].Overrides, qt.HasLen, 0)
	c.Assert(db.Fields[1].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"type": "TEXT"}})
	c.Assert(db.Indexes[0].Overrides, qt.HasLen, 0)
}

// TestProviderPlatformAttributesWithoutTheProvider_KeepsThem is the control: a
// run that did not select the provider keeps every platform property in the
// common model, so the removal above is the claim's doing.
func TestProviderPlatformAttributesWithoutTheProvider_KeepsThem(t *testing.T) {
	c := qt.New(t)

	runtime, err := engine.New()
	c.Assert(err, qt.IsNil)

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", platformGoSource)

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[0].Overrides["fixture"], qt.DeepEquals, map[string]string{"seconds": "30"})
}

const placedGoHead = "package p\n\n//ptah:schema:table name=\"records\"\n"

func TestProviderTableDirectiveDeclaresAColumnValue_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)
	source := placedGoHead + "//ptah:fixture:placed seconds=5 column=\"body\"\ntype Record struct {\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\"\n\tID int\n\t//ptah:schema:field name=\"body\" type=\"TEXT\"\n\tBody string\n}\n"

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", source)

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, db), qt.DeepEquals, []string{`column body {"seconds":5}`},
		qt.Commentf("the value belongs to the column, and the table keeps none"))
}

func TestProviderTableDirectiveDeclaresAColumnValue_Coverage(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)
	source := placedGoHead + "//ptah:fixture:placed seconds=5 column=\"body\"\ntype Record struct {\n\t//ptah:schema:field name=\"body\" type=\"TEXT\"\n\tBody string\n}\n"

	db, err := goschema.ParseSource(runtime.Annotations(), "records.go", source)

	c.Assert(err, qt.IsNil)
	var subjects []string
	for _, record := range db.FeatureCoverage.SubjectRecords() {
		subjects = append(subjects, string(record.Subject.Kind)+" "+record.Subject.Parent.Source+" "+record.Subject.Name.Source+" "+string(record.Knowledge.State))
	}
	c.Assert(subjects, qt.DeepEquals, []string{"column records body complete"})
}

func TestProviderTableDirectiveDeclaresAColumnValue_FailurePath(t *testing.T) {
	runtime := platformProvider(qt.New(t))
	const body = "type Record struct {\n\t//ptah:schema:field name=\"body\" type=\"TEXT\"\n\tBody string\n}\n"
	for _, row := range []struct{ name, source, want string }{
		{"a column the struct does not declare", placedGoHead + "//ptah:fixture:placed seconds=5 column=\"ghost\"\n" + body,
			`column "ghost" of struct Record is not declared in this file.* on //ptah:fixture:placed at Record`},
		{"a column of another table", placedGoHead + "//ptah:fixture:placed seconds=5 column=\"body\" of=\"other\"\n" + body,
			`decoded Go facet is owned by column "body" of \["other"\], which is not a column of the table "records" the directive declares on //ptah:fixture:placed at Record`},
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

const platformYAMLSource = `tables:
  records:
    columns:
      id: {type: INTEGER}
    platform:
      mysql: {engine: InnoDB}
      fixture: {seconds: 9007199254740993}
  plain:
    columns:
      id: {type: INTEGER}
`

func TestProviderClaimedPlatformGroup_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte(platformYAMLSource))

	c.Assert(err, qt.IsNil)
	c.Assert(scopedRetentions(c, runtime, *db), qt.DeepEquals, []string{`table records {"seconds":9007199254740993}`})
	c.Assert(db.Tables[0].Name, qt.Equals, "plain")
	c.Assert(db.Tables[1].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"engine": "InnoDB"}},
		qt.Commentf("the provider's group leaves the common overrides, and another engine's stays"))
}

func TestProviderClaimedPlatformGroup_Coverage(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte(platformYAMLSource))

	c.Assert(err, qt.IsNil)
	var subjects []string
	for _, record := range db.FeatureCoverage.SubjectRecords() {
		subjects = append(subjects, string(record.Subject.Kind)+" "+record.Subject.Name.Source+" "+string(record.Knowledge.State))
	}
	c.Assert(subjects, qt.DeepEquals, []string{"table records complete"})
}

func TestProviderClaimedPlatformGroup_FailurePath(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(),
		[]byte("tables:\n  records:\n    columns:\n      id: {type: INTEGER}\n    platform:\n      fixture: {seconds: soon}\n"))

	c.Assert(err, qt.ErrorMatches, `YAML document of example.org/fixture: provider decode source failed.*`)
	c.Assert(db, qt.IsNil)
}

// TestProviderPlatformGroupWithoutTheClaim_KeepsIt is the control: a provider
// that claims no group leaves the group in the overrides.
func TestProviderPlatformGroupWithoutTheClaim_KeepsIt(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProvider(c)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte(platformYAMLSource))

	c.Assert(err, qt.IsNil)
	c.Assert(db.Tables[1].Overrides["fixture"], qt.DeepEquals, map[string]string{"seconds": "9007199254740993"})
}

func TestProviderClaimOfAnotherEnginesPlatformGroup_FailurePath(t *testing.T) {
	c := qt.New(t)
	executable := independentCLIProviderCommand(c, "platform-foreign-group")
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)

	host, err := providerhost.Start(c.Context(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"})

	c.Assert(err, qt.ErrorMatches, `(?s).*claims the platform group "mysql", which names no engine of the provider.*`)
	c.Assert(host, qt.IsNil)
}

// TestProviderDecodedFacetCarriesTheEngineProfile pins that a value decoded
// from a source carries the target profile of the engine, as a native operation
// does. A comparison and a plan accept one profile per batch, and the profile of
// the first value sets it, so a decoded value without one made the plan refuse
// every native operation it held.
func TestProviderDecodedFacetCarriesTheEngineProfile(t *testing.T) {
	c := qt.New(t)
	runtime := platformProvider(c)

	db, err := yamlschema.Parse(c.Context(), runtime.YAML(), []byte(platformYAMLSource))

	c.Assert(err, qt.IsNil)
	encoded, err := runtime.Codecs().EncodeFacets(c.Context(), schemaext.Desired, db.Tables[1].Facets)
	c.Assert(err, qt.IsNil)
	c.Assert(encoded, qt.HasLen, 1)
	var wire struct{ Features []string }
	c.Assert(json.Unmarshal(encoded[0].Value.Payload, &wire), qt.IsNil)
	c.Assert(wire.Features, qt.DeepEquals, []string{"columns", "schema.common-table-identity.v1", "tables"})
}
