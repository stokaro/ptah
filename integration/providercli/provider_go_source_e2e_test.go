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
			`decode Go directives of engine "fixture": provider decode source failed .*`, ptaherr.ErrInvalidAttributeValue},
		{"struct without a table", "package p\n\n//ptah:fixture:retention seconds=60\ntype Record struct{}\n",
			`struct Record maps to no table in this file.* on //ptah:fixture:retention at Record`, ptaherr.ErrInvalidAttributeValue},
		{"declared twice", "package p\n\n//ptah:schema:table name=\"records\"\n//ptah:fixture:retention seconds=60\n//ptah:fixture:retention seconds=61\ntype Record struct{}\n",
			`decode Go directives of engine "fixture": provider decode source failed .*`, ptaherr.ErrInvalidAttributeValue},
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

func TestProviderGoDirectiveAtUnhostedScope_FailurePath(t *testing.T) {
	c := qt.New(t)
	executable := independentCLIProviderCommand(c, "column-go-directive")
	data, err := os.ReadFile(executable)
	c.Assert(err, qt.IsNil)
	digest := sha256.Sum256(data)
	host, err := providerhost.Start(c.Context(), providerhost.Spec{Executable: executable, SHA256: hex.EncodeToString(digest[:]), ProviderID: "example.org/fixture"})
	c.Assert(err, qt.ErrorMatches, `.*Go directive "ptah:fixture:retention" of engine "fixture" has scope COLUMN, which the host does not read.*`)
	c.Assert(host, qt.IsNil)
}
