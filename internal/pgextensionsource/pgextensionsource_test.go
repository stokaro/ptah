package pgextensionsource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlext"
	"ptah.run/core/yamlschema"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/pgextensionsource"
	"ptah.run/internal/pgpolicysource"
)

var (
	goOwners   = must.Must(annotation.NewSet(pgextensionsource.Annotations(), pgpolicysource.Annotations()))
	yamlOwners = must.Must(yamlext.NewSet(pgextensionsource.YAML(), pgpolicysource.YAML()))
)

// goSource is a Go package file that declares the given annotations on one
// struct.
func goSource(annotations string) string {
	return "package models\n\n" + annotations + "\ntype Extensions struct{}\n"
}

func parseGo(c *qt.C, name, annotations string) *schemamodel.Database {
	c.Helper()
	db := must.Must(goschema.ParseSource(goOwners, name, goSource(annotations)))
	return &db
}

func extension(c *qt.C, db *schemamodel.Database, name string) *pgextension.DesiredExtension {
	c.Helper()
	object, found, err := db.FeatureObjects.Get(pgextension.Ref(name))
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return object.Value.(*pgextension.DesiredExtension)
}

// TestReaders_OneDeclarationDecodesAlikeFromGoAndYAML pins that both
// frontends hand the owner one declaration the same way, every attribute
// included, and claim that the source describes every extension.
func TestReaders_OneDeclarationDecodesAlikeFromGoAndYAML(t *testing.T) {
	c := qt.New(t)

	fromGo := parseGo(c, "extensions.go",
		`//ptah:schema:extension name="pg_trgm" schema="ext" version="1.6" comment="trigrams" if_not_exists="true"`)
	fromYAML := must.Must(yamlschema.Parse(t.Context(), yamlOwners, []byte("extensions:\n  pg_trgm:\n    schema: ext\n    version: 1.6\n"+
		"    comment: trigrams\n    if_not_exists: true\n")))

	want := &pgextension.DesiredExtension{Schema: "ext", Version: "1.6", Comment: "trigrams", IfNotExists: true}
	c.Assert(extension(c, fromGo, "pg_trgm"), qt.DeepEquals, want)
	c.Assert(extension(c, fromYAML, "pg_trgm"), qt.DeepEquals, want)
	c.Assert(fromGo.FeatureCoverage.Lookup(pgextension.ExtensionKind, pgextension.Ref("citext")).State, qt.Equals, schemaext.Complete)
	c.Assert(fromYAML.FeatureCoverage.Lookup(pgextension.ExtensionKind, pgextension.Ref("citext")).State, qt.Equals, schemaext.Complete)
}

// TestReaders_ScopeDecidesTheOwner_HappyPath pins which declarations the
// owner reads: one with no scope and one scoped to the PostgreSQL family are
// its objects, with the scope they were written with.
func TestReaders_ScopeDecidesTheOwner_HappyPath(t *testing.T) {
	c := qt.New(t)

	db := parseGo(c, "extensions.go", `//ptah:schema:extension name="pg_trgm"
//ptah:schema:extension name="citext" dialects="postgres,cockroachdb"`)

	c.Assert(pgextension.Names(db.FeatureObjects), qt.DeepEquals, []string{"citext", "pg_trgm"})
	scoped, _, err := db.FeatureObjects.Get(pgextension.Ref("citext"))
	c.Assert(err, qt.IsNil)
	c.Assert(scoped.Targets, qt.DeepEquals, []string{"cockroachdb", "postgres"})
}

// TestReaders_ScopeDecidesTheOwner_FailurePath pins that a scope naming a
// target that cannot host an extension is refused, alone or beside one that
// can: the author named a target where no extension can exist. A declaration
// without a scope is not refused, because it names no target; a target that
// cannot host it leaves it out.
func TestReaders_ScopeDecidesTheOwner_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		scope string
		want  string
	}{
		{name: "only other targets", scope: "mysql,sqlite", want: `the scope names mysql,sqlite, which cannot host an extension`},
		{name: "a family target beside another", scope: "postgres,mysql", want: `the scope names mysql, which cannot host an extension`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := goschema.ParseSource(goOwners, "extensions.go",
				goSource(`//ptah:schema:extension name="vector" dialects="`+test.scope+`"`))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.want+`; scope the declaration to cockroachdb,postgres,spanner,yugabytedb, `+
				`or leave dialects out to declare it for every target`)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// source is one desired-schema source a test loads: a Go package file
// holding annotations, or a YAML document.
type source struct {
	format string
	text   string
}

// parsers reads a source by its format.
var parsers = map[string]func(context.Context, string) (*schemamodel.Database, error){
	"go": func(_ context.Context, text string) (*schemamodel.Database, error) {
		db, err := goschema.ParseSource(goOwners, "extensions.go", goSource(text))
		return &db, err
	},
	"yaml": func(ctx context.Context, text string) (*schemamodel.Database, error) {
		return yamlschema.Parse(ctx, yamlOwners, []byte(text))
	},
}

// load reads each source and merges them in order, as sources of one load
// are merged.
func load(c *qt.C, sources []source) (*schemamodel.Database, error) {
	c.Helper()
	merged, err := parsers[sources[0].format](c.Context(), sources[0].text)
	if err != nil {
		return nil, err
	}
	for _, next := range sources[1:] {
		db, err := parsers[next.format](c.Context(), next.text)
		if err != nil {
			return nil, err
		}
		if merged, err = schemamodel.Merge(merged, db); err != nil {
			return nil, err
		}
	}
	return merged, nil
}

// TestRedeclaration_AnEqualExtensionMerges pins that an extension is
// idempotent to declare: the same declaration in two files of one package,
// in two Go roots, and in a Go root and a YAML document is one extension.
func TestRedeclaration_AnEqualExtensionMerges(t *testing.T) {
	declaration := `//ptah:schema:extension name="pg_trgm" version="1.6"`
	tests := []struct {
		name    string
		sources []source
	}{
		{name: "one Go package", sources: []source{{format: "go", text: declaration + "\n" + declaration}}},
		{name: "two Go roots", sources: []source{{format: "go", text: declaration}, {format: "go", text: declaration}}},
		{name: "a Go root and a YAML document", sources: []source{{format: "go", text: declaration},
			{format: "yaml", text: "extensions:\n  pg_trgm:\n    version: \"1.6\"\n"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := load(c, test.sources)

			c.Assert(err, qt.IsNil)
			c.Assert(db.FeatureObjects.Refs(), qt.DeepEquals, []objectidentity.ID{pgextension.Ref("pg_trgm")})
			c.Assert(extension(c, db, "pg_trgm"), qt.DeepEquals, &pgextension.DesiredExtension{Version: "1.6"})
		})
	}
}

// TestRedeclaration_ADifferentExtensionIsRefused pins the other half: two
// declarations of one extension that differ are refused, naming it.
func TestRedeclaration_ADifferentExtensionIsRefused(t *testing.T) {
	first, second := `//ptah:schema:extension name="pg_trgm" version="1.5"`, `//ptah:schema:extension name="pg_trgm" version="1.6"`
	tests := []struct {
		name    string
		sources []source
		is      error
		want    string
	}{
		{name: "one Go package", sources: []source{{format: "go", text: first + "\n" + second}}, is: ptaherr.ErrInvalidAttributeValue,
			want: `(?s).*conflicting extension "pg_trgm" definitions on //ptah:schema:extension at pg_trgm.*`},
		{name: "two Go roots", sources: []source{{format: "go", text: first}, {format: "go", text: second}}, is: schemaext.ErrDuplicate,
			want: `.*conflicting definitions of ptah.run/pgextension/extension pg_trgm`},
		{name: "a Go root and a YAML document", is: schemaext.ErrDuplicate,
			sources: []source{{format: "go", text: first}, {format: "yaml", text: "extensions:\n  pg_trgm:\n    version: \"1.6\"\n"}},
			want:    `.*conflicting definitions of ptah.run/pgextension/extension pg_trgm`},
		{name: "another scope", is: schemaext.ErrDuplicate,
			sources: []source{{format: "go", text: first}, {format: "go", text: first + ` dialects="postgres"`}},
			want:    `.*conflicting definitions of ptah.run/pgextension/extension pg_trgm`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			db, err := load(c, test.sources)

			c.Assert(err, qt.ErrorIs, test.is)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestRedeclaration_PoliciesStayStrict is the control: a row-level security
// policy states nothing about being declared twice, so the same policy
// declared twice is refused as stokaro/ptah#2440 refuses it, in one package
// and across two Go roots.
func TestRedeclaration_PoliciesStayStrict(t *testing.T) {
	policy := `//ptah:schema:table name="docs"
//ptah:schema:rls:policy name="readers" table="docs" using="true"`
	field := "\n\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n"
	source := func(annotations string) string {
		return "package models\n\n" + annotations + "\ntype Doc struct {" + field + "}\n"
	}

	t.Run("one Go package", func(t *testing.T) {
		c := qt.New(t)

		_, err := goschema.ParseSource(goOwners, "docs.go", source(policy+"\n//ptah:schema:rls:policy name=\"readers\" table=\"docs\" using=\"true\""))

		c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidAttributeValue)
		c.Assert(err, qt.ErrorMatches, `(?s).*//ptah:schema:rls:policy at docs.go:4 and //ptah:schema:rls:policy at docs.go:5 both declare `+
			`policy "readers" on table public.docs; keep one declaration.*`)
	})
	t.Run("two Go roots", func(t *testing.T) {
		c := qt.New(t)
		first := must.Must(goschema.ParseSource(goOwners, "a.go", source(policy)))
		second := must.Must(goschema.ParseSource(goOwners, "b.go", source(policy)))

		_, err := schemamodel.Merge(&first, &second)

		c.Assert(err, qt.ErrorIs, schemaext.ErrDuplicate)
	})
}
