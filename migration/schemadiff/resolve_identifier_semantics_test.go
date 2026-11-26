package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/offlinenames"
	"ptah.run/migration/schemadiff"
)

// Two documents compared offline name objects whose identity, on SQL Server,
// only the collation they are created under decides. A dev server asked about
// the names a comparison meets answers for every one of them, and the
// comparison pairs them the way the answer does.

// TestResolveIdentifierSemantics_PairsTwoDocumentsAsTheServerDoes_HappyPath
// compares a declaration with a state read from a document, each naming the
// same objects in another case, through the rules a case-insensitive server
// answered. Each pair is the held object, changed.
func TestResolveIdentifierSemantics_PairsTwoDocumentsAsTheServerDoes_HappyPath(t *testing.T) {
	for _, test := range caseOnlyNamesakes() {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := must.Must(builtin.New())
			semantics, err := schemadiff.ResolveIdentifierSemantics(c.Context(), caseInsensitiveDatabase, test.desired, test.held, runtime)
			c.Assert(err, qt.IsNil)
			opts := config.DefaultCompareOptions()
			opts.Dialect = platform.SQLServer
			opts.IdentifierSemantics = &semantics

			diff, err := schemadiff.CompareWithOptions(c.Context(), test.desired, test.held, opts, runtime)

			c.Assert(err, qt.IsNil)
			c.Assert(namedObjectChanges(diff), qt.DeepEquals, test.insensitive)
		})
	}
}

func TestResolveIdentifierSemantics_FailurePath(t *testing.T) {
	desired := &schemamodel.Database{}
	held := &catalog.Database{}
	tests := []struct {
		name     string
		resolver schemadiff.IdentifierResolver
		desired  *schemamodel.Database
		held     *catalog.Database
	}{
		{name: "no server", desired: desired, held: held},
		{name: "no declaration", resolver: caseInsensitiveDatabase, held: held},
		{name: "no held state", resolver: caseInsensitiveDatabase, desired: desired},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			semantics, err := schemadiff.ResolveIdentifierSemantics(c.Context(), test.resolver, test.desired, test.held, must.Must(builtin.New()))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(semantics.IsZero(), qt.IsTrue)
		})
	}
}

// nonASCIIViews declares a view whose name carries a non-ASCII rune beside
// one whose name does not, in the order given.
func nonASCIIViews(names ...string) *schemamodel.Database {
	desired := &schemamodel.Database{}
	for _, name := range names {
		desired.Views = append(desired.Views, schemamodel.View{Name: name, Body: "SELECT 1 AS a"})
	}
	return desired
}

// TestValidateDesiredSchema_SQLServerOfflineNonASCIIDeclarations_FailurePath
// validates two views under the SQL Server offline rules, one of whose names
// carries a non-ASCII rune. Nothing says what that name is, so it may be the
// other one, in either order, and the refusal says Ptah cannot tell.
func TestValidateDesiredSchema_SQLServerOfflineNonASCIIDeclarations_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		desired *schemamodel.Database
		want    string
	}{
		{
			name:    "the non-ASCII name first",
			desired: nonASCIIViews("Größe", "v2"),
			want:    "without a connection Ptah cannot tell whether target views dbo\\.Größe and dbo\\.v2 are one name under the database collation",
		},
		{
			name:    "the non-ASCII name second",
			desired: nonASCIIViews("v2", "Größe"),
			want:    "without a connection Ptah cannot tell whether target views dbo\\.v2 and dbo\\.Größe are one name under the database collation",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemadiff.ValidateDesiredSchema(c.Context(), must.Must(builtin.New()), test.desired,
				catalog.ServerInfo{Dialect: platform.SQLServer})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*`+test.want+`.*`)
			c.Assert(offlinenames.Marked(err), qt.IsTrue)
		})
	}
}

// TestCompareWithDatabase_SQLServerNonASCIIDeclarations_HappyPath is the
// control: a server that was asked keeps the two names apart, and both views
// are created.
func TestCompareWithDatabase_SQLServerNonASCIIDeclarations_HappyPath(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabase(c.Context(), caseInsensitiveDatabase, nonASCIIViews("Größe", "v2"),
		&catalog.Database{}, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(namedObjectChanges(diff), qt.DeepEquals, []string{"view added Größe", "view added v2"})
}
