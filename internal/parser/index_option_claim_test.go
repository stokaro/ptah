package parser_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/internal/parser"
)

// claimRecorder answers every claim with a value that adds nothing, and keeps
// the claims it was handed.
type claimRecorder struct {
	claims *[]sqlext.Claim
}

func (r claimRecorder) ParseSQL(_ context.Context, request sqlext.Request) (sqlext.Result, error) {
	*r.claims = append(*r.claims, request.Claims...)
	return sqlext.Result{Values: make([]sqlext.Value, len(request.Claims))}, nil
}

// TestParse_AnIndexOptionClaimCarriesItsKey pins what an index option's claim
// tells its owner: the key's name, the key's number among the document's
// elements, the key's FULLTEXT or SPATIAL prefix, and the option's text and
// document offset, inside a version guard too.
func TestParse_AnIndexOptionClaimCarriesItsKey(t *testing.T) {
	c := qt.New(t)
	var claims []sqlext.Claim
	owners := must.Must(sqlext.NewSet(sqlext.Extension{
		Owner:    "example.org/tags",
		Targets:  []string{platform.MySQL},
		Facets:   []schemaext.Kind{"example.org/tags/tag"},
		Patterns: []sqlext.Pattern{{Name: "tag", Site: sqlext.IndexOption, Words: []string{"TAGGED"}}},
		Parser:   claimRecorder{claims: &claims},
	}))
	sql := "CREATE TABLE t (a int, b text, KEY k (a) TAGGED x, FULLTEXT KEY ft (b) /*!50100 TAGGED y */); " +
		"CREATE INDEX c ON t (a) TAGGED z;"

	_, err := parser.NewParser(sql, parser.WithDialect(platform.MySQL), parser.WithSQL(owners)).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(claims, qt.DeepEquals, []sqlext.Claim{
		{Pattern: "tag", Site: sqlext.IndexOption, Subject: "k", Text: "TAGGED x", Definition: 3, Offset: 41},
		{Pattern: "tag", Site: sqlext.IndexOption, Subject: "ft", Text: "TAGGED y ", Definition: 4, IndexType: "FULLTEXT", Offset: 80},
		{Pattern: "tag", Site: sqlext.IndexOption, Subject: "c", Text: "TAGGED z", Definition: 5, Offset: 118},
	})
}

// Each CREATE INDEX is an element of its own, so the options of two indexes
// are never combined.
func TestParse_EachCreateIndexIsItsOwnElement(t *testing.T) {
	c := qt.New(t)
	var claims []sqlext.Claim
	owners := must.Must(sqlext.NewSet(sqlext.Extension{
		Owner:    "example.org/tags",
		Targets:  []string{platform.MySQL},
		Facets:   []schemaext.Kind{"example.org/tags/tag"},
		Patterns: []sqlext.Pattern{{Name: "tag", Site: sqlext.IndexOption, Words: []string{"TAGGED"}}},
		Parser:   claimRecorder{claims: &claims},
	}))

	_, err := parser.NewParser("CREATE INDEX k ON t (a) TAGGED x; CREATE INDEX l ON t (b) TAGGED y;",
		parser.WithDialect(platform.MySQL), parser.WithSQL(owners)).Parse()

	c.Assert(err, qt.IsNil)
	c.Assert(claims, qt.HasLen, 2)
	c.Assert([]int{claims[0].Definition, claims[1].Definition}, qt.DeepEquals, []int{1, 2})
}
