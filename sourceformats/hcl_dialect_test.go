package sourceformats_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/coverage"
	"ptah.run/core/hclext"
	"ptah.run/core/sqlext"
	"ptah.run/core/yamlext"
	"ptah.run/sourceformats"
)

type dialectReader struct{ claims *[]hclext.Claim }

func (r dialectReader) DecodeHCL(_ context.Context, request hclext.Request) (hclext.Result, error) {
	*r.claims = append(*r.claims, request.Claims...)
	return hclext.Result{Declarations: make([]hclext.Declaration, len(request.Claims))}, nil
}

func TestReadHCL_PassesTheCallerDialect(t *testing.T) {
	c := qt.New(t)
	var claims []hclext.Claim
	owners := must.Must(hclext.NewSet(hclext.Extension{Owner: "example.org/custom", Decoder: dialectReader{claims: &claims},
		Blocks: []hclext.Block{{Site: hclext.TopLevel, Type: "shape", Naming: hclext.SchemaAndName, Dialects: []string{"custom"}}},
	}))
	formats := sourceformats.New(yamlext.None(), sqlext.None(), owners, coverage.Vocabulary{}, nil)
	db, err := formats.ReadHCL([]byte(`shape "s" {}`), "custom.hcl", "custom")
	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.IsNotNil)
	c.Assert(claims, qt.HasLen, 1)
	c.Assert(claims[0].Dialect, qt.Equals, "custom")
}
