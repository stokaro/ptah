package pgdefaultprivilegesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgdefaultprivilege"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/pgdefaultprivilegesource"
)

func TestHCLExport_RefusesInvalidPayloadWithoutPartialOutput(t *testing.T) {
	tests := []struct {
		name  string
		value schemaext.Value
	}{
		{name: "observed state", value: &pgdefaultprivilege.Observed{}},
		{name: "contradictory privileges", value: &pgdefaultprivilege.Desired{Privileges: []pgdefaultprivilege.Grant{{Privilege: "ALL"}}, Revoked: []string{"SELECT"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			first := must.Must(pgdefaultprivilegesource.Declare(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}, []string{"SELECT"}, nil, nil, "", nil))
			invalid := first
			invalid.Value = test.value
			result, err := builtintest.Runtime().HCL().Encode(t.Context(), hclext.EncodeRequest{Objects: []schemaext.Object{first, invalid}})
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(result, qt.DeepEquals, hclext.EncodeResult{})
		})
	}
}

func TestHCLExport_RefusesEmptyIntentWithoutPartialOutput(t *testing.T) {
	c := qt.New(t)
	first := must.Must(pgdefaultprivilegesource.Declare(pgdefaultprivilege.Identity{Grantor: "owner", ObjectType: "TABLES", Grantee: "reader"}, []string{"SELECT"}, nil, nil, "", nil))
	empty := first
	empty.Value = &pgdefaultprivilege.Desired{}
	result, err := builtintest.Runtime().HCL().Encode(t.Context(), hclext.EncodeRequest{Objects: []schemaext.Object{first, empty}})
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(result, qt.DeepEquals, hclext.EncodeResult{})
}
