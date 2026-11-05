package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
	"ptah.run/core/schemaext"
)

type removalFacet struct{ Setting string }

func (*removalFacet) Kind() schemaext.Kind     { return "example.org/removal/table-setting" }
func (v *removalFacet) Clone() schemaext.Value { return &removalFacet{Setting: v.Setting} }
func (v *removalFacet) Equal(other schemaext.Value) bool {
	value, ok := other.(*removalFacet)
	return ok && *v == *value
}

func TestTableRemovalReportContainsNamesOnly(t *testing.T) {
	for _, test := range []struct {
		name     string
		removals difftypes.TableRemovals
		want     string
	}{
		{name: "absent", want: "null"},
		{name: "empty", removals: difftypes.TableRemovals{}, want: "[]"},
		{name: "ordered names", removals: difftypes.TableRemovals{{Name: "orders"}, {Name: "items"}}, want: `["orders","items"]`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			encoded, err := json.Marshal(test.removals)
			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want)
		})
	}
}
