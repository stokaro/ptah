package difftypes_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/difftypes"
)

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
