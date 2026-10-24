package dbschema_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// On a connection Ptah reads itself the keyed read is ReadTableRows.
func TestReadKeyedTableRows_HappyPath(t *testing.T) {
	c := qt.New(t)
	conn := newRegionsConn(t)

	rows, err := dbschema.ReadKeyedTableRows(context.Background(), conn, "", "regions", []string{"code", "name"}, []string{"code"})
	c.Assert(err, qt.IsNil)
	sortByCode(rows)
	c.Assert(rows, qt.DeepEquals, []map[string]any{
		{"code": "CZ", "name": "Czechia"},
		{"code": "US", "name": "United States"},
	})
}

func TestReadKeyedTableRows_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		keys    []string
		wantErr string
	}{
		{"no key", nil, `dbschema: ReadKeyedTableRows requires at least one key column`},
		{"a key that is not read", []string{"population"}, `dbschema: key column "population" is not among the columns read`},
		{"a key spelled differently", []string{"CODE"}, `dbschema: key column "CODE" is not among the columns read`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			conn := newRegionsConn(t)
			rows, err := dbschema.ReadKeyedTableRows(context.Background(), conn, "", "regions", []string{"code", "name"}, row.keys)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(rows, qt.IsNil)
		})
	}
}
