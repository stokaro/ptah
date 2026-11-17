//go:build integration

package atlasschema_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// clickHouseGrantFrom is a database of one table in schema shop.
const clickHouseGrantFrom = `
schema "shop" {
}
table "orders" {
  schema = schema.shop
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`

// TestDiffPlansAClickHouseGrantOnTheDevServer is the live half of the control
// in internal/atlasschema: a diff of two documents opens the dev database it is
// given, so the grant the ClickHouse seam accepts is planned beside a server
// that answers.
func TestDiffPlansAClickHouseGrantOnTheDevServer(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	from := filepath.Join(dir, "from.hcl")
	to := filepath.Join(dir, "to.hcl")
	c.Assert(os.WriteFile(from, []byte(clickHouseGrantFrom), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(to, []byte(clickHouseGrantFrom+`
role "analyst" {
}
permission {
  to         = role.analyst
  privileges = ["SELECT"]
  for        = schema.shop
}
`), 0o600), qt.IsNil)

	report, err := atlasschema.Diff(c.Context(), atlasschema.DiffOptions{
		FromURLs: []string{"file://" + filepath.ToSlash(from)},
		ToURLs:   []string{"file://" + filepath.ToSlash(to)},
		DevURL:   dbtarget.URL(c, dbtarget.ClickHouse),
		Runtime:  must.Must(builtin.New()),
	})
	c.Assert(err, qt.IsNil)
	sql, err := report.MarshalSQL()

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "GRANT SELECT ON `shop`.* TO `analyst`")
}
