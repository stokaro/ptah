package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

// TestViewDefinitions_SQLSecurityIsCompared pins the security change a view
// comparison reports, and the mode the view it leaves behind states. A
// declaration that names no mode keeps the one the view holds, so it reports
// no change, and a replacement for another change states the held mode.
func TestViewDefinitions_SQLSecurityIsCompared(t *testing.T) {
	tests := []struct {
		name, dialect, declared, held string
		want                          string
		wantDesired                   string
	}{
		{"MySQL invoker kept by a declaration that names none", platform.MySQL, "", "INVOKER", "", "INVOKER"},
		{"MariaDB invoker kept by a declaration that names none", platform.MariaDB, "", "INVOKER", "", "INVOKER"},
		{"definer kept by a declaration that names none", platform.MySQL, "", "DEFINER", "", "DEFINER"},
		{"MySQL invoker to declared definer", platform.MySQL, "DEFINER", "INVOKER", "INVOKER -> DEFINER", "DEFINER"},
		{"MariaDB default definer to invoker", platform.MariaDB, "INVOKER", "DEFINER", "DEFINER -> INVOKER", "INVOKER"},
		{"explicit definer matches", platform.MySQL, "DEFINER", "DEFINER", "", "DEFINER"},
		{"explicit invoker matches", platform.MySQL, "INVOKER", "INVOKER", "", "INVOKER"},
		{"unsupported dialect reports requested mode", platform.Postgres, "INVOKER", "", "(unsupported) -> INVOKER", "INVOKER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.ViewDefinitionsWithDialect(schemamodel.View{Name: "v", Body: "SELECT 1", Security: test.declared},
				catalog.View{Name: "v", Body: "SELECT 1", Security: test.held}, test.dialect)
			c.Assert(diff.Changes["security"], qt.Equals, test.want)
			c.Assert(diff.Desired.Security, qt.Equals, test.wantDesired)
		})
	}
}

// TestViewDefinitions_BodyChangeKeepsSQLSecurity pins the view a body change
// leaves behind when the declaration names no mode: it states the mode the
// view holds, which the replacement then writes.
func TestViewDefinitions_BodyChangeKeepsSQLSecurity(t *testing.T) {
	c := qt.New(t)

	diff := compare.ViewDefinitionsWithDialect(schemamodel.View{Name: "v", Body: "SELECT 2"},
		catalog.View{Name: "v", Body: "SELECT 1", Security: "INVOKER"}, platform.MySQL)

	c.Assert(diff.Changes, qt.DeepEquals, map[string]string{"body": "SELECT 1 -> SELECT 2"})
	c.Assert(diff.Desired.Security, qt.Equals, "INVOKER")
}
