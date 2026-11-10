package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff/internal/compare"
)

func TestViewDefinitions_SQLSecurityIsCompared(t *testing.T) {
	tests := []struct {
		name, dialect, declared, held, want string
	}{
		{"MySQL invoker to default definer", platform.MySQL, "", "INVOKER", "INVOKER -> DEFINER"},
		{"MariaDB default definer to invoker", platform.MariaDB, "INVOKER", "DEFINER", "DEFINER -> INVOKER"},
		{"explicit definer matches default", platform.MySQL, "DEFINER", "DEFINER", ""},
		{"explicit invoker matches", platform.MySQL, "INVOKER", "INVOKER", ""},
		{"unsupported dialect reports requested mode", platform.Postgres, "INVOKER", "", "(unsupported) -> INVOKER"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := compare.ViewDefinitionsWithDialect(schemamodel.View{Name: "v", Body: "SELECT 1", Security: test.declared},
				catalog.View{Name: "v", Body: "SELECT 1", Security: test.held}, test.dialect)
			c.Assert(diff.Changes["security"], qt.Equals, test.want)
		})
	}
}
