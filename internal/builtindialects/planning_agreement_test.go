package builtindialects_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/builtindialects"
)

// TestDescriptors_PlanningGrammarAgreesWithTheFamilies pins the planning
// grammar facts to the family checks they replace in migration/planner,
// migration/generator and internal/concurrentindex: the PostgreSQL family
// builds indexes concurrently and adds a constraint NOT VALID, and the MySQL
// family asks for ALGORITHM= and LOCK=.
func TestDescriptors_PlanningGrammarAgreesWithTheFamilies(t *testing.T) {
	for _, dialect := range builtindialects.Descriptors() {
		t.Run(dialect.Name, func(t *testing.T) {
			c := qt.New(t)
			postgresFamily := platform.IsPostgresFamily(dialect.Name)
			mysqlFamily := dialect.Name == platform.MySQL || dialect.Name == platform.MariaDB
			c.Assert(dialect.ConcurrentIndexes, qt.Equals, postgresFamily)
			c.Assert(dialect.OnlineAlter == descriptor.OnlineAlterNotValid, qt.Equals, postgresFamily)
			c.Assert(dialect.OnlineAlter == descriptor.OnlineAlterAlgorithmLock, qt.Equals, mysqlFamily)
			c.Assert(dialect.OnlineAlter == "", qt.Equals, !postgresFamily && !mysqlFamily)
		})
	}
}
