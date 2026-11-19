package oracle_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/difftypes"
	"ptah.run/engine/builtin"
	mysqlplanner "ptah.run/internal/planner/dialects/mysql"
	oracleplanner "ptah.run/internal/planner/dialects/oracle"
)

// checkChange is a diff that replaces the CHECK the database holds as removed
// with the declared one, paired by identity as the comparison pairs a CHECK
// whose condition changed.
func checkChange(removed, added string) *difftypes.SchemaDiff {
	identity := difftypes.ConstraintIdentity{Table: "T1", Name: "CK11"}
	return &difftypes.SchemaDiff{
		ConstraintsRemoved: difftypes.ConstraintRemovals{{Name: removed, TableName: "T1", Type: "CHECK", Identity: identity}},
		ConstraintsAdded: difftypes.ConstraintAdditions{{
			Name: added, TableName: "T1", Type: "CHECK", CheckExpression: "a > 1", Identity: identity,
		}},
	}
}

// statementsOf answers the statements of a rendered plan, one per line, without
// the blank lines between them.
func statementsOf(sql string) []string {
	var statements []string
	for line := range strings.SplitSeq(sql, "\n") {
		if strings.TrimSpace(line) != "" {
			statements = append(statements, strings.TrimSpace(line))
		}
	}
	return statements
}

// Oracle commits each schema statement apart, so a plan that drops a changed
// CHECK before it adds the new one leaves the table with neither when the
// rows refuse the ADD with ORA-02293. The new CHECK goes in first under a
// temporary name, the old one goes, and the new one takes the declared name.
// Dropped first, the plan reads DROP CONSTRAINT CK11 and then ADD CONSTRAINT
// ck11.
func TestPlanner_OracleReplacesANamedCheckAddingItFirst(t *testing.T) {
	c := qt.New(t)

	nodes, err := oracleplanner.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), checkChange("CK11", "ck11"))
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("oracle", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(statementsOf(sql), qt.DeepEquals, []string{
		"ALTER TABLE T1 ADD CONSTRAINT ptah_e2c5b8dcf1d8c385 CHECK (a > 1);",
		"ALTER TABLE T1 DROP CONSTRAINT CK11;",
		"ALTER TABLE T1 RENAME CONSTRAINT ptah_e2c5b8dcf1d8c385 TO ck11;",
	})
}

// The control: MySQL sets no CheckAddedFirst, so the same change drops the
// CHECK and adds it again under its own name.
func TestPlanner_MySQLReplacesANamedCheckDroppingItFirst(t *testing.T) {
	c := qt.New(t)

	nodes, err := mysqlplanner.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), checkChange("ck11", "ck11"))
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("mysql", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(statementsOf(sql), qt.DeepEquals, []string{
		"ALTER TABLE `T1` DROP CONSTRAINT `ck11`;",
		"ALTER TABLE `T1` ADD CONSTRAINT `ck11` CHECK (a > 1);",
	})
}

// An unnamed CHECK Oracle named SYS_C008926 is not paired with a declared
// CHECK of another condition, so the plan adds the new CHECK with the other
// additions and drops the old one with the other removals, after it.
func TestPlanner_OracleAddsAnUnnamedCheckBeforeItDropsTheOldOne(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsRemoved: difftypes.ConstraintRemovals{{
			Name: "SYS_C008926", TableName: "T1", Type: "CHECK",
			Identity: difftypes.ConstraintIdentity{Table: "T1", Name: "SYS_C008926"},
		}},
		ConstraintsAdded: difftypes.ConstraintAdditions{{TableName: "T1", Type: "CHECK", CheckExpression: "a > 1"}},
	}

	nodes, err := oracleplanner.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("oracle", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(statementsOf(sql), qt.DeepEquals, []string{
		"ALTER TABLE T1 ADD CHECK (a > 1);",
		"ALTER TABLE T1 DROP CONSTRAINT SYS_C008926;",
	})
}

// A CHECK the database holds disabled, whose condition is the declared one,
// is enabled in place. ENABLE VALIDATE over a row the CHECK refuses answers
// ORA-02293 and leaves the CHECK disabled, where a drop and an add lose it.
func TestPlanner_OracleEnablesADisabledCheck(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsValidated: []difftypes.ConstraintValidation{{TableName: "T1", Name: "CK11"}},
	}

	nodes, err := oracleplanner.New().GenerateMigrationAST(t.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQL("oracle", nodes...)

	c.Assert(err, qt.IsNil)
	c.Assert(statementsOf(sql), qt.DeepEquals, []string{"ALTER TABLE T1 ENABLE VALIDATE CONSTRAINT CK11;"})
}
