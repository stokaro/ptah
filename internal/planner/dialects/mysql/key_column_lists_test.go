package mysql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/constraintscope"
	"ptah.run/internal/planner/dialects/mysql"
)

// oracleKeyIdentity is the identity the comparison gives a constraint of table
// on Oracle.
func oracleKeyIdentity(table, name string) difftypes.ConstraintIdentity {
	return constraintscope.Identity(identifier.ForDialect(platform.Oracle), table, name)
}

// keyAddition is a key constraint the plan adds to table on Oracle.
func keyAddition(table, name, constraintType string, columns ...string) difftypes.ConstraintAdditionInfo {
	return difftypes.ConstraintAdditionInfo{
		Name: name, TableName: table, Type: constraintType, Columns: columns, Identity: oracleKeyIdentity(table, name),
	}
}

// keyRemoval is a key constraint the plan removes from table on Oracle.
func keyRemoval(table, name, constraintType string, columns ...string) difftypes.ConstraintRemovalInfo {
	return difftypes.ConstraintRemovalInfo{
		Name: name, TableName: table, Type: constraintType, Columns: columns, Identity: oracleKeyIdentity(table, name),
	}
}

// planOracle renders the plan of diff for Oracle 23.
func planOracle(c *qt.C, diff *difftypes.SchemaDiff) string {
	nodes, err := mysql.NewForDialect(platform.Oracle, capability.Oracle23()).
		GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
	c.Assert(err, qt.IsNil)
	sql, err := builtin.RenderSQLWithCapabilities(platform.Oracle, capability.Oracle23(), nodes...)
	c.Assert(err, qt.IsNil)
	return sql
}

// TestPlanner_OracleKeyOverHeldColumns_HappyPath plans keys added over the
// ordered column list another key of the table holds. Oracle holds one key
// per list and answers the ADD with ORA-02261 while the old key stands, so the
// old key is dropped first:
//
//   - m: a named key the plan removes, its columns carried on the removal
//     where the observed table does not hold it, as in a rollback;
//   - n: a named key whose columns only the observed table knows;
//   - l: the key Oracle named for column a, which the read carries on the
//     column, so it is dropped by its column;
//   - o: a key over (b, a), another list than the (a, b) added, which stays
//     with the other removals;
//   - q: a key changed under its own name, which the constraint step drops
//     and adds itself, once.
func TestPlanner_OracleKeyOverHeldColumns_HappyPath(t *testing.T) {
	c := qt.New(t)
	observed := func(table string, unique bool, constraints ...catalog.Constraint) schemacapture.TableObservation {
		return schemacapture.TableObservation{
			Table: catalog.Table{Name: table, Schema: "APP", Columns: []catalog.Column{
				{Name: "A", IsUnique: unique}, {Name: "B"},
			}},
			Constraints: constraints,
		}
	}
	diff := &difftypes.SchemaDiff{
		ConstraintsAdded: difftypes.ConstraintAdditions{
			keyAddition("m", "m_k2", "UNIQUE", "a"),
			keyAddition("n", "n_k2", "UNIQUE", "a", "b"),
			keyAddition("l", "l_k2", "UNIQUE", "a"),
			keyAddition("o", "o_k2", "UNIQUE", "a", "b"),
			keyAddition("q", "q_k", "UNIQUE", "a"),
		},
		ConstraintsRemoved: difftypes.ConstraintRemovals{
			keyRemoval("APP.M", "M_K1", "UNIQUE", "A"),
			keyRemoval("APP.N", "N_K1", "UNIQUE"),
			keyRemoval("APP.O", "O_K1", "UNIQUE", "B", "A"),
			keyRemoval("q", "q_k", "UNIQUE", "a"),
		},
		ObservedConstraintHosts: []schemacapture.TableObservation{
			observed("M", false),
			observed("N", false, catalog.Constraint{Name: "N_K1", TableName: "N", Schema: "APP", Type: "UNIQUE", ColumnNames: []string{"A", "B"}}),
			observed("L", true),
			observed("O", false, catalog.Constraint{Name: "O_K1", TableName: "O", Schema: "APP", Type: "UNIQUE", ColumnNames: []string{"B", "A"}}),
		},
	}

	sql := planOracle(c, diff)

	c.Assert(sql, qt.Equals, "ALTER TABLE APP.M DROP CONSTRAINT M_K1;\n"+
		"ALTER TABLE APP.N DROP CONSTRAINT N_K1;\n"+
		"ALTER TABLE l DROP UNIQUE (a);\n"+
		"ALTER TABLE m ADD CONSTRAINT m_k2 UNIQUE (a);\n"+
		"ALTER TABLE n ADD CONSTRAINT n_k2 UNIQUE (a, b);\n"+
		"ALTER TABLE l ADD CONSTRAINT l_k2 UNIQUE (a);\n"+
		"ALTER TABLE o ADD CONSTRAINT o_k2 UNIQUE (a, b);\n"+
		"ALTER TABLE q DROP CONSTRAINT q_k;\n"+
		"ALTER TABLE q ADD CONSTRAINT q_k UNIQUE (a);\n"+
		"ALTER TABLE APP.O DROP CONSTRAINT O_K1;\n")
}

// TestPlanner_OracleColumnKeyRestored_HappyPath plans the removal of a named
// key over column a alone where the target declares a unique and names no key
// over it, which is the rollback of a named key that replaced the one Oracle
// named for the column. The column gets its own key back after the drop;
// without it the column is left with no key at all.
func TestPlanner_OracleColumnKeyRestored_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff := &difftypes.SchemaDiff{
		ConstraintsRemoved: difftypes.ConstraintRemovals{keyRemoval("l", "l_k2", "UNIQUE", "a")},
		DeclaredConstraintHosts: []schemacapture.TableDeclaration{{
			Table:  schemamodel.Table{StructName: "L", Name: "l"},
			Fields: []schemamodel.Field{{StructName: "L", Name: "a", Type: "VARCHAR2(20)", Nullable: true, Unique: true}},
		}},
	}

	sql := planOracle(c, diff)

	c.Assert(sql, qt.Equals, "ALTER TABLE l DROP CONSTRAINT l_k2;\n"+
		"ALTER TABLE l ADD UNIQUE (a);\n")
}

// TestPlanner_PrimaryKeyName_HappyPath plans a primary key added to an
// existing table. Oracle keeps the name a key is declared with, and a later
// file and the rollback drop it by that name, so the statement writes it.
// MySQL names every primary key PRIMARY, and the statement writes none.
func TestPlanner_PrimaryKeyName_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    string
	}{
		{name: "Oracle", dialect: platform.Oracle, caps: capability.Oracle23(), want: "ALTER TABLE s ADD CONSTRAINT s_pk PRIMARY KEY (id);\n"},
		{name: "MySQL", dialect: platform.MySQL, caps: capability.MySQL84(), want: "ALTER TABLE `s` ADD PRIMARY KEY (`id`);\n\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{
				Name: "s_pk", TableName: "s", Type: "PRIMARY KEY", Columns: []string{"id"},
				Identity: constraintscope.Identity(identifier.ForDialect(test.dialect), "s", "s_pk"),
			}}}

			nodes, err := mysql.NewForDialect(test.dialect, test.caps).
				GenerateMigrationAST(c.Context(), must.Must(builtin.New()), diff)
			c.Assert(err, qt.IsNil)
			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, nodes...)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}
