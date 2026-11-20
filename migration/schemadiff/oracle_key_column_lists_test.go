package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestPlan_OraclePrimaryKeyOverColumnKey_HappyPath plans a named primary key
// over a column whose UNIQUE Oracle named, where the file declares the column
// without it. The read carries that key on the column, the column change drops
// it, and the primary key follows. Dropped a second time ahead of the
// constraint additions, it answers ORA-02442 and the migration stops there.
func TestPlan_OraclePrimaryKeyOverColumnKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired, _, err := sqlschema.ReadWith(c.Context(),
		[]byte("CREATE TABLE p (a NUMBER(10) NOT NULL, CONSTRAINT pg_pk PRIMARY KEY (a));"),
		platform.Oracle, nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.IsNil)
	current := &catalog.Database{Tables: []catalog.Table{{Name: "P", Columns: []catalog.Column{
		{Name: "A", DataType: "NUMBER(10)", IsNullable: "NO", IsUnique: true},
	}}}}
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), &desired, current, platform.Oracle, runtime))

	plan, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Oracle,
		planext.Options{Capabilities: capability.ForDialect(platform.Oracle)})

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.DeepEquals, []string{
		"-- Modify table: p\n-- Modify column p.a: unique: true -> false\nALTER TABLE p DROP UNIQUE (a)",
		"ALTER TABLE p ADD CONSTRAINT pg_pk PRIMARY KEY (a)",
	})
}

// TestPlan_OracleKeyUnderForeignKey_HappyPath plans a UNIQUE replaced by
// another over the same column while a foreign key references it. Oracle
// answers the drop of a referenced key with ORA-02273, so the foreign key
// leaves first, the old key goes, the new key comes, and the foreign key
// comes back over it.
func TestPlan_OracleKeyUnderForeignKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired, _, err := sqlschema.ReadWith(c.Context(),
		[]byte("CREATE TABLE pp (id NUMBER(10) PRIMARY KEY, a NUMBER(10), CONSTRAINT pp_k2 UNIQUE (a));\n"+
			"CREATE TABLE pq (r NUMBER(10), CONSTRAINT pq_fk FOREIGN KEY (r) REFERENCES pp (a));"),
		platform.Oracle, nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.IsNil)
	current := keyUnderForeignKeyCatalog(
		catalog.Constraint{Name: "PP_K1", TableName: "PP", Type: "UNIQUE", ColumnNames: []string{"A"}, ColumnName: "A"})
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), &desired, current, platform.Oracle, runtime))

	plan, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Oracle,
		planext.Options{Capabilities: capability.ForDialect(platform.Oracle)})

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.DeepEquals, []string{
		"ALTER TABLE PQ DROP CONSTRAINT PQ_FK",
		"ALTER TABLE PP DROP CONSTRAINT PP_K1",
		"ALTER TABLE pp ADD CONSTRAINT pp_k2 UNIQUE (a)",
		"ALTER TABLE pq ADD CONSTRAINT pq_fk FOREIGN KEY (r) REFERENCES pp (a)",
	})
}

// keyUnderForeignKeyCatalog is table PP with a key over column A, which PQ_FK
// references, as the Oracle reader reports it: the read marks A unique, and
// keys holds the key rows. A key Oracle named for A alone has no row, and a
// key the account named, such as PP_K1, has one.
func keyUnderForeignKeyCatalog(keys ...catalog.Constraint) *catalog.Database {
	database := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "PP", Columns: []catalog.Column{
				{Name: "ID", DataType: "NUMBER(10)", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "A", DataType: "NUMBER(10)", IsNullable: "YES", IsUnique: true},
			}},
			{Name: "PQ", Columns: []catalog.Column{{Name: "R", DataType: "NUMBER(10)", IsNullable: "YES"}}},
		},
		Constraints: []catalog.Constraint{{
			Name: "PQ_FK", TableName: "PQ", Type: "FOREIGN KEY", ColumnNames: []string{"R"}, ColumnName: "R",
			ForeignTable: new("PP"), ForeignColumns: []string{"A"}, ForeignColumn: new("A"), DeleteRule: new("NO ACTION"),
		}},
	}
	database.Constraints = append(database.Constraints, keys...)
	return database
}

// TestPlan_OracleColumnKeyUnderForeignKey_HappyPath plans a named UNIQUE over
// a column whose own key Oracle named, while a foreign key references that
// key. The column's key is dropped by its column ahead of the named one, and
// the foreign key leaves before it and comes back after, as in
// [TestPlan_OracleKeyUnderForeignKey_HappyPath].
func TestPlan_OracleColumnKeyUnderForeignKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired, _, err := sqlschema.ReadWith(c.Context(),
		[]byte("CREATE TABLE pp (id NUMBER(10) PRIMARY KEY, a NUMBER(10), CONSTRAINT pp_k2 UNIQUE (a));\n"+
			"CREATE TABLE pq (r NUMBER(10), CONSTRAINT pq_fk FOREIGN KEY (r) REFERENCES pp (a));"),
		platform.Oracle, nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.IsNil)
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), &desired, keyUnderForeignKeyCatalog(), platform.Oracle, runtime))

	plan, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Oracle,
		planext.Options{Capabilities: capability.ForDialect(platform.Oracle)})

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.DeepEquals, []string{
		"ALTER TABLE PQ DROP CONSTRAINT PQ_FK",
		"ALTER TABLE pp DROP UNIQUE (a)",
		"ALTER TABLE pp ADD CONSTRAINT pp_k2 UNIQUE (a)",
		"ALTER TABLE pq ADD CONSTRAINT pq_fk FOREIGN KEY (r) REFERENCES pp (a)",
	})
}

// TestPlan_OracleColumnKeyOverDroppedPrimaryKey_HappyPath plans the rollback of
// [TestPlan_OraclePrimaryKeyOverColumnKey_HappyPath]: the named primary key
// goes and the column takes its own UNIQUE back. Oracle answers the column's
// UNIQUE with ORA-02261 while the primary key holds the column, so the key is
// dropped ahead of the column change.
func TestPlan_OracleColumnKeyOverDroppedPrimaryKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired, _, err := sqlschema.ReadWith(c.Context(),
		[]byte("CREATE TABLE p (a NUMBER(10) NOT NULL UNIQUE);"),
		platform.Oracle, nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.IsNil)
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "P", Columns: []catalog.Column{
			{Name: "A", DataType: "NUMBER(10)", IsNullable: "NO", IsPrimaryKey: true},
		}}},
		Constraints: []catalog.Constraint{{Name: "PG_PK", TableName: "P", Type: "PRIMARY KEY", ColumnNames: []string{"A"}, ColumnName: "A"}},
	}
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), &desired, current, platform.Oracle, runtime))

	plan, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Oracle,
		planext.Options{Capabilities: capability.ForDialect(platform.Oracle)})

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.DeepEquals, []string{
		"ALTER TABLE P DROP CONSTRAINT PG_PK",
		"-- Modify table: p\n-- Modify column p.a: unique: false -> true\nALTER TABLE p MODIFY (a NUMBER(10) UNIQUE)",
	})
}
