//go:build integration

package integration_test

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/dbtarget"
)

// Collations that Oracle refuses partway through a plan.
//
// Oracle commits each DDL statement as it runs, so a statement the server
// refuses leaves every statement before it applied: a partial apply. Each test
// here declares a change the server would refuse after an earlier statement of
// the same plan had committed, expects the plan to be refused before it runs,
// and reads the catalog back to show that nothing was applied. The statements
// that reach the server in the happy-path tests are the control that the
// refusals are not blanket.

const oraclePartialPrefix = "ptah_c64_"

// openOraclePartialApply connects, drops the tables the test names before and
// after it runs, and creates the tables of setup with the statements given.
func openOraclePartialApply(
	c *qt.C,
	ctx context.Context,
	tables []string,
	setup ...string,
) *dbschema.DatabaseConnection {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(ctx, dbtarget.URL(c, dbtarget.Oracle))
	c.Assert(err, qt.IsNil)
	drop := func(ctx context.Context) {
		for _, table := range tables {
			_, err := conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+table+" PURGE")
			c.Check(err, qt.IsNil)
		}
	}
	drop(ctx)
	c.Cleanup(func() {
		drop(context.WithoutCancel(ctx))
		dbschema.CloseAndWarn(conn)
	})
	for _, statement := range setup {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return conn
}

// oraclePartialPlan is the plan `ptah schema apply` prepares, narrowed to the
// statements that reach a table of this file; the account holds other tables
// and the plan drops each of them.
func oraclePartialPlan(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	desired *schemamodel.Database,
) ([]string, error) {
	plan, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{Desired: desired, Runtime: must.Must(builtin.New())})
	if err != nil {
		return nil, err
	}
	statements, err := plan.Statements()
	if err != nil {
		return nil, err
	}
	return slices.DeleteFunc(statements, func(statement string) bool {
		return !strings.Contains(strings.ToLower(statement), oraclePartialPrefix)
	}), nil
}

// applyOraclePartialPlan runs the plan statement by statement, as the apply
// does, so a statement the server refuses fails the test at that statement.
func applyOraclePartialPlan(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, desired *schemamodel.Database) []string {
	c.Helper()
	statements, err := oraclePartialPlan(ctx, conn, desired)
	c.Assert(err, qt.IsNil)
	for _, statement := range statements {
		_, err := conn.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement: %s", statement))
	}
	return statements
}

// oracleColumnsOf reads the collation of every column of table, in column
// order, from the catalog.
func oracleColumnsOf(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection, table string) []string {
	c.Helper()
	return oracleDeclaredRowValues(c, ctx, conn,
		"SELECT column_name || '=' || NVL(collation, '-') FROM user_tab_columns WHERE table_name = '"+strings.ToUpper(table)+"' ORDER BY column_id")
}

// oraclePartialTables lists the tables of this file the account holds.
func oraclePartialTables(c *qt.C, ctx context.Context, conn *dbschema.DatabaseConnection) []string {
	c.Helper()
	return oracleDeclaredRowValues(c, ctx, conn,
		"SELECT table_name FROM user_tables WHERE table_name LIKE 'PTAH\\_C64\\_%' ESCAPE '\\' ORDER BY table_name")
}

// oraclePartialTable is a table and its fields. The struct name of each field is
// the table's name.
type oraclePartialTable struct {
	name   string
	fields []schemamodel.Field
}

// oraclePartialSchema declares the tables given, with the indexes and
// constraints given, each of which names its table in TableName or Table.
func oraclePartialSchema(
	indexes []schemamodel.Index,
	constraints []schemamodel.Constraint,
	tables ...oraclePartialTable,
) *schemamodel.Database {
	db := &schemamodel.Database{Indexes: indexes, Constraints: constraints}
	for _, table := range tables {
		db.Tables = append(db.Tables, schemamodel.Table{StructName: table.name, Name: table.name})
		for _, field := range table.fields {
			field.StructName = table.name
			db.Fields = append(db.Fields, field)
		}
	}
	for i := range db.Indexes {
		db.Indexes[i].StructName = db.Indexes[i].TableName
	}
	for i := range db.Constraints {
		db.Constraints[i].StructName = db.Constraints[i].Table
	}
	schemamodel.Finalize(db)
	return db
}

func oracleID() schemamodel.Field {
	return schemamodel.Field{Name: "id", Type: "NUMBER(10)", Primary: true}
}

// TestOracleColumnCollationLive_RefusesAKeyOracleNamedItself holds the change of
// a column that sits in a PRIMARY KEY or a UNIQUE key written inline on the
// column. Oracle names the key itself, the reader carries it on the column's
// flags and not among the constraints, and ORA-43923 answers the MODIFY. The
// companion table's change is planned first and would commit, so the refusal
// has to come before it: the catalog read back shows it kept USING_NLS_COMP.
func TestOracleColumnCollationLive_RefusesAKeyOracleNamedItself(t *testing.T) {
	tests := []struct {
		name   string
		setup  string
		keyed  oraclePartialTable
		holder string
	}{
		{
			name:   "primary key",
			setup:  `CREATE TABLE ptah_c64_keyed (pk VARCHAR2(10) COLLATE BINARY_CI PRIMARY KEY)`,
			keyed:  oraclePartialTable{name: "ptah_c64_keyed", fields: []schemamodel.Field{{Name: "pk", Type: "VARCHAR2(10)", Primary: true, Collate: "BINARY"}}},
			holder: "the primary key",
		},
		{
			name:  "unique key",
			setup: `CREATE TABLE ptah_c64_keyed (id NUMBER(10), u VARCHAR2(10) COLLATE BINARY_CI UNIQUE)`,
			keyed: oraclePartialTable{name: "ptah_c64_keyed", fields: []schemamodel.Field{
				{Name: "id", Type: "NUMBER(10)", Nullable: true},
				{Name: "u", Type: "VARCHAR2(10)", Nullable: true, Unique: true, Collate: "BINARY"},
			}},
			holder: "a unique key",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_ta", "ptah_c64_keyed"},
				`CREATE TABLE ptah_c64_ta (id NUMBER(10) PRIMARY KEY, n VARCHAR2(10))`, tc.setup)
			desired := oraclePartialSchema(nil, nil,
				oraclePartialTable{name: "ptah_c64_ta", fields: []schemamodel.Field{
					oracleID(), {Name: "n", Type: "VARCHAR2(10)", Nullable: true, Collate: "BINARY_AI"},
				}},
				tc.keyed,
			)

			_, err := oraclePartialPlan(ctx, conn, desired)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*collation of column "(pk|u)" of table "ptah_c64_keyed".*`+tc.holder+` uses the column.*`)
			c.Assert(oracleColumnsOf(c, ctx, conn, "ptah_c64_ta"), qt.DeepEquals, []string{"ID=-", "N=USING_NLS_COMP"})
		})
	}
}

// TestOracleColumnCollationLive_RefusesAColumnTheSamePlanAdds holds the plan
// that adds a generated column over a column and changes that column's
// collation. The plan adds columns before it changes them, the ADD commits, and
// the MODIFY answers ORA-43922 because the generated column now holds the
// column. The refusal comes first, so the catalog still has no such column.
func TestOracleColumnCollationLive_RefusesAColumnTheSamePlanAdds(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_tm"},
		`CREATE TABLE ptah_c64_tm (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20 CHAR))`)
	desired := oraclePartialSchema(nil, nil, oraclePartialTable{name: "ptah_c64_tm", fields: []schemamodel.Field{
		oracleID(),
		{Name: "a", Type: "VARCHAR2(20 CHAR)", Nullable: true, Collate: "BINARY_CI"},
		{Name: "g", Type: "VARCHAR2(100)", Nullable: true, Collate: "BINARY_CI", GeneratedExpression: "UPPER(a)", GeneratedKind: "VIRTUAL"},
	}})

	_, err := oraclePartialPlan(ctx, conn, desired)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*collation of column "a" of table "ptah_c64_tm".*the generated column "g" the same diff adds uses the column.*`)
	c.Assert(oracleColumnsOf(c, ctx, conn, "ptah_c64_tm"), qt.DeepEquals, []string{"ID=-", "A=USING_NLS_COMP"})
}

// TestOracleColumnCollationLive_AddsAnIndexAndACheckAfterTheChange is the
// control for the refusal above: an index and a table CHECK over the column come
// after the MODIFY in the plan, so the plan that adds them with the change
// applies in full. The statement positions are asserted, because the refusal
// leaves them out on the strength of that order.
func TestOracleColumnCollationLive_AddsAnIndexAndACheckAfterTheChange(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_tm"},
		`CREATE TABLE ptah_c64_tm (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20 CHAR))`)
	desired := oraclePartialSchema(
		[]schemamodel.Index{{TableName: "ptah_c64_tm", Name: "ptah_c64_tm_a_idx", Fields: []string{"a"}}},
		[]schemamodel.Constraint{{Table: "ptah_c64_tm", Name: "ptah_c64_tm_ck", Type: "CHECK", CheckExpression: "a <> 'x'"}},
		oraclePartialTable{name: "ptah_c64_tm", fields: []schemamodel.Field{
			oracleID(), {Name: "a", Type: "VARCHAR2(20 CHAR)", Nullable: true, Collate: "BINARY_CI"},
		}},
	)

	statements := applyOraclePartialPlan(c, ctx, conn, desired)

	position := func(fragment string) int {
		return slices.IndexFunc(statements, func(statement string) bool { return strings.Contains(strings.ToUpper(statement), fragment) })
	}
	c.Assert(position("MODIFY"), qt.Not(qt.Equals), -1)
	c.Assert(position("CREATE INDEX"), qt.Not(qt.Equals), -1)
	c.Assert(position("PTAH_C64_TM_CK"), qt.Not(qt.Equals), -1)
	c.Assert(position("MODIFY") < position("CREATE INDEX"), qt.IsTrue)
	c.Assert(position("MODIFY") < position("PTAH_C64_TM_CK"), qt.IsTrue)
	c.Assert(oracleColumnsOf(c, ctx, conn, "ptah_c64_tm"), qt.DeepEquals, []string{"ID=-", "A=BINARY_CI"})
	c.Assert(oracleDeclaredRowValues(c, ctx, conn,
		`SELECT index_name FROM user_indexes WHERE table_name = 'PTAH_C64_TM' AND index_name = 'PTAH_C64_TM_A_IDX'`),
		qt.DeepEquals, []string{"PTAH_C64_TM_A_IDX"})
	c.Assert(oracleDeclaredRowValues(c, ctx, conn,
		`SELECT constraint_name FROM user_constraints WHERE constraint_name = 'PTAH_C64_TM_CK'`),
		qt.DeepEquals, []string{"PTAH_C64_TM_CK"})
}

// TestOracleColumnCollationLive_RefusesAnUnknownCollationName holds a schema
// shared with MySQL. The first table is created, the second names utf8mb4_bin,
// and ORA-12746 answers after the first has committed. The refusal names the
// column and comes before the first statement: neither table exists.
func TestOracleColumnCollationLive_RefusesAnUnknownCollationName(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_t1", "ptah_c64_t2"})
	desired := oraclePartialSchema(nil, nil,
		oraclePartialTable{name: "ptah_c64_t1", fields: []schemamodel.Field{oracleID()}},
		oraclePartialTable{name: "ptah_c64_t2", fields: []schemamodel.Field{
			oracleID(), {Name: "v", Type: "VARCHAR2(20)", Nullable: true, Collate: "utf8mb4_bin"},
		}},
	)

	_, err := oraclePartialPlan(ctx, conn, desired)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*column "v" of table "ptah_c64_t2" declares collation "utf8mb4_bin", which this database does not know \(ORA-12746\).*`)
	c.Assert(oraclePartialTables(c, ctx, conn), qt.HasLen, 0)
}

// TestOracleColumnCollationLive_AcceptsTheNamesTheServerLists is the control:
// a sort the server lists, with _CI or _AI after it, and the three names the
// check adds are all created and read back as declared.
func TestOracleColumnCollationLive_AcceptsTheNamesTheServerLists(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_names"})
	desired := oraclePartialSchema(nil, nil, oraclePartialTable{name: "ptah_c64_names", fields: []schemamodel.Field{
		oracleID(),
		{Name: "a", Type: "VARCHAR2(20)", Nullable: true, Collate: "GERMAN_CI"},
		{Name: "b", Type: "VARCHAR2(20)", Nullable: true, Collate: "xfrench_ai"},
		{Name: "c", Type: "VARCHAR2(20)", Nullable: true, Collate: "BINARY_AI"},
		{Name: "d", Type: "VARCHAR2(20)", Nullable: true, Collate: "FRENCH"},
		{Name: "e", Type: "VARCHAR2(20)", Nullable: true, Collate: "USING_NLS_COMP"},
	}})

	applyOraclePartialPlan(c, ctx, conn, desired)

	c.Assert(oracleColumnsOf(c, ctx, conn, "ptah_c64_names"), qt.DeepEquals, []string{
		"ID=-", "A=GERMAN_CI", "B=XFRENCH_AI", "C=BINARY_AI", "D=FRENCH", "E=USING_NLS_COMP",
	})
}

// TestOracleColumnCollationLive_ReadsTheCollationSortsOfTheServer holds the
// connection's list to the server's own: what the check accepts is what
// V$NLS_VALID_VALUES names.
func TestOracleColumnCollationLive_ReadsTheCollationSortsOfTheServer(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, nil)

	listed := oracleDeclaredRowValues(c, ctx, conn, `SELECT UPPER(value) FROM v$nls_valid_values WHERE parameter = 'SORT'`)

	c.Assert(listed, qt.Not(qt.HasLen), 0)
	c.Assert(slices.Sorted(slices.Values(conn.Info().CollationNames)), qt.DeepEquals, slices.Sorted(slices.Values(listed)))
}

// TestOracleColumnCollationLive_RefusesADescendingKeyAnotherIndexHolds holds
// the plan that creates an index with a DESC key part over a collated column
// that another index already holds. Oracle answers ORA-54034 to the second
// index, measured as a plain ascending index first, and the index created
// before it has committed. The refusal names the index and the catalog read
// back shows the first index absent.
func TestOracleColumnCollationLive_RefusesADescendingKeyAnotherIndexHolds(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_td"},
		`CREATE TABLE ptah_c64_td (id NUMBER(10) PRIMARY KEY, c VARCHAR2(10) COLLATE BINARY_CI, a VARCHAR2(10))`,
		`CREATE INDEX ptah_c64_td_c ON ptah_c64_td (c)`)
	desired := oraclePartialSchema(
		[]schemamodel.Index{
			{TableName: "ptah_c64_td", Name: "ptah_c64_td_c", Fields: []string{"c"}},
			{TableName: "ptah_c64_td", Name: "ptah_c64_td_a", Fields: []string{"a"}},
			{TableName: "ptah_c64_td", Name: "ptah_c64_td_c_desc", Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}}},
		},
		nil,
		oraclePartialTable{name: "ptah_c64_td", fields: []schemamodel.Field{
			oracleID(),
			{Name: "c", Type: "VARCHAR2(10)", Nullable: true, Collate: "BINARY_CI"},
			{Name: "a", Type: "VARCHAR2(10)", Nullable: true},
		}},
	)

	_, err := oraclePartialPlan(ctx, conn, desired)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*index "ptah_c64_td_c_desc" of table "ptah_c64_td" orders column "c" DESC.*ORA-54034.*`)
	c.Assert(oracleDeclaredRowValues(c, ctx, conn,
		`SELECT index_name FROM user_indexes WHERE table_name = 'PTAH_C64_TD' AND index_name LIKE 'PTAH\_C64\_TD\_%' ESCAPE '\' ORDER BY index_name`),
		qt.DeepEquals, []string{"PTAH_C64_TD_C"})
}

// TestOracleColumnCollationLive_AppliesTheFirstDescendingKeyOverACollatedColumn
// is the control: the first index over a collated column takes DESC, as does a
// DESC part over an uncollated column beside a collated one. The plan applies in
// full and is empty on the next run.
func TestOracleColumnCollationLive_AppliesTheFirstDescendingKeyOverACollatedColumn(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_td"})
	desired := oraclePartialSchema(
		[]schemamodel.Index{
			{TableName: "ptah_c64_td", Name: "ptah_c64_td_c_desc", Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}}},
			{TableName: "ptah_c64_td", Name: "ptah_c64_td_na", Parts: []schemamodel.IndexPart{{Name: "n"}, {Name: "a", Desc: true}}},
		},
		nil,
		oraclePartialTable{name: "ptah_c64_td", fields: []schemamodel.Field{
			oracleID(),
			{Name: "c", Type: "VARCHAR2(10)", Nullable: true, Collate: "BINARY_CI"},
			{Name: "a", Type: "VARCHAR2(10)", Nullable: true},
			{Name: "n", Type: "VARCHAR2(10)", Nullable: true, Collate: "BINARY_CI"},
		}},
	)

	applyOraclePartialPlan(c, ctx, conn, desired)

	c.Assert(oracleDeclaredRowValues(c, ctx, conn,
		`SELECT index_name FROM user_indexes WHERE table_name = 'PTAH_C64_TD' AND index_name LIKE 'PTAH\_C64\_TD\_%' ESCAPE '\' ORDER BY index_name`),
		qt.DeepEquals, []string{"PTAH_C64_TD_C_DESC", "PTAH_C64_TD_NA"})
	statements, err := oraclePartialPlan(ctx, conn, desired)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 0)
}

// TestOracleColumnCollationLive_ReadsAnNLSSortOverAnUncollatedColumnAsAFunction
// holds a user-written index. NLSSORT(c, 'nls_sort=binary_ci') over a column
// that declares no collation lists the same expression as the key over a
// collated column, and reading it as a plain index on c would hide the function.
// The index is read as an expression.
func TestOracleColumnCollationLive_ReadsAnNLSSortOverAnUncollatedColumnAsAFunction(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_ns"},
		`CREATE TABLE ptah_c64_ns (id NUMBER(10) PRIMARY KEY, c VARCHAR2(10))`,
		`CREATE INDEX ptah_c64_ns_user ON ptah_c64_ns (NLSSORT(c, 'nls_sort=binary_ci'))`)

	read, err := conn.Reader().ReadSchemaContext(ctx)

	c.Assert(err, qt.IsNil)
	at := slices.IndexFunc(read.Indexes, func(index catalog.Index) bool { return index.Name == "PTAH_C64_NS_USER" })
	c.Assert(at, qt.Not(qt.Equals), -1)
	c.Assert(read.Indexes[at].Parts, qt.HasLen, 1)
	c.Assert(read.Indexes[at].Parts[0].Name, qt.Equals, "")
	c.Assert(read.Indexes[at].Parts[0].Expr, qt.Contains, "NLSSORT")
}

// oracleDescOrderTable declares table ptah_c64_tq with a collated c, a plain d
// and the indexes given.
func oracleDescOrderTable(indexes ...schemamodel.Index) *schemamodel.Database {
	for i := range indexes {
		indexes[i].TableName = "ptah_c64_tq"
	}
	return oraclePartialSchema(indexes, nil, oraclePartialTable{name: "ptah_c64_tq", fields: []schemamodel.Field{
		oracleID(),
		{Name: "c", Type: "VARCHAR2(10)", Nullable: true, Collate: "BINARY_CI"},
		{Name: "d", Type: "VARCHAR2(10)", Nullable: true},
	}})
}

const oracleTqIndexes = `SELECT index_name FROM user_indexes WHERE table_name = 'PTAH_C64_TQ' AND index_name LIKE 'PTAH\_C64\_TQ\_%' ESCAPE '\' ORDER BY index_name`

// TestOracleColumnCollationLive_AppliesDescendingKeysCreatedBeforeAscendingOnes
// holds the order the server accepts. A plan creates a table's new indexes in
// name order, and a DESC key part over a collated column is accepted until an
// index holds the column ascending, so these names put every DESC shape ahead of
// the ascending index. All four apply and the next plan is empty. The first two
// are the shapes a reviewer measured: (c DESC) then (c), and (c DESC) then
// (d, c DESC).
func TestOracleColumnCollationLive_AppliesDescendingKeysCreatedBeforeAscendingOnes(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_tq"})
	desired := oracleDescOrderTable(
		schemamodel.Index{Name: "ptah_c64_tq_1", Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}}},
		schemamodel.Index{Name: "ptah_c64_tq_2", Parts: []schemamodel.IndexPart{{Name: "d"}, {Name: "c", Desc: true}}},
		schemamodel.Index{Name: "ptah_c64_tq_3", Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}, {Name: "d"}}},
		schemamodel.Index{Name: "ptah_c64_tq_4", Fields: []string{"c"}},
	)

	statements := applyOraclePartialPlan(c, ctx, conn, desired)

	creates := slices.DeleteFunc(slices.Clone(statements), func(statement string) bool { return !strings.Contains(statement, "CREATE INDEX") })
	c.Assert(creates, qt.HasLen, 4)
	c.Assert(creates[0], qt.Contains, "ptah_c64_tq_1")
	c.Assert(creates[3], qt.Contains, "ptah_c64_tq_4")
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleTqIndexes), qt.DeepEquals,
		[]string{"PTAH_C64_TQ_1", "PTAH_C64_TQ_2", "PTAH_C64_TQ_3", "PTAH_C64_TQ_4"})
	again, err := oraclePartialPlan(ctx, conn, desired)
	c.Assert(err, qt.IsNil)
	c.Assert(again, qt.HasLen, 0)
}

// TestOracleColumnCollationLive_RefusesADescendingKeyCreatedAfterAnAscendingOne
// is the same declaration with the names the other way round: the ascending index
// sorts first, the plan creates it first, and the DESC index would answer
// ORA-54034 after it. The refusal comes before the first statement, so the table
// does not exist.
func TestOracleColumnCollationLive_RefusesADescendingKeyCreatedAfterAnAscendingOne(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_tq"})
	desired := oracleDescOrderTable(
		schemamodel.Index{Name: "ptah_c64_tq_1", Fields: []string{"c"}},
		schemamodel.Index{Name: "ptah_c64_tq_2", Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}}},
	)

	_, err := oraclePartialPlan(ctx, conn, desired)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*index "ptah_c64_tq_2" of table "ptah_c64_tq" orders column "c" DESC.*index "ptah_c64_tq_1" already holds it in an ascending key.*ORA-54034.*`)
	c.Assert(oraclePartialTables(c, ctx, conn), qt.HasLen, 0)
}

// TestOracleColumnCollationLive_AppliesADescendingKeyAfterAnExistingDescendingOne
// holds the database read: an index the database holds with c DESC is read as
// DESC, so a new (d, c DESC) beside it is accepted and applies. Read as
// ascending it would be refused for a statement the server takes.
func TestOracleColumnCollationLive_AppliesADescendingKeyAfterAnExistingDescendingOne(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_tq"},
		`CREATE TABLE ptah_c64_tq (id NUMBER(10) PRIMARY KEY, c VARCHAR2(10) COLLATE BINARY_CI, d VARCHAR2(10))`,
		`CREATE INDEX ptah_c64_tq_1 ON ptah_c64_tq (c DESC)`)
	desired := oracleDescOrderTable(
		schemamodel.Index{Name: "ptah_c64_tq_1", Parts: []schemamodel.IndexPart{{Name: "c", Desc: true}}},
		schemamodel.Index{Name: "ptah_c64_tq_2", Parts: []schemamodel.IndexPart{{Name: "d"}, {Name: "c", Desc: true}}},
	)

	applyOraclePartialPlan(c, ctx, conn, desired)

	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleTqIndexes), qt.DeepEquals, []string{"PTAH_C64_TQ_1", "PTAH_C64_TQ_2"})
}

// TestOracleColumnCollationLive_RefusesAColumnNamedAfterAnAlternativeQuotedLiteral
// holds a generated column whose expression has a q-quoted literal with an
// apostrophe before the column's name: `q'[it's]' || a`. Read with the apostrophe
// ending the literal, the name is swallowed and the plan is not refused, the ADD
// commits and the MODIFY answers ORA-43922. The column is added by the plan in
// the first case and held by the table in the second, which is read from the
// catalog's own text of the expression.
func TestOracleColumnCollationLive_RefusesAColumnNamedAfterAnAlternativeQuotedLiteral(t *testing.T) {
	const expression = `q'[it's]' || a`
	tests := []struct {
		name  string
		setup []string
		want  []string
	}{
		{
			name:  "added by the plan",
			setup: []string{`CREATE TABLE ptah_c64_tm (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20 CHAR))`},
			want:  []string{"ID=-", "A=USING_NLS_COMP"},
		},
		{
			name: "held by the table",
			setup: []string{`CREATE TABLE ptah_c64_tm (id NUMBER(10) PRIMARY KEY, a VARCHAR2(20 CHAR),
				g VARCHAR2(100) GENERATED ALWAYS AS (` + expression + `) VIRTUAL)`},
			want: []string{"ID=-", "A=USING_NLS_COMP", "G=USING_NLS_COMP"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
			defer cancel()
			conn := openOraclePartialApply(c, ctx, []string{"ptah_c64_tm"}, tc.setup...)
			desired := oraclePartialSchema(nil, nil, oraclePartialTable{name: "ptah_c64_tm", fields: []schemamodel.Field{
				oracleID(),
				{Name: "a", Type: "VARCHAR2(20 CHAR)", Nullable: true, Collate: "BINARY_CI"},
				{Name: "g", Type: "VARCHAR2(100)", Nullable: true, GeneratedExpression: expression, GeneratedKind: "VIRTUAL"},
			}})

			_, err := oraclePartialPlan(ctx, conn, desired)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*collation of column "a" of table "ptah_c64_tm".*the generated column "[gG]".*uses the column.*`)
			c.Assert(oracleColumnsOf(c, ctx, conn, "ptah_c64_tm"), qt.DeepEquals, tc.want)
		})
	}
}
