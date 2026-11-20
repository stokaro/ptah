//go:build integration

package integration_test

import (
	"context"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlasschema"
)

// A column collation against Oracle, where the server decides what a name
// compares as, which database accepts one, and which column may change it.
//
// Oracle takes a column COLLATE only with MAX_STRING_SIZE=EXTENDED, which
// the Oracle service of go-integration-tests.yml converts to before the suite
// runs. These tests drive the plan `ptah schema apply` prepares into the
// server and read the catalog and the rows back, because a plan that reads
// correctly says nothing about whether the server kept the collation or
// compares the way the collation says.

const (
	oracleCollationTable = "ptah_col_coll"
	oracleCollationIndex = "ptah_col_coll_code_idx"
)

// oracleCollationIndexes is the index over the column that declares a
// collation. Each call returns a slice of its own, because finalizing a
// schema writes into it.
func oracleCollationIndexes() []schemamodel.Index {
	return []schemamodel.Index{{StructName: "Name", Name: oracleCollationIndex, Fields: []string{"code"}}}
}

// oracleCollationSchema is one table with a plain column and a column that
// declares collation, holding the indexes given.
func oracleCollationSchema(collation string, indexes []schemamodel.Index) *schemamodel.Database {
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Name", Name: oracleCollationTable}},
		Fields: []schemamodel.Field{
			{StructName: "Name", FieldName: "ID", Name: "id", Type: "NUMBER(10)", Primary: true},
			{StructName: "Name", FieldName: "Code", Name: "code", Type: "VARCHAR2(20)", Nullable: true, Collate: collation},
			{StructName: "Name", FieldName: "Plain", Name: "plain", Type: "VARCHAR2(20)", Nullable: true},
		},
		Indexes: indexes,
	}
	schemamodel.Finalize(db)
	return db
}

const oracleCollationsRead = `SELECT column_name || '=' || NVL(collation, '-') FROM user_tab_columns
	WHERE table_name = 'PTAH_COL_COLL' ORDER BY column_id`

// TestOracleColumnCollationLive_HappyPath declares a collation, applies it,
// reads it back, shows what it does to a comparison, and plans nothing on the
// next run.
//
// The row is 'Abc'. Under BINARY_CI the column equals 'ABC', and the column
// that declared none does not, so the second count is the control that the
// first match came from the collation and not from the session.
func TestOracleColumnCollationLive_HappyPath(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleCollationTable)
	desired := oracleCollationSchema("BINARY_CI", nil)

	statements := oracleDeclaredRowsPlan(c, ctx, conn, desired, oracleCollationTable)
	c.Assert(statements[0], qt.Contains, "code VARCHAR2(20) COLLATE BINARY_CI")
	applyOracleDeclaredRows(c, ctx, conn, desired, oracleCollationTable)

	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleCollationsRead), qt.DeepEquals,
		[]string{"ID=-", "CODE=BINARY_CI", "PLAIN=USING_NLS_COMP"})

	_, err := conn.ExecContext(ctx, `INSERT INTO ptah_col_coll (id, code, plain) VALUES (1, 'Abc', 'Abc')`)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, "SELECT COUNT(*) FROM ptah_col_coll WHERE code = 'ABC'"),
		qt.DeepEquals, []string{"1"})
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, "SELECT COUNT(*) FROM ptah_col_coll WHERE plain = 'ABC'"),
		qt.DeepEquals, []string{"0"})

	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, desired, oracleCollationTable), qt.HasLen, 0)
}

// TestOracleColumnCollationLive_ChangeInPlace plans a change of the collation
// as one MODIFY, applies it, and reads the comparison change back: the column
// that matched 'ABC' under BINARY_CI does not under BINARY. A declaration that
// goes back to USING_NLS_COMP is planned too, since a column that declares
// none is not compared and so only the name changes it.
func TestOracleColumnCollationLive_ChangeInPlace(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleCollationTable)
	applyOracleDeclaredRows(c, ctx, conn, oracleCollationSchema("BINARY_CI", nil), oracleCollationTable)
	_, err := conn.ExecContext(ctx, `INSERT INTO ptah_col_coll (id, code) VALUES (1, 'Abc')`)
	c.Assert(err, qt.IsNil)

	toBinary := oracleCollationSchema("BINARY", nil)
	statements := oracleDeclaredRowsPlan(c, ctx, conn, toBinary, oracleCollationTable)
	c.Assert(statements[len(statements)-1], qt.Equals, "ALTER TABLE ptah_col_coll MODIFY (code VARCHAR2(20) COLLATE BINARY)")
	applyOracleDeclaredRows(c, ctx, conn, toBinary, oracleCollationTable)

	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleCollationsRead), qt.DeepEquals,
		[]string{"ID=-", "CODE=BINARY", "PLAIN=USING_NLS_COMP"})
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, "SELECT COUNT(*) FROM ptah_col_coll WHERE code = 'ABC'"),
		qt.DeepEquals, []string{"0"})
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, toBinary, oracleCollationTable), qt.HasLen, 0)

	toInherited := oracleCollationSchema("USING_NLS_COMP", nil)
	applyOracleDeclaredRows(c, ctx, conn, toInherited, oracleCollationTable)
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleCollationsRead), qt.DeepEquals,
		[]string{"ID=-", "CODE=USING_NLS_COMP", "PLAIN=USING_NLS_COMP"})
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, toInherited, oracleCollationTable), qt.HasLen, 0)
}

// TestOracleColumnCollationLive_RefusesAnIndexedColumn is the failure path of
// the change above. Oracle answers ORA-43923 to a MODIFY of a collation on a
// column an index uses, and commits each statement of a plan as it runs, so the
// plan has to stop before its first statement. The assertion that nothing ran
// reads the collation back. The index is read first: it matches its
// declaration, so the refusal is the only thing the plan holds.
func TestOracleColumnCollationLive_RefusesAnIndexedColumn(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleCollationTable)
	indexed := oracleCollationSchema("BINARY_CI", oracleCollationIndexes())
	applyOracleDeclaredRows(c, ctx, conn, indexed, oracleCollationTable)
	// An index over a column with a collation is a function-based index whose
	// key the catalog lists under a hidden column, SYS_NC00004$ here. The
	// plan that finds nothing to do is the read naming the column the
	// declaration names.
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, indexed, oracleCollationTable), qt.HasLen, 0)

	_, err := atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{
		Desired: oracleCollationSchema("BINARY", oracleCollationIndexes()), Runtime: must.Must(builtin.New()),
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*collation of column "code" of table "ptah_col_coll".*index "PTAH_COL_COLL_CODE_IDX" uses the column.*`)
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleCollationsRead), qt.DeepEquals,
		[]string{"ID=-", "CODE=BINARY_CI", "PLAIN=USING_NLS_COMP"})
}

// TestOracleColumnCollationLive_ReadsTheStringSizeOfTheServer holds the
// connection's capability to the server's own answer, whichever setting the
// server runs: StandardStringSize is what the connector reads from
// DATABASE_PROPERTIES, and the render of a collation refuses by name on exactly
// that setting.
func TestOracleColumnCollationLive_ReadsTheStringSizeOfTheServer(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleCollationTable)

	size := oracleDeclaredRowValues(c, ctx, conn,
		`SELECT property_value FROM database_properties WHERE property_name = 'MAX_STRING_SIZE'`)

	c.Assert(size, qt.HasLen, 1)
	c.Assert(conn.Info().Capabilities.Has(capability.StandardStringSize), qt.Equals, size[0] == "STANDARD")
}

// TestOracleColumnCollationLive_TableDefaultCollation holds a column to its
// table's DEFAULT COLLATION, a table Ptah did not write. A column without a
// COLLATE of its own reads the table's collation, so a declaration that
// restates it holds, and the plan is empty. A declaration of USING_NLS_COMP on
// such a column is a change: the column then names the collation itself.
func TestOracleColumnCollationLive_TableDefaultCollation(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleCollationTable)
	_, err := conn.ExecContext(ctx, `CREATE TABLE ptah_col_coll (
	  id NUMBER(10) PRIMARY KEY, code VARCHAR2(20), plain VARCHAR2(20)) DEFAULT COLLATION BINARY_CI`)
	c.Assert(err, qt.IsNil)
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleCollationsRead), qt.DeepEquals,
		[]string{"ID=-", "CODE=BINARY_CI", "PLAIN=BINARY_CI"})

	restated := oracleCollationSchema("BINARY_CI", nil)
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, restated, oracleCollationTable), qt.HasLen, 0)

	toInherited := oracleCollationSchema("USING_NLS_COMP", nil)
	statements := oracleDeclaredRowsPlan(c, ctx, conn, toInherited, oracleCollationTable)
	c.Assert(statements[len(statements)-1], qt.Equals,
		"ALTER TABLE ptah_col_coll MODIFY (code VARCHAR2(20) COLLATE USING_NLS_COMP)")
	applyOracleDeclaredRows(c, ctx, conn, toInherited, oracleCollationTable)

	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleCollationsRead), qt.DeepEquals,
		[]string{"ID=-", "CODE=USING_NLS_COMP", "PLAIN=BINARY_CI"})
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, toInherited, oracleCollationTable), qt.HasLen, 0)
	c.Assert(oracleDeclaredRowsPlan(c, ctx, conn, restated, oracleCollationTable), qt.HasLen, 1)
}

// TestOracleColumnCollationLive_RefusesAFunctionBasedIndex is the failure path
// for an index that is not the one Oracle builds for a collated column. An
// index over UPPER(code) answers ORA-43922 to the change, and the plan stops
// before its first statement, so the table is as it was.
func TestOracleColumnCollationLive_RefusesAFunctionBasedIndex(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()

	conn := openOracleDeclaredRows(c, ctx, oracleCollationTable)
	applyOracleDeclaredRows(c, ctx, conn, oracleCollationSchema("", nil), oracleCollationTable)
	_, err := conn.ExecContext(ctx, `CREATE INDEX ptah_col_coll_upper_idx ON ptah_col_coll (UPPER(code))`)
	c.Assert(err, qt.IsNil)

	_, err = atlasschema.PlanApply(ctx, conn, atlasschema.ApplyOptions{
		Desired: oracleCollationSchema("BINARY_CI", nil), Runtime: must.Must(builtin.New()),
	})

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*collation of column "code" of table "ptah_col_coll".*index "PTAH_COL_COLL_UPPER_IDX" uses the column.*`)
	c.Assert(oracleDeclaredRowValues(c, ctx, conn, oracleCollationsRead), qt.DeepEquals,
		[]string{"ID=-", "CODE=USING_NLS_COMP", "PLAIN=USING_NLS_COMP"})
}
