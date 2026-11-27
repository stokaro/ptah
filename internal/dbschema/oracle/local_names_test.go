package oracle_test

import (
	"context"
	"database/sql/driver"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/synonym"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/oracle"
	"ptah.run/internal/planstyle"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// accountCatalog answers a read of the account APP holding a view of its own
// table, a view of another account's table, a function and a procedure, and
// hands every other read an empty result.
func accountCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_views"):
		return dbtest.QueryResult{Columns: []string{"VIEW_NAME", "TEXT_VC"}, Rows: [][]driver.Value{
			{"VV", "SELECT id FROM t"},
			{"VX", "SELECT id FROM crm.accounts"},
		}}, nil
	case strings.Contains(folded, "from all_procedures"):
		return dbtest.QueryResult{Columns: []string{"OBJECT_NAME", "OBJECT_TYPE", "AUTHID", "DETERMINISTIC"}, Rows: [][]driver.Value{
			{"RP", "PROCEDURE", "DEFINER", "NO"},
			{"RT", "FUNCTION", "DEFINER", "NO"},
		}}, nil
	case strings.Contains(folded, "from all_arguments"):
		return dbtest.QueryResult{Columns: []string{"OBJECT_NAME", "POSITION", "ARGUMENT_NAME", "DATA_TYPE", "IN_OUT"}, Rows: [][]driver.Value{
			{"RT", int64(0), " ", "NUMBER", "OUT"},
			{"RT", int64(1), "X", "NUMBER", "IN"},
		}}, nil
	case strings.Contains(folded, "from all_source"):
		return dbtest.QueryResult{Columns: []string{"NAME", "TYPE", "TEXT"}, Rows: [][]driver.Value{
			{"RP", "PROCEDURE", "PROCEDURE rp IS\n"},
			{"RP", "PROCEDURE", "BEGIN\n"},
			{"RP", "PROCEDURE", "  NULL;\n"},
			{"RP", "PROCEDURE", "END;\n"},
			{"RT", "FUNCTION", "FUNCTION rt(x NUMBER) RETURN NUMBER IS\n"},
			{"RT", "FUNCTION", "BEGIN\n"},
			{"RT", "FUNCTION", "  RETURN x * 2;\n"},
			{"RT", "FUNCTION", "END;\n"},
		}}, nil
	}
	return dbtest.QueryResult{Columns: []string{"unused"}}, nil
}

// readAccount reads the account APP from accountCatalog.
func readAccount(c *qt.C) *catalog.Database {
	c.Helper()
	db := dbtest.Open(c, accountCatalog)
	read, err := oracle.NewOracleReader(db.SQL, "APP").ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	return read
}

// TestReader_NamesNoSchemaOnAViewOrARoutine reads the account APP: its views
// and its routines carry no schema, since the account is the schema every
// object the read describes is in, and a view body keeps the schema it names
// (stokaro/ptah#6710).
func TestReader_NamesNoSchemaOnAViewOrARoutine(t *testing.T) {
	c := qt.New(t)

	read := readAccount(c)

	var got []string
	for _, view := range read.Views {
		got = append(got, "view "+view.QualifiedName()+": "+view.Body)
	}
	for _, routine := range read.Functions {
		got = append(got, "routine "+routine.QualifiedName())
	}
	c.Assert(got, qt.DeepEquals, []string{
		"view VV: SELECT id FROM t",
		"view VX: SELECT id FROM crm.accounts",
		"routine RP",
		"routine RT",
	})
}

// generatedFromRead plans, from a read of an account and a desired state, the
// migration and its rollback, the way `ptah migrations generate` plans a
// migration from a read, and returns their statements.
func generatedFromRead(c *qt.C, read *catalog.Database, desired *schemamodel.Database) (up, down []string) {
	c.Helper()
	ctx := context.Background()
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDialect(ctx, desired, read, platform.Oracle, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(ctx, generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: read, Dialect: platform.Oracle,
		Policy: generator.BidirectionalPlanPolicy{Statements: planstyle.Statements},
	})
	c.Assert(err, qt.IsNil)
	options := planext.Options{Capabilities: capability.ForDialect(platform.Oracle), Statements: planstyle.Statements}
	up, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, runtime, plan.Forward.Diff, platform.Oracle, options)
	c.Assert(err, qt.IsNil)
	down, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, runtime, plan.Reverse.Diff, platform.Oracle, options)
	c.Assert(err, qt.IsNil)
	return up, down
}

// desiredFromRead is the read as a desired state, the way a migration
// generated from a database declares it.
func desiredFromRead(c *qt.C, read *catalog.Database) *schemamodel.Database {
	c.Helper()
	desired, err := dbschematogo.ConvertDBSchemaToGoSchema(context.Background(), read, platform.Oracle, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	return desired
}

// TestGeneratedMigration_NamesNoAccountOfTheRead plans, from a read of the
// account APP, the migration that removes its views and routines, and the
// rollback that creates them again, the way `ptah migrations generate` plans a
// migration from a read. No statement names APP, so the file runs as the
// account that applies it: `DROP VIEW APP.VV` run as another account answers
// ORA-00942 or drops APP's view.
func TestGeneratedMigration_NamesNoAccountOfTheRead(t *testing.T) {
	c := qt.New(t)
	read := readAccount(c)
	desired := desiredFromRead(c, read)
	desired.Views, desired.Functions = nil, nil

	up, down := generatedFromRead(c, read, desired)

	c.Assert(up, qt.DeepEquals, []string{"DROP FUNCTION RT", "DROP PROCEDURE RP", "DROP VIEW VV", "DROP VIEW VX"})
	c.Assert(down, qt.DeepEquals, []string{
		"CREATE OR REPLACE FUNCTION RT(x in number) RETURN number AUTHID DEFINER IS\nBEGIN\n  RETURN x * 2;\nEND;",
		"CREATE OR REPLACE PROCEDURE RP AUTHID DEFINER IS\nBEGIN\n  NULL;\nEND;",
		"CREATE VIEW VV AS\nSELECT id FROM t",
		"CREATE VIEW VX AS\nSELECT id FROM crm.accounts",
	})
}

// accountTablesCatalog is accountCatalog with the tables T1 and T2 of APP, a
// foreign key from T2 into T1 and one into the table ACCOUNTS of the account
// CRM, an index on T2, a sequence, a materialized view, and a synonym for T1
// and one for CRM's table.
func accountTablesCatalog(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_tables t"):
		return dbtest.QueryResult{Columns: []string{"TABLE_NAME", "COMMENTS"}, Rows: [][]driver.Value{{"T1", " "}, {"T2", " "}}}, nil
	case strings.Contains(folded, "from all_tab_cols c") && strings.Contains(folded, "c.data_default"):
		return dbtest.QueryResult{
			Columns: []string{
				"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "VECTOR_INFO", "CHAR_LENGTH", "CHAR_USED",
				"DATA_PRECISION", "DATA_SCALE", "NULLABLE", "COLUMN_ID",
				"IDENTITY_COLUMN", "VIRTUAL_COLUMN", "DATA_DEFAULT", "COMMENTS", "GENERATION_TYPE",
				"COLLATION", "DEFAULT_COLLATION",
			},
			Rows: [][]driver.Value{
				{"T1", "ID", "NUMBER", nil, nil, nil, int64(10), int64(0), "N", int64(1), "NO", "NO", nil, nil, nil, nil, nil},
				{"T2", "ID", "NUMBER", nil, nil, nil, int64(10), int64(0), "N", int64(1), "NO", "NO", nil, nil, nil, nil, nil},
				{"T2", "T1_ID", "NUMBER", nil, nil, nil, int64(10), int64(0), "Y", int64(2), "NO", "NO", nil, nil, nil, nil, nil},
				{"T2", "CRM_ID", "NUMBER", nil, nil, nil, int64(10), int64(0), "Y", int64(3), "NO", "NO", nil, nil, nil, nil, nil},
			},
		}, nil
	case strings.Contains(folded, "nvl(c.r_owner"):
		return dbtest.QueryResult{
			Columns: []string{
				"CONSTRAINT_NAME", "CONSTRAINT_TYPE", "GENERATED", "TABLE_NAME", "SEARCH_CONDITION_VC", "R_OWNER",
				"R_CONSTRAINT_NAME", "DELETE_RULE", "DEFERRABLE", "DEFERRED", "STATUS", "VALIDATED", "COLUMN_NAME", "POSITION",
			},
			Rows: [][]driver.Value{
				{"T1_PK", "P", "USER NAME", "T1", " ", " ", " ", " ", "NOT DEFERRABLE", "IMMEDIATE", "ENABLED", "VALIDATED", "ID", int64(1)},
				{"T2_CRM_FK", "R", "USER NAME", "T2", " ", "CRM", "ACCOUNTS_PK", "NO ACTION", "NOT DEFERRABLE", "IMMEDIATE", "ENABLED", "VALIDATED", "CRM_ID", int64(1)},
				{"T2_PK", "P", "USER NAME", "T2", " ", " ", " ", " ", "NOT DEFERRABLE", "IMMEDIATE", "ENABLED", "VALIDATED", "ID", int64(1)},
				{"T2_T1_FK", "R", "USER NAME", "T2", " ", "APP", "T1_PK", "NO ACTION", "NOT DEFERRABLE", "IMMEDIATE", "ENABLED", "VALIDATED", "T1_ID", int64(1)},
			},
		}, nil
	case strings.Contains(folded, "select c.owner, c.constraint_name"):
		return dbtest.QueryResult{Columns: []string{"OWNER", "CONSTRAINT_NAME", "TABLE_NAME", "COLUMN_NAME"}, Rows: [][]driver.Value{
			{"APP", "T1_PK", "T1", "ID"},
			{"APP", "T2_PK", "T2", "ID"},
			{"CRM", "ACCOUNTS_PK", "ACCOUNTS", "ID"},
		}}, nil
	case strings.Contains(folded, "from all_indexes i"):
		return dbtest.QueryResult{Columns: []string{"INDEX_NAME", "TABLE_NAME", "UNIQUENESS", "INDEX_TYPE"}, Rows: [][]driver.Value{
			{"T2_NOTE", "T2", "NONUNIQUE", "NORMAL"},
		}}, nil
	case strings.Contains(folded, "from all_ind_columns ic"):
		return dbtest.QueryResult{Columns: []string{"INDEX_NAME", "TABLE_NAME", "COLUMN_NAME", "DESCEND", "COLUMN_POSITION", "COLUMN_EXPRESSION"}, Rows: [][]driver.Value{
			{"T2_NOTE", "T2", "T1_ID", "ASC", int64(1), nil},
		}}, nil
	case strings.Contains(folded, "from all_sequences s"):
		return dbtest.QueryResult{
			Columns: []string{"SEQUENCE_NAME", "MIN_VALUE", "MAX_VALUE", "INCREMENT_BY", "CYCLE_FLAG", "CACHE_SIZE", "LAST_NUMBER"},
			Rows:    [][]driver.Value{{"S1", "1", "9999999999", "1", "N", "20", "5"}},
		}, nil
	case strings.Contains(folded, "from all_mviews m\nwhere m.owner"):
		return dbtest.QueryResult{Columns: []string{"MVIEW_NAME", "QUERY"}, Rows: [][]driver.Value{{"MV1", "SELECT id FROM t1"}}}, nil
	case strings.Contains(folded, "from all_synonyms s"):
		return dbtest.QueryResult{Columns: []string{"SYNONYM_NAME", "TABLE_OWNER", "TABLE_NAME", "DB_LINK"}, Rows: [][]driver.Value{
			{"SYN_CRM", "CRM", "ACCOUNTS", nil},
			{"SYN_T1", "APP", "T1", nil},
		}}, nil
	}
	return accountCatalog(query, args)
}

// readAccountTables reads the account APP from accountTablesCatalog.
func readAccountTables(c *qt.C) *catalog.Database {
	c.Helper()
	db := dbtest.Open(c, accountTablesCatalog)
	read, err := oracle.NewOracleReader(db.SQL, "APP").ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	return read
}

// accountObjectNames lists the tables, constraints, indexes, sequences,
// materialized views and synonyms of a read, each with the schema it names,
// and a foreign key with the table it references.
func accountObjectNames(c *qt.C, read *catalog.Database) []string {
	c.Helper()
	var got []string
	for _, table := range read.Tables {
		got = append(got, "table "+table.QualifiedName())
	}
	for _, constraint := range read.Constraints {
		line := "constraint " + constraint.Schema + "/" + constraint.TableName + "." + constraint.Name
		if constraint.ForeignTable != nil {
			line += " references " + constraint.ForeignSchema + "/" + *constraint.ForeignTable
		}
		got = append(got, line)
	}
	for _, index := range read.Indexes {
		got = append(got, "index "+index.Schema+"/"+index.TableName+"."+index.Name)
	}
	for _, sequence := range read.Sequences {
		got = append(got, "sequence "+sequence.QualifiedName())
	}
	for _, view := range read.MatViews {
		got = append(got, "materialized view "+view.QualifiedName())
	}
	objects, err := read.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	for _, object := range objects {
		observed, ok := object.Value.(*synonym.ObservedSynonym)
		c.Assert(ok, qt.IsTrue)
		got = append(got, "synonym "+observed.QualifiedName()+" for "+observed.Target)
	}
	return got
}

// TestReader_NamesNoSchemaOnTheAccountsObjects reads the account APP: none of
// its tables, constraints, indexes, sequences, materialized views and
// synonyms carries the account, and neither does a foreign key into its own
// table or a synonym for one. A foreign key into a table of CRM and a synonym
// for one keep CRM, since only the name tells that table from one of APP's
// (stokaro/ptah#6710).
func TestReader_NamesNoSchemaOnTheAccountsObjects(t *testing.T) {
	c := qt.New(t)

	read := readAccountTables(c)

	c.Assert(accountObjectNames(c, read), qt.DeepEquals, []string{
		"table T1",
		"table T2",
		"constraint /T1.T1_PK",
		"constraint /T2.T2_CRM_FK references CRM/ACCOUNTS",
		"constraint /T2.T2_PK",
		"constraint /T2.T2_T1_FK references /T1",
		"index /T2.T2_NOTE",
		"sequence S1",
		"materialized view MV1",
		"synonym SYN_CRM for CRM.ACCOUNTS",
		"synonym SYN_T1 for T1",
	})
}

// TestGeneratedMigration_NamesNoAccountOnTheAccountsObjects plans, from a read
// of the account APP, the migration that removes every object it holds, and
// the rollback that creates each again. No statement names APP: run as
// another account, `DROP TABLE APP.T2 PURGE` answers ORA-00942 or drops APP's
// table, and `REFERENCES APP.T1` creates a key into it. The foreign key into
// CRM's table and the synonym for it name CRM.
func TestGeneratedMigration_NamesNoAccountOnTheAccountsObjects(t *testing.T) {
	c := qt.New(t)
	read := readAccountTables(c)
	desired := desiredFromRead(c, read)
	desired.Tables, desired.Fields, desired.Indexes, desired.Constraints = nil, nil, nil, nil
	desired.Sequences, desired.MaterializedViews, desired.Views, desired.Functions = nil, nil, nil, nil
	desired.FeatureObjects = schemaext.Objects{}

	up, down := generatedFromRead(c, read, desired)

	c.Assert(up, qt.DeepEquals, []string{
		"DROP FUNCTION RT",
		"DROP PROCEDURE RP",
		"ALTER TABLE T2 DROP CONSTRAINT T2_T1_FK",
		"DROP SYNONYM IF EXISTS SYN_CRM",
		"DROP SYNONYM IF EXISTS SYN_T1",
		"ALTER TABLE T2 DROP CONSTRAINT T2_CRM_FK",
		"DROP MATERIALIZED VIEW IF EXISTS MV1",
		"DROP VIEW IF EXISTS VV",
		"DROP VIEW IF EXISTS VX",
		"DROP INDEX IF EXISTS T2_NOTE",
		"DROP TABLE IF EXISTS T1 PURGE",
		"DROP TABLE IF EXISTS T2 PURGE",
		"DROP SEQUENCE IF EXISTS S1",
	})
	c.Assert(down, qt.DeepEquals, []string{
		"CREATE SEQUENCE S1 START WITH 5 INCREMENT BY 1 MINVALUE 1 MAXVALUE 9999999999 CACHE 20",
		"CREATE OR REPLACE FUNCTION RT(x in number) RETURN number AUTHID DEFINER IS\nBEGIN\n  RETURN x * 2;\nEND;",
		"CREATE OR REPLACE PROCEDURE RP AUTHID DEFINER IS\nBEGIN\n  NULL;\nEND;",
		"CREATE TABLE T1 (\n  ID NUMBER(10) NOT NULL PRIMARY KEY\n)",
		"CREATE TABLE T2 (\n  ID NUMBER(10) NOT NULL PRIMARY KEY,\n  T1_ID NUMBER(10),\n  CRM_ID NUMBER(10)\n)",
		"CREATE VIEW VV AS\nSELECT id FROM t",
		"CREATE VIEW VX AS\nSELECT id FROM crm.accounts",
		"CREATE MATERIALIZED VIEW MV1 AS\nSELECT id FROM t1",
		"CREATE SYNONYM SYN_CRM FOR CRM.ACCOUNTS",
		"CREATE SYNONYM SYN_T1 FOR T1",
		"CREATE INDEX T2_NOTE ON T2 (T1_ID)",
		"ALTER TABLE T2 ADD CONSTRAINT T2_T1_FK FOREIGN KEY (T1_ID) REFERENCES T1 (ID)",
		"ALTER TABLE T2 ADD CONSTRAINT T2_CRM_FK FOREIGN KEY (CRM_ID) REFERENCES CRM.ACCOUNTS (ID)",
	})
}

// TestRenderedRead_KeepsAKeyIntoAnotherAccount renders the read of APP as
// `ptah schema inspect --format sql` writes it. The key into CRM's table names
// CRM, and a table of an account the read does not describe is not one the
// render can refuse as unknown: Oracle accepts the key from an account
// granted REFERENCES on that table, and reads it back with CRM's name.
func TestRenderedRead_KeepsAKeyIntoAnotherAccount(t *testing.T) {
	c := qt.New(t)
	read := readAccountTables(c)
	desired := desiredFromRead(c, read)

	result, err := renderer.RenderSchema(t.Context(), must.Must(builtin.New()), renderer.SchemaRequest{
		Target: platform.Oracle, Schema: desired, Capabilities: capability.ForDialect(platform.Oracle),
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Statements, qt.Contains,
		"ALTER TABLE T2 ADD CONSTRAINT T2_CRM_FK FOREIGN KEY (CRM_ID) REFERENCES CRM.ACCOUNTS (ID);\n")
}

// accountExpressionsCatalog is accountTablesCatalog with T2 holding a column
// defaulted from APP's sequence, one defaulted from CRM's, a virtual column
// calling APP's function and one calling CRM's, and T2_NOTE keyed on APP's
// function, each as Oracle stores it: qualified with the owner whether or not
// the statement named one.
func accountExpressionsCatalog(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_tab_cols c") && strings.Contains(folded, "c.data_default"):
		return dbtest.QueryResult{
			Columns: []string{
				"TABLE_NAME", "COLUMN_NAME", "DATA_TYPE", "VECTOR_INFO", "CHAR_LENGTH", "CHAR_USED",
				"DATA_PRECISION", "DATA_SCALE", "NULLABLE", "COLUMN_ID",
				"IDENTITY_COLUMN", "VIRTUAL_COLUMN", "DATA_DEFAULT", "COMMENTS", "GENERATION_TYPE",
				"COLLATION", "DEFAULT_COLLATION",
			},
			Rows: [][]driver.Value{
				{"T1", "ID", "NUMBER", nil, nil, nil, int64(10), int64(0), "N", int64(1), "NO", "NO", nil, nil, nil, nil, nil},
				{"T2", "ID", "NUMBER", nil, nil, nil, int64(10), int64(0), "N", int64(1), "NO", "NO", nil, nil, nil, nil, nil},
				{"T2", "K", "NUMBER", nil, nil, nil, nil, nil, "Y", int64(2), "NO", "NO", `"APP"."S1"."NEXTVAL"`, nil, nil, nil, nil},
				{"T2", "KX", "NUMBER", nil, nil, nil, nil, nil, "Y", int64(3), "NO", "NO", `"CRM"."S9"."NEXTVAL"`, nil, nil, nil, nil},
				{"T2", "V", "NUMBER", nil, nil, nil, nil, nil, "Y", int64(4), "NO", "YES", `"APP"."DBL"("ID")`, nil, nil, nil, nil},
				{"T2", "VX", "NUMBER", nil, nil, nil, nil, nil, "Y", int64(5), "NO", "YES", `"CRM"."DBL"("ID")`, nil, nil, nil, nil},
			},
		}, nil
	case strings.Contains(folded, "from all_ind_columns ic"):
		return dbtest.QueryResult{Columns: []string{"INDEX_NAME", "TABLE_NAME", "COLUMN_NAME", "DESCEND", "COLUMN_POSITION", "COLUMN_EXPRESSION"}, Rows: [][]driver.Value{
			{"T2_NOTE", "T2", "SYS_NC00006$", "ASC", int64(1), `"APP"."DBL"("ID")`},
		}}, nil
	}
	return accountTablesCatalog(query, args)
}

// readExpressions lists the column defaults, virtual columns and index key
// expressions of a read.
func readExpressions(read *catalog.Database) []string {
	var got []string
	for _, table := range read.Tables {
		for _, column := range table.Columns {
			if column.GeneratedExpression != nil {
				got = append(got, table.Name+"."+column.Name+" AS "+*column.GeneratedExpression)
			}
			if column.ColumnDefault != nil {
				got = append(got, table.Name+"."+column.Name+" DEFAULT "+*column.ColumnDefault)
			}
		}
	}
	for _, index := range read.Indexes {
		for _, part := range index.Parts {
			got = append(got, index.Name+" ON "+part.Expr)
		}
	}
	return got
}

// TestReader_NamesNoAccountInAnExpression reads the account APP: a default
// reading APP's sequence, a virtual column calling APP's function and an
// index keyed on it name no account, so a migration written from the read
// draws from and calls the objects of the account that runs it. The default
// and the virtual column reading CRM's keep CRM (stokaro/ptah#6710).
func TestReader_NamesNoAccountInAnExpression(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(c, accountExpressionsCatalog)

	read, err := oracle.NewOracleReader(db.SQL, "APP").ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(readExpressions(read), qt.DeepEquals, []string{
		`T2.K DEFAULT "S1"."NEXTVAL"`,
		`T2.KX DEFAULT "CRM"."S9"."NEXTVAL"`,
		`T2.V AS "DBL"("ID")`,
		`T2.VX AS "CRM"."DBL"("ID")`,
		`T2_NOTE ON "DBL"("ID")`,
	})
}
