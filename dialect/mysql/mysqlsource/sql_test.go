package mysqlsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/internal/parser"
)

// parseMySQL parses sql in dialect with the MySQL owner's column options.
func parseMySQL(dialect, sql string) (*ast.StatementList, error) {
	return parser.NewParser(sql, parser.WithDialect(dialect), parser.WithSQL(must.Must(sqlext.NewSet(mysqlsource.SQL())))).Parse()
}

// columnSettings answers the settings of each column of a statement's table.
func columnSettings(c *qt.C, node ast.Node) []mysqlschema.ColumnSettings {
	c.Helper()
	var columns []*ast.ColumnNode
	switch typed := node.(type) {
	case *ast.CreateTableNode:
		columns = typed.Columns
	case *ast.AlterTableNode:
		for _, operation := range typed.Operations {
			columns = append(columns, operation.(*ast.ModifyColumnOperation).Column)
		}
	}
	settings := make([]mysqlschema.ColumnSettings, 0, len(columns))
	for _, column := range columns {
		value, _, err := mysqlschema.Settings(column.Facets)
		c.Assert(err, qt.IsNil)
		settings = append(settings, value)
	}
	return settings
}

// A column's character set and ON UPDATE clause are read into one facet,
// whatever order they come in and whatever host clauses stand between them;
// a clause written twice keeps the last, as MySQL does; and each column keeps
// its own.
func TestSQL_ReadsColumnSettings(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		want    []mysqlschema.ColumnSettings
	}{
		{name: "both, around host clauses", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (ts timestamp CHARSET utf8mb4 NOT NULL ON UPDATE CURRENT_TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP COMMENT 'x');",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP(6)"}}},
		{name: "the last of two", dialect: platform.MariaDB,
			sql:  "CREATE TABLE t (v varchar(10) CHARACTER SET latin1 COLLATE latin1_bin CHARSET utf8mb4);",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4"}}},
		{name: "two options side by side", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (b timestamp CHARSET latin1 ON UPDATE NOW() CHARACTER SET utf8mb4);",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4", OnUpdate: "NOW()"}}},
		{name: "each column its own", dialect: platform.MySQL,
			sql:  "CREATE TABLE t (a varchar(10) CHARSET latin1, b timestamp ON UPDATE NOW(), c int);",
			want: []mysqlschema.ColumnSettings{{Charset: "latin1"}, {OnUpdate: "NOW()"}, {}}},
		{name: "a modified column", dialect: platform.MySQL,
			sql:  "ALTER TABLE t MODIFY COLUMN ts timestamp ON UPDATE CURRENT_TIMESTAMP NOT NULL;",
			want: []mysqlschema.ColumnSettings{{OnUpdate: "CURRENT_TIMESTAMP"}}},
		{name: "a document that names no target", dialect: "",
			sql:  "CREATE TABLE t (v varchar(10) character set utf8mb4 not null);",
			want: []mysqlschema.ColumnSettings{{Charset: "utf8mb4"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL(test.dialect, test.sql)

			c.Assert(err, qt.IsNil)
			c.Assert(columnSettings(c, statements.Statements[0]), qt.DeepEquals, test.want)
		})
	}
}

// A malformed option is refused at its position, an option of another
// dialect's document is no column attribute the grammar reads, and a CHARACTER
// without SET opens no option.
func TestSQL_RefusesAMalformedOption(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{name: "no character set name", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARACTER SET 'utf8');",
			wantErr: `line 1, column 38: expected a character set name after CHARACTER SET`},
		{name: "no charset name", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARSET 'utf8');",
			wantErr: `line 1, column 32: expected charset name: expected identifier, got String`},
		{name: "words after the name", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARSET utf8 BINARY);",
			wantErr: `line 1, column 37: unexpected "BINARY" after the character set`},
		{name: "no expression", dialect: platform.MySQL, sql: "CREATE TABLE t (ts timestamp ON UPDATE NOT NULL);",
			wantErr: `line 1, column 40: expected an expression after ON UPDATE`},
		{name: "CHARACTER without SET", dialect: platform.MySQL, sql: "CREATE TABLE t (v text CHARACTER utf8);",
			wantErr: `line 1, column 24: unsupported column attribute: CHARACTER`},
		{name: "another dialect", dialect: platform.Postgres, sql: "CREATE TABLE t (v text CHARSET utf8);",
			wantErr: `line 1, column 24: unsupported column attribute: CHARSET`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL(test.dialect, test.sql)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// An option ends where the next clause of the column begins, whichever
// clause the grammar reads there, so the clause is read by the grammar and
// not refused as part of the option.
func TestSQL_AnOptionEndsAtTheNextClause(t *testing.T) {
	for _, clause := range []string{
		"NOT NULL", "NULL", "PRIMARY KEY", "UNIQUE", "AUTO_INCREMENT", "DEFAULT 'x'", "CHECK (v <> '')",
		"CONSTRAINT v_set CHECK (v <> '')", "REFERENCES p (v)", "COLLATE utf8mb4_bin", "COMMENT 'x'",
		"AS (concat(v, 'x'))", "GENERATED ALWAYS AS (concat(v, 'x'))",
	} {
		t.Run(clause, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL("", "CREATE TABLE t (v varchar(10) CHARSET utf8mb4 "+clause+");")

			c.Assert(err, qt.IsNil)
			c.Assert(columnSettings(c, statements.Statements[0]), qt.DeepEquals, []mysqlschema.ColumnSettings{{Charset: "utf8mb4"}})
		})
	}
}

// The clauses the grammar reads only after a constraint end an option too, so
// the grammar, and not the owner, answers one written after an option.
func TestSQL_AnOptionEndsAtAClauseOfAConstraint(t *testing.T) {
	for _, test := range []struct {
		clause  string
		wantErr string
	}{
		{clause: "ENFORCED", wantErr: `line 1, column 47: misplaced ENFORCED clause: .*`},
		{clause: "DEFERRABLE", wantErr: `line 1, column 47: misplaced deferral clause: .*`},
		{clause: "INITIALLY DEFERRED", wantErr: `line 1, column 47: misplaced deferral clause: .*`},
	} {
		t.Run(test.clause, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL("", "CREATE TABLE t (v varchar(10) CHARSET utf8mb4 "+test.clause+");")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A MySQL or MariaDB document's PARTITION BY is claimed by the owner and
// refused by name, since Ptah does not model MySQL partitioning. Read as the
// grammar every target shares, it would land among the table options as
// PARTITION_BY and render as PARTITION_BY=, which no server parses.
func TestSQL_RefusesPartitionBy(t *testing.T) {
	const sql = "CREATE TABLE sales (id int NOT NULL, region varchar(10) NOT NULL, PRIMARY KEY (id, region)) " +
		"PARTITION BY LIST COLUMNS(region) (PARTITION p_east VALUES IN ('east'), PARTITION p_west VALUES IN ('west'));"
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := parseMySQL(dialect, sql)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `.*PARTITION BY on table sales: Ptah does not model MySQL or MariaDB partitioning \(stokaro/ptah#6357\).*`)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// An ALTER TABLE that opens with an operation on the table's partitions is
// refused with the same message, naming the operation and the table as the
// statement writes it.
func TestSQL_RefusesAnAlterTableOfPartitions(t *testing.T) {
	const create = "CREATE TABLE app.sales (id int NOT NULL, PRIMARY KEY (id));\n"
	rows := []struct{ name, alter, wantErr string }{
		{name: "PARTITION BY", alter: "ALTER TABLE app.sales PARTITION BY HASH (id) PARTITIONS 2;", wantErr: `PARTITION BY on table app\.sales`},
		{name: "REMOVE PARTITIONING", alter: "ALTER TABLE app.sales REMOVE PARTITIONING;", wantErr: `REMOVE PARTITIONING on table app\.sales`},
		{name: "ADD PARTITION", alter: "ALTER TABLE app.sales ADD PARTITION (PARTITION p2);", wantErr: `ADD PARTITION on table app\.sales`},
		{name: "DROP PARTITION", alter: "ALTER TABLE `app`.`sales` DROP PARTITION p0;", wantErr: "DROP PARTITION on table `app`\\.`sales`"},
		{name: "a maintenance operation", alter: "ALTER TABLE app.sales OPTIMIZE PARTITION p0;", wantErr: `OPTIMIZE PARTITION on table app\.sales`},
		{name: "CONVERT TABLE", alter: "ALTER TABLE app.sales CONVERT TABLE old TO PARTITION p2 VALUES LESS THAN (40);", wantErr: `CONVERT TABLE on table app\.sales`},
	}
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, row := range rows {
			t.Run(dialect+"/"+row.name, func(t *testing.T) {
				c := qt.New(t)

				statements, err := parseMySQL(dialect, create+row.alter)

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches, `.*`+row.wantErr+`: Ptah does not model MySQL or MariaDB partitioning \(stokaro/ptah#6357\).*`)
				c.Assert(statements, qt.IsNil)
			})
		}
	}
}

// A document that names no target is read with every owner's syntax that is
// not bound to a target, and the PARTITION BY of a ClickHouse table is one, so
// the owner's refusal is bound to MySQL and MariaDB documents and leaves this
// one to the shared grammar.
func TestSQL_LeavesPartitionByOfADocumentWithoutATarget(t *testing.T) {
	c := qt.New(t)

	statements, err := parseMySQL("", "CREATE TABLE hits (d Date) ENGINE = MergeTree PARTITION BY toYYYYMM(d) ORDER BY d;")

	c.Assert(err, qt.IsNil)
	c.Assert(statements.Statements, qt.HasLen, 1)
	c.Assert(statements.Statements[0].(*ast.CreateTableNode).Options["PARTITION_BY"], qt.Equals, "toYYYYMM(d)")
}
