package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

// PostgreSQL's ALTER INDEX ... RENAME TO renders as written, the index quoted
// and qualified as the statement names it.
func TestRenderSQL_AlterIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.AlterIndexNode
		want string
	}{
		{
			name: "a bare name",
			node: &ast.AlterIndexNode{Name: "ix", NewName: "other"},
			want: `ALTER INDEX "ix" RENAME TO "other";` + "\n",
		},
		{
			name: "IF EXISTS and a schema",
			node: &ast.AlterIndexNode{Name: "app.ix", IfExists: true, NewName: "other"},
			want: `ALTER INDEX IF EXISTS "app"."ix" RENAME TO "other";` + "\n",
		},
		{
			name: "names written quoted",
			node: &ast.AlterIndexNode{Name: `"App"."Ix"`, NewName: `"Other"`},
			want: `ALTER INDEX "App"."Ix" RENAME TO "Other";` + "\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(platform.Postgres, test.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, test.want)
		})
	}
}

// Only PostgreSQL's reading of the statement is measured, so every other
// target refuses the node rather than writes SQL nothing has checked. The
// MySQL family renames an index with ALTER TABLE, which needs the table the
// node does not name.
func TestRenderSQL_AlterIndex_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		wantErr string
	}{
		{dialect: platform.CockroachDB, wantErr: `unsupported feature: cockroachdb: ALTER INDEX ix RENAME TO other is written for PostgreSQL only`},
		{dialect: platform.YugabyteDB, wantErr: `unsupported feature: yugabytedb: ALTER INDEX ix RENAME TO other is written for PostgreSQL only`},
		{dialect: platform.Spanner, wantErr: `unsupported feature: spanner: ALTER INDEX ix RENAME TO other is written for PostgreSQL only`},
		{dialect: platform.MySQL, wantErr: `unsupported feature: mysql: ALTER INDEX ix RENAME TO other is PostgreSQL's statement, and this renderer does not write it`},
		{dialect: platform.MariaDB, wantErr: `unsupported feature: mariadb: ALTER INDEX ix RENAME TO other is PostgreSQL's statement, .*`},
		{dialect: platform.SQLite, wantErr: `unsupported feature: sqlite: ALTER INDEX ix RENAME TO other is PostgreSQL's statement, .*`},
		{dialect: platform.SQLServer, wantErr: `unsupported feature: sqlserver: ALTER INDEX ix RENAME TO other is PostgreSQL's statement, .*`},
		{dialect: platform.Oracle, wantErr: `unsupported feature: oracle: ALTER INDEX ix RENAME TO other is PostgreSQL's statement, .*`},
		{dialect: platform.ClickHouse, wantErr: `unsupported feature: clickhouse: ALTER INDEX ix RENAME TO other is PostgreSQL's statement, .*`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQL(test.dialect, &ast.AlterIndexNode{Name: "ix", NewName: "other"})

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
