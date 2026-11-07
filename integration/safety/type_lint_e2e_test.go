//go:build integration

package safety_test

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/clirun"
)

// TestLintE2E_AGeneratedTypeChangeIsReported plans a type change on a real server, writes
// the migration through the shipped binary, and lints the file it wrote. A file holds no
// previous type, so lint says only that a type changes, and it says so for a narrowing and
// a widening alike. What matters is that the statement Ptah itself writes is one the rule
// reads, in each engine's spelling and across the families a plan rates.
func TestLintE2E_AGeneratedTypeChangeIsReported(t *testing.T) {
	engines := []struct {
		name    string
		engine  temporalEngine
		dialect string
		pairs   [][2]string
	}{
		{
			name: "sqlserver", engine: sqlServerTemporal, dialect: "sqlserver",
			pairs: [][2]string{
				{"datetime2(3)", "datetime2(0)"}, {"datetime2(0)", "datetime2(3)"}, {"varchar(max)", "varchar(50)"}, {"nvarchar(100)", "nvarchar(5)"},
				{"nvarchar(max)", "nvarchar(50)"}, {"nvarchar(50)", "nvarchar(max)"}, {"decimal(10,2)", "decimal"},
				{"varbinary(max)", "varbinary(50)"}, {"decimal(12,2)", "decimal(10,2)"}, {"float", "real"}, {"bigint", "int"}, {"money", "smallmoney"},
			},
		},
		{
			name: "postgres", engine: postgresTemporal, dialect: "postgres",
			pairs: [][2]string{
				{"double precision", "real"}, {"numeric(12,2)", "numeric(10,2)"}, {"varbit(8)", "varbit(4)"}, {"real", "double precision"},
				{"numeric", "numeric(10,2)"}, {"numeric(10,2)", "numeric"},
			},
		},
		{
			name: "cockroachdb", engine: cockroachTemporal, dialect: "cockroachdb",
			pairs: [][2]string{{"string(100)", "string(5)"}, {"float8", "float4"}, {"decimal(12,2)", "decimal(10,2)"}, {"decimal", "decimal(10,2)"}},
		},
		{
			name: "yugabytedb", engine: yugabyteTemporal, dialect: "yugabytedb",
			pairs: [][2]string{{"double precision", "real"}, {"varchar(100)", "varchar(5)"}},
		},
		{
			name: "mysql", engine: mysqlTemporal, dialect: "mysql",
			pairs: [][2]string{{"longtext", "text"}, {"blob", "tinyblob"}, {"double", "float"}, {"smallint", "tinyint"}, {"int", "int unsigned"}, {"decimal(12,2)", "decimal(10,2)"}, {"decimal(10,2)", "decimal"}},
		},
		{
			name: "mariadb", engine: mariadbTemporal, dialect: "mariadb",
			pairs: [][2]string{{"longtext", "text"}, {"double", "float"}},
		},
		{
			name: "clickhouse", engine: clickHouseTemporal, dialect: "clickhouse",
			pairs: [][2]string{{"Float64", "Float32"}, {"String", "FixedString(2)"}, {"Decimal64(2)", "Decimal32(2)"}, {"DateTime64(6)", "DateTime64(3)"}},
		},
	}
	for _, engine := range engines {
		t.Run(engine.name, func(t *testing.T) {
			conn, address := temporalScratch(qt.New(t), engine.engine)
			for _, pair := range engine.pairs {
				t.Run(pair[0]+" to "+pair[1], func(t *testing.T) {
					c := qt.New(t)
					_, err := conn.ExecContext(c.Context(), "DROP TABLE IF EXISTS t")
					c.Assert(err, qt.IsNil)
					_, err = conn.ExecContext(c.Context(), fmt.Sprintf(engine.engine.table, pair[0]))
					c.Assert(err, qt.IsNil)
					schema := schemaFile(c, fmt.Sprintf(engine.engine.table, pair[1]))
					dir := c.TempDir()
					ptah := func(args ...string) clirun.Result {
						return clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()}, args...)
					}
					generated := ptah("migrations", "generate", "--db-url", address, "--schema-file", schema, "--migrations-dir", dir, "--allow-destructive")
					c.Assert(generated.ExitCode, qt.Equals, 0, qt.Commentf("%s", generated.Stderr))

					lint := ptah("migrations", "lint", "--dir", dir, "--dialect", engine.dialect, "--format", "json")

					c.Assert(lint.Stdout, qt.Contains, `"rule": "DS103"`, qt.Commentf("%s\n%s", lint.Stdout, lint.Stderr))
				})
			}
		})
	}
}

// TestLintE2E_SQLServerStateSettlesARestatedType replays the directory on a real SQL
// Server. Its ALTER COLUMN has to name a type to change a nullability, so a clause that
// names the type the column holds changed no type, and only the state can say so.
func TestLintE2E_SQLServerStateSettlesARestatedType(t *testing.T) {
	rows := []struct {
		name         string
		alter        string
		wantTypeRule bool
	}{
		{"the type the column holds", "ALTER TABLE [t] ALTER COLUMN [c] int NOT NULL;", false},
		{"the type in a length", "ALTER TABLE [t] ALTER COLUMN [n] nvarchar(10) NOT NULL;", false},
		{"another type", "ALTER TABLE [t] ALTER COLUMN [c] bigint NOT NULL;", true},
		{"another length", "ALTER TABLE [t] ALTER COLUMN [n] nvarchar(20) NULL;", true},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			_, devAddress := temporalScratch(c, sqlServerTemporal)
			dir := c.TempDir()
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_base.up.sql"),
				[]byte("CREATE TABLE [t] ([id] int PRIMARY KEY, [c] int NULL, [n] nvarchar(10) NULL);\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000001_base.down.sql"), []byte("DROP TABLE [t];\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_change.up.sql"), []byte(row.alter+"\n"), 0o600), qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, "0000000002_change.down.sql"), []byte("SELECT 1;\n"), 0o600), qt.IsNil)

			lint := clirun.Run(c, clirun.Ptah, clirun.Options{Dir: c.TempDir()},
				"migrations", "lint", "--dir", dir, "--dialect", "sqlserver", "--dev-url", devAddress, "--format", "json")

			c.Assert(lint.Stdout, qt.Contains, `"findings"`, qt.Commentf("%s\n%s", lint.Stdout, lint.Stderr))
			c.Assert(containsTypeRule(lint.Stdout), qt.Equals, row.wantTypeRule, qt.Commentf("%s", lint.Stdout))
		})
	}
}

// typeRuleRe finds the rule in a lint document.
var typeRuleRe = regexp.MustCompile(`"rule": "DS103"`)

func containsTypeRule(document string) bool {
	return typeRuleRe.MatchString(document)
}
