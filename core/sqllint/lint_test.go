package sqllint

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/platform/capability"
)

func TestLintSource_TableWithoutPrimaryKey(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "schema.sql",
		SQL:  "CREATE TABLE users (email TEXT NOT NULL);",
	}, Options{Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, RuleTableWithoutPrimaryKey)
	c.Assert(findings[0].Severity, qt.Equals, SeverityWarning)
	c.Assert(findings[0].File, qt.Equals, "schema.sql")
}

func TestLintSource_TablePrimaryKeyPasses(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "schema.sql",
		SQL:  "CREATE TABLE users (id BIGINT PRIMARY KEY, email TEXT NOT NULL);",
	}, Options{Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

func TestLintSource_DisabledRules(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "schema.sql",
		SQL:  "CREATE TABLE users (email TEXT NOT NULL);",
	}, Options{
		Dialect:       platform.Postgres,
		DisabledRules: []string{RuleTableWithoutPrimaryKey},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}

func TestLintSource_UnsupportedStatementIsExplicit(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "query.sql",
		SQL:  "SELECT 1;",
	}, Options{Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
	c.Assert(findings[0].Severity, qt.Equals, SeverityError)
	c.Assert(findings[0].Message, qt.Contains, "SELECT")
}

func TestLintSource_UnsupportedStatementLocationSkipsLeadingComments(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "query.sql",
		SQL:  "-- heading comment\nSELECT 1;",
	}, Options{Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
	c.Assert(findings[0].Line, qt.Equals, 2)
	c.Assert(findings[0].Column, qt.Equals, 1)
}

func TestLintSource_UnsupportedParserErrorIsExplicit(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "policy.sql",
		SQL:  "CREATE POLICY p ON users USING (true);",
	}, Options{Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
	c.Assert(findings[0].Severity, qt.Equals, SeverityError)
	c.Assert(findings[0].Title, qt.Equals, "Unsupported SQL statement")
}

func TestLintSource_RawSQLNodesAreExplicitUnsupportedFindings(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "create function",
			sql:  "CREATE FUNCTION f() RETURNS void AS $$ BEGIN RAISE NOTICE 'x'; END $$ LANGUAGE plpgsql;",
		},
		{
			name: "create procedure",
			sql:  "CREATE PROCEDURE p() LANGUAGE SQL AS $$ SELECT 1 $$;",
		},
		{
			name: "create trigger execute function",
			sql:  "CREATE TRIGGER trg AFTER INSERT ON users FOR EACH ROW EXECUTE FUNCTION audit_user();",
		},
		{
			name: "do block",
			sql:  "DO $$ BEGIN RAISE NOTICE 'x'; END $$;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := LintSource(Source{
				Name: "raw.sql",
				SQL:  tt.sql,
			}, Options{Dialect: platform.Postgres})

			c.Assert(err, qt.IsNil)
			c.Assert(findings, qt.HasLen, 1)
			c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
			c.Assert(findings[0].Severity, qt.Equals, SeverityError)
			c.Assert(findings[0].Line, qt.Equals, 1)
			c.Assert(findings[0].Column, qt.Equals, 1)
		})
	}
}

func TestLintSource_MySQLRoutineNodesAreExplicitUnsupportedFindings(t *testing.T) {
	c := qt.New(t)

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		findings, err := LintSource(Source{
			Name: "routine.sql",
			SQL: `DELIMITER //
CREATE PROCEDURE p1()
BEGIN
  SELECT 1;
END//
DELIMITER ;`,
		}, Options{Dialect: dialect})

		c.Assert(err, qt.IsNil)
		c.Assert(findings, qt.HasLen, 1)
		c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
		c.Assert(findings[0].Severity, qt.Equals, SeverityError)
		c.Assert(findings[0].Message, qt.Contains, "CREATE PROCEDURE")
	}
}

func TestLintSource_SQLServerRoutineNodesAreExplicitUnsupportedFindings(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "routine.sql",
		SQL: `CREATE PROCEDURE [dbo].[p1] AS
BEGIN
  SELECT 1;
END`,
	}, Options{Dialect: platform.SQLServer})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
	c.Assert(findings[0].Severity, qt.Equals, SeverityError)
	c.Assert(findings[0].Message, qt.Contains, "CREATE PROCEDURE")
}

func TestLintSource_UnsupportedStatementDoesNotMaskLaterDDL(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "mixed.sql",
		SQL: `CREATE POLICY p ON users USING (true);
CREATE TABLE audit_log (message TEXT NOT NULL);`,
	}, Options{Dialect: platform.Postgres})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 2)
	c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
	c.Assert(findings[1].Rule, qt.Equals, RuleTableWithoutPrimaryKey)
	c.Assert(findings[1].Line, qt.Equals, 2)
}

func TestLintSource_SQLServerProcedureWithoutBeginStaysSingleStatement(t *testing.T) {
	c := qt.New(t)

	findings, err := LintSource(Source{
		Name: "proc.sql",
		SQL: `CREATE PROCEDURE [dbo].[list_users] AS
SELECT 1 AS [first];
SELECT 2 AS [second];`,
	}, Options{Dialect: platform.SQLServer})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, RuleUnsupportedStatement)
	c.Assert(findings[0].Message, qt.Contains, "CREATE PROCEDURE")
}

func TestLintSource_CapabilityAwareCreateIndexConcurrently(t *testing.T) {
	c := qt.New(t)
	source := Source{
		Name: "index.sql",
		SQL:  "CREATE INDEX CONCURRENTLY idx_users_email ON users (email);",
	}

	postgresFindings, err := LintSource(source, Options{
		Dialect:      platform.Postgres,
		Capabilities: capability.Postgres16(),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(postgresFindings, qt.HasLen, 0)

	cockroachFindings, err := LintSource(source, Options{
		Dialect:      platform.CockroachDB,
		Capabilities: capability.CockroachDB23(),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(cockroachFindings, qt.HasLen, 1)
	c.Assert(cockroachFindings[0].Rule, qt.Equals, RuleUnsupportedCapability)
	c.Assert(cockroachFindings[0].Severity, qt.Equals, SeverityError)
}
