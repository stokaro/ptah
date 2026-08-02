package migrator

// White-box testing required: these tests verify the raw Atlas txtar section
// model, internal check-group directives, and driver scalar normalization,
// which are not observable through exported parser state.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
)

func TestParseAtlasTxtarSQLCapturesChecksSection(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- checks.sql --
SELECT NOT EXISTS (SELECT * FROM users);

-- migration.sql --
ALTER TABLE users ADD COLUMN email TEXT;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.checkFiles, qt.HasLen, 1)
	c.Assert(parsed.checkFiles[0].name, qt.Equals, "checks.sql")
	c.Assert(parsed.checkFiles[0].sql, qt.Contains, "SELECT NOT EXISTS")
	c.Assert(parsed.migrationSQL, qt.Contains, "ALTER TABLE users")
}

func TestParseAtlasTxtarSQLCapturesNamedChecksInArchiveOrder(t *testing.T) {
	c := qt.New(t)

	parsed, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- checks/users.sql --
SELECT 1;

-- schema.sql --
CREATE TABLE ignored (id int);

-- checks/roles.sql --
SELECT 2;

-- migration.sql --
SELECT 3;
`)
	c.Assert(err, qt.IsNil)
	c.Assert(ok, qt.IsTrue)
	c.Assert(parsed.checkFiles, qt.HasLen, 2)
	c.Assert(parsed.checkFiles[0].name, qt.Equals, "checks/users.sql")
	c.Assert(parsed.checkFiles[0].sql, qt.Equals, "SELECT 1;\n\n")
	c.Assert(parsed.checkFiles[1].name, qt.Equals, "checks/roles.sql")
	c.Assert(parsed.checkFiles[1].sql, qt.Equals, "SELECT 2;\n\n")
}

func TestParseAtlasTxtarSQLRejectsDuplicateChecksSection(t *testing.T) {
	c := qt.New(t)

	_, ok, err := parseAtlasTxtarSQL("20240305171146_seed.sql", `-- atlas:txtar

-- checks.sql --
SELECT 1;

-- checks.sql --
SELECT 2;

-- migration.sql --
SELECT 3;
`)
	c.Assert(ok, qt.IsTrue)
	c.Assert(err, qt.ErrorMatches, `invalid Atlas txtar migration 20240305171146_seed.sql: duplicate checks.sql section`)
}

func TestParseAtlasTxtarChecks(t *testing.T) {
	c := qt.New(t)

	checks := parseAtlasTxtarChecks("checks.sql", `-- users must be empty
SELECT NOT EXISTS (SELECT * FROM users);
SELECT count(*) = 0 FROM posts;
`, platform.Postgres)

	c.Assert(checks, qt.HasLen, 2)
	c.Assert(checks[0].Name, qt.Equals, "checks.sql#1")
	// Preserve comments because MySQL/MariaDB executable comments affect SQL
	// semantics. Only the trailing terminator is dropped for driver portability.
	c.Assert(checks[0].Assert, qt.Equals, "-- users must be empty\nSELECT NOT EXISTS (SELECT * FROM users)")
	c.Assert(checks[0].OnFail, qt.Equals, OnFailAbort)
	c.Assert(checks[1].Name, qt.Equals, "checks.sql#2")
	c.Assert(checks[1].Assert, qt.Equals, "SELECT count(*) = 0 FROM posts")
}

func TestParseAtlasTxtarChecks_MySQLBackslashEscaping(t *testing.T) {
	c := qt.New(t)

	checks := parseAtlasTxtarChecks("checks/escaped.sql", `SELECT '\''; SELECT 2;`, platform.MySQL)

	c.Assert(checks, qt.HasLen, 2)
	c.Assert(checks[0].Name, qt.Equals, "checks/escaped.sql#1")
	c.Assert(checks[0].Assert, qt.Equals, `SELECT '\''`)
	c.Assert(checks[1].Assert, qt.Equals, "SELECT 2")
}

func TestParseAtlasTxtarChecks_PostgresEscapeString(t *testing.T) {
	c := qt.New(t)

	checks := parseAtlasTxtarChecks("checks/escaped.sql", `SELECT E'it\'s; safe'; SELECT 2;`, platform.Postgres)

	c.Assert(checks, qt.HasLen, 2)
	c.Assert(checks[0].Assert, qt.Equals, `SELECT E'it\'s; safe'`)
	c.Assert(checks[1].Assert, qt.Equals, "SELECT 2")
}

func TestParseAtlasTxtarChecks_MySQLSemanticComments(t *testing.T) {
	c := qt.New(t)

	checks := parseAtlasTxtarChecks("checks/comments.sql", `SELECT 0 /*! + 1 */; SELECT -1--1;`, platform.MySQL)

	c.Assert(checks, qt.HasLen, 2)
	c.Assert(checks[0].Assert, qt.Equals, "SELECT 0 /*! + 1 */")
	c.Assert(checks[1].Assert, qt.Equals, "SELECT -1--1")
}

func TestParseAtlasTxtarChecksEmptySection(t *testing.T) {
	c := qt.New(t)

	c.Assert(parseAtlasTxtarChecks("checks.sql", "", platform.Postgres), qt.HasLen, 0)
	c.Assert(parseAtlasTxtarChecks("checks.sql", "-- comments only\n", platform.Postgres), qt.HasLen, 0)
}

func TestAtlasCheckFileMode(t *testing.T) {
	c := qt.New(t)

	c.Assert(atlasCheckFileMode("-- atlas:assert oneof\nSELECT 0;\nSELECT 1;\n", platform.Postgres), qt.Equals, checkGroupOneOf)
	c.Assert(atlasCheckFileMode("-- atlas:assert DS102\nSELECT 1;\n", platform.Postgres), qt.Equals, checkGroupAll)
	c.Assert(atlasCheckFileMode("SELECT 1;\n-- atlas:assert oneof\nSELECT 0;\n", platform.Postgres), qt.Equals, checkGroupAll)
}

func TestCheckTransactionOptions_ReadOnlyDialects(t *testing.T) {
	c := qt.New(t)
	dialects := []string{
		platform.Postgres,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Spanner,
		platform.MySQL,
		platform.MariaDB,
	}

	for _, dialect := range dialects {
		c.Run(dialect, func(c *qt.C) {
			c.Assert(checkTransactionOptions(dialect).ReadOnly, qt.IsTrue)
		})
	}
}

func TestValidateCheckAssertion_Accepted(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name          string
		assertion     string
		dialect       string
		serverVersion string
	}{
		{name: "MySQL whole executable comment", assertion: "/*! SELECT 1 */", dialect: platform.MySQL, serverVersion: "8.4.6"},
		{name: "MariaDB whole executable comment", assertion: "/*M! SELECT 1 */", dialect: platform.MariaDB, serverVersion: "10.11.14-MariaDB"},
		{name: "MySQL ignores MariaDB comment", assertion: "/*M! DELETE FROM users */ SELECT 1", dialect: platform.MySQL, serverVersion: "8.4.6"},
		{name: "MariaDB ignores MySQL 50700 comment", assertion: "/*!50700 DELETE FROM users */ SELECT 1", dialect: platform.MariaDB, serverVersion: "10.11.14-MariaDB"},
		{name: "MySQL ignores future version guard", assertion: "/*!99999 DELETE FROM users */ SELECT 1", dialect: platform.MySQL, serverVersion: "8.4.6"},
		{name: "MySQL short numeric prefix is SQL", assertion: "SELECT /*!1234 + 1 */ = 1235", dialect: platform.MySQL, serverVersion: "8.4.6"},
		{name: "MariaDB short numeric prefix is SQL", assertion: "SELECT /*!1234 + 1 */ = 1235", dialect: platform.MariaDB, serverVersion: "10.11.14-MariaDB"},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(validateCheckAssertion(test.assertion, test.dialect, test.serverVersion), qt.IsNil)
		})
	}
}

func TestValidateCheckAssertion_Rejected(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name          string
		assertion     string
		dialect       string
		serverVersion string
		wantErr       string
	}{
		{name: "MySQL executable DELETE", assertion: "/*! DELETE FROM users */ SELECT 1", dialect: platform.MySQL, serverVersion: "8.4.6", wantErr: "check assertion must be a read-only SELECT statement"},
		{name: "MariaDB executable DELETE", assertion: "/*M! DELETE FROM users */ SELECT 1", dialect: platform.MariaDB, serverVersion: "10.11.14-MariaDB", wantErr: "check assertion must be a read-only SELECT statement"},
		{name: "MariaDB pre-50700 executable DELETE", assertion: "/*!50699 DELETE FROM users */ SELECT 1", dialect: platform.MariaDB, serverVersion: "10.11.14-MariaDB", wantErr: "check assertion must be a read-only SELECT statement"},
		{name: "MySQL current version guard", assertion: "/*!80000 DELETE FROM users */ SELECT 1", dialect: platform.MySQL, serverVersion: "8.4.6", wantErr: "check assertion must be a read-only SELECT statement"},
		{name: "multiple statements inside executable comment", assertion: "/*! SELECT 1; COMMIT; DROP TABLE users */", dialect: platform.MySQL, serverVersion: "8.4.6", wantErr: "check assertion must be one read-only SELECT statement, got 3 statements"},
		{name: "MySQL numeric body is not SELECT", assertion: "/*!1234 SELECT 1 */", dialect: platform.MySQL, serverVersion: "8.4.6", wantErr: "check assertion must be a read-only SELECT statement"},
		{name: "MariaDB numeric body is not SELECT", assertion: "/*M!1234 SELECT 1 */", dialect: platform.MariaDB, serverVersion: "10.11.14-MariaDB", wantErr: "check assertion must be a read-only SELECT statement"},
		{name: "unparseable server version", assertion: "/*!80000 SELECT 1 */", dialect: platform.MySQL, serverVersion: "development", wantErr: `check assertion cannot evaluate executable-comment guard against server version "development"`},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(validateCheckAssertion(test.assertion, test.dialect, test.serverVersion), qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestContainsIdentifierSequence_SQLServerNextValueFor(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"SELECT NEXT VALUE FOR dbo.order_sequence",
		"SELECT next /* sequence */ value\nfor dbo.order_sequence",
	}
	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			c.Assert(
				containsIdentifierSequence(statement, platform.SQLServer, "NEXT", "VALUE", "FOR"),
				qt.IsTrue,
			)
		})
	}
}

func TestContainsIdentifierSequence_IgnoresNonCodeAndBrokenSequences(t *testing.T) {
	c := qt.New(t)

	statements := []string{
		"SELECT 'NEXT VALUE FOR dbo.order_sequence'",
		"SELECT 1 -- NEXT VALUE FOR dbo.order_sequence",
		"SELECT NEXT + VALUE + FOR FROM counters",
	}
	for _, statement := range statements {
		c.Run(statement, func(c *qt.C) {
			c.Assert(
				containsIdentifierSequence(statement, platform.SQLServer, "NEXT", "VALUE", "FOR"),
				qt.IsFalse,
			)
		})
	}
}

func TestAssertionPassed_NonZeroNumericTypes(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		value any
	}{
		{name: "int", value: int(1)},
		{name: "int8", value: int8(1)},
		{name: "int16", value: int16(1)},
		{name: "int32", value: int32(1)},
		{name: "int64", value: int64(1)},
		{name: "uint", value: uint(1)},
		{name: "uint8", value: uint8(1)},
		{name: "uint16", value: uint16(1)},
		{name: "uint32", value: uint32(1)},
		{name: "uint64", value: uint64(1)},
		{name: "float32", value: float32(1)},
		{name: "float64", value: float64(1)},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(assertionPassed(test.value), qt.IsTrue)
		})
	}
}

func TestAssertionPassed_ZeroNumericTypes(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name  string
		value any
	}{
		{name: "int", value: int(0)},
		{name: "int8", value: int8(0)},
		{name: "int16", value: int16(0)},
		{name: "int32", value: int32(0)},
		{name: "int64", value: int64(0)},
		{name: "uint", value: uint(0)},
		{name: "uint8", value: uint8(0)},
		{name: "uint16", value: uint16(0)},
		{name: "uint32", value: uint32(0)},
		{name: "uint64", value: uint64(0)},
		{name: "float32", value: float32(0)},
		{name: "float64", value: float64(0)},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(assertionPassed(test.value), qt.IsFalse)
		})
	}
}
