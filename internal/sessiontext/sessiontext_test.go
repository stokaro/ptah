package sessiontext_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/sessiontext"
)

// A session whose sql_mode names NO_BACKSLASH_ESCAPES reads a backslash in a
// string as the character, so each literal is sent with the characters
// Ptah's reading gives it. The rows are what the MySQL renderer writes for a
// default, a comment and an enum value, and what a schema file may hold.
func TestFor_WithoutBackslashEscapes(t *testing.T) {
	caps := capability.MySQL84().With(capability.NoBackslashEscapes, true)
	tests := []struct {
		name, statement, want string
	}{
		{
			name:      "a default and a comment",
			statement: "CREATE TABLE `notes` (`a` VARCHAR(30) COMMENT 'c\\\\d' DEFAULT 'a\\\\b')",
			want:      "CREATE TABLE `notes` (`a` VARCHAR(30) COMMENT 'c\\d' DEFAULT 'a\\b')",
		},
		{
			name:      "a quote behind a backslash, and a semicolon after it",
			statement: "ALTER TABLE `notes` ALTER COLUMN `q` SET DEFAULT 'it\\'s;x'",
			want:      "ALTER TABLE `notes` ALTER COLUMN `q` SET DEFAULT 'it''s;x'",
		},
		{
			name:      "an enum value",
			statement: "ALTER TABLE `notes` MODIFY COLUMN `e` ENUM('e\\\\f','g') DEFAULT 'g'",
			want:      "ALTER TABLE `notes` MODIFY COLUMN `e` ENUM('e\\f','g') DEFAULT 'g'",
		},
		{
			name:      "a control character",
			statement: "SELECT 'a\\nb'",
			want:      "SELECT 'a\nb'",
		},
		{
			name:      "the LIKE escapes, which keep the backslash",
			statement: "SELECT 'a\\%b\\_c'",
			want:      "SELECT 'a\\%b\\_c'",
		},
		{
			name:      "a character set introducer",
			statement: "SELECT _utf8mb4'a\\\\b'",
			want:      "SELECT _utf8mb4'a\\b'",
		},
		{
			name:      "a quoted identifier and a comment keep their backslashes",
			statement: "SELECT 'a\\\\b' AS `x\\\\y` /* p\\\\q */",
			want:      "SELECT 'a\\b' AS `x\\\\y` /* p\\\\q */",
		},
		{
			name:      "a literal with no backslash",
			statement: "SELECT 'it''s', X'6A', b'101'",
			want:      "SELECT 'it''s', X'6A', b'101'",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sessiontext.For(platform.MySQL, caps, test.statement)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// A session that reads a backslash as an escape, and every engine that has no
// such setting, receives the statement as Ptah wrote it.
func TestFor_KeepsTheStatement(t *testing.T) {
	tests := []struct {
		name, dialect string
		caps          capability.Capabilities
	}{
		{name: "MySQL under the default sql_mode", dialect: platform.MySQL, caps: capability.MySQL84()},
		{name: "MariaDB under the default sql_mode", dialect: platform.MariaDB, caps: capability.MariaDB1011()},
		{name: "PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18()},
	}
	const statement = "ALTER TABLE `notes` ALTER COLUMN `a` SET DEFAULT 'a\\\\b'"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := sessiontext.For(test.dialect, test.caps, statement)

			c.Assert(got, qt.Equals, statement)
		})
	}
}
