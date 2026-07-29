package parseutils_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema/internal/parseutils"
)

func TestParseKeyValueComment_UnquotesEscapedValues(t *testing.T) {
	c := qt.New(t)

	kv := parseutils.ParseKeyValueComment(`//ptah:schema:function name="normalize" body="BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;" path="C:\\tmp"`)

	c.Assert(kv["name"], qt.Equals, "normalize")
	c.Assert(kv["body"], qt.Equals, "BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;")
	c.Assert(kv["path"], qt.Equals, `C:\tmp`)
}

func TestParseKeyValueComment_EnumDirectiveTokenIsNotBooleanAttribute(t *testing.T) {
	c := qt.New(t)

	kv := parseutils.ParseKeyValueComment(`//ptah:schema:enum name="status" values="active,inactive"`)

	c.Assert(kv["enum"], qt.Equals, "")
	c.Assert(kv["name"], qt.Equals, "status")
	c.Assert(kv["values"], qt.Equals, "active,inactive")
}

func TestParsePlatformSpecificUsesSharedPlatformAttributeShape(t *testing.T) {
	c := qt.New(t)

	platform := parseutils.ParsePlatformSpecific(map[string]string{
		"platform.mysql.type":                   "INT",
		"platform.postgres.identity.generation": "BY DEFAULT",
		"platform.mysql":                        "ignored",
		"platform.mysql.type-name":              "ignored",
	})

	c.Assert(platform["mysql"]["type"], qt.Equals, "INT")
	c.Assert(platform["postgres"]["identity.generation"], qt.Equals, "BY DEFAULT")
	c.Assert(platform["mysql"]["type-name"], qt.Equals, "")
}

func TestParsePlatformSpecific_ExplicitOverridesTakePrecedence(t *testing.T) {
	c := qt.New(t)

	platform := parseutils.ParsePlatformSpecific(map[string]string{
		"engine":                    "InnoDB",
		"comment":                   "Default comment",
		"platform.mysql.engine":     "MyISAM",
		"platform.mysql.comment":    "MySQL comment",
		"platform.postgres.comment": "PostgreSQL comment",
	})

	c.Assert(platform, qt.DeepEquals, map[string]map[string]string{
		"mariadb": {
			"comment": "Default comment",
			"engine":  "InnoDB",
		},
		"mysql": {
			"comment": "MySQL comment",
			"engine":  "MyISAM",
		},
		"postgres": {
			"comment": "PostgreSQL comment",
		},
	})
}
