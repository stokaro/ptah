package parseutils

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestParseKeyValueComment_UnquotesEscapedValues(t *testing.T) {
	c := qt.New(t)

	kv := ParseKeyValueComment(`//migrator:schema:function name="normalize" body="BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;" path="C:\\tmp"`)

	c.Assert(kv["name"], qt.Equals, "normalize")
	c.Assert(kv["body"], qt.Equals, "BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;")
	c.Assert(kv["path"], qt.Equals, `C:\tmp`)
}

func TestParseKeyValueComment_EnumDirectiveTokenIsNotBooleanAttribute(t *testing.T) {
	c := qt.New(t)

	kv := ParseKeyValueComment(`//migrator:schema:enum name="status" values="active,inactive"`)

	c.Assert(kv["enum"], qt.Equals, "")
	c.Assert(kv["name"], qt.Equals, "status")
	c.Assert(kv["values"], qt.Equals, "active,inactive")
}

func TestParsePlatformSpecificUsesSharedPlatformAttributeShape(t *testing.T) {
	c := qt.New(t)

	platform := ParsePlatformSpecific(map[string]string{
		"platform.mysql.type":                   "INT",
		"platform.postgres.identity.generation": "BY DEFAULT",
		"platform.mysql":                        "ignored",
		"platform.mysql.type-name":              "ignored",
	})

	c.Assert(platform["mysql"]["type"], qt.Equals, "INT")
	c.Assert(platform["postgres"]["identity.generation"], qt.Equals, "BY DEFAULT")
	c.Assert(platform["mysql"]["type-name"], qt.Equals, "")
}
