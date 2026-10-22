package parseutils_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/internal/annotationmeta"
)

func TestParseKeyValueComment_UnquotesEscapedValues(t *testing.T) {
	c := qt.New(t)

	kv := parseutils.NewKeyValueParser(annotationmeta.Common()).ParseKeyValueComment(`//ptah:schema:function name="normalize" body="BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;" path="C:\\tmp"`)

	c.Assert(kv["name"], qt.Equals, "normalize")
	c.Assert(kv["body"], qt.Equals, "BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;")
	c.Assert(kv["path"], qt.Equals, `C:\tmp`)
}

func TestParseKeyValueComment_EnumDirectiveTokenIsNotBooleanAttribute(t *testing.T) {
	c := qt.New(t)

	kv := parseutils.NewKeyValueParser(annotationmeta.Common()).ParseKeyValueComment(`//ptah:schema:enum name="status" values="active,inactive"`)

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

// Every table and field directive of a schema is read through
// ParsePlatformSpecific, so it must not copy the dialect descriptors to learn
// which of them read the shortcuts. Copying them all cost a drift of a
// two-table schema about a megabyte.
func TestParsePlatformSpecific_CopiesNoDialectDescriptor(t *testing.T) {
	c := qt.New(t)
	kv := map[string]string{"engine": "InnoDB"}
	parseutils.ParsePlatformSpecific(kv) // derives the shortcut dialects once

	allocs := testing.AllocsPerRun(100, func() { parseutils.ParsePlatformSpecific(kv) })

	// The result holds a map for MySQL and one for MariaDB, which takes a few
	// allocations; copying every descriptor took over 400.
	c.Assert(allocs < 10, qt.IsTrue, qt.Commentf("allocations per call: %v", allocs))
}
