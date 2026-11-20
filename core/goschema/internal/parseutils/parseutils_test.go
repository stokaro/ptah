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
			"engine": "InnoDB",
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

// A plain comment is the table's or the column's own, which the directive's
// Comment carries, so it is no platform property: a provider that receives no
// platform properties refuses one, and a MySQL-family plan writes the comment
// without it.
func TestParsePlatformSpecific_CommentIsNoProperty(t *testing.T) {
	c := qt.New(t)

	platform := parseutils.ParsePlatformSpecific(map[string]string{"comment": "People who sign in"})

	c.Assert(platform, qt.HasLen, 0)
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

func TestBoolAttribute_HappyPath(t *testing.T) {
	tests := []struct {
		name        string
		attributes  map[string]string
		wantValue   bool
		wantPresent bool
	}{
		{name: "absent", attributes: make(map[string]string), wantValue: false, wantPresent: false},
		{name: "true", attributes: map[string]string{"period": "true"}, wantValue: true, wantPresent: true},
		{name: "false", attributes: map[string]string{"period": "false"}, wantValue: false, wantPresent: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, present, err := parseutils.BoolAttribute(test.attributes, "period")

			c.Assert(err, qt.IsNil)
			c.Assert(value, qt.Equals, test.wantValue)
			c.Assert(present, qt.Equals, test.wantPresent)
		})
	}
}

// A written value that is not exactly true or false is refused rather than
// read as false. Another case is refused, and so is an empty value, which is
// written and says neither.
func TestBoolAttribute_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  string
	}{
		{name: "empty", value: "", want: `without_overlaps="" is not a boolean; write true or false`},
		{name: "another case", value: "TRUE", want: `without_overlaps="TRUE" is not a boolean; write true or false`},
		{name: "surrounding space", value: " true ", want: `without_overlaps=" true " is not a boolean; write true or false`},
		{name: "yes", value: "yes", want: `without_overlaps="yes" is not a boolean; write true or false`},
		{name: "one", value: "1", want: `without_overlaps="1" is not a boolean; write true or false`},
		{name: "a column name", value: "valid_at", want: `without_overlaps="valid_at" is not a boolean; write true or false`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			value, present, err := parseutils.BoolAttribute(map[string]string{"without_overlaps": test.value}, "without_overlaps")

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(value, qt.IsFalse)
			c.Assert(present, qt.IsTrue)
		})
	}
}
