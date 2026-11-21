package parseutils_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/internal/annotationmeta"
)

func TestParseKeyValueComment_UnquotesEscapedValues(t *testing.T) {
	c := qt.New(t)

	kv, err := parseutils.NewKeyValueParser(annotationmeta.Common()).ParseKeyValueComment(`//ptah:schema:function name="normalize" body="BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;" path="C:\\tmp"`)

	c.Assert(err, qt.IsNil)
	c.Assert(kv["name"], qt.Equals, "normalize")
	c.Assert(kv["body"], qt.Equals, "BEGIN RAISE NOTICE \"hello\";\nRETURN NEW; END;")
	c.Assert(kv["path"], qt.Equals, `C:\tmp`)
}

func TestParseKeyValueComment_EnumDirectiveTokenIsNotBooleanAttribute(t *testing.T) {
	c := qt.New(t)

	kv, err := parseutils.NewKeyValueParser(annotationmeta.Common()).ParseKeyValueComment(`//ptah:schema:enum name="status" values="active,inactive"`)

	c.Assert(err, qt.IsNil)
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

// A bare word reads as true when the comment's directive declares it a bare
// boolean, and when the directive does not declare it or retired it, so that
// validation refuses the last two by name. The word is decided by the
// directive it is written on: stored is a boolean of a field, not of a table.
func TestParseKeyValueComment_BareWord_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    map[string]string
	}{
		{
			name:    "a bare boolean of the directive",
			comment: `//ptah:schema:role name="app" login createdb`,
			want:    map[string]string{"name": "app", "login": "true", "createdb": "true"},
		},
		{
			name:    "an alias of a bare boolean",
			comment: `//ptah:schema:grant role="app" privileges="SELECT" on_table="t" grant_option`,
			want:    map[string]string{"role": "app", "privileges": "SELECT", "on_table": "t", "grant_option": "true"},
		},
		{
			name:    "a word the directive does not declare",
			comment: `//ptah:schema:table name="t" stored`,
			want:    map[string]string{"name": "t", "stored": "true"},
		},
		{
			name:    "a retired attribute",
			comment: `//ptah:schema:matview name="v" body="SELECT 1" refresh_strategy`,
			want:    map[string]string{"name": "v", "body": "SELECT 1", "refresh_strategy": "true"},
		},
		{
			name:    "a value wins over the bare word",
			comment: `//ptah:schema:sequence name="s" cycle cycle="false"`,
			want:    map[string]string{"name": "s", "cycle": "false"},
		},
		{
			name:    "a word of the directive name",
			comment: `//ptah:schema:enum name="status" values="a,b"`,
			want:    map[string]string{"name": "status", "values": "a,b"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kv, err := parseutils.NewKeyValueParser(annotationmeta.Common()).ParseKeyValueComment(test.comment)

			c.Assert(err, qt.IsNil)
			c.Assert(kv, qt.DeepEquals, test.want)
		})
	}
}

// A bare word naming an attribute that takes a value is refused by name.
// Dropped, a bare schema would put the table in the default schema and a bare
// table would leave the policy table unnamed; read as true, either would name
// an object "true". A platform override takes a value too, and so does an
// attribute whose name is also a word of a directive name.
func TestParseKeyValueComment_BareWord_FailurePath(t *testing.T) {
	tests := []struct {
		name          string
		comment       string
		wantDirective string
		wantAttribute string
		wantMessage   string
	}{
		{
			name:          "a string attribute",
			comment:       `//ptah:schema:role name`,
			wantDirective: "ptah:schema:role",
			wantAttribute: "name",
			wantMessage:   `name on //ptah:schema:role takes a value and is written bare; write name="..."`,
		},
		{
			name:          "an attribute named like a directive",
			comment:       `//ptah:schema:table name="t" schema`,
			wantDirective: "ptah:schema:table",
			wantAttribute: "schema",
			wantMessage:   `schema on //ptah:schema:table takes a value and is written bare; write schema="..."`,
		},
		{
			name:          "the table of a row-level security switch",
			comment:       `//ptah:schema:rls:enable table force`,
			wantDirective: "ptah:schema:rls:enable",
			wantAttribute: "table",
			wantMessage:   `table on //ptah:schema:rls:enable takes a value and is written bare; write table="..."`,
		},
		{
			name:          "a platform override",
			comment:       `//ptah:schema:field name="a" type="INT" platform.mysql.type`,
			wantDirective: "ptah:schema:field",
			wantAttribute: "platform.mysql.type",
			wantMessage:   `platform.mysql.type on //ptah:schema:field takes a value and is written bare; write platform.mysql.type="..."`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			kv, err := parseutils.NewKeyValueParser(annotationmeta.Common()).ParseKeyValueComment(test.comment)

			c.Assert(err, qt.ErrorMatches, test.wantMessage)
			var bare *parseutils.BareValueError
			c.Assert(err, qt.ErrorAs, &bare)
			c.Assert(bare.Directive, qt.Equals, test.wantDirective)
			c.Assert(bare.Attribute, qt.Equals, test.wantAttribute)
			c.Assert(kv, qt.IsNil)
		})
	}
}
