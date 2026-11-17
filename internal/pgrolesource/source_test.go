package pgrolesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
	"ptah.run/internal/pgrolesource"
)

func TestParseSettings_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, written string
		want          []pgrole.Setting
	}{
		{name: "one", written: "statement_timeout = '30s'", want: []pgrole.Setting{{Name: "statement_timeout", Value: "'30s'"}}},
		{name: "list value", written: "search_path = '$user', public", want: []pgrole.Setting{{Name: "search_path", Value: "'$user', public"}}},
		{name: "separator in a literal", written: "app.note = 'a;b=c'; work_mem = '8MB';", want: []pgrole.Setting{
			{Name: "app.note", Value: "'a;b=c'"}, {Name: "work_mem", Value: "'8MB'"},
		}},
		{name: "quoted name keeps its case", written: `"App"."Tenant" = 'acme'`, want: []pgrole.Setting{{Name: "App.Tenant", Value: "'acme'"}}},
		{name: "name read in lower case", written: "Statement_Timeout = 30000", want: []pgrole.Setting{{Name: "statement_timeout", Value: "30000"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgrolesource.ParseSettings(test.written)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestParseSettings_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, written, want string }{
		{name: "nothing", written: " ; ", want: `.*settings lists no setting`},
		{name: "no value", written: "statement_timeout", want: `.*role setting "statement_timeout" is not name = value`},
		{name: "from current", written: "work_mem FROM CURRENT", want: `.*FROM CURRENT stores the value of the session that runs the statement.*`},
		{name: "default", written: "work_mem = DEFAULT", want: `.*role setting "work_mem" is DEFAULT, which resets it; name it in reset_settings instead`},
		{name: "unit without quotes", written: "statement_timeout = 30s", want: `.*a value with a unit is a string literal: '30s'`},
		{name: "bad name", written: "app-tenant = 'x'", want: `.*role setting name "app-tenant" is not one name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgrolesource.ParseSettings(test.written)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

func TestParseResets_HappyPath(t *testing.T) {
	c := qt.New(t)
	got, err := pgrolesource.ParseResets(` work_mem, "App"."Tenant" ,`)
	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, []pgrole.Setting{{Name: "work_mem", Reset: true}, {Name: "App.Tenant", Reset: true}})
}

func TestParseResets_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, written, want string }{
		{name: "nothing", written: " , ", want: `.*reset_settings lists no setting`},
		{name: "all", written: "work_mem, all", want: `.*reset_settings names ALL, which would reset settings no declaration names; name each setting`},
		{name: "bad name", written: "app tenant", want: `.*role setting name "app tenant" is not one name`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgrolesource.ParseResets(test.written)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(got, qt.IsNil)
		})
	}
}

// Decode and Encode are the two halves an annotation and a YAML entry are read
// and written through, so what Encode writes must decode to the same facets.
func TestDecode_RoundTripsThroughEncode(t *testing.T) {
	c := qt.New(t)
	facets, err := pgrolesource.Decode(map[string]string{
		pgrolesource.SettingsAttribute: `statement_timeout = '30s'; "App"."Tenant" = 'a;b'`,
		pgrolesource.ResetAttribute:    "work_mem",
	})
	c.Assert(err, qt.IsNil)

	settings, resets, err := pgrolesource.Encode(facets)

	c.Assert(err, qt.IsNil)
	c.Assert(settings, qt.Equals, `"App"."Tenant" = 'a;b'; "statement_timeout" = '30s'`)
	c.Assert(resets, qt.Equals, `"work_mem"`)
	again, err := pgrolesource.Decode(map[string]string{pgrolesource.SettingsAttribute: settings, pgrolesource.ResetAttribute: resets})
	c.Assert(err, qt.IsNil)
	c.Assert(again.Equal(facets), qt.IsTrue)
}

func TestDecode_HappyPath(t *testing.T) {
	c := qt.New(t)
	facets, err := pgrolesource.Decode(nil)
	c.Assert(err, qt.IsNil)
	c.Assert(facets.IsZero(), qt.IsTrue)
}

func TestDecode_FailurePath(t *testing.T) {
	c := qt.New(t)
	facets, err := pgrolesource.Decode(map[string]string{
		pgrolesource.SettingsAttribute: "work_mem = '8MB'",
		pgrolesource.ResetAttribute:    "Work_Mem",
	})
	c.Assert(err, qt.ErrorMatches, `.*role setting "work_mem" is named twice`)
	c.Assert(facets.IsZero(), qt.IsTrue)
}

func TestEncode_HappyPath(t *testing.T) {
	c := qt.New(t)
	settings, resets, err := pgrolesource.Encode(schemaext.Facets{})
	c.Assert(err, qt.IsNil)
	c.Assert([]string{settings, resets}, qt.DeepEquals, []string{"", ""})
}
