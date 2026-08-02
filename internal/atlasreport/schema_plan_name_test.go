package atlasreport_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/internal/atlasreport"
)

func TestRenderSchemaPlanName(t *testing.T) {
	c := qt.New(t)
	data := atlasreport.SchemaPlanName{
		FromHash: "ERERERERERERERERERERERERERERERERERERERERERE=",
		ToHash:   "IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=",
	}

	tests := []struct {
		name   string
		format string
		want   string
	}{
		{
			name: "atlas documented example shape",
			// The example Atlas's own help advertises. `slice` is a
			// text/template builtin, so it works with no FuncMap and selects
			// 48 bits from the untagged Base64 digest.
			format: "plan_{{ slice .ToHash 0 8 }}",
			want:   "plan_IiIiIiIi",
		},
		{name: "to hash", format: "{{ .ToHash }}", want: data.ToHash},
		{name: "from hash", format: "{{ .FromHash }}", want: data.FromHash},
		{name: "both hashes", format: "{{ slice .FromHash 0 4 }}-{{ slice .ToHash 0 4 }}", want: "ERER-IiIi"},
		{name: "literal", format: "fixed_name", want: "fixed_name"},
		{
			// The Atlas helper set is shared with every other Go-template
			// surface in this package: a template that runs on `migrate apply
			// --format` must not fail here just because it is naming a plan.
			name:   "atlas helper set is available",
			format: "{{ upper (slice .ToHash 0 4) }}",
			want:   "IIII",
		},
		{name: "add helper", format: "plan_{{ add 1 6 }}", want: "plan_7"},
		{name: "surrounding whitespace is trimmed", format: "\n\t  spaced \n", want: "spaced"},
		{name: "printf builtin", format: `{{ printf "%s_%d" "plan" 7 }}`, want: "plan_7"},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := atlasreport.RenderSchemaPlanName(tt.format, data)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestNewSchemaPlanName_HappyPath(t *testing.T) {
	c := qt.New(t)

	got, err := atlasreport.NewSchemaPlanName(
		"sha256:1111111111111111111111111111111111111111111111111111111111111111",
		"sha256:2222222222222222222222222222222222222222222222222222222222222222",
	)

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.DeepEquals, atlasreport.SchemaPlanName{
		FromHash: "ERERERERERERERERERERERERERERERERERERERERERE=",
		ToHash:   "IiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiIiI=",
	})
}

func TestNewSchemaPlanName_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Run("invalid from fingerprint", func(c *qt.C) {
		got, err := atlasreport.NewSchemaPlanName("not-a-digest", "sha256:"+strings.Repeat("2", 64))

		c.Assert(err, qt.ErrorMatches, `parse from schema fingerprint for --name-format: .*`)
		c.Assert(got, qt.DeepEquals, atlasreport.SchemaPlanName{})
	})

	c.Run("invalid to fingerprint", func(c *qt.C) {
		got, err := atlasreport.NewSchemaPlanName("sha256:"+strings.Repeat("1", 64), "not-a-digest")

		c.Assert(err, qt.ErrorMatches, `parse to schema fingerprint for --name-format: .*`)
		c.Assert(got, qt.DeepEquals, atlasreport.SchemaPlanName{})
	})
}

func TestRenderSchemaPlanNameErrors(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "unterminated action", format: "plan_{{ .ToHash", want: `parse --name-format template: .*`},
		{name: "unknown function", format: "{{ nosuchfunc .ToHash }}", want: `parse --name-format template: .*`},
		{name: "unknown field", format: "{{ .Nope }}", want: `execute --name-format template: .*Nope.*`},
		{name: "slice out of range", format: "{{ slice .ToHash 0 999 }}", want: `execute --name-format template: .*`},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			got, err := atlasreport.RenderSchemaPlanName(tt.format, atlasreport.SchemaPlanName{ToHash: "sha256:abc"})

			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestValidateSchemaPlanNameTemplateSeparatesParseFromExecute(t *testing.T) {
	c := qt.New(t)

	// Validation is a parse, so it accepts a template that only fails once it
	// meets its data. That is the intended split: the command validates before
	// connecting to a database, then reports execution failures later.
	c.Assert(atlasreport.ValidateSchemaPlanNameTemplate("plan_{{ .Nope }}"), qt.IsNil)
	c.Assert(atlasreport.ValidateSchemaPlanNameTemplate("plan_{{ slice .ToHash 0 8 }}"), qt.IsNil)
	c.Assert(atlasreport.ValidateSchemaPlanNameTemplate("plan_{{ .ToHash"), qt.ErrorMatches,
		`parse --name-format template: .*`)
}
