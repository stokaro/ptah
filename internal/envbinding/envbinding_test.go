package envbinding_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/pflag"

	"ptah.run/internal/envbinding"
)

// listRule is the rule of a schema list.
var listRule = envbinding.EmptyRule{Omitted: "select every schema", List: true}

func TestRefuseEmpty_HappyPath(t *testing.T) {
	rules := []envbinding.EmptyRule{
		listRule,
		{Omitted: "use the default"},
		{Omitted: "stop at --limit", Unless: []string{"limit", "to-tag"}},
		{Omitted: "select every schema", List: true, Unless: []string{"schema"}},
	}
	for _, rule := range rules {
		t.Run(rule.Omitted, func(t *testing.T) {
			c := qt.New(t)
			flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
			flags.String("schemas", "", "Schemas")
			flag := flags.Lookup("schemas")

			err := envbinding.RefuseEmpty(flag, rule)
			got, refused := envbinding.EmptyRefused(flag)

			c.Assert(err, qt.IsNil)
			c.Assert(refused, qt.IsTrue)
			c.Assert(got, qt.DeepEquals, rule)
		})
	}
}

// TestRefuseEmpty_FailurePath refuses a rule that would refuse nothing: with
// no Omitted, Check returns nil for every value, so the declaration would
// read as in effect while the empty value still reads as unset.
func TestRefuseEmpty_FailurePath(t *testing.T) {
	c := qt.New(t)
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("schemas", "", "Schemas")
	flag := flags.Lookup("schemas")

	err := envbinding.RefuseEmpty(flag, envbinding.EmptyRule{List: true})
	rule, refused := envbinding.EmptyRefused(flag)

	c.Assert(err, qt.ErrorMatches, `--schemas: the rule names nothing that leaving the flag out does, so it would refuse nothing`)
	c.Assert(refused, qt.IsFalse)
	c.Assert(rule, qt.DeepEquals, envbinding.EmptyRule{})
}

// TestEmptyRefused_UndeclaredFlag is the control: a flag nobody declared, and
// no flag at all, refuse nothing.
func TestEmptyRefused_UndeclaredFlag(t *testing.T) {
	c := qt.New(t)
	flags := pflag.NewFlagSet("test", pflag.ContinueOnError)
	flags.String("schemas", "", "Schemas")

	rule, refused := envbinding.EmptyRefused(flags.Lookup("schemas"))
	c.Assert(refused, qt.IsFalse)
	c.Assert(rule, qt.DeepEquals, envbinding.EmptyRule{})

	rule, refused = envbinding.EmptyRefused(nil)
	c.Assert(refused, qt.IsFalse)
	c.Assert(rule, qt.DeepEquals, envbinding.EmptyRule{})
}

func TestEmptyRuleCheck_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		rule    envbinding.EmptyRule
		value   string
		wantErr string
	}{
		{
			name:    "a list, empty",
			rule:    listRule,
			wantErr: `PTAH_SCHEMA is set but empty: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "a list, a space",
			rule:    listRule,
			value:   " ",
			wantErr: `PTAH_SCHEMA is set to " ", which names nothing: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "a list, a comma",
			rule:    listRule,
			value:   ",",
			wantErr: `PTAH_SCHEMA is set to ",", which names nothing: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "a list, blank fields",
			rule:    listRule,
			value:   " , ",
			wantErr: `PTAH_SCHEMA is set to " , ", which names nothing: give it a value, or unset it and leave --schema out to select every schema`,
		},
		{
			name:    "a single value, empty",
			rule:    envbinding.EmptyRule{Omitted: "use the default"},
			wantErr: `PTAH_SCHEMA is set but empty: give it a value, or unset it and leave --schema out to use the default`,
		},
		{
			name:    "a single value, spaces",
			rule:    envbinding.EmptyRule{Omitted: "use the default"},
			value:   "  ",
			wantErr: `PTAH_SCHEMA is set to "  ", which names nothing: give it a value, or unset it and leave --schema out to use the default`,
		},
		{
			name:    "a single value, a tab",
			rule:    envbinding.EmptyRule{Omitted: "use the default"},
			value:   "\t",
			wantErr: `PTAH_SCHEMA is set to "\t", which names nothing: give it a value, or unset it and leave --schema out to use the default`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.rule.Check("PTAH_SCHEMA", "schema", test.value)

			c.Assert(err, qt.ErrorIs, envbinding.ErrEmptyValue)
			c.Assert(err, qt.ErrorMatches, regexp.QuoteMeta(test.wantErr))
		})
	}
}

// TestEmptyRuleCheck_HappyPath holds the refusal to values that name nothing.
// A single value is not split, so a comma between spaces is a value there;
// that row is the control for the list reading.
func TestEmptyRuleCheck_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		rule  envbinding.EmptyRule
		value string
	}{
		{name: "a name", rule: listRule, value: "app"},
		{name: "a name in spaces", rule: listRule, value: " app "},
		{name: "a name after a blank field", rule: listRule, value: ",app"},
		{name: "a name before a blank field", rule: listRule, value: "app,"},
		{name: "a quoted empty name is left to the flag", rule: listRule, value: `""`},
		{name: "a single value holding blank fields", rule: envbinding.EmptyRule{Omitted: "use the default"}, value: " , "},
		{name: "a single value in spaces", rule: envbinding.EmptyRule{Omitted: "use the default"}, value: " 1 "},
		{name: "a single zero", rule: envbinding.EmptyRule{Omitted: "use the default"}, value: "0"},
		{name: "the zero rule", value: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.rule.Check("PTAH_SCHEMA", "schema", test.value), qt.IsNil)
		})
	}
}
