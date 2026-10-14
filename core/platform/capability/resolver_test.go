package capability_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/descriptor"
)

// acme describes an engine Ptah does not ship, by data alone: a family head
// with a ladder of two steps and an identifier limit.
func acme() descriptor.Dialect {
	return descriptor.Dialect{
		Name:        "acme",
		Family:      "acme",
		Placeholder: descriptor.PlaceholderQuestion,
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "MySQL84",
			BannerTokens:  []string{"acme server"},
			Versions: descriptor.Versions{
				Scheme: "dotted",
				Floor:  "MySQLLegacy",
				Steps: []descriptor.VersionStep{
					{Min: "2.0", Preset: "MySQL8016"},
					{Min: "3.1", Preset: "MySQL84"},
				},
				Attribution: "measured-lines",
				Measured:    []string{"2.0", "3.1"},
				Newest:      "3.1",
			},
		},
		IdentifierLimit: descriptor.IdentifierLimit{Max: 30, Unit: "bytes"},
	}
}

// acmeCloud is a member of the acme family with one preset and no ladder,
// whose banner also names the head of its family.
func acmeCloud() descriptor.Dialect {
	return descriptor.Dialect{
		Name:        "acmecloud",
		Aliases:     []string{"acme-cloud"},
		Family:      "acme",
		Placeholder: descriptor.PlaceholderQuestion,
		Capabilities: descriptor.Capabilities{
			DefaultPreset: "SpannerPostgres",
			BannerTokens:  []string{"acme cloud"},
		},
	}
}

func acmeResolver(c *qt.C) *capability.Resolver {
	c.Helper()
	registry, err := descriptor.NewRegistry(acme(), acmeCloud())
	c.Assert(err, qt.IsNil)
	resolver, err := capability.NewResolver(registry)
	c.Assert(err, qt.IsNil)
	return resolver
}

// TestNewResolver_ReadsEveryBuiltInDescriptor holds the built-in descriptors
// to the checks an engine Ptah does not ship is held to. The package-level
// functions read them leniently, so a descriptor this refuses would answer
// as one that states nothing, and only this test would say so.
func TestNewResolver_ReadsEveryBuiltInDescriptor(t *testing.T) {
	c := qt.New(t)

	resolver, err := capability.NewResolver(platform.Dialects())

	c.Assert(err, qt.IsNil)
	c.Assert(resolver.Dialects(), qt.HasLen, len(platform.Dialects().Dialects()))
	c.Assert(resolver.Dialects(), qt.DeepEquals, capability.DefaultDialects())
}

// TestResolver_ResolveServerVersion_ReadsTheDescriptor resolves versions of an
// engine no code in this repository names. Every answer comes from the
// descriptors above, which is what lets a new engine be added without editing
// this package.
func TestResolver_ResolveServerVersion_ReadsTheDescriptor(t *testing.T) {
	tests := []struct {
		name                string
		dialect             string
		version             string
		wantCaps            capability.Capabilities
		wantVersionSpecific bool
		wantSaturated       bool
		wantNewestMeasured  string
		wantRecognized      bool
		wantResolvedDialect string
	}{
		{"a banner on its newest measured line", "acme", "Acme Server 3.1.4", capability.MySQL84(), true, false, "3.1", true, "acme"},
		{"a gap between measured lines", "acme", "2.5.0", capability.MySQL8016(), false, false, "3.1", true, "acme"},
		{"below every step", "acme", "1.9", capability.MySQLLegacy(), false, false, "3.1", true, "acme"},
		{"past the newest measured line", "acme", "4.0", capability.MySQL84(), false, true, "3.1", true, "acme"},
		{"no number at all", "acme", "acme", capability.MySQL84(), false, false, "", false, "acme"},
		{"an alias of a member with no ladder", "acme-cloud", "Acme Cloud on acme server 3.1", capability.SpannerPostgres(), true, false, "", true, "acmecloud"},
		{"the head's banner on a member keeps the member", "acmecloud", "acme server 3.1", capability.SpannerPostgres(), false, false, "", true, "acmecloud"},
		{"a banner outranks an unknown dialect", "unknown", "acme server 2.0.3", capability.MySQL8016(), true, false, "3.1", true, "acme"},
		{"an unknown dialect with a bare version", "unknown", "2.0", capability.Capabilities{}, false, false, "", true, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			resolver := acmeResolver(c)

			got := resolver.ResolveServerVersion(tt.dialect, tt.version)

			c.Assert(got.Capabilities, qt.DeepEquals, tt.wantCaps)
			c.Assert(got.VersionSpecific, qt.Equals, tt.wantVersionSpecific)
			c.Assert(got.Saturated, qt.Equals, tt.wantSaturated)
			c.Assert(got.NewestMeasured, qt.Equals, tt.wantNewestMeasured)
			c.Assert(got.Recognized, qt.Equals, tt.wantRecognized)
			c.Assert(got.ResolvedDialect, qt.Equals, tt.wantResolvedDialect)
		})
	}
}

// TestResolver_AnswersTheDescriptorsStaticQuestions covers the answers that
// take no version: the default preset, the identifier limit and the product a
// string names.
func TestResolver_AnswersTheDescriptorsStaticQuestions(t *testing.T) {
	c := qt.New(t)
	resolver := acmeResolver(c)

	c.Assert(resolver.ForDialect(" ACME "), qt.DeepEquals, capability.MySQL84())
	c.Assert(resolver.ForDialect("acme-cloud"), qt.DeepEquals, capability.SpannerPostgres())
	c.Assert(resolver.ForDialect("postgres"), qt.DeepEquals, capability.Capabilities{})
	c.Assert(resolver.Identifiers("acme"), qt.Equals, capability.IdentifierLimit{Max: 30, Unit: capability.IdentifierBytes})
	c.Assert(resolver.Identifiers("acmecloud").Unlimited(), qt.IsTrue)
	c.Assert(resolver.Dialects(), qt.DeepEquals, []string{"acme", "acmecloud"})
	c.Assert(resolver.BannerPlatform("acme server 3.1"), qt.Equals, "acme")
	c.Assert(resolver.BannerPlatform("ACME CLOUD (acme server 3.1)"), qt.Equals, "acmecloud")
	c.Assert(resolver.BannerPlatform("PostgreSQL 16.3"), qt.Equals, "")
}

// TestResolver_BannerPlatform_TriesFamiliesInRegistryOrder pins the order
// banner tokens are tried in: a family at the position of its head, members
// before the head. Each string names two products, which no real banner does
// unless one is the head of the other's family, so the order is the whole
// answer.
func TestResolver_BannerPlatform_TriesFamiliesInRegistryOrder(t *testing.T) {
	product := func(name, family, token string) descriptor.Dialect {
		return descriptor.Dialect{
			Name:         name,
			Family:       family,
			Placeholder:  descriptor.PlaceholderQuestion,
			Capabilities: descriptor.Capabilities{DefaultPreset: "SQLite3", BannerTokens: []string{token}},
		}
	}
	tests := []struct {
		name    string
		version string
		want    string
	}{
		{"a member outranks its head", "head 1 member 2", "member"},
		{"a family registered first outranks a later one", "solo 1 head 2", "head"},
		{"a member outranks a family registered later", "solo 1 member 2", "member"},
		{"a single product", "solo 1", "solo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			registry, err := descriptor.NewRegistry(
				product("head", "head", "head"),
				product("solo", "", "solo"),
				product("member", "head", "member"),
			)
			c.Assert(err, qt.IsNil)
			resolver, err := capability.NewResolver(registry)
			c.Assert(err, qt.IsNil)

			c.Assert(resolver.BannerPlatform(tt.version), qt.Equals, tt.want)
		})
	}
}

// TestNewResolver_HappyPath_NilRegistry answers every question like a
// registry that knows no dialect.
func TestNewResolver_HappyPath_NilRegistry(t *testing.T) {
	c := qt.New(t)

	resolver, err := capability.NewResolver(nil)

	c.Assert(err, qt.IsNil)
	c.Assert(resolver.ForDialect("postgres"), qt.DeepEquals, capability.Capabilities{})
	c.Assert(resolver.Dialects(), qt.HasLen, 0)
	c.Assert(resolver.BannerPlatform("PostgreSQL 16.3"), qt.Equals, "")
	c.Assert(resolver.ResolveServerVersion("postgres", "16.3").Recognized, qt.IsTrue)
}

// TestNewResolver_FailurePath refuses each malformed statement a descriptor
// can make, naming the dialect and the part it got wrong.
func TestNewResolver_FailurePath(t *testing.T) {
	ladder := func(versions descriptor.Versions) descriptor.Capabilities {
		return descriptor.Capabilities{DefaultPreset: "MySQL84", Versions: versions}
	}
	tests := []struct {
		name         string
		capabilities descriptor.Capabilities
		limit        descriptor.IdentifierLimit
		wantErr      string
	}{
		{
			name:         "an unknown default preset",
			capabilities: descriptor.Capabilities{DefaultPreset: "Postgres99"},
			wantErr:      `.*dialect "acme": default preset: preset "Postgres99" is not one this package ships`,
		},
		{
			name:         "a banner token in upper case",
			capabilities: descriptor.Capabilities{DefaultPreset: "MySQL84", BannerTokens: []string{"Acme"}},
			wantErr:      `.*banner token "Acme" must be non-empty lowercase text`,
		},
		{
			name:         "an empty banner token",
			capabilities: descriptor.Capabilities{DefaultPreset: "MySQL84", BannerTokens: []string{""}},
			wantErr:      `.*banner token "" must be non-empty lowercase text`,
		},
		{
			name:         "a banner token with no default preset",
			capabilities: descriptor.Capabilities{BannerTokens: []string{"acme"}},
			wantErr:      `.*banner tokens name a product with no default preset`,
		},
		{
			name: "a ladder with no default preset",
			capabilities: descriptor.Capabilities{Versions: descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest",
			}},
			wantErr: `.*a version ladder needs a default preset`,
		},
		{
			name:         "an undeclared scheme",
			capabilities: ladder(descriptor.Versions{Scheme: "semver", Floor: "MySQL84", Attribution: "through-newest"}),
			wantErr:      `.*versions: scheme "semver" is not one of .*`,
		},
		{
			name:         "an undeclared attribution",
			capabilities: ladder(descriptor.Versions{Scheme: "dotted", Floor: "MySQL84", Attribution: "always"}),
			wantErr:      `.*versions: attribution "always" is not one of .*`,
		},
		{
			name:         "no floor",
			capabilities: ladder(descriptor.Versions{Scheme: "dotted", Attribution: "through-newest"}),
			wantErr:      `.*versions: floor: preset "" is not one this package ships`,
		},
		{
			name: "a step that is not a version",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest",
				Steps: []descriptor.VersionStep{{Min: "2.x", Preset: "MySQL84"}},
			}),
			wantErr: `.*versions: step 0: "2.x" is not a version of one to three numbers`,
		},
		{
			name: "a step of four numbers",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest",
				Steps: []descriptor.VersionStep{{Min: "2.0.0.1", Preset: "MySQL84"}},
			}),
			wantErr: `.*versions: step 0: "2.0.0.1" is not a version of one to three numbers`,
		},
		{
			name: "steps out of order",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest",
				Steps: []descriptor.VersionStep{{Min: "3", Preset: "MySQL84"}, {Min: "2.9", Preset: "MySQL84"}},
			}),
			wantErr: `.*versions: step 1: "2.9" does not follow "3"`,
		},
		{
			name: "two steps at one version",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest",
				Steps: []descriptor.VersionStep{{Min: "3", Preset: "MySQL84"}, {Min: "3.0.0", Preset: "MySQL84"}},
			}),
			wantErr: `.*versions: step 1: "3.0.0" does not follow "3"`,
		},
		{
			name: "a step naming an unknown preset",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest",
				Steps: []descriptor.VersionStep{{Min: "3", Preset: "MySQL99"}},
			}),
			wantErr: `.*versions: step 0: preset "MySQL99" is not one this package ships`,
		},
		{
			name: "an unknown unreadable preset",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest",
				Unreadable: descriptor.UnreadableVersion{Preset: "MySQL99"},
			}),
			wantErr: `.*versions: unreadable: preset "MySQL99" is not one this package ships`,
		},
		{
			name: "a measured line of three numbers",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "measured-lines",
				Measured: []string{"2.0.1"}, Newest: "2.0.1",
			}),
			wantErr: `.*versions: measured line "2.0.1" is not major.minor`,
		},
		{
			name: "a newest line that was not measured",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "measured-lines",
				Measured: []string{"2.0"}, Newest: "3.0",
			}),
			wantErr: `.*versions: newest line "3.0" is not one of the measured lines \["2.0"\]`,
		},
		{
			name: "measured lines with no newest",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "measured-lines", Measured: []string{"2.0"},
			}),
			wantErr: `.*versions: newest line "" is not one of the measured lines \["2.0"\]`,
		},
		{
			name: "measured lines the attribution does not read",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest", Measured: []string{"2.0"},
			}),
			wantErr: `.*versions: attribution "through-newest" reads no measured lines, and \["2.0"\] are stated`,
		},
		{
			name: "a newest major with a minor",
			capabilities: ladder(descriptor.Versions{
				Scheme: "dotted", Floor: "MySQL84", Attribution: "through-newest", Newest: "18.1",
			}),
			wantErr: `.*versions: newest line "18.1" is not a major version`,
		},
		{
			name:         "a ladder with no scheme that states a floor",
			capabilities: ladder(descriptor.Versions{Floor: "MySQL84"}),
			wantErr:      `.*versions: a ladder with no scheme must state nothing else`,
		},
		{
			name:    "a limit with no unit",
			limit:   descriptor.IdentifierLimit{Max: 30},
			wantErr: `.*identifier limit: unit "" is not "bytes" or "characters"`,
		},
		{
			name:    "a limit in an undeclared unit",
			limit:   descriptor.IdentifierLimit{Max: 30, Unit: "runes"},
			wantErr: `.*identifier limit: unit "runes" is not "bytes" or "characters"`,
		},
		{
			name:    "a unit with no limit",
			limit:   descriptor.IdentifierLimit{Unit: "bytes"},
			wantErr: `.*identifier limit: max 0 is not a positive length`,
		},
		{
			name:    "a negative limit",
			limit:   descriptor.IdentifierLimit{Max: -1, Unit: "bytes"},
			wantErr: `.*identifier limit: max -1 is not a positive length`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			described := acme()
			described.Capabilities = tt.capabilities
			described.IdentifierLimit = tt.limit
			registry, err := descriptor.NewRegistry(described)
			c.Assert(err, qt.IsNil)

			resolver, err := capability.NewResolver(registry)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(resolver, qt.IsNil)
		})
	}
}
