package capability_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

func TestIdentifiers_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		wantMax  int
		wantUnit capability.IdentifierUnit
	}{
		{name: "postgres", dialect: platform.Postgres, wantMax: 63, wantUnit: capability.IdentifierBytes},
		{name: "cockroachdb", dialect: platform.CockroachDB, wantMax: 63, wantUnit: capability.IdentifierBytes},
		{name: "yugabytedb", dialect: platform.YugabyteDB, wantMax: 63, wantUnit: capability.IdentifierBytes},
		{name: "mysql", dialect: platform.MySQL, wantMax: 64, wantUnit: capability.IdentifierCharacters},
		{name: "mariadb", dialect: platform.MariaDB, wantMax: 64, wantUnit: capability.IdentifierCharacters},
		{name: "sqlserver", dialect: platform.SQLServer, wantMax: 128, wantUnit: capability.IdentifierCharacters},
		{name: "spanner", dialect: platform.Spanner, wantMax: 128, wantUnit: capability.IdentifierCharacters},
		{name: "unnormalized spelling", dialect: "PostgreSQL", wantMax: 63, wantUnit: capability.IdentifierBytes},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			limit := capability.Identifiers(test.dialect)
			c.Assert(limit.Max, qt.Equals, test.wantMax)
			c.Assert(limit.Unit, qt.Equals, test.wantUnit)
			c.Assert(limit.Unlimited(), qt.IsFalse)
		})
	}
}

// TestIdentifiers_UnlimitedDialects pins the dialects for which Ptah models no
// limit. They are a separate test rather than zero-valued rows above because
// the claim is different in kind: not "the limit is zero" but "this repository
// has no limit to state", which is what the zero value is spelling.
func TestIdentifiers_UnlimitedDialects(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "clickhouse", dialect: platform.ClickHouse},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "unknown dialect", dialect: "oracle"},
		{name: "empty dialect", dialect: ""},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			limit := capability.Identifiers(test.dialect)
			c.Assert(limit.Unlimited(), qt.IsTrue)
			c.Assert(limit.Max, qt.Equals, 0)
			c.Assert(limit.String(), qt.Equals, "unlimited")
			c.Assert(limit.Exceeds(strings.Repeat("a", 4096)), qt.IsFalse)
		})
	}
}

// TestIdentifierLimit_Exceeds_CountsTheDeclaredUnit is the discriminating test:
// each row is a name that one unit accepts and the other refuses, so a rule
// that counted the wrong thing cannot pass it. A row whose verdict is the same
// under both units would pass whatever the code does.
func TestIdentifierLimit_Exceeds_CountsTheDeclaredUnit(t *testing.T) {
	tests := []struct {
		name  string
		limit capability.IdentifierLimit
		value string
		want  bool
	}{
		{
			name:  "63 bytes refuses 32 two-byte characters",
			limit: capability.IdentifierLimit{Max: 63, Unit: capability.IdentifierBytes},
			value: strings.Repeat("é", 32),
			want:  true,
		},
		{
			name:  "63 characters accepts the same 32 two-byte characters",
			limit: capability.IdentifierLimit{Max: 63, Unit: capability.IdentifierCharacters},
			value: strings.Repeat("é", 32),
			want:  false,
		},
		{
			name:  "128 characters accepts 100 three-byte characters",
			limit: capability.IdentifierLimit{Max: 128, Unit: capability.IdentifierCharacters},
			value: strings.Repeat("界", 100),
			want:  false,
		},
		{
			name:  "128 bytes refuses the same 100 three-byte characters",
			limit: capability.IdentifierLimit{Max: 128, Unit: capability.IdentifierBytes},
			value: strings.Repeat("界", 100),
			want:  true,
		},
		{
			name:  "the limit itself fits",
			limit: capability.IdentifierLimit{Max: 63, Unit: capability.IdentifierBytes},
			value: strings.Repeat("a", 63),
			want:  false,
		},
		{
			name:  "one past the limit does not",
			limit: capability.IdentifierLimit{Max: 63, Unit: capability.IdentifierBytes},
			value: strings.Repeat("a", 64),
			want:  true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.limit.Exceeds(test.value), qt.Equals, test.want)
		})
	}
}

func TestIdentifierLimit_String_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		limit capability.IdentifierLimit
		want  string
	}{
		{name: "bytes", limit: capability.IdentifierLimit{Max: 63, Unit: capability.IdentifierBytes}, want: "63 bytes"},
		{name: "characters", limit: capability.IdentifierLimit{Max: 128, Unit: capability.IdentifierCharacters}, want: "128 characters"},
		{name: "zero value", limit: capability.IdentifierLimit{}, want: "unlimited"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.limit.String(), qt.Equals, test.want)
		})
	}
}

// TestEnumModeling_ReadsThePresets asserts the mode against every preset rather
// than against hand-built sets. The derivation is only worth having if it
// agrees with the sets Ptah actually ships, and a hand-built set proves the
// switch works while saying nothing about what MariaDB resolves to.
func TestEnumModeling_ReadsThePresets(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    capability.EnumMode
	}{
		{name: "postgres declares a named type", dialect: platform.Postgres, want: capability.EnumNamedType},
		{name: "cockroachdb declares a named type", dialect: platform.CockroachDB, want: capability.EnumNamedType},
		{name: "mysql spells values inline", dialect: platform.MySQL, want: capability.EnumInline},
		{name: "mariadb spells values inline", dialect: platform.MariaDB, want: capability.EnumInline},
		{name: "clickhouse spells values inline", dialect: platform.ClickHouse, want: capability.EnumInline},
		{name: "sqlite models enums neither way", dialect: platform.SQLite, want: capability.EnumUnsupported},
		{name: "sqlserver models enums neither way", dialect: platform.SQLServer, want: capability.EnumUnsupported},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(capability.ForDialect(test.dialect).EnumModeling(), qt.Equals, test.want)
		})
	}
}

func TestForeignKeyReference_ReadsThePresets(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    capability.ReferencePolicy
	}{
		{name: "postgres requires a unique reference", dialect: platform.Postgres, want: capability.ReferenceUnique},
		{name: "mysql requires a unique reference", dialect: platform.MySQL, want: capability.ReferenceUnique},
		{name: "mariadb requires an indexed reference", dialect: platform.MariaDB, want: capability.ReferenceIndexed},
		{name: "spanner creates the backing index", dialect: platform.Spanner, want: capability.ReferenceBackingIndex},
		{name: "clickhouse has no foreign keys", dialect: platform.ClickHouse, want: capability.ReferenceUnsupported},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(capability.ForDialect(test.dialect).ForeignKeyReference(), qt.Equals, test.want)
		})
	}
}

// TestTraits_NilSetIsConservative pins the nil-set reading against the same
// promise Has makes: absent everything. A nil set arriving somewhere it should
// not is a bug to find elsewhere; answering it with a mode nobody declared
// would be a bug here.
func TestTraits_NilSetIsConservative(t *testing.T) {
	c := qt.New(t)

	traits := capability.TraitsFor(platform.Postgres, nil)

	c.Assert(traits.EnumModeling, qt.Equals, capability.EnumUnsupported)
	c.Assert(traits.ForeignKeyReference, qt.Equals, capability.ReferenceUnsupported)
	c.Assert(traits.Identifiers.Max, qt.Equals, 63)
	c.Assert(traits.Identifiers.Unit, qt.Equals, capability.IdentifierBytes)
}

// TestTraits_ContradictorySetsResolveToUnspecified covers the two readings that
// exist only for a set Validate rejects. They report the zero value rather than
// choosing one of the claims, because choosing would hand a caller a mode the
// set never named.
func TestTraits_ContradictorySetsResolveToUnspecified(t *testing.T) {
	t.Run("both enum modes at once", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.Capabilities{
			capability.EnumInlineColumn: true,
			capability.EnumCustomType:   true,
		}

		c.Assert(caps.Validate(), qt.IsNotNil)
		c.Assert(caps.EnumModeling(), qt.Equals, capability.EnumUnspecified)
	})

	t.Run("two reference policies at once", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.Capabilities{
			capability.ForeignKeys:                        true,
			capability.ForeignKeysRequireUniqueReference:  true,
			capability.ForeignKeysRequireIndexedReference: true,
		}

		c.Assert(caps.Validate(), qt.IsNotNil)
		c.Assert(caps.ForeignKeyReference(), qt.Equals, capability.ReferenceUnspecified)
	})

	t.Run("foreign keys with no policy at all", func(t *testing.T) {
		c := qt.New(t)
		caps := capability.Capabilities{capability.ForeignKeys: true}

		c.Assert(caps.Validate(), qt.IsNotNil)
		c.Assert(caps.ForeignKeyReference(), qt.Equals, capability.ReferenceUnspecified)
	})
}

// The census that keeps referencePolicyNames in step with the policy list lives
// in traits_internal_test.go rather than here. From outside the package the
// list can only be retyped by hand, and a hand-typed list is the one place a
// fourth policy is guaranteed not to appear: the literal that used to stand
// here stayed green through a fourth policy added to the registry, to the mutex
// group, to foreignKeyReferencePolicies and to every preset.
