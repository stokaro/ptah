// Package registration_test drives platform.Register, which changes the
// process registry for the rest of the process.
//
// The directory holds only this test so that the registrations stay in a
// test binary of their own. Beside core/platform they would be visible to every
// other test there, such as the ones that list DialectSpellings.
package registration_test

import (
	"fmt"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin"
)

func described(name, family string, aliases ...string) descriptor.Dialect {
	return descriptor.Dialect{
		Name:          name,
		Aliases:       aliases,
		Family:        family,
		DefaultSchema: "app",
		Identifiers:   descriptor.Identifiers{IndexNamespace: "schema", IndexNames: "exact", TableNames: "ascii_case_insensitive", ColumnNames: "exact"},
		Placeholder:   descriptor.PlaceholderDollar,
	}
}

// TestRegister_HappyPath registers an engine Ptah does not ship, in the
// PostgreSQL family, and reads it through every package-level function that
// reads the process registry.
func TestRegister_HappyPath(t *testing.T) {
	c := qt.New(t)
	before := platform.Dialects()
	builtins := len(before.Dialects())

	err := platform.Register(described("acme", platform.Postgres, "acmedb"))

	c.Assert(err, qt.IsNil)
	c.Assert(platform.NormalizeDialect(" ACMEDB "), qt.Equals, "acme")
	c.Assert(platform.IsPostgresFamily("acmedb"), qt.IsTrue)
	c.Assert(platform.DialectSpellings()[len(before.Spellings()):], qt.DeepEquals, []string{"acme", "acmedb"})
	c.Assert(identifier.ForDialect("acmedb"), qt.DeepEquals, identifier.Semantics{
		DefaultSchema:  "app",
		IndexNamespace: identifier.IndexNamespaceSchema,
		IndexNames:     identifier.ComparisonExact,
		TableNames:     identifier.ComparisonASCIIInsensitive,
		ColumnNames:    identifier.ComparisonExact,
	})
	c.Assert(platform.Dialects().Dialects()[builtins:], qt.DeepEquals, []descriptor.Dialect{described("acme", platform.Postgres, "acmedb")})
	// The snapshot taken before is a registry of its own and does not change.
	c.Assert(before.Normalize("acme"), qt.Equals, "")
	c.Assert(platform.NormalizeDialect("pgx"), qt.Equals, platform.Postgres)
}

// TestRegister_TheSameDescriptorTwice pins that registering a provider's
// descriptors again, as a second runtime of one program would, is accepted
// and adds nothing.
func TestRegister_TheSameDescriptorTwice(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Register(described("twice", "")), qt.IsNil)
	spellings := platform.DialectSpellings()

	err := platform.Register(described("twice", ""))

	c.Assert(err, qt.IsNil)
	c.Assert(platform.DialectSpellings(), qt.DeepEquals, spellings)
}

// TestRegister_IsNotABuiltIn pins that a registered dialect gets no bundled
// provider: engine/builtin assembles the engines Ptah ships, not whatever the
// process registry holds.
func TestRegister_IsNotABuiltIn(t *testing.T) {
	c := qt.New(t)
	c.Assert(platform.Register(described("outside", "")), qt.IsNil)
	runtime, err := builtin.New()
	c.Assert(err, qt.IsNil)

	_, err = runtime.ResolveTarget("outside")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
}

func TestRegister_FailurePath(t *testing.T) {
	malformed := described("malformed", "")
	malformed.Placeholder = ""
	tests := []struct {
		name     string
		dialects []descriptor.Dialect
		wantIs   error
		wantErr  string
	}{
		{name: "an alias a built-in owns", dialects: []descriptor.Dialect{described("refused-first", ""), described("refused-second", "", "pgx")},
			wantIs: descriptor.ErrConflict, wantErr: `register dialects: conflicting dialect descriptor: dialect "refused-second" claims "pgx", which dialect "postgres" already owns`},
		{name: "a built-in described differently", dialects: []descriptor.Dialect{described("refused-first", ""), described(platform.YDB, "")},
			wantIs: descriptor.ErrConflict, wantErr: `register dialects: conflicting dialect descriptor: dialect "ydb" is already described differently`},
		{name: "a malformed descriptor", dialects: []descriptor.Dialect{described("refused-first", ""), malformed},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `register dialects: invalid dialect descriptor: dialect "malformed": placeholder "" is not one of .*`},
		{name: "a family no registered dialect heads", dialects: []descriptor.Dialect{described("refused-first", "nobody")},
			wantIs: descriptor.ErrInvalidDescriptor, wantErr: `register dialects: invalid dialect descriptor: dialect "refused-first" names family "nobody", which is no dialect's name`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			spellings := platform.DialectSpellings()

			err := platform.Register(test.dialects...)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			// Nothing of a refused call is added, not even the descriptors
			// before the refused one.
			c.Assert(platform.DialectSpellings(), qt.DeepEquals, spellings)
			c.Assert(platform.NormalizeDialect("refused-first"), qt.Equals, "")
		})
	}
}

// TestRegister_Concurrently registers from several goroutines while others
// read. Run under -race, it is the check that readers and Register share no
// unsynchronized state; without it, it checks that no registration is lost.
func TestRegister_Concurrently(t *testing.T) {
	c := qt.New(t)
	var group sync.WaitGroup
	errs := make([]error, 16)
	for i := range errs {
		group.Go(func() {
			errs[i] = platform.Register(described(fmt.Sprintf("concurrent-%d", i), ""))
		})
		group.Go(func() {
			_ = platform.NormalizeDialect(fmt.Sprintf("concurrent-%d", i))
			_ = identifier.ForDialect(platform.Postgres)
		})
	}

	group.Wait()

	c.Assert(errs, qt.DeepEquals, make([]error, 16))
	for i := range errs {
		c.Assert(platform.NormalizeDialect(fmt.Sprintf("concurrent-%d", i)), qt.Equals, fmt.Sprintf("concurrent-%d", i))
	}
}
