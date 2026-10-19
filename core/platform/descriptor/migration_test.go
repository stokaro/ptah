package descriptor_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/descriptor"
)

// migrating is a descriptor carrying every field migration.go declares, each
// at a value the checks accept.
func migrating() descriptor.Dialect {
	described := head("alpha")
	described.Migration = descriptor.Migration{
		DirectivesWithoutDialect: true,
		ReplayedSession: []descriptor.SessionSetting{
			{Name: "alpha checks", Query: "SHOW checks", On: "SET checks = 1", Off: "SET checks = 0"},
			{Name: "alpha triggers", Query: "SHOW triggers", On: "SET triggers = 1", Off: "SET triggers = 0"},
		},
		EnumValuesCommitFirst: true,
	}
	return described
}

func TestNewRegistry_Migration_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		described descriptor.Dialect
	}{
		{name: "every field at a value the checks accept", described: migrating()},
		{name: "an engine that reads its own lexer decides directives", described: withMigration(func(d *descriptor.Dialect) {
			d.Lexical = descriptor.Lexical{Lexer: descriptor.LexerYQL}
		})},
		{name: "alternative quoting without directives", described: withMigration(func(d *descriptor.Dialect) {
			d.Lexical.AlternativeQuoting = true
			d.Migration.DirectivesWithoutDialect = false
		})},
		{name: "an enum column type without enum values", described: withMigration(func(d *descriptor.Dialect) {
			d.Preparation.Enums = descriptor.EnumColumnType
			d.Migration.EnumValuesCommitFirst = false
		})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.described)

			c.Assert(err, qt.IsNil)
			described, found := registry.Lookup("alpha")
			c.Assert(found, qt.IsTrue)
			c.Assert(described, qt.DeepEquals, test.described)
		})
	}
}

func TestNewRegistry_Migration_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		edit    descriptor.Dialect
		wantErr string
	}{
		{
			name:    "directives without a dialect on an engine with alternative quoting",
			edit:    withMigration(func(d *descriptor.Dialect) { d.Lexical.AlternativeQuoting = true }),
			wantErr: `invalid dialect descriptor: dialect "alpha": the shared lexer does not read alternative quoting, so the engine's reading cannot decide directives without a dialect`,
		},
		{
			name:    "enum values on an engine whose enum is a column type",
			edit:    withMigration(func(d *descriptor.Dialect) { d.Preparation.Enums = descriptor.EnumCheck }),
			wantErr: `invalid dialect descriptor: dialect "alpha": enum values commit first on an engine whose enum is "check" rather than a type of its own`,
		},
		{
			name:    "a session setting without a name",
			edit:    withMigration(func(d *descriptor.Dialect) { d.Migration.ReplayedSession[0].Name = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": replayed session setting \{Name: Query:SHOW checks On:SET checks = 1 Off:SET checks = 0\} needs a name, a query and both statements`,
		},
		{
			name:    "a session setting without a query",
			edit:    withMigration(func(d *descriptor.Dialect) { d.Migration.ReplayedSession[0].Query = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": replayed session setting \{Name:alpha checks Query: On:SET checks = 1 Off:SET checks = 0\} needs a name, a query and both statements`,
		},
		{
			name:    "a session setting without the statement that turns it on",
			edit:    withMigration(func(d *descriptor.Dialect) { d.Migration.ReplayedSession[0].On = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": replayed session setting \{Name:alpha checks Query:SHOW checks On: Off:SET checks = 0\} needs a name, a query and both statements`,
		},
		{
			name:    "a session setting without the statement that turns it off",
			edit:    withMigration(func(d *descriptor.Dialect) { d.Migration.ReplayedSession[0].Off = "" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": replayed session setting \{Name:alpha checks Query:SHOW checks On:SET checks = 1 Off:\} needs a name, a query and both statements`,
		},
		{
			name:    "a session setting stated twice",
			edit:    withMigration(func(d *descriptor.Dialect) { d.Migration.ReplayedSession[1].Name = "alpha checks" }),
			wantErr: `invalid dialect descriptor: dialect "alpha": replayed session setting "alpha checks" is stated twice`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			registry, err := descriptor.NewRegistry(test.edit)

			c.Assert(err, qt.ErrorIs, descriptor.ErrInvalidDescriptor)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(registry, qt.IsNil)
		})
	}
}

// withMigration returns migrating with one change applied, so each row
// carries the one value it is about.
func withMigration(change func(*descriptor.Dialect)) descriptor.Dialect {
	described := migrating()
	change(&described)
	return described
}
