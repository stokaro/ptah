package descriptor

import "fmt"

// Migration states how migration files are read, replayed and generated for
// the engine where engines differ. The zero value takes no part in a read of
// directives whose dialect is unknown, restores no session setting after a
// replay and lets a generated ALTER TYPE ... ADD VALUE share a transaction.
type Migration struct {
	// DirectivesWithoutDialect reports that the engine's reading of a
	// migration file decides its directives when the file's dialect is not
	// known, as on a migration import that names no target dialect. Such a
	// read keeps a directive only where the lexer of every engine stating it
	// sees the same directive at the same place, so a line that one of them
	// reads inside a string stays SQL. An engine whose text the shared lexer
	// cannot read as the engine does, such as one with alternative quoting,
	// cannot state it.
	DirectivesWithoutDialect bool `json:"directives_without_dialect,omitempty"`
	// ReplayedSession are the session settings a migration may change and a
	// replay of a migration directory restores on its session afterwards, in
	// the order they are captured.
	ReplayedSession []SessionSetting `json:"replayed_session,omitempty"`
	// EnumValuesCommitFirst reports that a value added to an existing enum
	// type cannot be used until the transaction that added it commits, as on
	// PostgreSQL, which answers SQLSTATE 55P04. A generated ALTER TYPE ... ADD
	// VALUE always adds to an existing type, so it runs outside the migration's
	// transaction, before the statements that use the value. Only an engine
	// that keeps an enum a type of its own (an empty [Preparation.Enums]) can
	// state it.
	EnumValuesCommitFirst bool `json:"enum_values_commit_first,omitempty"`
}

// SessionSetting is one boolean setting of a session, read before a replay
// and set back to the value read when the replay ends.
type SessionSetting struct {
	// Name says what the setting is, in the words an error names it in, such
	// as "SQLite foreign key state".
	Name string `json:"name"`
	// Query selects the setting's value as one boolean, such as
	// "PRAGMA foreign_keys".
	Query string `json:"query"`
	// On is the statement that sets the setting to true.
	On string `json:"on"`
	// Off is the statement that sets the setting to false.
	Off string `json:"off"`
}

// validateMigration checks the fields declared in this file.
func (d Dialect) validateMigration() error {
	migration := d.Migration
	if migration.DirectivesWithoutDialect && d.Lexical.AlternativeQuoting {
		return fmt.Errorf("%w: dialect %q: the shared lexer does not read alternative quoting, so the engine's reading cannot decide directives without a dialect",
			ErrInvalidDescriptor, d.Name)
	}
	if migration.EnumValuesCommitFirst && d.Preparation.Enums != "" {
		return fmt.Errorf("%w: dialect %q: enum values commit first on an engine whose enum is %q rather than a type of its own",
			ErrInvalidDescriptor, d.Name, d.Preparation.Enums)
	}
	names := make(map[string]bool, len(migration.ReplayedSession))
	for _, setting := range migration.ReplayedSession {
		if setting.Name == "" || setting.Query == "" || setting.On == "" || setting.Off == "" {
			return fmt.Errorf("%w: dialect %q: replayed session setting %+v needs a name, a query and both statements",
				ErrInvalidDescriptor, d.Name, setting)
		}
		if names[setting.Name] {
			return fmt.Errorf("%w: dialect %q: replayed session setting %q is stated twice",
				ErrInvalidDescriptor, d.Name, setting.Name)
		}
		names[setting.Name] = true
	}
	return nil
}
