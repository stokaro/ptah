package migrator_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/migrator"
)

func TestParseMigrationTxMode_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  migrator.MigrationTxMode
	}{
		{name: "empty defaults to file", value: "", want: migrator.MigrationTxModeFile},
		{name: "file", value: "file", want: migrator.MigrationTxModeFile},
		{name: "all", value: "all", want: migrator.MigrationTxModeAll},
		{name: "none", value: "none", want: migrator.MigrationTxModeNone},
		{name: "case and whitespace", value: " None ", want: migrator.MigrationTxModeNone},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := migrator.ParseMigrationTxMode(tc.value)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

func TestParseMigrationTxMode_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := migrator.ParseMigrationTxMode("statement")

	c.Assert(err, qt.ErrorMatches, `invalid tx-mode "statement": expected file, all, or none`)
	c.Assert(got, qt.Equals, migrator.MigrationTxMode(""))
}
