package migrateflags_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/cli/internal/migrateflags"
)

// A runner names the tool a directory is written for, and golang-migrate is
// the one whose files open their own transactions.
func TestParseRunner_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		in    string
		want  migrateflags.Runner
		owner lintext.TransactionOwner
	}{
		{name: "default", in: "", want: migrateflags.RunnerPtah, owner: lintext.TransactionOwnerMigrator},
		{name: "ptah", in: "ptah", want: migrateflags.RunnerPtah, owner: lintext.TransactionOwnerMigrator},
		{name: "golang-migrate", in: "golang-migrate", want: migrateflags.RunnerGolangMigrate, owner: lintext.TransactionOwnerFile},
		{name: "trim and case", in: " Golang-Migrate ", want: migrateflags.RunnerGolangMigrate, owner: lintext.TransactionOwnerFile},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := migrateflags.ParseRunner(test.in)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
			c.Assert(got.TransactionOwner(), qt.Equals, test.owner)
		})
	}
}

// A tool whose way of running a file Ptah does not model is refused rather
// than read as Ptah's, which would judge its files for a runner they are not
// written for.
func TestParseRunner_FailurePath(t *testing.T) {
	for _, in := range []string{"goose", "atlas", "golang_migrate"} {
		t.Run(in, func(t *testing.T) {
			c := qt.New(t)

			got, err := migrateflags.ParseRunner(in)

			c.Assert(err, qt.ErrorMatches, `invalid runner "`+in+`": expected ptah or golang-migrate`)
			c.Assert(got, qt.Equals, migrateflags.Runner(""))
		})
	}
}
