package repaircommand_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/repaircommand"
)

func TestRepair_HappyPath(t *testing.T) {
	tests := []struct {
		name           string
		version, flags string
		runner         repaircommand.Runner
		selection      repaircommand.Selection
		want           string
	}{
		{name: "plain", version: "2", want: "'ptah migrations repair --version 2'"},
		{
			name: "resume", version: "000001", flags: " --resume-from 2",
			want: "'ptah migrations repair --version 000001 --resume-from 2'",
		},
		{
			name: "own transactions", version: "v1", flags: " --force", runner: repaircommand.GolangMigrate,
			want: "'ptah migrations repair --version v1 --force --runner golang-migrate'",
		},
		{name: "dash", version: "-5", want: "'ptah migrations repair --version -5'"},
		{
			name: "space", version: "release candidate", flags: " --force",
			want: "'ptah migrations repair --version 'release candidate' --force'",
		},
		{name: "semicolon", version: "1;rm", want: "'ptah migrations repair --version '1;rm''"},
		{name: "single quote", version: "it's", want: `'ptah migrations repair --version 'it'\''s''`},
		{name: "dollar", version: "$HOME", want: "'ptah migrations repair --version '$HOME''"},
		{
			name: "atlas selection", version: "v1", flags: " --force", runner: repaircommand.GolangMigrate,
			selection: repaircommand.AtlasDirectory + repaircommand.AtlasTable,
			want: "'ptah migrations repair --version v1 --force --runner golang-migrate " +
				"--dir-format atlas --revision-format atlas'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(repaircommand.Repair(tt.version, tt.flags, tt.runner, tt.selection), qt.Equals, tt.want)
		})
	}
}

func TestReconciled_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(repaircommand.Reconciled("2", repaircommand.DefaultRunner, ""), qt.Equals,
		"'ptah migrations repair --version 2 --force'")
	c.Assert(repaircommand.Reconciled("2", repaircommand.GolangMigrate, ""), qt.Equals,
		"'ptah migrations repair --version 2 --force --runner golang-migrate'")
}

// TestSetPrevious_HappyPath carries each selection last.
func TestSetPrevious_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		selection repaircommand.Selection
		want      string
	}{
		{name: "defaults", want: "'ptah migrations set --version <previous>'"},
		{name: "table", selection: repaircommand.AtlasTable, want: "'ptah migrations set --version <previous> --revision-format atlas'"},
		{
			name: "both", selection: repaircommand.AtlasDirectory + repaircommand.AtlasTable,
			want: "'ptah migrations set --version <previous> --dir-format atlas --revision-format atlas'",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(repaircommand.SetPrevious(tt.selection), qt.Equals, tt.want)
		})
	}
}
