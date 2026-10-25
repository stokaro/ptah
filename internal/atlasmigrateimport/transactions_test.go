package atlasmigrateimport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/atlasmigrateimport"
)

// golang-migrate is the one layout whose tool opens no transaction around a
// file. Every other layout, and a value no layout spells, answers the zero
// value: a migrator that wraps each file.
func TestFormat_TransactionOwner(t *testing.T) {
	for _, test := range []struct {
		name   string
		format atlasmigrateimport.Format
		want   lintext.TransactionOwner
	}{
		{"golang-migrate", atlasmigrateimport.FormatGolangMigrate, lintext.TransactionOwnerFile},
		{"atlas", atlasmigrateimport.FormatAtlas, lintext.TransactionOwnerMigrator},
		{"goose", atlasmigrateimport.FormatGoose, lintext.TransactionOwnerMigrator},
		{"flyway", atlasmigrateimport.FormatFlyway, lintext.TransactionOwnerMigrator},
		{"liquibase", atlasmigrateimport.FormatLiquibase, lintext.TransactionOwnerMigrator},
		{"dbmate", atlasmigrateimport.FormatDBMate, lintext.TransactionOwnerMigrator},
		{"no layout", atlasmigrateimport.Format(""), lintext.TransactionOwnerMigrator},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.format.TransactionOwner(), qt.Equals, test.want)
		})
	}
}
