package migrationversion_test

import (
	"math"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/migrationversion"
	"go.5x5.cz/ptah/migration/migrator"
)

// The two bounds are different numbers for different reasons, so every row below
// states which one it is exercising. The Atlas layout renders the version with
// %d and only requires it to be positive, so its ceiling is the int64 ceiling;
// the paired layout renders it with %010d and parses exactly ten digits, so its
// ceiling is 9999999999. A single shared ceiling would be wrong in both
// directions: it would refuse the 4611686018427469511 that
// `migrate import --dir-format flyway` writes, or it would accept a paired
// 10000000000 no reader can read.
func TestNext(t *testing.T) {
	tests := []struct {
		name    string
		latest  int64
		format  migrator.MigrationDirFormat
		want    int64
		wantErr bool
	}{
		{name: "ptah below the ceiling", latest: 1786000000, format: migrator.MigrationDirFormatPtah, want: 1786000001},
		{name: "ptah one below the ceiling", latest: 9999999998, format: migrator.MigrationDirFormatPtah, want: 9999999999},
		{name: "ptah at the ceiling", latest: 9999999999, format: migrator.MigrationDirFormatPtah, wantErr: true},
		{name: "ptah past the ceiling", latest: 10000000000, format: migrator.MigrationDirFormatPtah, wantErr: true},
		{name: "auto follows the paired ceiling", latest: 9999999999, format: migrator.MigrationDirFormatAuto, wantErr: true},
		{
			name:   "atlas accepts what the flyway importer writes",
			latest: 4611686018427469511, format: migrator.MigrationDirFormatAtlas, want: 4611686018427469512,
		},
		{
			name:   "atlas is not bounded by the paired ceiling",
			latest: 9999999999, format: migrator.MigrationDirFormatAtlas, want: 10000000000,
		},
		{name: "atlas at the ceiling", latest: math.MaxInt64, format: migrator.MigrationDirFormatAtlas, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			next, err := migrationversion.Next(test.latest, test.format)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("err: %v", err))
			c.Assert(next, qt.Equals, test.want)
		})
	}
}

func TestCheck(t *testing.T) {
	tests := []struct {
		name    string
		version int64
		format  migrator.MigrationDirFormat
		wantErr bool
	}{
		{name: "ptah ordinary", version: 1786000000, format: migrator.MigrationDirFormatPtah},
		{name: "ptah at the ceiling", version: 9999999999, format: migrator.MigrationDirFormatPtah},
		{name: "ptah past the ceiling", version: 10000000000, format: migrator.MigrationDirFormatPtah, wantErr: true},
		{name: "ptah zero", version: 0, format: migrator.MigrationDirFormatPtah, wantErr: true},
		{name: "atlas wrapped", version: math.MinInt64, format: migrator.MigrationDirFormatAtlas, wantErr: true},
		{name: "atlas at the ceiling", version: math.MaxInt64, format: migrator.MigrationDirFormatAtlas},
		{name: "atlas timestamp", version: 20260809042338, format: migrator.MigrationDirFormatAtlas},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := migrationversion.Check(test.version, test.format)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("err: %v", err))
		})
	}
}
