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

// Every Atlas-format version this binary writes is rendered as a UTC
// yyyyMMddHHmmss instant, so a fourteen-digit version is READ as a date. The
// rows below separate "is a number a file name can carry", which is [Check],
// from "is a number the instant it looks like", which is [IsStamp]: 20991231235960
// passes the first and fails the second, and it is exactly what integer
// arithmetic produced on a directory whose newest migration ended at :59
// (stokaro/ptah#938).
//
// The cheaper wrong implementation is a seconds-only check --
// `version%100 <= 59` -- which is what anyone writes first after seeing
// `...235960`. The February and month-13 rows are the ones that refuse it.
func TestIsStamp(t *testing.T) {
	tests := []struct {
		name    string
		version int64
		want    bool
	}{
		{name: "an ordinary second", version: 20260809042338, want: true},
		{name: "the last second of a year", version: 29991231235959, want: true},
		{name: "a leap day", version: 20240229120000, want: true},
		{name: "sixty seconds", version: 29991231235960},
		{name: "sixty-one seconds, the second checkpoint's answer", version: 29991231235961},
		{name: "sixty minutes", version: 20260809046038},
		{name: "hour twenty-four", version: 20260809240000},
		{name: "the thirty-first of February", version: 20260231000000},
		{name: "the twenty-ninth of a common February", version: 20260229000000},
		{name: "month thirteen", version: 20261301000000},
		{name: "day zero", version: 20260800000000},
		{name: "a ten-digit epoch is not a stamp at all", version: 1786000000},
		{name: "int64 max is not a stamp at all", version: math.MaxInt64},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(migrationversion.IsStamp(test.version), qt.Equals, test.want)
		})
	}
}

// TestWritable pins where a candidate lands. The `want` column is the smallest
// legal answer at or above the input in every row, so a mutant that overshoots
// -- rounding a bad second up to the next whole minute, say -- is as red as one
// that does not move at all.
func TestWritable(t *testing.T) {
	tests := []struct {
		name      string
		candidate int64
		format    migrator.MigrationDirFormat
		want      int64
		wantErr   bool
	}{
		{
			name:      "a real second is returned untouched",
			candidate: 20260809042338, format: migrator.MigrationDirFormatAtlas, want: 20260809042338,
		},
		{
			name:      "sixty seconds rolls to the next minute",
			candidate: 20260809055860, format: migrator.MigrationDirFormatAtlas, want: 20260809055900,
		},
		{
			name:      "the carry runs all the way through the year",
			candidate: 29991231235960, format: migrator.MigrationDirFormatAtlas, want: 30000101000000,
		},
		{
			name:      "a second checkpoint's 235961 lands on the same second",
			candidate: 29991231235961, format: migrator.MigrationDirFormatAtlas, want: 30000101000000,
		},
		{
			name:      "the thirty-first of February rolls into March",
			candidate: 20260231000000, format: migrator.MigrationDirFormatAtlas, want: 20260301000000,
		},
		{
			name:      "month thirteen rolls into the next January",
			candidate: 20261301000000, format: migrator.MigrationDirFormatAtlas, want: 20270101000000,
		},
		{
			name:      "past the last representable instant it stops claiming to be one",
			candidate: 99991231235960, format: migrator.MigrationDirFormatAtlas, want: 100000000000000,
		},
		{
			name:      "a ten-digit epoch claims no instant and is left alone",
			candidate: 1786000000, format: migrator.MigrationDirFormatAtlas, want: 1786000000,
		},
		{
			name:      "what the flyway importer writes claims no instant either",
			candidate: 4611686018427469511, format: migrator.MigrationDirFormatAtlas, want: 4611686018427469511,
		},
		{
			name:      "a paired version is ten digits, far below any stamp",
			candidate: 9999999999, format: migrator.MigrationDirFormatPtah, want: 9999999999,
		},
		{
			name:      "the paired ceiling still refuses rather than steps",
			candidate: 10000000000, format: migrator.MigrationDirFormatPtah, wantErr: true,
		},
		{
			name:      "a wrapped version is still a refusal",
			candidate: math.MinInt64, format: migrator.MigrationDirFormatAtlas, wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			version, err := migrationversion.Writable(test.candidate, test.format)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("err: %v", err))
			c.Assert(version, qt.Equals, test.want)
		})
	}
}

// TestAdvance is the rule `migrate checkpoint` steps with. It has to move past
// latest -- that is what a checkpoint is for -- and it has to land on an instant.
//
// The cheaper wrong implementation is Advance == Next: bounded integer
// arithmetic, which is what this branch shipped and what wrote
// `29991231235960_cp1.sql` beside a `29991231235959_future.sql`.
func TestAdvance(t *testing.T) {
	tests := []struct {
		name    string
		latest  int64
		format  migrator.MigrationDirFormat
		want    int64
		wantErr bool
	}{
		{
			name:   "an ordinary second advances by one second",
			latest: 20260809042338, format: migrator.MigrationDirFormatAtlas, want: 20260809042339,
		},
		{
			name:   "the last second of a minute advances to the next minute",
			latest: 20260809042359, format: migrator.MigrationDirFormatAtlas, want: 20260809042400,
		},
		{
			name:   "the last second of 2999 advances to 3000",
			latest: 29991231235959, format: migrator.MigrationDirFormatAtlas, want: 30000101000000,
		},
		{
			name:   "a ten-digit epoch neighbor advances by one",
			latest: 1786000000, format: migrator.MigrationDirFormatAtlas, want: 1786000001,
		},
		{
			name:   "the paired layout keeps plain arithmetic, being far below any stamp",
			latest: 1786000000, format: migrator.MigrationDirFormatPtah, want: 1786000001,
		},
		{
			name:   "the paired ceiling refuses",
			latest: 9999999999, format: migrator.MigrationDirFormatPtah, wantErr: true,
		},
		{
			name:   "the atlas ceiling refuses",
			latest: math.MaxInt64, format: migrator.MigrationDirFormatAtlas, wantErr: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			version, err := migrationversion.Advance(test.latest, test.format)

			c.Assert(err != nil, qt.Equals, test.wantErr, qt.Commentf("err: %v", err))
			c.Assert(version, qt.Equals, test.want)
		})
	}
}

// TestWritableRun covers the batch case. `migrate diff` stages a plan whose
// concurrent-index half is a second file at version+1, so a base of
// 20260809042359 would put that file at 20260809042360. The run has to be
// checked, not only its first slot.
//
// The cheaper wrong implementation is WritableRun == Writable, ignoring count.
func TestWritableRun(t *testing.T) {
	tests := []struct {
		name      string
		candidate int64
		count     int
		want      int64
	}{
		{name: "a single file needs only its own slot", candidate: 20260809042359, count: 1, want: 20260809042359},
		{name: "two files may not straddle the minute", candidate: 20260809042359, count: 2, want: 20260809042400},
		{name: "two files inside a minute are fine", candidate: 20260809042358, count: 2, want: 20260809042358},
		{name: "a count below one is one file", candidate: 20260809042359, count: 0, want: 20260809042359},
		{name: "the base itself is raised first", candidate: 20260809042360, count: 2, want: 20260809042400},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			base, err := migrationversion.WritableRun(test.candidate, test.count, migrator.MigrationDirFormatAtlas)

			c.Assert(err, qt.IsNil)
			c.Assert(base, qt.Equals, test.want)
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
