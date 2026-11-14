package atlasreport_test

import (
	"errors"
	"fmt"
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/migration/migrator"
)

func TestNewMigrateApplyOutOfOrderError_HappyPath(t *testing.T) {
	atlasDir := fstest.MapFS{
		"1_a.sql":   {Data: []byte("SELECT 1;")},
		"2_b.sql":   {Data: []byte("SELECT 2;")},
		"3_c.sql":   {Data: []byte("SELECT 3;")},
		"4_d.sql":   {Data: []byte("SELECT 4;")},
		"atlas.sum": {Data: []byte("h1:x=\n")},
	}
	flyway := map[int64]atlasreport.MigrateApplySourceFile{
		4611686018427471935: {Name: "V1.5__c.sql", Description: "c"},
		4611686018427836747: {Name: "V10__b.sql", Description: "b"},
	}
	tests := []struct {
		name     string
		versions []int64
		fsys     fs.FS
		sources  map[int64]atlasreport.MigrateApplySourceFile
		want     string
	}{
		{
			name:     "one file",
			versions: []int64{2},
			fsys:     atlasDir,
			want:     "migration file 2_b.sql was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error",
		},
		{
			name:     "two files in execution order",
			versions: []int64{2, 3},
			fsys:     atlasDir,
			want:     "migration files 2_b.sql, 3_c.sql were added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error",
		},
		{
			name:     "converted files are named as the directory holds them",
			versions: []int64{4611686018427471935, 4611686018427836747},
			fsys:     fstest.MapFS{},
			sources:  flyway,
			want:     "migration files V1.5__c.sql, V10__b.sql were added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error",
		},
		{
			name:     "a version no file holds keeps its number",
			versions: []int64{9},
			fsys:     atlasDir,
			want:     "migration file 9 was added out of order. See: https://atlasgo.io/versioned/apply#non-linear-error",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			cause := fmt.Errorf("apply: %w", &migrator.OutOfOrderError{CurrentVersion: 4, Versions: tt.versions})

			got, err := atlasreport.NewMigrateApplyOutOfOrderError(cause, tt.fsys, tt.sources)

			c.Assert(err, qt.IsNil)
			c.Assert(got.Error(), qt.Equals, tt.want)
			c.Assert(got, qt.ErrorIs, cause)
		})
	}
}

func TestNewMigrateApplyOutOfOrderError_KeepsTheMigratorRefusal(t *testing.T) {
	c := qt.New(t)
	refusal := &migrator.OutOfOrderError{CurrentVersion: 3, Versions: []int64{2}}

	got, err := atlasreport.NewMigrateApplyOutOfOrderError(refusal, fstest.MapFS{"2_b.sql": {}}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(got.Files, qt.DeepEquals, []string{"2_b.sql"})
	found, ok := errors.AsType[*migrator.OutOfOrderError](got)
	c.Assert(ok, qt.IsTrue)
	c.Assert(found, qt.Equals, refusal)
}

func TestNewMigrateApplyOutOfOrderError_OtherErrorIsNotARefusal(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "nil", err: nil},
		{name: "unrelated error", err: errors.New("database is locked")},
		{name: "refusal listing nothing", err: &migrator.OutOfOrderError{CurrentVersion: 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasreport.NewMigrateApplyOutOfOrderError(tt.err, fstest.MapFS{}, nil)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, (*atlasreport.MigrateApplyOutOfOrderError)(nil))
		})
	}
}

func TestNewMigrateApplyOutOfOrderError_FailurePath(t *testing.T) {
	c := qt.New(t)
	refusal := &migrator.OutOfOrderError{CurrentVersion: 3, Versions: []int64{2}}

	got, err := atlasreport.NewMigrateApplyOutOfOrderError(refusal, unreadableFS{}, nil)

	c.Assert(err, qt.ErrorIs, errUnreadableDir)
	c.Assert(err, qt.ErrorMatches, `discover Atlas migration files: .*`)
	c.Assert(got, qt.Equals, (*atlasreport.MigrateApplyOutOfOrderError)(nil))
}

var errUnreadableDir = errors.New("directory cannot be read")

// unreadableFS is a migration directory nothing can be read from.
type unreadableFS struct{}

func (unreadableFS) Open(string) (fs.File, error) { return nil, errUnreadableDir }
