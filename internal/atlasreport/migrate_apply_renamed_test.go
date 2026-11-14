package atlasreport_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
)

// TestRenamedMigrateApplySourceFiles_HappyPath keys each renamed file by the
// version of the converted file it became, so a golang-migrate directory is
// named as it holds its files.
func TestRenamedMigrateApplySourceFiles_HappyPath(t *testing.T) {
	converted := fstest.MapFS{
		"1_a.sql":        {Data: []byte("CREATE TABLE a (id int);\n")},
		"000002_b_c.sql": {Data: []byte("CREATE TABLE b (id int);\n")},
		"3_kept.sql":     {Data: []byte("CREATE TABLE d (id int);\n")},
	}
	tests := []struct {
		name    string
		renamed map[string]string
		want    map[int64]atlasreport.MigrateApplySourceFile
	}{
		{
			name:    "golang-migrate up files",
			renamed: map[string]string{"1_a.sql": "1_a.up.sql", "000002_b_c.sql": "000002_b_c.up.sql"},
			want: map[int64]atlasreport.MigrateApplySourceFile{
				1: {Name: "1_a.up.sql", Description: "a"},
				2: {Name: "000002_b_c.up.sql", Description: "b_c"},
			},
		},
		{
			name:    "a renamed file the directory does not hold",
			renamed: map[string]string{"9_gone.sql": "9_gone.up.sql"},
			want:    make(map[int64]atlasreport.MigrateApplySourceFile),
		},
		{
			name:    "nothing renamed",
			renamed: nil,
			want:    nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := atlasreport.RenamedMigrateApplySourceFiles(converted, tt.renamed)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.DeepEquals, tt.want)
		})
	}
}

// TestRenamedMigrateApplySourceFiles_FailurePath reports a directory it cannot
// read.
func TestRenamedMigrateApplySourceFiles_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := atlasreport.RenamedMigrateApplySourceFiles(unreadableFS{}, map[string]string{"1_a.sql": "1_a.up.sql"})

	c.Assert(err, qt.ErrorIs, errUnreadableDir)
	c.Assert(err, qt.ErrorMatches, `discover Atlas migration files: .*`)
	c.Assert(got, qt.IsNil)
}
