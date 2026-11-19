package migratesum_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratesum"
	"ptah.run/migration/migrationfile"
)

// A sum covers a migration file's bytes, byte order mark included, although
// the migrator removes the mark before it runs the file. The atlas.sum values
// are the ones the pinned community binary writes for the same files,
// so the two sums stay byte-identical; the ptah.sum value for the repeatable
// is the digest the run records for it.
func TestComputeWithFormat_HashesTheByteOrderMark(t *testing.T) {
	const widgets = "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n"
	const widgetIDs = "CREATE OR REPLACE VIEW widget_ids AS SELECT id FROM widgets;\n"
	tests := []struct {
		name    string
		fsys    fstest.MapFS
		format  migrationfile.DirFormat
		want    []migratesum.Entry
		wantDir string
	}{
		{
			name:    "atlas.sum, marked file",
			fsys:    fstest.MapFS{"20260101000000_widgets.sql": {Data: []byte("\xEF\xBB\xBF" + widgets)}},
			format:  migrationfile.DirFormatAtlas,
			want:    []migratesum.Entry{{Name: "20260101000000_widgets.sql", Hash: "h1:uHr8zgYrqaKspuCTI4ZWmyFuQTXgloko20USkGnd42E="}},
			wantDir: "h1:y280ZoW0Rn3ZSB+FNLwkwvTpZGlM4N8pdiDsQWvMZaw=",
		},
		{
			name:    "atlas.sum, the same file without the mark",
			fsys:    fstest.MapFS{"20260101000000_widgets.sql": {Data: []byte(widgets)}},
			format:  migrationfile.DirFormatAtlas,
			want:    []migratesum.Entry{{Name: "20260101000000_widgets.sql", Hash: "h1:eNMkIcWA7QXiyRjITlxOOpi46WM8pcGRX6HryW/XWiU="}},
			wantDir: "h1:j1FfFpuCg0vt/du8pzYwzxbdW6EB1xWlTeNJlvui+7Q=",
		},
		{
			// The community binary reads `atlas:sum ignore` only when nothing
			// but printable ASCII stands before it, so behind a mark the file
			// is hashed like any other.
			name: "atlas.sum, atlas:sum ignore behind a mark",
			fsys: fstest.MapFS{
				"20260101000000_a.sql": {Data: []byte("CREATE TABLE a (id INT);\n")},
				"20260101000001_b.sql": {Data: []byte("\xEF\xBB\xBF-- atlas:sum ignore\nCREATE TABLE b (id INT);\n")},
			},
			format: migrationfile.DirFormatAtlas,
			want: []migratesum.Entry{
				{Name: "20260101000000_a.sql", Hash: "h1:3+JSMv7xYqfszVR0LtCKsfyGBeKjScSzoDTYpy3Q9UI="},
				{Name: "20260101000001_b.sql", Hash: "h1:uvybfUyVRwjhVZByubR+JbjWTGsrnoFx8Tyk/BpQv48="},
			},
			wantDir: "h1:0s0n1IsRi1oMJXsincSS0/g3SjHCOoKC2cA8tfFphSg=",
		},
		{
			name: "ptah.sum, marked repeatable",
			fsys: fstest.MapFS{
				"0000000001_widgets.up.sql":   {Data: []byte("\xEF\xBB\xBF" + widgets)},
				"0000000001_widgets.down.sql": {Data: []byte("DROP TABLE widgets;\n")},
				"R__widget_ids.sql":           {Data: []byte("\xEF\xBB\xBF" + widgetIDs)},
			},
			format: migrationfile.DirFormatPtah,
			want: []migratesum.Entry{
				{Name: "0000000001_widgets.down.sql", Hash: "h1:D16OqVwa+lyV8cXbpILYpkD2fkxV4E53VWbYRMUao3w="},
				{Name: "0000000001_widgets.up.sql", Hash: "h1:FtWUiXeh9iV+pdkpH44pNHoJMrnVKzp1GKTA8t5LA10="},
				{Name: "R__widget_ids.sql", Hash: "h1:MEsVb8j+KhwzSR3eJCMvt4DpS9h4wSj5W/ig313Xdhk="},
			},
			wantDir: "h1:2VkR1bLwKTI6BLUe5vxYVqi1ATD19auPsoJD3gHxQxc=",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			sum, err := migratesum.ComputeWithFormat(tt.fsys, tt.format)
			c.Assert(err, qt.IsNil)
			c.Assert(sum.Entries, qt.DeepEquals, tt.want)
			c.Assert(sum.DirHash, qt.Equals, tt.wantDir)
		})
	}
}
