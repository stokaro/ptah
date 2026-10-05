package lint_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// commentedNotes creates a table and comments a column and an index of it,
// with Ptah's own statements.
const commentedNotes = "CREATE TABLE `app/notes` (id Uint64 NOT NULL, body Utf8, tag Utf8, PRIMARY KEY (id), " +
	"INDEX notes_tag GLOBAL ON (tag));\n" +
	"COMMENT ON COLUMN `app/notes`.`body` IS 'The text';\n" +
	"COMMENT ON INDEX `notes_tag` ON `app/notes` IS 'By tag';\n"

// YD150 reports a drop or a rename that leaves a comment on the table under
// the old name, where nothing after it in the file removes or moves it: a
// comment an earlier migration set, and one set earlier in the same file.
func TestYDBRules_CommentLeftBehind(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a dropped column, an index dropped and one renamed",
			files: map[string]string{
				"0001_notes.up.sql": commentedNotes,
				"0002_drop.up.sql": "ALTER TABLE `app/notes` DROP INDEX notes_tag;\n" +
					"ALTER TABLE `app/notes` DROP COLUMN body;\n",
			},
			want: []string{"0002_drop.up.sql:1:YD150", "0002_drop.up.sql:2:YD150"},
		},
		{
			name: "an index renamed",
			files: map[string]string{
				"0001_notes.up.sql":  commentedNotes,
				"0002_rename.up.sql": "ALTER TABLE `app/notes` RENAME INDEX notes_tag TO notes_by_tag;\n",
			},
			want: []string{"0002_rename.up.sql:1:YD150"},
		},
		{
			name: "a comment set and its column dropped in one file",
			files: map[string]string{
				"0001_notes.up.sql": commentedNotes + "ALTER TABLE `app/notes` DROP COLUMN body;\n",
			},
			want: []string{"0001_notes.up.sql:4:YD150"},
		},
		{
			name: "a renamed table",
			files: map[string]string{
				"0001_notes.up.sql": commentedNotes,
				"0002_move.up.sql":  "ALTER TABLE `app/notes` RENAME TO `app/old_notes`;\nALTER TABLE `app/old_notes` DROP COLUMN body;\n",
			},
			want: []string{"0002_move.up.sql:2:YD150"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSitesOf(c, test.files, "YD150"), qt.DeepEquals, test.want)
		})
	}
}

// YD150 stays silent where nothing is left behind: a comment removed or moved
// after the drop or the rename, as Ptah's own plans write it; an object
// without a comment; a comment removed before the drop; and a dropped table,
// which takes its attributes with it.
func TestYDBRules_CommentLeftBehind_Controls(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{
			name: "the comments removed and moved after the changes",
			files: map[string]string{
				"0001_notes.up.sql": commentedNotes,
				"0002_change.up.sql": "ALTER TABLE `app/notes` RENAME INDEX `notes_tag` TO `notes_by_tag`;\n" +
					"ALTER TABLE `app/notes` DROP COLUMN `body`;\n" +
					"COMMENT ON COLUMN `app/notes`.`body` IS NULL;\n" +
					"COMMENT ON INDEX `notes_by_tag` ON `app/notes` IS 'By tag';\n" +
					"COMMENT ON INDEX `notes_tag` ON `app/notes` IS NULL;\n",
			},
		},
		{
			name: "objects without a comment",
			files: map[string]string{
				"0001_notes.up.sql": "CREATE TABLE `app/notes` (id Uint64 NOT NULL, body Utf8, tag Utf8, PRIMARY KEY (id), " +
					"INDEX notes_tag GLOBAL ON (tag));\nCOMMENT ON TABLE `app/notes` IS 'Notes';\n",
				"0002_drop.up.sql": "ALTER TABLE `app/notes` DROP INDEX notes_tag;\nALTER TABLE `app/notes` DROP COLUMN body;\n",
			},
		},
		{
			name: "a comment removed before the drop",
			files: map[string]string{
				"0001_notes.up.sql": commentedNotes,
				"0002_drop.up.sql":  "COMMENT ON COLUMN `app/notes`.`body` IS NULL;\nALTER TABLE `app/notes` DROP COLUMN body;\n",
			},
		},
		{
			name: "a dropped table",
			files: map[string]string{
				"0001_notes.up.sql": commentedNotes,
				"0002_drop.up.sql":  "DROP TABLE `app/notes`;\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSitesOf(c, test.files, "YD150"), qt.HasLen, 0)
		})
	}
}

// The finding says which attribute stays and the statement that removes it.
func TestYDBRules_CommentLeftBehind_Message(t *testing.T) {
	c := qt.New(t)
	target, err := lint.ResolveTarget("ydb", "")
	c.Assert(err, qt.IsNil)
	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_notes.up.sql": commentedNotes,
		"0002_drop.up.sql":  "ALTER TABLE `app/notes` DROP COLUMN body;\nALTER TABLE `app/notes` RENAME INDEX notes_tag TO by_tag;\n",
	}), lint.Options{Dialect: "ydb", Target: target})
	c.Assert(err, qt.IsNil)

	var messages []string
	for _, finding := range findings {
		messages = append(messages, finding.Rule+": "+finding.Message)
	}
	c.Assert(messages, qt.Contains, "YD150: DROP COLUMN body leaves its comment on the table as the attribute "+
		"ptah.comment.column.body, which YDB keeps after the column is gone and a column added later under the name "+
		"reads as its own; remove it with COMMENT ON COLUMN `app/notes`.`body` IS NULL")
	c.Assert(messages, qt.Contains, "YD150: RENAME INDEX notes_tag TO by_tag leaves the index's comment under the "+
		"old name, as the attribute ptah.comment.index.notes_tag; move it with COMMENT ON INDEX `by_tag` ON "+
		"`app/notes` and COMMENT ON INDEX `notes_tag` ON `app/notes` IS NULL")
}

// ydbSitesOf lints files on the newest YDB line and keeps the sites of rule.
func ydbSitesOf(c *qt.C, files map[string]string, rule string) []string {
	c.Helper()
	return slices.DeleteFunc(ydbLint(c, files, ""), func(site string) bool { return !strings.HasSuffix(site, ":"+rule) })
}
