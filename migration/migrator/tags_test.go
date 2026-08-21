package migrator_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/migration/migrator"
)

// A tag names a migration-directory state, so it lives in a table of its own
// rather than as a column on a revision: two revisions can share one tag, and a
// tag can name a directory whose migrations were never applied
// (stokaro/ptah#1621).

// newTagMigrator returns a migrator over a live SQLite database with two
// migrations available but nothing applied.
func newTagMigrator(c *qt.C, format migrator.RevisionTableFormat) (*dbschema.DatabaseConnection, *migrator.Migrator) {
	c.Helper()
	ctx := context.Background()
	conn, err := dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(c.TempDir(), "tags.db"))
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = conn.Close() })

	m, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"1_create_parent.sql": &fstest.MapFile{Data: []byte("CREATE TABLE parent (id INTEGER PRIMARY KEY);\n")},
		"2_create_child.sql":  &fstest.MapFile{Data: []byte("CREATE TABLE child (id INTEGER PRIMARY KEY);\n")},
	})
	c.Assert(err, qt.IsNil)
	return conn, m.WithRevisionTableFormat(format)
}

func TestRecordAndResolveMigrationTag(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)

	c.Assert(m.RecordMigrationTag(ctx, "v1.0.0", 20240101120000), qt.IsNil)
	version, err := m.ResolveMigrationTag(ctx, "v1.0.0")

	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(20240101120000))
}

// TestResolveUnknownMigrationTagIsNotVersionZero is the reason resolution
// returns an error rather than a zero value.
//
// Zero is a real schema version -- an empty database -- so a caller that could
// not tell "no such tag" from "revert everything" would do the second when it
// meant to report the first.
func TestResolveUnknownMigrationTagIsNotVersionZero(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)
	c.Assert(m.RecordMigrationTag(ctx, "v1.0.0", 20240101120000), qt.IsNil)

	version, err := m.ResolveMigrationTag(ctx, "v9.9.9")

	c.Assert(err, qt.ErrorIs, migrator.ErrMigrationTagNotFound)
	c.Assert(err.Error(), qt.Contains, `"v9.9.9"`)
	c.Assert(version, qt.Equals, int64(0))
}

// TestResolveMigrationTagBeforeAnyTagExists covers the same distinction one
// step earlier, when the namespace itself has never been created. An absent
// table must answer "no such tag", not zero and not a missing-relation error.
func TestResolveMigrationTagBeforeAnyTagExists(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)

	_, err := m.ResolveMigrationTag(ctx, "v1.0.0")

	c.Assert(err, qt.ErrorIs, migrator.ErrMigrationTagNotFound)
	c.Assert(err.Error(), qt.Not(qt.Contains), "no such table")
}

// TestRecordMigrationTagMovesAnExistingTag keeps re-tagging working. The
// registry tags this mirrors are movable pointers, and refusing to move one
// would leave an operator who re-tagged a directory unable to say so.
func TestRecordMigrationTagMovesAnExistingTag(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)

	c.Assert(m.RecordMigrationTag(ctx, "latest", 100), qt.IsNil)
	c.Assert(m.RecordMigrationTag(ctx, "latest", 200), qt.IsNil)

	version, err := m.ResolveMigrationTag(ctx, "latest")
	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(200))

	// Moved, not duplicated: a second row under the same name would make
	// resolution depend on which one the database returned first.
	tags, err := m.MigrationTags(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.HasLen, 1)
}

// TestMigrationTagsAreOrderedByName pins the listing order. Two entries are the
// minimum that can tell ordering from coincidence -- the reverse of a
// one-element list is itself.
func TestMigrationTagsAreOrderedByName(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)
	c.Assert(m.RecordMigrationTag(ctx, "v2.0.0", 200), qt.IsNil)
	c.Assert(m.RecordMigrationTag(ctx, "v1.0.0", 100), qt.IsNil)

	tags, err := m.MigrationTags(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.HasLen, 2)
	c.Assert(tags[0].Tag, qt.Equals, "v1.0.0")
	c.Assert(tags[1].Tag, qt.Equals, "v2.0.0")
	c.Assert(tags[0].Version, qt.Equals, int64(100))
	c.Assert(tags[1].Version, qt.Equals, int64(200))
}

// TestMigrationTagsOnAnUntaggedDatabaseIsEmptyNotAnError keeps `migrations
// status` working on every project that never tags anything.
func TestMigrationTagsOnAnUntaggedDatabaseIsEmptyNotAnError(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)

	tags, err := m.MigrationTags(ctx)

	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.HasLen, 0)
}

// TestMigrationTagRecordsWhenItResolved keeps the timestamp, which is what
// distinguishes a tag still pointing where it was set from one moved since.
func TestMigrationTagRecordsWhenItResolved(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)
	before := time.Now().UTC().Add(-time.Minute)

	c.Assert(m.RecordMigrationTag(ctx, "v1.0.0", 100), qt.IsNil)

	tags, err := m.MigrationTags(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tags, qt.HasLen, 1)
	c.Assert(tags[0].RecordedAt.After(before), qt.IsTrue)
}

// TestDeleteMigrationTagReportsAMissingTag keeps a script deleting a typo from
// reading as success.
func TestDeleteMigrationTagReportsAMissingTag(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)
	c.Assert(m.RecordMigrationTag(ctx, "v1.0.0", 100), qt.IsNil)

	c.Assert(m.DeleteMigrationTag(ctx, "v1.0.0"), qt.IsNil)
	err := m.DeleteMigrationTag(ctx, "v1.0.0")

	c.Assert(err, qt.ErrorIs, migrator.ErrMigrationTagNotFound)
}

// TestMigrationTagNamespaceWorksUnderTheAtlasRevisionFormat is why the tags
// live in their own table.
//
// The Atlas revision layout is a compatibility contract, so Ptah adds no column
// to it. A tag column could only have existed under one of the two formats; a
// separate table carries the namespace under both, and this is the branch that
// a column-shaped implementation would have left unanswered.
func TestMigrationTagNamespaceWorksUnderTheAtlasRevisionFormat(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	conn, m := newTagMigrator(c, migrator.RevisionTableFormatAtlas)

	c.Assert(m.RecordMigrationTag(ctx, "v1.0.0", 100), qt.IsNil)
	version, err := m.ResolveMigrationTag(ctx, "v1.0.0")

	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(100))

	// And the Atlas-shaped revision table gained no column for it.
	var count int
	c.Assert(conn.QueryRow(
		"SELECT count(*) FROM pragma_table_info('atlas_schema_revisions') WHERE name = 'tag'",
	).Scan(&count), qt.IsNil)
	c.Assert(count, qt.Equals, 0)
}

func TestNormalizeMigrationTagRefusesUnusableNames(t *testing.T) {
	for _, test := range []struct {
		name string
		tag  string
		want string
	}{
		{name: "empty", tag: "", want: "is empty"},
		{name: "only whitespace", tag: "   ", want: "is empty"},
		{name: "inner space", tag: "v1 0", want: "contains whitespace"},
		{name: "tab", tag: "v1\t0", want: "contains whitespace"},
		{name: "too long", tag: strings.Repeat("a", 256), want: "over the 255"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := migrator.NormalizeMigrationTag(test.tag)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.want)
		})
	}
}

// TestNormalizeMigrationTagTrimsSurroundingWhitespace keeps a tag typed with a
// stray space resolving to the one recorded without it, rather than to a second
// tag that looks identical in every listing.
func TestNormalizeMigrationTagTrimsSurroundingWhitespace(t *testing.T) {
	c := qt.New(t)
	ctx := context.Background()
	_, m := newTagMigrator(c, migrator.RevisionTableFormatPtah)
	c.Assert(m.RecordMigrationTag(ctx, "  v1.0.0  ", 100), qt.IsNil)

	version, err := m.ResolveMigrationTag(ctx, "v1.0.0")

	c.Assert(err, qt.IsNil)
	c.Assert(version, qt.Equals, int64(100))
	tags, err := m.MigrationTags(ctx)
	c.Assert(err, qt.IsNil)
	c.Assert(tags[0].Tag, qt.Equals, "v1.0.0")
}
