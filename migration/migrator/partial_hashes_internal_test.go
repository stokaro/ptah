package migrator

// White-box testing required: the count invariant below is between two
// splitters, one of which is unexported, and the digest computation is not
// reachable through an exported API — it exists only as a bound argument on a
// revision UPDATE.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/sqlutil"
)

// TestAtlasPartialHashValues pins the `partial_hashes` column against the
// pinned Atlas community binary v1.3.0 (stokaro/ptah#1196).
//
// Every want below was read out of a SQLite database that binary wrote, by
// applying the same migration with the same `-- atlas:txmode none` directive
// and letting the last statement fail. They are not derived from this
// implementation.
//
// The second row is the one that separates a correct implementation from a
// plausible one: with the terminator on its own line the digest covers
// "CREATE TABLE q (id int)\n;", so anything that hashes the executor's
// normalized statement plus a semicolon produces a different value — silently,
// and only for files written that way.
func TestAtlasPartialHashValues(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		applied int
		total   int
		want    []string
		wantOK  bool
	}{
		{
			name:    "one statement applied",
			sql:     "-- atlas:txmode none\n\nCREATE TABLE ok_table (id int);\nINSERT INTO missing_table (id) VALUES (1);\n",
			applied: 1,
			total:   2,
			want:    []string{"h1:8jgptPnsBJEGnJcSP/+SQfPIpzKcTTrznDR0HMTsIG0="},
			wantOK:  true,
		},
		{
			name:    "the terminator on its own line is part of the digest",
			sql:     "-- atlas:txmode none\n\nCREATE TABLE q (id int)\n;\nINSERT INTO nope (id) VALUES (1);\n",
			applied: 1,
			total:   2,
			want:    []string{"h1:QaXL7OuhISU7XRm163GWS3emXNCpNe7H10aYOCtdbp8="},
			wantOK:  true,
		},
		{
			name:    "each entry covers every statement up to it",
			sql:     "-- atlas:txmode none\n\nCREATE TABLE a (id int);\nCREATE TABLE b (name text NOT NULL);\nINSERT INTO missing_table (id) VALUES (1);\n",
			applied: 2,
			total:   3,
			want: []string{
				"h1:Fo9K92y+zoKkGFxsVzZuB4g8bkFV8lzihVkBjZlDO4E=",
				"h1:PHiabMqgRVQ+Q6kLY6ZLG6LaSfJUmW12KwtOaPgLbug=",
			},
			wantOK: true,
		},
		{
			name:    "a comment between statements is not in any digest",
			sql:     "-- atlas:txmode none\n\nCREATE TABLE m (id int);\n-- a comment between statements\nCREATE TABLE n (v text);\nINSERT INTO nope (id) VALUES (1);\n",
			applied: 2,
			total:   3,
			want: []string{
				"h1:lFi6f+BorWqEW6ilAJebYqdkILXV8DyZXW/ttLZg1ws=",
				"h1:ahmDNZltrM4zYl43yk/J7MRbAF6HqfbvjtksH1z62/I=",
			},
			wantOK: true,
		},
		{
			name:    "blank lines between statements are not in any digest",
			sql:     "-- atlas:txmode none\n\nCREATE TABLE x (id int);\n\nCREATE TABLE y (v text);\nCREATE TABLE z (w int NOT NULL DEFAULT 3);\nINSERT INTO nope (id) VALUES (1);\n",
			applied: 3,
			total:   4,
			want: []string{
				"h1:dPM5U4jjwgTZgQABLUveEHkcVOS1mAmu2al8ougQWVs=",
				"h1:KSJHnVIWODojTFGEbln+Vb97fOt7NlbvyDNhqTERoWg=",
				"h1:UosdOJPBbQ6nr0sLOR8k39EP4YpBfa+j630JoXmDMQo=",
			},
			wantOK: true,
		},
		{
			// The binary records the JSON null here, not an empty array.
			name:    "nothing applied has no prefix to record",
			sql:     "CREATE TABLE a (id int);\nCREATE TABLE b (v text);\n",
			applied: 0,
			total:   2,
			wantOK:  false,
		},
		{
			// And here, because a clean success has nothing to resume.
			name:    "a complete application records nothing",
			sql:     "CREATE TABLE a (id int);\nCREATE TABLE b (v text);\n",
			applied: 2,
			total:   2,
			wantOK:  false,
		},
		{
			name:    "more statements applied than the source has is refused",
			sql:     "CREATE TABLE a (id int);\n",
			applied: 2,
			total:   3,
			wantOK:  false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			got, ok := atlasPartialHashValues(test.sql, "sqlite", test.applied, test.total)

			c.Assert(ok, qt.Equals, test.wantOK)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestSourceStatementCountMatchesTheExecutorSplit is the invariant
// atlasPartialHashValues rests on.
//
// It indexes source statements by the count the executor reports, which comes
// from splitSQLStatementsForDialect. A shape where the two splits disagree
// would attach a digest to the wrong statement — a wrong value in a column
// whose whole purpose is to be trusted on resume, which is worse than the null
// it replaces.
func TestSourceStatementCountMatchesTheExecutorSplit(t *testing.T) {
	sources := []string{
		"-- atlas:txmode none\n\nCREATE TABLE a (id int);\nCREATE TABLE b (v text);\n",
		"CREATE TABLE q (id int)\n;\n-- between\nCREATE TABLE n (v text);\n",
		"CREATE TABLE only (id int)\n",
		"-- just a comment\n",
		"CREATE TABLE a (id int);;\nCREATE TABLE b (v text);\n",
		"CREATE TABLE a (id int); /* trailing */\n",
		"INSERT INTO t (v) VALUES ('a;b');\nINSERT INTO t (v) VALUES ('c');\n",
	}

	for _, dialect := range []string{"sqlite", "postgres", "mysql", "sqlserver"} {
		t.Run(dialect, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)

			for _, source := range sources {
				c.Assert(
					sqlutil.SplitSourceStatements(source, dialect),
					qt.HasLen,
					len(splitSQLStatementsForDialect(source, dialect)),
					qt.Commentf("source %q", source),
				)
			}
		})
	}
}
