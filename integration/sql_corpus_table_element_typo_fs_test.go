//go:build integration

// The table-element refusal, measured against every SQL file this repository
// tracks.
//
// stokaro/ptah#2753 is a refusal, and a refusal's risk is the schema it stops
// somebody applying. The rule rests on a conjunction -- a type whose
// parenthesised arguments are all bare identifiers, all of them columns of the
// same table -- and each half is what keeps a real type out of it:
// `geometry(Point, 4326)` fails the first because `4326` is not an identifier,
// `Nullable(String)` fails the second in a table with no `String` column.
//
// Widening either half is a one-character edit with no local signal. This test
// is that signal: it parses the whole corpus in every dialect and requires the
// rule to refuse nothing, which is what it does today and what a widened
// version would stop doing.

package integration_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/parser"
)

// TestTableElementTypoRefusesNothingTheCorpusHoldsFS is the false-positive
// control.
//
// Every tracked `.sql` file, parsed once per dialect. A file that is not for
// the dialect it is being parsed with fails for its own reasons and is not this
// test's business; what is refused for THIS reason is, and the count has to be
// zero.
//
// The corpus comes from `git ls-files` rather than a filesystem walk, for the
// reason `scripts/list-go-modules.sh` already documents: a walk descends into
// the linked worktrees parked under this repository and would measure another
// branch's files.
func TestTableElementTypoRefusesNothingTheCorpusHoldsFS(t *testing.T) {
	c := qt.New(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	files := trackedSQLFiles(c, ctx)
	// A corpus that came back empty would make every assertion below vacuous,
	// which is the shape `check-documented-install.sh` fails closed on.
	c.Assert(len(files) > 100, qt.IsTrue,
		qt.Commentf("the corpus is %d files, too few to have been discovered", len(files)))

	dialects := []string{
		platform.Postgres, platform.MySQL, platform.MariaDB, platform.SQLite,
		platform.ClickHouse, platform.SQLServer, platform.Oracle,
	}

	var refused []string
	parsed := 0
	for _, path := range files {
		document := readTrackedFile(c, path)
		for _, dialect := range dialects {
			_, err := parser.NewParser(document, parser.WithDialect(dialect)).Parse()
			parsed++
			refused = append(refused, typoRefusalOf(path, dialect, err)...)
		}
	}

	c.Assert(parsed, qt.Equals, len(files)*len(dialects))
	c.Assert(refused, qt.HasLen, 0,
		qt.Commentf("the refusal fired on schemas this repository ships:\n%s",
			strings.Join(refused, "\n")))
}

// typoRefusalOf is the one-element slice naming this parse when it failed for
// the table-element reason, and nothing otherwise.
//
// Matched on the sentence the refusal owns rather than on the error being
// non-nil, because most of these parses fail for an unrelated reason: a file
// written for one engine parsed as another. Returning a slice rather than a
// boolean keeps the caller free of the conditional the style rules forbid.
func typoRefusalOf(path, dialect string, err error) []string {
	if err == nil || !strings.Contains(err.Error(), "a type cannot name a column") {
		return nil
	}
	return []string{path + " [" + dialect + "]: " + err.Error()}
}

// trackedSQLFiles asks git which SQL files this working tree holds.
func trackedSQLFiles(c *qt.C, ctx context.Context) []string {
	c.Helper()
	root := repositoryRoot(c, ctx)
	listed, err := exec.CommandContext(ctx, "git", "-C", root, "ls-files", "*.sql").Output()
	c.Assert(err, qt.IsNil)

	var files []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(listed)), "\n") {
		name := strings.TrimSpace(line)
		files = append(files, filepath.Join(root, name))
	}
	return files
}

// repositoryRoot is the working tree this test is running in.
func repositoryRoot(c *qt.C, ctx context.Context) string {
	c.Helper()
	top, err := exec.CommandContext(ctx, "git", "rev-parse", "--show-toplevel").Output()
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(string(top))
}

// readTrackedFile reads one corpus file.
func readTrackedFile(c *qt.C, path string) string {
	c.Helper()
	data, err := os.ReadFile(path) // #nosec G304 -- a path git named in this working tree.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", path))
	return string(data)
}
