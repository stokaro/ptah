package ptahdirective_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/go-cmp/cmp/cmpopts"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/ptahdirective"
)

// retiredReaders are the dialects of the retired list whose lexers decided
// the directives of a file without a dialect, in its order. The descriptors
// name the same engines in registry order, which cannot change an
// intersection.
var retiredReaders = []string{
	"postgres", "mysql", "mariadb", "sqlite", "clickhouse", "cockroachdb", "yugabytedb", "sqlserver", "spanner",
}

// retiredConservativeMarkers is the reading the retired list gave: the markers
// every reader in it sees at the same offset with the same body.
func retiredConservativeMarkers(sql string) []ptahdirective.Marker {
	common := slices.Collect(ptahdirective.Markers(sql, dialectlexer.Options(retiredReaders[0])))
	for _, dialect := range retiredReaders[1:] {
		seen := slices.Collect(ptahdirective.Markers(sql, dialectlexer.Options(dialect)))
		common = slices.DeleteFunc(common, func(marker ptahdirective.Marker) bool {
			return !slices.Contains(seen, marker)
		})
	}
	return common
}

// quoteOpeners each open a quoted token, a comment or nothing, depending on
// whose lexer reads them; quoteClosers end what one of them opened. Every
// pairing is read around a directive line, so a directive that one reader
// sees inside a token and another outside is in the corpus.
var (
	quoteOpeners = []string{
		"SELECT 1;", "SELECT 'a\\'", "SELECT E'a\\'", "SELECT U&'a'", "SELECT 'a'", "SELECT \"a",
		"SELECT `a", "SELECT [a", "SELECT $$a", "SELECT $tag$a", "# note '", "#temp '", "--x '",
		"/*! '", "/*M! '", "/* '", "SELECT @@session.sql_mode;", "SELECT @@a", "SELECT q'[a",
	}
	quoteClosers = []string{"", "'", "\"", "`", "]", "$$", "$tag$", "*/", "@@", "]'"}
	directives   = []string{"-- +ptah no_transaction", "  -- +ptah", "-- +ptah check name=\"x\""}
)

// TestConservativeMarkers_AgreesWithTheRetiredList reads every pairing of an
// opener and a closer around every directive line, and the same text with a
// second directive after the closer.
func TestConservativeMarkers_AgreesWithTheRetiredList(t *testing.T) {
	var corpus []string
	for _, opener := range quoteOpeners {
		for _, closer := range quoteClosers {
			for _, directive := range directives {
				corpus = append(corpus,
					opener+"\n"+directive+"\n"+closer+";\n",
					opener+"\n"+directive+"\n"+closer+";\n-- +ptah no_transaction\nSELECT 2;\n",
				)
			}
		}
	}
	c := qt.New(t)
	c.Assert(corpus, qt.HasLen, len(quoteOpeners)*len(quoteClosers)*len(directives)*2)
	for _, sql := range corpus {
		t.Run(sql, func(t *testing.T) {
			c := qt.New(t)
			got := slices.Collect(ptahdirective.ConservativeMarkers(sql))
			c.Assert(got, qt.CmpEquals(cmpopts.EquateEmpty()), retiredConservativeMarkers(sql))
		})
	}
}
