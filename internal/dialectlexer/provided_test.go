package dialectlexer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dialectlexer"
	"ptah.run/internal/lexer"
	"ptah.run/internal/lexerext"
	"ptah.run/internal/providerscan"
	"ptah.run/internal/providerscan/scantest"
)

// An engine whose provider registered a scanner is read by that scanner and
// by nothing else, under each of its names; once the registration ends, the
// engine is read by the shared rules again. A built-in dialect keeps its
// descriptor's rules throughout.
func TestOptions_AnswersAProvidersScanner(t *testing.T) {
	c := qt.New(t)
	scanner := providerscan.New(scantest.Scan)
	release, err := lexerext.Provide(scanner, "scanned-engine", "scanned-alias")
	c.Assert(err, qt.IsNil)
	c.Assert(dialectlexer.Options("scanned-engine"), qt.Equals, lexer.Options{Scanner: scanner})
	c.Assert(dialectlexer.Options("Scanned-Alias"), qt.Equals, lexer.Options{Scanner: scanner})
	c.Assert(dialectlexer.Options("sqlite"), qt.DeepEquals, switchAnswers["sqlite"])
	release()
	c.Assert(dialectlexer.Options("scanned-engine"), qt.DeepEquals, lexer.Options{StandardStrings: true})
}
