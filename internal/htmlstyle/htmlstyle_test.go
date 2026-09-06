package htmlstyle_test

import (
	"maps"
	"regexp"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/htmlstyle"
)

var (
	declarationPattern = regexp.MustCompile(`(--[a-z0-9-]+)\s*:`)
	usePattern         = regexp.MustCompile(`var\((--[a-z0-9-]+)`)
	blockPattern       = regexp.MustCompile(`(?s)\{([^{}]*--[a-z-]+:[^{}]*)\}`)
)

// TestBase_ResolvesEveryCustomPropertyItUses is the assertion the shared
// appearance cannot make about itself by looking correct.
//
// A var() naming a token nothing declares does not fail: the browser discards
// the declaration it appears in and reports nothing. Every page built on this
// would inherit the hole.
func TestBase_ResolvesEveryCustomPropertyItUses(t *testing.T) {
	c := qt.New(t)

	declared := tokensIn(htmlstyle.Tokens())
	used := usedIn(htmlstyle.Base())

	c.Assert(len(used) > 0, qt.IsTrue, qt.Commentf("the base stylesheet uses no tokens at all"))
	for _, token := range slices.Sorted(maps.Keys(used)) {
		c.Assert(declared[token], qt.IsTrue,
			qt.Commentf("var(%s) resolves to nothing: Tokens declares no such value", token))
	}
}

// TestTokens_DefinesEveryColorInEveryTheme keeps a theme block from introducing
// a token the base block does not have.
//
// A color defined only under prefers-color-scheme is missing for a reader who
// chose light explicitly, and the element it paints falls back to whatever the
// browser decides -- which is how a page ends up with black text on a dark card.
func TestTokens_DefinesEveryColorInEveryTheme(t *testing.T) {
	c := qt.New(t)

	blocks := blockPattern.FindAllStringSubmatch(htmlstyle.Tokens(), -1)
	c.Assert(blocks, qt.HasLen, 3, qt.Commentf("expected a base block and two theme blocks"))

	base := tokensIn(blocks[0][1])
	for _, block := range blocks[1:] {
		for _, token := range slices.Sorted(maps.Keys(tokensIn(block[1]))) {
			c.Assert(base[token], qt.IsTrue,
				qt.Commentf("%s is defined in a theme block but not in the base one", token))
		}
	}
}

// TestFooter_NamesTheBinaryThatWroteTheFile pins the one thing a shared and
// archived document cannot otherwise answer.
func TestFooter_NamesTheBinaryThatWroteTheFile(t *testing.T) {
	c := qt.New(t)

	footer := htmlstyle.Footer("A note the caller wrote.")

	c.Assert(footer, qt.Contains, "A note the caller wrote.")
	c.Assert(footer, qt.Contains, `class="footer-mark"`)
	c.Assert(footer, qt.Contains, "ptah ")
}

// TestHead_FetchesNothing is the property every page built on this inherits.
func TestHead_FetchesNothing(t *testing.T) {
	c := qt.New(t)

	head := htmlstyle.Head("A title", "")

	c.Assert(regexp.MustCompile(`(?i)(https?:)?//[a-z0-9.-]+\.[a-z]{2,}`).FindAllString(head, -1), qt.HasLen, 0)
	for _, element := range []string{"<script", "<link", "<img", "@import"} {
		c.Assert(head, qt.Not(qt.Contains), element,
			qt.Commentf("%s is how a page reaches for something it does not carry", element))
	}
}

func tokensIn(css string) map[string]bool {
	found := make(map[string]bool)
	for _, match := range declarationPattern.FindAllStringSubmatch(css, -1) {
		found[match[1]] = true
	}
	return found
}

func usedIn(css string) map[string]bool {
	found := make(map[string]bool)
	for _, match := range usePattern.FindAllStringSubmatch(css, -1) {
		found[match[1]] = true
	}
	return found
}
