package atlasscript_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlasscript"
)

// The cursor is read in source order -- stokaro/ptah#1017.
//
// hclsyntax keeps a block's attributes in a map and Go randomizes that
// iteration, so a parser that ranged over it would order the cursor differently
// on every run. The cursor's order is what the next batch's placeholders are
// positioned against, so getting it from a map is a defect that reproduces one
// run in N -- the worst kind to ship, because it passes review and passes CI.
//
// Six columns rather than two: with two, map order matches source order half
// the time and the test would pass by luck.
func TestParse_TheCursorKeepsSourceOrder(t *testing.T) {
	c := qt.New(t)

	scripts := parse(c, `
script "loop" "purge" {
  iterator "keyset" {
    cursor {
      alpha   = string
      bravo   = int
      charlie = string
      delta   = int
      echo    = string
      foxtrot = int
    }
    init {
      sql = "SELECT alpha, bravo, charlie, delta, echo, foxtrot FROM t LIMIT 1"
    }
    next {
      sql  = "SELECT alpha FROM t WHERE alpha > ? LIMIT 1"
      args = [cursor.alpha]
    }
  }
  do {
    exec "e" { sql = "DELETE FROM t" }
  }
}
`)

	c.Assert(scripts[0].Iterator, qt.IsNotNil)
	c.Assert(scripts[0].Iterator.Cursor, qt.DeepEquals,
		[]string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"})
}

// The same document parsed repeatedly gives the same cursor.
//
// One parse can agree with source order by chance; this is the assertion that a
// map-ordered read cannot pass. Go re-seeds map iteration per range statement,
// so twenty parses of a six-name cursor make a lucky pass vanishingly unlikely.
func TestParse_TheCursorIsTheSameOnEveryParse(t *testing.T) {
	c := qt.New(t)
	document := `
script "loop" "purge" {
  iterator "keyset" {
    cursor {
      alpha   = string
      bravo   = int
      charlie = string
      delta   = int
      echo    = string
      foxtrot = int
    }
    init {
      sql = "SELECT alpha FROM t LIMIT 1"
    }
    next {
      sql  = "SELECT alpha FROM t WHERE alpha > ? LIMIT 1"
      args = [cursor.alpha]
    }
  }
  do {
    exec "e" { sql = "DELETE FROM t" }
  }
}
`
	want := []string{"alpha", "bravo", "charlie", "delta", "echo", "foxtrot"}

	for range 20 {
		scripts, err := atlasscript.Parse([]byte(document), "script.hcl")
		c.Assert(err, qt.IsNil)
		c.Assert(scripts[0].Iterator.Cursor, qt.DeepEquals, want)
	}
}
