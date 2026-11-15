package embedguard

// White-box testing required: the names a reading notes are what it costs
// under -race, and no exported answer changes with them. Scan reports the same
// findings whether noteCalls notes every name a file uses or only the declared
// ones, so only the maps themselves can show which it does.

import (
	"go/parser"
	"go/token"
	"testing"

	qt "github.com/frankban/quicktest"
)

// callerSource uses one declared name, through an import and through an
// interface, beside names nothing in the vertical declares.
const callerSource = `package caller

import "example.invalid/guarded/internal/embedfixture"

type runner interface {
	Uncalled()
	Other()
}

func Use(r runner) {
	embedfixture.Uncalled()
	helper(r)
}

func helper(runner) {}
`

// TestNoteCalls_NotesOnlyDeclaredNames holds a reading to the names the
// vertical declares: helper, runner, Other and the rest of what the file uses
// are not noted, because nothing asks about them.
func TestNoteCalls_NotesOnlyDeclaredNames(t *testing.T) {
	c := qt.New(t)
	file, err := parser.ParseFile(token.NewFileSet(), "caller.go", callerSource, parser.SkipObjectResolution)
	c.Assert(err, qt.IsNil)
	declared := map[string]bool{"Uncalled": true}
	called := make(map[string]map[string]bool)
	viaInterface := make(map[string]bool)

	noteCalls(called, declared, "example.invalid/guarded", "internal/caller/caller.go", file)
	noteInterfaceMethods(viaInterface, declared, file)

	c.Assert(called, qt.DeepEquals, map[string]map[string]bool{
		"Uncalled": {
			"example.invalid/guarded/internal/caller":       true,
			"example.invalid/guarded/internal/embedfixture": true,
		},
	})
	c.Assert(viaInterface, qt.DeepEquals, map[string]bool{"Uncalled": true})
}
