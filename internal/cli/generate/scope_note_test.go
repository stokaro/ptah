package generate_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/generate"
)

// scopedEntities declares an extension and a function, each scoped to
// PostgreSQL, beside one table.
const scopedEntities = `package models

//ptah:schema:extension name="pg_trgm" dialects="postgres"
//ptah:schema:function name="touch" returns="trigger" language="plpgsql" body="BEGIN RETURN NEW; END;" dialects="postgres"
//ptah:schema:table name="docs"
type Doc struct {
	//ptah:schema:field name="id" type="INTEGER" primary="true"
	ID int
}
`

// TestGenerateCommand_NamesAScopedObjectItLeavesOut pins the note a render
// writes for each object scoped to other targets: an owner's object by the
// label its owner states for the model, the one its skip line uses, and a
// shared declaration by its kind.
func TestGenerateCommand_NamesAScopedObjectItLeavesOut(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(dir, "models.go"), []byte(scopedEntities), 0o600), qt.IsNil)
	cmd := generate.NewGenerateCommand()
	cmd.SetArgs([]string{"--root-dir", dir, "--dialect", "mysql"})

	_, stderr, err := executeGenerate(c, cmd)

	c.Assert(err, qt.IsNil, qt.Commentf("generate stderr:\n%s", stderr))
	c.Assert(stderr, qt.Contains, "note: mysql: PostgreSQL extension pg_trgm is declared for postgres and is not part of this target's schema\n")
	c.Assert(stderr, qt.Contains, "note: mysql: function touch is declared for postgres and is not part of this target's schema\n")
}
