package sourceformats_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/builtintest"
	"ptah.run/sourceformats"
)

// dropsBesideAnExtension declares an extension, a standalone object, and drops
// a table beside it.
const dropsBesideAnExtension = "CREATE EXTENSION citext;\nCREATE TABLE a (id int);\nCREATE TABLE b (id int);\nDROP TABLE a;\n"

// TestReadSQL_DropsATableWithRelations pins that the readers ask relations
// which table a standalone object hangs off: a DROP TABLE reads beside an
// extension, which is attached to no table and stays.
func TestReadSQL_DropsATableWithRelations(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	formats := sourceformats.New(runtime.YAML(), runtime.SQL(), runtime.HCL(), coverage.Vocabulary{}, runtime)

	database, err := formats.ReadSQL(t.Context(), []byte(dropsBesideAnExtension), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 1)
}

// TestReadSQL_RefusesADropTableWithoutRelations pins that readers built
// without relations refuse the same document rather than guess.
func TestReadSQL_RefusesADropTableWithoutRelations(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	formats := sourceformats.New(runtime.YAML(), runtime.SQL(), runtime.HCL(), coverage.Vocabulary{}, nil)

	database, err := formats.ReadSQL(t.Context(), []byte(dropsBesideAnExtension), "postgres")

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `DROP TABLE a: .*no runtime can say which tables the standalone feature objects bind or are attached to`)
	c.Assert(database, qt.IsNil)
}
