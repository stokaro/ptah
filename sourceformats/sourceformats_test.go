package sourceformats_test

import (
	"strings"
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

// TestNewWithNotices_WarnsOnACutName pins that the readers NewWithNotices
// returns write one warning for a PostgreSQL name past 63 bytes, naming what
// was read, and still record the table under the name the server keeps.
func TestNewWithNotices_WarnsOnACutName(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	var notices strings.Builder
	formats := sourceformats.NewWithNotices(runtime.YAML(), runtime.SQL(), runtime.HCL(), coverage.Vocabulary{}, runtime, &notices, "schema command ./export")
	long := strings.Repeat("t", 70)

	database, err := formats.ReadSQL(t.Context(), []byte("CREATE TABLE "+long+" (id int);"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
	c.Assert(database.Tables[0].Name, qt.Equals, long[:63])
	c.Assert(notices.String(), qt.Equals, "warning: schema command ./export: table \""+long+"\" in schema \"public\" is 70 bytes; the server keeps its first 63: \""+long[:63]+"\"\n")
}

// TestNewWithNotices_NilNoticesWritesNothing pins that a nil writer reads as
// New does.
func TestNewWithNotices_NilNoticesWritesNothing(t *testing.T) {
	c := qt.New(t)
	runtime := builtintest.Runtime()
	formats := sourceformats.NewWithNotices(runtime.YAML(), runtime.SQL(), runtime.HCL(), coverage.Vocabulary{}, runtime, nil, "schema command ./export")

	database, err := formats.ReadSQL(t.Context(), []byte("CREATE TABLE "+strings.Repeat("t", 70)+" (id int);"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(database.Tables, qt.HasLen, 1)
}
