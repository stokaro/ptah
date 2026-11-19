package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
)

// atlasRepeatableGateFS is an Atlas directory whose R__x.sql, the version R,
// runs after 1_a.sql under the key 2 and drops a table, under a policy that
// also gates on the migration-form family.
func atlasRepeatableGateFS() fstest.MapFS {
	return fstest.MapFS{
		lint.ConfigFileName: {Data: []byte("gate:\n  families: [DS, MF]\n")},
		"1_a.sql":           {Data: []byte("CREATE TABLE a (id INTEGER);\n")},
		"R__x.sql":          {Data: []byte("DROP TABLE a;\n")},
	}
}

// The migrator runs R__x.sql under the key above every numbered file, 2, so a
// plan that applies it hands the gate that key, and the gate judges the file.
// Over the Atlas revision table, which keeps the version R, the gate reports
// what the file does and not a refusal the apply does not issue.
func TestAnalyzeWithPolicy_AtlasRepeatable_FailurePath(t *testing.T) {
	c := qt.New(t)
	fsys := atlasRepeatableGateFS()
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), fsys, "sqlite")
	c.Assert(err, qt.IsNil)

	findings, err := migrationlintgate.AnalyzeWithPolicy(c.Context(), builtintest.Runtime(), fsys,
		migrationlintgate.Pending{Versions: []int64{1, 2}}, policy.WithAtlasRevisionTable(), "")

	c.Assert(err, qt.IsNil)
	c.Assert(gateSites(findings), qt.DeepEquals, []string{"R__x.sql:1:DS101"})
}

// Judged for Ptah's revision table, which records numbers, the same plan
// carries the refusal a run over that table issues before anything runs.
func TestAnalyzeWithPolicy_AtlasRepeatableOverTheNumericTable_FailurePath(t *testing.T) {
	c := qt.New(t)
	fsys := atlasRepeatableGateFS()
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), fsys, "sqlite")
	c.Assert(err, qt.IsNil)

	findings, err := migrationlintgate.AnalyzeWithPolicy(c.Context(), builtintest.Runtime(), fsys,
		migrationlintgate.Pending{Versions: []int64{1, 2}}, policy, "")

	c.Assert(err, qt.IsNil)
	c.Assert(gateSites(findings), qt.DeepEquals, []string{"R__x.sql:0:MF105P", "R__x.sql:1:DS101"})
}

// A plan that has applied R__x.sql already hands the gate 1 alone, and the
// gate judges nothing of it.
func TestAnalyzeWithPolicy_AtlasRepeatable_HappyPath(t *testing.T) {
	c := qt.New(t)
	fsys := atlasRepeatableGateFS()
	policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), fsys, "sqlite")
	c.Assert(err, qt.IsNil)

	findings, err := migrationlintgate.AnalyzeWithPolicy(c.Context(), builtintest.Runtime(), fsys,
		migrationlintgate.Pending{Versions: []int64{1}}, policy.WithAtlasRevisionTable(), "")

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 0)
}
