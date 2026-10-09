//go:build integration

package generator_test

import (
	"path/filepath"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/config"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlschema"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/shadow"
)

// TestVerifyMigration_LeavesTheChangeRequestsOut verifies a candidate with
// compare options that carry a change request. The request belongs to the
// plan that wrote the candidate, so the convergence check of the replayed
// database leaves it out: asked again, a rotation the candidate made would
// plan its statement a second time and read as a mismatch. Here the target
// has no owner that accepts the request at all, so a check that kept it
// would fail outright.
func TestVerifyMigration_LeavesTheChangeRequestsOut(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	target, err := shadowClaimTarget(c.Context(), dir)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(target)
	desired := must.Must(yamlschema.Parse([]byte("tables:\n  users:\n    columns:\n      id:\n        type: INTEGER\n        primary: true\n" +
		"  posts:\n    columns:\n      id:\n        type: INTEGER\n        primary: true\n")))
	request := schemaext.ChangeRequest{Action: "rotate",
		Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts("ptah.run/ydb/secret", "ext", "pw")}

	err = shadow.VerifyMigration(c.Context(), shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(dir, "shadow.db"),
		TargetConnection:  target,
		MigrationsFS: fstest.MapFS{
			"0000000001_users.up.sql":   shadowClaimHistory["0000000001_users.up.sql"],
			"0000000001_users.down.sql": shadowClaimHistory["0000000001_users.down.sql"],
		},
		Dialect: "sqlite",
		Candidates: []shadow.Candidate{{Version: 2, Name: "posts",
			UpSQL: "CREATE TABLE posts (id INTEGER PRIMARY KEY);", DownSQL: "DROP TABLE posts;"}},
		Generated:   desired,
		CompareOpts: &config.CompareOptions{FeatureRequests: []schemaext.ChangeRequest{request}},
		Runtime:     must.Must(builtin.New())})

	c.Assert(err, qt.IsNil)
}
