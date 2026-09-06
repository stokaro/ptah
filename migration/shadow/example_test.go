package shadow_test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"ptah.run/core/yamlschema"
	"ptah.run/dbschema"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
	"ptah.run/migration/shadow"
)

// ExampleVerifyMigration runs the generator's pre-write check standalone: the
// prior history plus one planned candidate are replayed into a disposable
// SQLite database, the catalog that results is compared with the desired
// schema, and the candidate is rolled back and reapplied. Every database here
// is a file in a temporary directory, which is the pattern to copy: the shadow
// database is dropped clean, so it must never point at anything shared, and
// the target and shadow must be two different databases by construction.
func ExampleVerifyMigration() {
	ctx := context.Background()
	dir := must.Must(os.MkdirTemp("", "shadow-example"))
	defer os.RemoveAll(dir)

	// The target is the database the candidate would eventually be applied
	// to. Verification only reads it, to prove the shadow is a different
	// realm.
	target := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(dir, "target.db")))
	defer dbschema.CloseAndWarn(target)

	// The already-written history, replayed before the candidate.
	history := fstest.MapFS{
		"0000000001_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")},
		"0000000001_users.down.sql": {Data: []byte("DROP TABLE users;")},
	}

	// The desired schema: what the history plus the candidate should produce.
	desired := must.Must(yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id:
        type: INTEGER
        primary: true
      email:
        type: TEXT
        not_null: true
  posts:
    columns:
      id:
        type: INTEGER
        primary: true
      user_id:
        type: INTEGER
        not_null: true
`)))

	err := shadow.VerifyMigration(ctx, shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(dir, "shadow.db"),
		TargetConnection:  target,
		MigrationsFS:      history,
		Dialect:           "sqlite",
		Candidates: []shadow.Candidate{{
			Version: 2,
			Name:    "create_posts",
			UpSQL:   "CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL);",
			DownSQL: "DROP TABLE posts;",
		}},
		Generated: desired,
	})
	if err != nil {
		fmt.Println("verification failed:", err)
		return
	}
	fmt.Println("candidate verified: replay, schema match, and round trip all passed")

	// Output:
	// candidate verified: replay, schema match, and round trip all passed
}

// ExampleVerifyMigration_mismatch shows the structured-failure contract: a
// candidate whose SQL does not produce the desired schema fails with a
// *VerificationError, and errors.AsType recovers the stage that stopped and the
// deterministic mismatch list, object by object. This is the machine-readable
// side of the same failure the error text renders.
func ExampleVerifyMigration_mismatch() {
	ctx := context.Background()
	dir := must.Must(os.MkdirTemp("", "shadow-example"))
	defer os.RemoveAll(dir)

	target := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(dir, "target.db")))
	defer dbschema.CloseAndWarn(target)

	// The desired schema declares an email column.
	desired := must.Must(yamlschema.Parse([]byte(`
tables:
  users:
    columns:
      id:
        type: INTEGER
        primary: true
      email:
        type: TEXT
        not_null: true
`)))

	// The candidate forgets it.
	err := shadow.VerifyMigration(ctx, shadow.MigrationVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(dir, "shadow.db"),
		TargetConnection:  target,
		Dialect:           "sqlite",
		Candidates: []shadow.Candidate{{
			Version: 1,
			Name:    "create_users",
			UpSQL:   "CREATE TABLE users (id INTEGER PRIMARY KEY);",
			DownSQL: "DROP TABLE users;",
		}},
		Generated: desired,
	})

	fmt.Println(err)
	if verificationErr, ok := errors.AsType[*shadow.VerificationError](err); ok {
		fmt.Println("stage:", verificationErr.Result.Stage)
		for _, mismatch := range verificationErr.Result.Mismatches {
			fmt.Printf("%s %s\n", mismatch.Kind, mismatch.Object)
		}
	}

	// Output:
	// shadow check failed: missing column users.email
	// stage: schema-match
	// missing_column users.email
}

// ExampleVerifyBaseline makes recording a baseline honest rather than assumed:
// the history is replayed up to the requested version on a disposable shadow
// database and the schema that results is compared with the target. Success
// means the target really is what the history says version 1 looks like.
func ExampleVerifyBaseline() {
	ctx := context.Background()
	dir := must.Must(os.MkdirTemp("", "shadow-example"))
	defer os.RemoveAll(dir)

	// The target already carries the schema, applied outside migration
	// tooling.
	target := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(dir, "target.db")))
	defer dbschema.CloseAndWarn(target)
	must.Must(target.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL)"))

	// The history that claims to describe it.
	history := fstest.MapFS{
		"0000000001_users.up.sql":   {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT NOT NULL);")},
		"0000000001_users.down.sql": {Data: []byte("DROP TABLE users;")},
	}

	err := shadow.VerifyBaseline(ctx, shadow.BaselineVerifyOptions{
		ShadowDatabaseURL: "sqlite://" + filepath.Join(dir, "shadow.db"),
		TargetConn:        target,
		MigrationsFS:      history,
		Version:           1,
		Dialect:           "sqlite",
	})
	if err != nil {
		fmt.Println("baseline refused:", err)
		return
	}
	fmt.Println("baseline verified: version 1 reproduces the target schema")

	// Output:
	// baseline verified: version 1 reproduces the target schema
}

// ExamplePlanDynamicRollback derives rollback statements without reading a
// down file: the dev database is migrated up to the target version so it
// holds exactly the schema that version defines, and the difference between
// the live database and that state is the plan. The migration history here
// carries no down bodies at all, which is the reason to reach for this entry
// point.
func ExamplePlanDynamicRollback() {
	ctx := context.Background()
	dir := must.Must(os.MkdirTemp("", "shadow-example"))
	defer os.RemoveAll(dir)

	// The live database sits at version 2: both migrations applied.
	live := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite://"+filepath.Join(dir, "live.db")))
	defer dbschema.CloseAndWarn(live)
	must.Must(live.ExecContext(ctx, "CREATE TABLE users (id INTEGER PRIMARY KEY)"))
	must.Must(live.ExecContext(ctx, "CREATE TABLE posts (id INTEGER PRIMARY KEY)"))

	// An Atlas-format directory: up bodies only, no down file anywhere.
	history := fstest.MapFS{
		"1_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER PRIMARY KEY);")},
		"2_posts.sql": {Data: []byte("CREATE TABLE posts (id INTEGER PRIMARY KEY);")},
	}

	statements, err := shadow.PlanDynamicRollback(ctx, shadow.DynamicRollbackOptions{
		TargetConnection: live,
		// A separate file: a dev URL that resolves to the live database is
		// refused, because the dev database is dropped clean.
		DevDatabaseURL: "sqlite://" + filepath.Join(dir, "dev.db"),
		FS:             history,
		TargetVersion:  1,
		ProviderOptions: []migrator.FSProviderOption{
			migrator.WithMigrationDirFormat(migrationfile.DirFormatAtlas),
		},
	})
	if err != nil {
		fmt.Println("planning failed:", err)
		return
	}
	for _, statement := range statements {
		fmt.Println(statement)
	}

	// Output:
	// -- WARNING: This will delete all data!
	// DROP TABLE IF EXISTS "posts"
}
