package seeder_test

import (
	"context"
	"fmt"
	"testing/fstest"

	"github.com/go-extras/go-kit/must"

	"ptah.run/dbschema"
	"ptah.run/migration/seeder"
)

// ExampleDiscover walks a filesystem of seed files and reports what each name
// parses into. The filename grammar is NNN_description.env.sql: the version is
// numeric, so 005 sorts before 010, and the env segment is lowercased, so an
// authored PROD and prod are one environment. A .sql file that does not match
// the grammar would fail discovery with an error naming it rather than being
// silently dropped.
func ExampleDiscover() {
	fsys := fstest.MapFS{
		"010_countries.all.sql": {Data: []byte("INSERT INTO countries (code) VALUES ('cz');")},
		"020_users.DEV.sql":     {Data: []byte("INSERT INTO users (name) VALUES ('dev-admin');")},
		"005_plans.prod.sql":    {Data: []byte("INSERT INTO plans (name) VALUES ('paid');")},
	}

	seeds := must.Must(seeder.Discover(fsys))
	for _, seed := range seeds {
		fmt.Printf("%d %s env=%s\n", seed.Version, seed.Description, seed.Env)
	}

	// Output:
	// 5 plans env=prod
	// 10 countries env=all
	// 20 users env=dev
}

// ExampleSelect narrows discovered seeds to one environment. A seed whose env
// is "all" belongs to every environment, and the requested env is normalized
// before the comparison, so "DEV" selects the dev seeds.
func ExampleSelect() {
	seeds := []seeder.SeedFile{
		{Filename: "005_plans.prod.sql", Env: "prod"},
		{Filename: "010_countries.all.sql", Env: "all"},
		{Filename: "020_users.dev.sql", Env: "dev"},
	}

	for _, seed := range seeder.Select(seeds, "DEV") {
		fmt.Println(seed.Filename)
	}

	// Output:
	// 010_countries.all.sql
	// 020_users.dev.sql
}

// ExampleApply runs the whole pipeline against an in-memory SQLite database:
// the first run applies the seed and records it in the schema_seeds tracker
// table, and the second run finds the recorded checksum unchanged and skips
// the file instead of inserting the rows again.
func ExampleApply() {
	ctx := context.Background()

	conn := must.Must(dbschema.ConnectToDatabase(ctx, "sqlite://dev?mode=memory"))
	defer dbschema.CloseAndWarn(conn)

	must.Must(conn.ExecContext(ctx, "CREATE TABLE countries (code TEXT NOT NULL)"))

	seeds := fstest.MapFS{
		"010_countries.all.sql": {Data: []byte("INSERT INTO countries (code) VALUES ('cz'), ('sk');")},
	}

	first, err := seeder.Apply(ctx, conn, seeds, seeder.Options{Env: "dev"})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("applied=%d skipped=%d\n", len(first.Applied), len(first.Skipped))

	second := must.Must(seeder.Apply(ctx, conn, seeds, seeder.Options{Env: "dev"}))
	fmt.Printf("applied=%d skipped=%d\n", len(second.Applied), len(second.Skipped))

	// Output:
	// applied=1 skipped=0
	// applied=0 skipped=1
}

// ExampleValidateOptions shows the protected-environment refusal: with
// ProtectedEnvs empty the defaults apply, so seeding "prod" needs an explicit
// AllowProd. Apply performs the same check; calling ValidateOptions directly
// lets a caller refuse early, before connecting anywhere.
func ExampleValidateOptions() {
	err := seeder.ValidateOptions(seeder.Options{Env: "prod"})
	fmt.Println("prod refused:", err != nil)

	err = seeder.ValidateOptions(seeder.Options{Env: "prod", AllowProd: true})
	fmt.Println("prod with AllowProd refused:", err != nil)

	err = seeder.ValidateOptions(seeder.Options{})
	fmt.Println("no environment refused:", err != nil)

	// Output:
	// prod refused: true
	// prod with AllowProd refused: false
	// no environment refused: true
}
