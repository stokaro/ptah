//go:build integration

package ydb_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dbtarget"
	"ptah.run/internal/ydburl"
)

// The directory the binary's secret runs write into, and the variable and
// values its secret takes, each carrying a marker no output may show.
const (
	secretsE2EDir    = "ptah_ydb_secrets_e2e"        // #nosec G101 -- a directory name, not a credential
	secretsE2EEnv    = "PTAH_SECRET_E2E_PG_PASSWORD" // #nosec G101 -- a variable name, not a credential
	secretsE2EFirst  = "e2e-SENTINEL-first"          // #nosec G101 -- a made-up value every output is searched for
	secretsE2ESecond = "e2e-SENTINEL-second"         // #nosec G101 -- a made-up value every output is searched for
)

// secretsE2EEntities declares a table and the secret beside it.
const secretsE2EEntities = `package entities

//ptah:schema:table name="accounts" schema="` + secretsE2EDir + `"
type Account struct {
	//ptah:schema:field name="id" type="BIGINT" primary
	ID int64
}

//ptah:schema:secret name="pg_password" schema="` + secretsE2EDir + `" value_env="` + secretsE2EEnv + `"
type Credentials struct{}
`

// secretsE2ERefused declares, beside the same table and secret, a secret
// whose name holds a space, which the server refuses at execution: `symbol '
// ' is not allowed in the path part`.
const secretsE2ERefused = secretsE2EEntities + `
//ptah:schema:secret name="bad name" schema="` + secretsE2EDir + `" value_env="` + secretsE2EEnv + `"
type Refused struct{}
`

// TestYDBBinary_KeepsASecretsValueOutOfEveryOutput drives the shipped binary
// through a secret's whole life -- created by schema apply, compared, rotated
// in a dry run and in a generated migration that migrations up applies, and
// one refused by the server -- with the value in the environment, and then
// searches everything the runs wrote, the streams and the migration files,
// for the value. It is nowhere; the variable's name is where the statements
// are.
func TestYDBBinary_KeepsASecretsValueOutOfEveryOutput(t *testing.T) {
	t.Setenv(secretsE2EEnv, secretsE2EFirst)
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	line := lineNamed(c, "26.2")
	url := dbtarget.URL(c, line.engine)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	conn := openYDB(c, line)
	clean := func() {
		dropSecrets(c, conn, []string{secretsE2EDir})
		dropTables(c, conn, []string{secretsE2EDir})
		dropDirectory(c, conn, secretsE2EDir)
	}
	clean()
	c.Cleanup(clean)
	entities, refusing, migrations := c.TempDir(), c.TempDir(), c.TempDir()
	c.Assert(os.WriteFile(filepath.Join(entities, "schema.go"), []byte(secretsE2EEntities), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(filepath.Join(refusing, "schema.go"), []byte(secretsE2ERefused), 0o600), qt.IsNil)
	scope := []string{"--db-url", url, "--schemas", secretsE2EDir}
	rotate := []string{"--rotate-secret", secretsE2EDir + "/pg_password"}

	applied, applyErr := runBinary(ctx, binary, append([]string{"schema", "apply", "--root-dir", entities,
		"--auto-approve"}, scope...)...)
	compared, compareErr := runBinary(ctx, binary, append([]string{"schema", "compare", "--root-dir", entities,
		"--exit-code"}, scope...)...)
	t.Setenv(secretsE2EEnv, secretsE2ESecond)
	previewed, previewErr := runBinary(ctx, binary, append(append([]string{"schema", "apply", "--root-dir", entities,
		"--dry-run"}, scope...), rotate...)...)
	generated, generateErr := runBinary(ctx, binary, append(append([]string{"migrations", "generate", "--root-dir",
		entities, "--migrations-dir", migrations, "--name", "rotate"}, scope...), rotate...)...)
	upped, upErr := runBinary(ctx, binary, "migrations", "up", "--db-url", url, "--migrations-dir", migrations,
		"--migrations-schema", secretsE2EDir)
	refused, refusedErr := runBinary(ctx, binary, append([]string{"schema", "apply", "--root-dir", refusing,
		"--auto-approve"}, scope...)...)

	c.Assert(applyErr, qt.IsNil, qt.Commentf("schema apply:\n%s", applied))
	c.Assert(applied, qt.Contains, "CREATE SECRET `"+secretsE2EDir+"/pg_password` WITH (value = $"+secretsE2EEnv+")")
	c.Assert(compareErr, qt.IsNil, qt.Commentf("schema compare:\n%s", compared))
	c.Assert(previewErr, qt.IsNil, qt.Commentf("schema apply --dry-run:\n%s", previewed))
	c.Assert(previewed, qt.Contains, "ALTER SECRET `"+secretsE2EDir+"/pg_password` WITH (value = $"+secretsE2EEnv+")")
	c.Assert(generateErr, qt.IsNil, qt.Commentf("migrations generate:\n%s", generated))
	c.Assert(upErr, qt.IsNil, qt.Commentf("migrations up:\n%s", upped))
	c.Assert(refusedErr, qt.IsNotNil)
	c.Assert(refused, qt.Contains, "is not allowed in the path part")
	c.Assert(refused, qt.Contains, "CREATE SECRET `"+secretsE2EDir+"/bad name` WITH (value = $"+secretsE2EEnv+")")

	upFiles, err := filepath.Glob(filepath.Join(migrations, "*rotate*.up.sql"))
	c.Assert(err, qt.IsNil)
	c.Assert(upFiles, qt.HasLen, 1)
	upSQL, err := os.ReadFile(upFiles[0])
	c.Assert(err, qt.IsNil)
	c.Assert(string(upSQL), qt.Contains, "ALTER SECRET `"+secretsE2EDir+"/pg_password` WITH (value = $"+secretsE2EEnv+");")
	written := []string{applied, compared, previewed, generated, upped, refused}
	files, err := os.ReadDir(migrations)
	c.Assert(err, qt.IsNil)
	for _, file := range files {
		body, err := os.ReadFile(filepath.Join(migrations, file.Name()))
		c.Assert(err, qt.IsNil)
		written = append(written, string(body))
	}
	for _, text := range written {
		c.Assert(text, qt.Not(qt.Contains), "SENTINEL")
	}
	c.Assert(written, qt.HasLen, 6+len(files))
	c.Assert(len(files) >= 2, qt.IsTrue, qt.Commentf("migrations generate wrote %d files", len(files)))
}

// TestYDBBinary_ReplaysASecretInADevRealm validates a migration directory that
// creates and rotates a secret on a dev realm in the test database: the
// secret is a path the realm confines, the replay takes its value from the
// environment as an apply does, and the realm is gone, with the secret in it,
// when the binary exits. The deprecated secret object belongs to a user
// rather than to a path, so a replay refuses it.
func TestYDBBinary_ReplaysASecretInADevRealm(t *testing.T) {
	t.Setenv(secretsE2EEnv, secretsE2EFirst)
	c := qt.New(t)
	binary := buildBinary(c, c.Context())
	line := lineNamed(c, "26.2")
	url := dbtarget.URL(c, line.engine)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	confined, refusing := filepath.Join(c.TempDir(), "migrations"), filepath.Join(c.TempDir(), "migrations")
	writeFiles(c, confined, map[string]string{ // #nosec G101 -- statements that name a variable, not a credential
		"0000000001_secret.up.sql": "CREATE SECRET `ptah_ydb_devrealm_secrets/pw` WITH (value = $" + secretsE2EEnv + ");\n" +
			"ALTER SECRET `ptah_ydb_devrealm_secrets/pw` WITH (value = $" + secretsE2EEnv + ");\n",
		"0000000001_secret.down.sql": "DROP SECRET `ptah_ydb_devrealm_secrets/pw`;\n",
	})
	writeFiles(c, refusing, map[string]string{ // #nosec G101 -- a made-up value the replay refuses
		"0000000001_secret.up.sql":   "CREATE OBJECT ptah_ydb_devrealm_secret (TYPE SECRET) WITH value = 'x';\n",
		"0000000001_secret.down.sql": "DROP OBJECT ptah_ydb_devrealm_secret (TYPE SECRET);\n",
	})
	for _, dir := range []string{confined, refusing} {
		hashed, err := runBinary(ctx, binary, "migrations", "hash", "--dir", dir)
		c.Assert(err, qt.IsNil, qt.Commentf("%s", hashed))
	}

	validated, validateErr := runBinary(ctx, binary, "migrations", "validate", "--dir", confined, "--dev-url", url)
	refused, refusedErr := runBinary(ctx, binary, "migrations", "validate", "--dir", refusing, "--dev-url", url)

	c.Assert(validateErr, qt.IsNil, qt.Commentf("%s", validated))
	c.Assert(validated, qt.Contains, "OK: migration SQL validated on dev database")
	c.Assert(validated, qt.Not(qt.Contains), "SENTINEL")
	c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), "ptah_ydb_devrealm_secrets")
	c.Assert(directoryNames(c, ctx, line), qt.Not(qt.Contains), ydburl.RealmDirectory)
	c.Assert(refusedErr, qt.IsNotNil)
	c.Assert(refused, qt.Contains, "ydb migration replay rejects an object of the whole database because its "+
		"effects cannot be confined to the disposable database realm")
}
