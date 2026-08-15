//go:build integration

package generator_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/generator"
)

func TestShadowIdentifierSemanticsMatch_SQLServerLive(t *testing.T) {
	c := qt.New(t)
	targetURL := provisionShadowCollationDatabase(t, "Turkish_100_CI_AS")
	matchingShadowURL := provisionShadowCollationDatabase(t, "Turkish_100_CI_AS")
	mismatchedShadowURL := provisionShadowCollationDatabase(t, "SQL_Latin1_General_CP1_CI_AS")
	target := connectShadowCollationDatabase(t, targetURL)
	mismatchedShadow := connectShadowCollationDatabase(t, mismatchedShadowURL)

	migrationSQL := `CREATE TABLE ptah_shadow_semantics (
	id INT NOT NULL,
	CONSTRAINT pk_ptah_shadow_semantics PRIMARY KEY (id)
);
-- Turkish case-insensitive catalogs keep I and i distinct, while the Latin
-- shadow collation folds them into one identifier-equivalence class.
CREATE TABLE [ptah_shadow_I] (id INT NOT NULL);
CREATE TABLE [ptah_shadow_i] (id INT NOT NULL);
`
	_, err := target.ExecContext(t.Context(), migrationSQL)
	c.Assert(err, qt.IsNil)
	_, err = mismatchedShadow.ExecContext(t.Context(), "CREATE TABLE preserve_before_semantics_check (id INT NOT NULL)")
	c.Assert(err, qt.IsNil)

	migrationsDir := t.TempDir()
	c.Assert(os.WriteFile(filepath.Join(migrationsDir, "0000000001_init.up.sql"), []byte(migrationSQL), 0o600), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(migrationsDir, "0000000001_init.down.sql"),
		[]byte("DROP TABLE [ptah_shadow_i]; DROP TABLE [ptah_shadow_I]; DROP TABLE ptah_shadow_semantics;\n"),
		0o600,
	), qt.IsNil)
	info := target.Info()

	err = generator.VerifyBaselineShadow(t.Context(), generator.BaselineShadowVerifyOptions{
		ShadowDatabaseURL: matchingShadowURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
	})
	c.Assert(err, qt.IsNil)

	err = generator.VerifyBaselineShadow(t.Context(), generator.BaselineShadowVerifyOptions{
		ShadowDatabaseURL: mismatchedShadowURL,
		TargetConn:        target,
		MigrationsDir:     migrationsDir,
		Version:           1,
		Dialect:           info.Dialect,
		Capabilities:      info.Capabilities,
	})
	c.Assert(err, qt.ErrorMatches, `baseline shadow check failed: shadow database identifier semantics do not match target sqlserver catalog semantics`)
	var shadowErr *generator.ShadowVerificationError
	c.Assert(err, qt.ErrorAs, &shadowErr)
	c.Assert(shadowErr.Result.Stage, qt.Equals, "identifier-semantics-check")
	c.Assert(shadowErr.Result.Mismatches, qt.DeepEquals, []generator.ShadowMismatch{{
		Kind:    "identifier_semantics_mismatch",
		Message: "shadow database identifier semantics do not match target sqlserver catalog semantics",
	}})
	c.Assert(sqlServerShadowTableExists(c, mismatchedShadow, "preserve_before_semantics_check"), qt.IsTrue)
}

func provisionShadowCollationDatabase(t testing.TB, collation string) string {
	t.Helper()
	c := qt.New(t)
	adminURL := sqlServerAdminTestURL(t)
	databaseName := fmt.Sprintf("ptah_777_shadow_%d", time.Now().UnixNano())
	admin := connectShadowCollationDatabase(t, adminURL)
	_, err := admin.ExecContext(
		t.Context(),
		"CREATE DATABASE "+quoteShadowIdentifier(databaseName)+
			" COLLATE "+collation,
	)
	c.Assert(err, qt.IsNil)

	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(
			context.Background(),
			30*time.Second,
		)
		defer cancelCleanup()
		cleanup, cleanupErr := dbschema.ConnectToDatabase(cleanupCtx, adminURL)
		c.Assert(cleanupErr, qt.IsNil)
		defer dbschema.CloseAndWarn(cleanup)
		_, cleanupErr = cleanup.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quoteShadowIdentifier(databaseName)+
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+
				quoteShadowIdentifier(databaseName),
		)
		c.Assert(cleanupErr, qt.IsNil)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", databaseName)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func sqlServerShadowTableExists(c *qt.C, conn *dbschema.DatabaseConnection, tableName string) bool {
	c.Helper()
	var count int
	err := conn.QueryRowContext(
		c.Context(),
		"SELECT COUNT(*) FROM sys.tables WHERE name = @p1",
		tableName,
	).Scan(&count)
	c.Assert(err, qt.IsNil)
	return count == 1
}

func sqlServerAdminTestURL(t testing.TB) string {
	t.Helper()
	return dbtarget.URL(t, dbtarget.SQLServer)
}

func connectShadowCollationDatabase(
	t testing.TB,
	databaseURL string,
) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), databaseURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	return conn
}

func quoteShadowIdentifier(value string) string {
	return "[" + strings.ReplaceAll(value, "]", "]]") + "]"
}
