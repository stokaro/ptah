//go:build ptah_live_generator

package generator

// White-box testing required: shadowIdentifierSemanticsMatch is the preflight
// boundary that must compare the target snapshot with the shadow catalog before
// any destructive shadow reset, and that ordering is not observable through a
// public API without executing the complete generator workflow.

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
)

func TestShadowIdentifierSemanticsMatch_SQLServerLive(t *testing.T) {
	c := qt.New(t)
	targetURL := provisionShadowCollationDatabase(t)
	target := connectShadowCollationDatabase(t, targetURL)
	admin := connectShadowCollationDatabase(t, sqlServerAdminTestURL(t))
	semantics, err := target.ResolveIdentifierSemantics(
		t.Context(),
		[]string{"I", "i", "\u0131"},
	)
	c.Assert(err, qt.IsNil)

	matches, err := shadowIdentifierSemanticsMatch(
		t.Context(),
		target,
		platform.SQLServer,
		semantics,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.IsTrue)

	matches, err = shadowIdentifierSemanticsMatch(
		t.Context(),
		admin,
		platform.SQLServer,
		semantics,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(matches, qt.IsFalse)
}

func provisionShadowCollationDatabase(t testing.TB) string {
	t.Helper()
	c := qt.New(t)
	adminURL := sqlServerAdminTestURL(t)
	databaseName := fmt.Sprintf("ptah_777_shadow_%d", time.Now().UnixNano())
	admin := connectShadowCollationDatabase(t, adminURL)
	_, err := admin.ExecContext(
		t.Context(),
		"CREATE DATABASE "+quoteShadowIdentifier(databaseName)+
			" COLLATE Turkish_100_CI_AS",
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

func sqlServerAdminTestURL(t testing.TB) string {
	t.Helper()
	adminURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if adminURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live generator tests")
	}
	return adminURL
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
