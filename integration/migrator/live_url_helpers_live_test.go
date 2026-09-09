//go:build integration

package migrator_test

import (
	"testing"

	"ptah.run/internal/dbtarget"
)

// sqlServerTestURL resolves the live SQL Server address. dbtarget declares the
// sqlserver scheme for this engine and refuses anything else, so a dialect
// guard in this helper would have nothing to catch.
func sqlServerTestURL(t *testing.T) string {
	t.Helper()

	return dbtarget.URL(t, dbtarget.SQLServer)
}
