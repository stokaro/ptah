//go:build integration

package migrator_test

import (
	"testing"

	"go.5x5.cz/ptah/internal/dbtarget"
)

// sqlServerTestURL resolves the live SQL Server address. dbtarget declares the
// sqlserver scheme for this engine and refuses anything else, so the dialect
// guard this helper used to carry has nothing left to catch.
func sqlServerTestURL(t *testing.T) string {
	t.Helper()

	return dbtarget.URL(t, dbtarget.SQLServer)
}
