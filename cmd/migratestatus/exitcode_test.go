package migratestatus

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/exitcode"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestMigrateStatusExitCode(t *testing.T) {
	tests := []struct {
		name     string
		status   *migrator.MigrationStatus
		wantCode int
	}{
		{name: "clean", status: &migrator.MigrationStatus{}, wantCode: 0},
		{name: "pending", status: &migrator.MigrationStatus{HasPendingChanges: true}, wantCode: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := pendingMigrationsExitCode(tt.status)

			if tt.wantCode == 0 {
				c.Assert(err, qt.IsNil)
				return
			}
			c.Assert(err, qt.IsNotNil)
			c.Assert(exitcode.Code(err, 0), qt.Equals, tt.wantCode)
		})
	}
}
