package migrator

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestParseMigrationLockTimeout(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		want    time.Duration
		wantErr string
	}{
		{
			name:  "empty waits indefinitely",
			value: "",
			want:  0,
		},
		{
			name:  "valid duration",
			value: "2m",
			want:  2 * time.Minute,
		},
		{
			name:    "zero rejected",
			value:   "0s",
			wantErr: "invalid migration lock timeout: must be greater than zero",
		},
		{
			name:    "negative rejected",
			value:   "-1s",
			wantErr: "invalid migration lock timeout: must be greater than zero",
		},
		{
			name:    "invalid rejected",
			value:   "soon",
			wantErr: `invalid migration lock timeout: time: invalid duration "soon"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := ParseMigrationLockTimeout(tt.value)
			if tt.wantErr != "" {
				c.Assert(err, qt.ErrorMatches, tt.wantErr)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestIsMigrationLockTimeout(t *testing.T) {
	c := qt.New(t)

	err := fmt.Errorf("wrapped: %w", &MigrationLockTimeoutError{
		Dialect: "postgres",
		Name:    migrationAdvisoryLockName,
		Timeout: 250 * time.Millisecond,
	})

	c.Assert(IsMigrationLockTimeout(err), qt.IsTrue)
	c.Assert(IsMigrationLockTimeout(fmt.Errorf("other error")), qt.IsFalse)
}

func TestPostgresMigrationLockKeyStable(t *testing.T) {
	c := qt.New(t)

	c.Assert(postgresMigrationLockKey(""), qt.Equals, int64(2705505214))
	c.Assert(postgresMigrationLockKey(migrationAdvisoryLockName), qt.Equals, int64(2705505214))
	c.Assert(postgresMigrationLockKey("custom-lock"), qt.Not(qt.Equals), int64(2705505214))
	c.Assert(postgresMigrationLockKey(" custom-lock "), qt.Equals, postgresMigrationLockKey("custom-lock"))
}

func TestWithMigrationLockName(t *testing.T) {
	c := qt.New(t)

	base := NewMigrator(nil, nil)
	c.Assert(base.effectiveMigrationLockName(), qt.Equals, migrationAdvisoryLockName)

	custom := base.WithMigrationLockName("custom-lock")
	c.Assert(custom.effectiveMigrationLockName(), qt.Equals, "custom-lock")
	c.Assert(base.effectiveMigrationLockName(), qt.Equals, migrationAdvisoryLockName)

	blank := custom.WithMigrationLockName(" ")
	c.Assert(blank.effectiveMigrationLockName(), qt.Equals, migrationAdvisoryLockName)
}
