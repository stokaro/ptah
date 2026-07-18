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

	c.Assert(postgresMigrationLockKey(), qt.Equals, int64(-7752083082818440098))
}

func TestMySQLMigrationLockTimeoutSeconds(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		timeout time.Duration
		want    float64
	}{
		{
			name:    "mysql default uses native infinite timeout",
			dialect: "mysql",
			want:    -1,
		},
		{
			name:    "mariadb default avoids unsupported negative timeout",
			dialect: "mariadb",
			want:    mariaDBDefaultAdvisoryLockTimeoutSeconds,
		},
		{
			name:    "mysql explicit subsecond rounds up",
			dialect: "mysql",
			timeout: 500 * time.Millisecond,
			want:    1,
		},
		{
			name:    "mariadb explicit duration",
			dialect: "mariadb",
			timeout: 2 * time.Second,
			want:    2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(mySQLMigrationLockTimeoutSeconds(tt.dialect, tt.timeout), qt.Equals, tt.want)
		})
	}
}
