package migrateflags_test

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/migrateflags"
	"go.5x5.cz/ptah/migration/migrator"
)

func TestParseExecOrder(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want migrator.ExecOrder
	}{
		{name: "default", in: "", want: migrator.ExecOrderLinear},
		{name: "linear", in: "linear", want: migrator.ExecOrderLinear},
		{name: "linear skip", in: "linear-skip", want: migrator.ExecOrderLinearSkip},
		{name: "non linear", in: "non-linear", want: migrator.ExecOrderNonLinear},
		{name: "trim and case", in: " Non-Linear ", want: migrator.ExecOrderNonLinear},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := migrateflags.ParseExecOrder(tt.in)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestParseExecOrderRejectsUnknownValue(t *testing.T) {
	c := qt.New(t)

	_, err := migrateflags.ParseExecOrder("latest")

	c.Assert(err, qt.ErrorMatches, `invalid exec-order "latest": expected linear, linear-skip, or non-linear`)
}

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

			got, err := migrateflags.ParseMigrationLockTimeout(tt.value)
			if tt.wantErr != "" {
				c.Assert(err, qt.ErrorMatches, tt.wantErr)
				return
			}

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tt.want)
		})
	}
}

func TestParseMigrationTimeouts(t *testing.T) {
	c := qt.New(t)

	timeouts, err := migrateflags.ParseMigrationTimeouts("3s", "30s")
	c.Assert(err, qt.IsNil)
	c.Assert(timeouts.HasLockTimeout, qt.IsTrue)
	c.Assert(timeouts.LockTimeout, qt.Equals, 3*time.Second)
	c.Assert(timeouts.HasStatementTimeout, qt.IsTrue)
	c.Assert(timeouts.StatementTimeout, qt.Equals, 30*time.Second)
}

func TestParseMigrationTimeouts_Invalid(t *testing.T) {
	c := qt.New(t)

	_, err := migrateflags.ParseMigrationTimeouts("0s", "")
	c.Assert(err, qt.ErrorMatches, "invalid lock timeout: must be greater than zero")
}

func TestParseMigrationTxMode_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  migrator.MigrationTxMode
	}{
		{name: "empty defaults to file", value: "", want: migrator.MigrationTxModeFile},
		{name: "file", value: "file", want: migrator.MigrationTxModeFile},
		{name: "all", value: "all", want: migrator.MigrationTxModeAll},
		{name: "none", value: "none", want: migrator.MigrationTxModeNone},
		{name: "case and whitespace", value: " None ", want: migrator.MigrationTxModeNone},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := migrateflags.ParseMigrationTxMode(tc.value)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

func TestParseMigrationTxMode_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := migrateflags.ParseMigrationTxMode("statement")

	c.Assert(err, qt.ErrorMatches, `invalid tx-mode "statement": expected file, all, or none`)
	c.Assert(got, qt.Equals, migrator.MigrationTxMode(""))
}

func TestParseRevisionTableFormat(t *testing.T) {
	tests := []struct {
		name  string
		value string
		want  migrator.RevisionTableFormat
	}{
		{name: "empty defaults to ptah", value: "", want: migrator.RevisionTableFormatPtah},
		{name: "ptah", value: "ptah", want: migrator.RevisionTableFormatPtah},
		{name: "atlas", value: "atlas", want: migrator.RevisionTableFormatAtlas},
		{name: "case and whitespace", value: " Atlas ", want: migrator.RevisionTableFormatAtlas},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := migrateflags.ParseRevisionTableFormat(tc.value)

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

func TestParseRevisionTableFormatRejectsUnknownValue(t *testing.T) {
	c := qt.New(t)

	_, err := migrateflags.ParseRevisionTableFormat("flyway")

	c.Assert(err, qt.ErrorMatches, `unknown revision table format "flyway": expected ptah or atlas`)
}
