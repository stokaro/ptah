package introspect

// White-box testing required: validateOptions is an unexported validation
// helper whose exact option error paths are easier to exercise directly than
// through Cobra command execution.

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
)

func TestValidateOptions_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		opts options
		want string
	}{
		{
			name: "missing db URL",
			opts: options{outDir: "models", packageName: "models"},
			want: "--db-url is required",
		},
		{
			name: "missing out",
			opts: options{dbURL: "postgres://localhost/db", packageName: "models"},
			want: "--out is required",
		},
		{
			name: "missing package",
			opts: options{dbURL: "postgres://localhost/db", outDir: "models"},
			want: "--package is required",
		},
		{
			name: "conflicting layout flags",
			opts: options{dbURL: "postgres://localhost/db", outDir: "models", packageName: "models", perTable: true, singleFile: true},
			want: "--single-file and --per-table are mutually exclusive",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := validateOptions(tt.opts)

			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

func TestValidateOptions_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		opts options
	}{
		{
			name: "valid default per table",
			opts: options{dbURL: "postgres://localhost/db", outDir: "models", packageName: "models"},
		},
		{
			name: "valid explicit per table",
			opts: options{dbURL: "postgres://localhost/db", outDir: "models", packageName: "models", perTable: true},
		},
		{
			name: "valid single file",
			opts: options{dbURL: "postgres://localhost/db", outDir: "models", packageName: "models", singleFile: true},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			err := validateOptions(tt.opts)

			c.Assert(err, qt.IsNil)
		})
	}
}

func TestNewIntrospectCommandHelpShowsImportFlags(t *testing.T) {
	c := qt.New(t)

	cmd := NewIntrospectCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs([]string{"--help"})

	err := cmd.Execute()

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "Generate annotated Go models from a live database")
	c.Assert(out.String(), qt.Contains, "--db-url")
	c.Assert(out.String(), qt.Contains, "--out")
	c.Assert(out.String(), qt.Contains, "--package")
	c.Assert(out.String(), qt.Contains, "--single-file")
}

func TestNewIntrospectCommandDefaultsConnectTimeout(t *testing.T) {
	c := qt.New(t)

	cmd := NewIntrospectCommand()
	flag := cmd.Flags().Lookup(dbcli.ConnectTimeoutFlagName)

	c.Assert(flag, qt.IsNotNil)
	c.Assert(flag.DefValue, qt.Equals, dbcli.DefaultConnectTimeout.String())
	c.Assert(flag.Value.String(), qt.Equals, dbcli.DefaultConnectTimeout.String())
}
