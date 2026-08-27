package quickstart_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/quickstart"
)

// TestRenderScript_HappyPath pins the parts of the generated script that decide
// whether a failing step ends the run.
//
// Both shells need this and neither gets it for free: Bash needs `set -e`, and
// PowerShell's $ErrorActionPreference does not govern a native binary's exit
// status at all, so every step checks $LASTEXITCODE itself.
func TestRenderScript_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		shell        quickstart.Shell
		wantContains []string
	}{
		{
			name:  "bash",
			shell: quickstart.Bash,
			wantContains: []string{
				"set -euo pipefail\n",
				"cat >'schema.sql' <<'__ptah_quickstart_file__'\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n__ptah_quickstart_file__\n",
				"mkdir work && cd work\n",
				"printf '%s\\n' '__ptah_quickstart_step_1__'\n",
				"printf '%s\\n' '__ptah_quickstart_step_1__' >&2\n",
			},
		},
		{
			name:  "powershell",
			shell: quickstart.PowerShell,
			wantContains: []string{
				"$ErrorActionPreference = 'Stop'\n",
				"$PSNativeCommandUseErrorActionPreference = $false\n",
				"[System.IO.File]::WriteAllText($ptahQuickstartPath, @'\nCREATE TABLE users (id INTEGER PRIMARY KEY);\n'@ + \"`n\")\n",
				"Set-Location work\n",
				"if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) { exit $LASTEXITCODE }\n",
				"Write-Output '__ptah_quickstart_step_1__'\n",
				"[Console]::Error.WriteLine('__ptah_quickstart_step_1__')\n",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page := loadPage(c, optedInPage)
			found := program(c, page, test.shell)

			script, err := quickstart.RenderScript(found)

			c.Assert(err, qt.IsNil)
			for _, want := range test.wantContains {
				c.Assert(script, qt.Contains, want)
			}
		})
	}
}

// TestRenderScript_FailurePath refuses a file whose own contents would close
// the here-document early. Emitting it anyway would run the rest of the file as
// shell commands.
func TestRenderScript_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		shell   quickstart.Shell
		body    string
		wantErr string
	}{
		{
			name:    "bash here-document terminator",
			shell:   quickstart.Bash,
			body:    "SELECT 1;\n__ptah_quickstart_file__\nSELECT 2;",
			wantErr: `file schema.sql contains the here-document terminator __ptah_quickstart_file__ on a line of its own`,
		},
		{
			name:    "powershell here-string terminator",
			shell:   quickstart.PowerShell,
			body:    "SELECT 1;\n'@\nSELECT 2;",
			wantErr: `file schema.sql contains the here-string terminator '@ on a line of its own`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			found := &quickstart.Program{
				Shell: test.shell,
				Actions: []quickstart.Action{
					{Kind: quickstart.ActionFile, Line: 1, Path: "schema.sql", Body: test.body},
				},
			}

			script, err := quickstart.RenderScript(found)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(script, qt.Equals, "")
		})
	}
}
