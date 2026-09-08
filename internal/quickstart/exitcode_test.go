package quickstart_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/quickstart"
)

// exitsPage is a page whose second step declares the status it must end with,
// which is what a drift check needs: `ptah schema drift` exits 1 when it finds
// drift, and that is the command working (stokaro/ptah#3018).
const exitsPage = "testdata/exits/start/declares-an-exit-status.mdx"

// TestExtract_ReadsADeclaredExitStatus_HappyPath pins that the fence option
// reaches the action, and that a fence declaring nothing still means zero.
func TestExtract_ReadsADeclaredExitStatus_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		shell quickstart.Shell
	}{
		{name: "bash", shell: quickstart.Bash},
		{name: "powershell", shell: quickstart.PowerShell},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page := loadPage(c, exitsPage)
			found := program(c, page, test.shell)

			var codes []int
			for _, action := range found.Actions {
				codes = append(codes, action.ExitCode)
			}

			c.Assert(codes, qt.DeepEquals, []int{0, 1, 0})
		})
	}
}

// TestExtract_ReadsADeclaredExitStatus_FailurePath refuses a status that is not
// one. A page that meant `exits=1` and typed something else would otherwise get
// a step expected to succeed, which is the reading it was trying to change.
func TestExtract_ReadsADeclaredExitStatus_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		option  string
		wantErr string
	}{
		{name: "not a number", option: "exits=one", wantErr: `.*exits= takes a process exit status from 0 to 255, not "one".*`},
		{name: "negative", option: "exits=-1", wantErr: `.*exits= takes a process exit status from 0 to 255, not "-1".*`},
		{name: "above a status", option: "exits=256", wantErr: `.*exits= takes a process exit status from 0 to 255, not "256".*`},
		{name: "empty", option: "exits=", wantErr: `.*exits= takes a process exit status from 0 to 255, not "".*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page, err := quickstart.Extract("p.mdx", []byte(
				"---\nquickstart: true\n---\n\nRun it:\n\n```console "+test.option+"\nptah version\n```\n"))

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(page, qt.IsNil)
		})
	}
}

// TestRenderScript_HoldsAStepToItsDeclaredStatus_HappyPath is what makes the
// declaration worth having. The generated script has to fail three ways: on
// another status, on success where the page said the command fails, and never
// on the declared one.
//
// The assertions are on the script text rather than on a run, because the run
// is what CI does with it; what a unit test can settle is that the comparison
// is emitted at all, and that a step declaring nothing is untouched.
func TestRenderScript_HoldsAStepToItsDeclaredStatus_HappyPath(t *testing.T) {
	tests := []struct {
		name         string
		shell        quickstart.Shell
		wantContains []string
		wantAbsent   string
	}{
		{
			name:  "bash",
			shell: quickstart.Bash,
			wantContains: []string{
				// set -e would end the run before the status could be read.
				"set +e\n",
				"quickstart_status=$?\nset -e\n",
				`if [ "$quickstart_status" -ne 1 ]; then`,
			},
			// The step that declared nothing keeps the plain shape.
			wantAbsent: "quickstart_status=$?\nset -e\nif [ \"$quickstart_status\" -ne 0 ]",
		},
		{
			name:  "powershell",
			shell: quickstart.PowerShell,
			wantContains: []string{
				"if ($LASTEXITCODE -ne 1) { exit 1 }\n",
				// A step expected to succeed still exits with what the command
				// returned, so a failing page reports the command's own status.
				"if ($null -ne $LASTEXITCODE -and $LASTEXITCODE -ne 0) { exit $LASTEXITCODE }\n",
			},
			wantAbsent: "if ($LASTEXITCODE -ne 0) { exit 1 }\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			page := loadPage(c, exitsPage)
			script, err := quickstart.RenderScript(program(c, page, test.shell))

			c.Assert(err, qt.IsNil)
			for _, want := range test.wantContains {
				c.Assert(script, qt.Contains, want)
			}
			c.Assert(script, qt.Not(qt.Contains), test.wantAbsent)
		})
	}
}
