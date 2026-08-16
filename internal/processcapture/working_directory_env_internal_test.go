package processcapture

// White-box testing required: the environment a child would receive is only
// observable on the exec.Cmd before it starts. Asserting it through a spawned
// process instead would pass on POSIX whatever this code does, because os/exec
// maintains PWD there by itself -- and would therefore assert nothing about the
// platform this exists for.

import (
	"os/exec"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestSetWorkingDirectoryEnv_MakesPWDAgreeWithDir pins the contract
// [Command.Dir] documents: PWD names the directory the program runs in, not the
// one Ptah was started from.
//
// os/exec keeps only the POSIX half -- it appends PWD=<abs Dir> there and
// documents that Windows and Plan 9 "do not use the PWD variable". Ptah started
// from git-bash, MSYS2 or Cygwin inherits a PWD naming Ptah's own directory.
func TestSetWorkingDirectoryEnv_MakesPWDAgreeWithDir(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	absolute, err := filepath.Abs(dir)
	c.Assert(err, qt.IsNil)

	cmd := exec.Command("go", "version")
	cmd.Dir = dir
	// A stale value, the shape an MSYS2 shell hands down.
	cmd.Env = append(cmd.Environ(), "PWD=/somewhere/else")

	c.Assert(setWorkingDirectoryEnv(cmd), qt.IsNil)

	c.Assert(lastEnvValue(cmd.Env, "PWD"), qt.Equals, absolute)
}

// TestSetWorkingDirectoryEnv_LeavesAnInheritedDirectoryAlone is the control: a
// command with no Dir runs where Ptah runs, so PWD must not be rewritten.
func TestSetWorkingDirectoryEnv_LeavesAnInheritedDirectoryAlone(t *testing.T) {
	c := qt.New(t)
	cmd := exec.Command("go", "version")
	cmd.Env = append(cmd.Environ(), "PWD=/somewhere/else")

	c.Assert(setWorkingDirectoryEnv(cmd), qt.IsNil)

	c.Assert(lastEnvValue(cmd.Env, "PWD"), qt.Equals, "/somewhere/else")
}

// lastEnvValue returns the value os/exec would give key, which deduplicates by
// keeping the last occurrence.
func lastEnvValue(env []string, key string) string {
	for _, entry := range slices.Backward(env) {
		if name, value, found := splitEnvEntry(entry); found && name == key {
			return value
		}
	}
	return ""
}

func splitEnvEntry(entry string) (name, value string, found bool) {
	for i := range len(entry) {
		if entry[i] == '=' {
			return entry[:i], entry[i+1:], true
		}
	}
	return "", "", false
}
