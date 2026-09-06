package oci_test

import (
	"bytes"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/cmd/oci"
)

// runOCI executes the oci command tree with a scripted stdin and returns
// everything it wrote.
func runOCI(t *testing.T, stdin string, args ...string) (string, error) {
	t.Helper()

	cmd := oci.NewCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetIn(strings.NewReader(stdin))
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

// The login and logout verbs exist and carry the flags the flow needs.
//
// Their absence was the defect: `ptah oci login` answered "unexpected
// positional arguments", and on a machine without Docker there was no supported
// way to authenticate at all (stokaro/ptah#2241).
func TestCommandTree_CarriesLoginAndLogout(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()

	login, _, err := cmd.Find([]string{"login"})
	c.Assert(err, qt.IsNil)
	c.Assert(login.CommandPath(), qt.Equals, "oci login")
	c.Assert(login.Flag("username"), qt.IsNotNil)
	c.Assert(login.Flag("password-stdin"), qt.IsNotNil)
	c.Assert(login.Flag("plain-http"), qt.IsNotNil)

	logout, _, err := cmd.Find([]string{"logout"})
	c.Assert(err, qt.IsNil)
	c.Assert(logout.CommandPath(), qt.Equals, "oci logout")
}

// There is no --password flag, and there must never be one.
//
// A credential passed on a command line lands in shell history and in the
// process list of every user on the machine. That rule is why Ptah had no login
// verb; the verb keeps the rule rather than trading it away.
func TestLogin_HasNoPasswordFlag(t *testing.T) {
	c := qt.New(t)
	cmd := oci.NewCommand()
	login, _, err := cmd.Find([]string{"login"})
	c.Assert(err, qt.IsNil)

	c.Assert(login.Flag("password"), qt.IsNil)
	c.Assert(login.Flag("pass"), qt.IsNil)
	c.Assert(login.Flag("secret"), qt.IsNil)
}

// --password-stdin without a username is refused before anything is read.
//
// Reading the password first and then failing would consume a secret from a
// pipe to report a usage error.
func TestLogin_PasswordStdinRequiresAUsername(t *testing.T) {
	c := qt.New(t)

	out, err := runOCI(t, "secret", "login", "registry.invalid", "--password-stdin")

	c.Assert(err, qt.ErrorMatches, `.*--password-stdin requires --username.*`)
	c.Assert(out, qt.Not(qt.Contains), "secret")
}

// An empty password is refused rather than sent to the registry.
func TestLogin_RefusesAnEmptyPassword(t *testing.T) {
	c := qt.New(t)

	_, err := runOCI(t, "\n", "login", "registry.invalid", "--username", "u")

	c.Assert(err, qt.IsNotNil)
}

// The secret never reaches the output, on the failure path as well as the
// success one.
//
// The failure path is the one worth pinning: an error that quoted what it had
// just read would put the password in the terminal, in CI logs, and in whatever
// collects them.
func TestLogin_NeverEchoesTheSecret(t *testing.T) {
	c := qt.New(t)

	out, err := runOCI(t, "hunter2", "login", "registry.invalid",
		"--username", "u", "--password-stdin", "--plain-http")

	c.Assert(err, qt.IsNotNil)
	c.Assert(out, qt.Not(qt.Contains), "hunter2")
	c.Assert(err.Error(), qt.Not(qt.Contains), "hunter2")
}
