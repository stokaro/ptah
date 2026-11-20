//go:build !windows

package cloudtoken_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cloudtoken"
)

// fakeAzureCLI puts an `az` on PATH that records its arguments and answers
// with a token, and narrows the chain to the Azure CLI credential so nothing
// else is tried first. It returns the file the arguments are written to.
//
// azidentity runs the CLI through /bin/sh here and through cmd.exe on Windows,
// so the fake is a shell script and this file is built where one runs. What it
// checks, the tenant reaching the credential, does not vary by platform.
func fakeAzureCLI(c *qt.C) string {
	dir := c.TempDir()
	args := filepath.Join(dir, "args.txt")
	script := "#!/bin/sh\n" +
		"printf '%s\\n' \"$*\" >> '" + args + "'\n" +
		`printf '%s' '{"accessToken":"cli-token","expiresOn":"2099-01-01 00:00:00.000000","expires_on":4070908800,"tokenType":"Bearer"}'` + "\n"
	// #nosec G306 -- the fake CLI has to be executable, and it lives in a
	// directory only this test can reach.
	c.Assert(os.WriteFile(filepath.Join(dir, "az"), []byte(script), 0o700), qt.IsNil)
	c.Setenv("PATH", dir)
	c.Setenv("AZURE_TOKEN_CREDENTIALS", "AzureCLICredential")
	return args
}

// TestAzureDBToken_SignsInToTheTenantItNames pins the one attribute whose loss
// would sign in somewhere else without a word: a tenant the project names
// reaches the credential, and no tenant means the CLI's own.
func TestAzureDBToken_SignsInToTheTenantItNames(t *testing.T) {
	cases := []struct {
		name     string
		tenant   string
		wantArgs string
	}{
		{
			name:     "a named tenant",
			tenant:   "11111111-2222-3333-4444-555555555555",
			wantArgs: "account get-access-token -o json --resource https://ossrdbms-aad.database.windows.net --tenant 11111111-2222-3333-4444-555555555555\n",
		},
		{
			name:     "no tenant",
			tenant:   "",
			wantArgs: "account get-access-token -o json --resource https://ossrdbms-aad.database.windows.net\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			isolateAzureChain(c)
			args := fakeAzureCLI(c)

			token, err := cloudtoken.AzureDBToken(t.Context(), cloudtoken.AzureDBOptions{TenantID: tc.tenant})

			c.Assert(err, qt.IsNil)
			c.Assert(token, qt.Equals, "cli-token")
			recorded, err := os.ReadFile(args)
			c.Assert(err, qt.IsNil)
			c.Assert(string(recorded), qt.Equals, tc.wantArgs)
		})
	}
}
