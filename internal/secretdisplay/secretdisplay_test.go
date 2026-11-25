package secretdisplay_test

import (
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/secretdisplay"
)

func TestSanitize_RedactsEnvironmentAndArgumentSecrets(t *testing.T) {
	c := qt.New(t)

	got := secretdisplay.Sanitize(
		"password=env-secret token=arg-secret url=https://app:url-secret@example.test/hook?token=query-secret mysql=mysql://app:mysql-secret@tcp(db:3306)/shop?token=mysql-token encoded=postgres://app:p%40ss@db/app?token=t%2Fok",
		[]string{
			"DATABASE_PASSWORD=env-secret",
			"DATABASE_URL=mysql://app:mysql-secret@tcp(db:3306)/shop?token=mysql-token",
			"SECONDARY_DATABASE_URL=postgres://app:p%40ss@db/app?token=t%2Fok",
		},
		[]string{
			"loader",
			"--token",
			"arg-secret",
			"https://app:url-secret@example.test/hook?token=query-secret",
		},
	)

	c.Assert(got, qt.Not(qt.Contains), "env-secret")
	c.Assert(got, qt.Not(qt.Contains), "arg-secret")
	c.Assert(got, qt.Not(qt.Contains), "url-secret")
	c.Assert(got, qt.Not(qt.Contains), "query-secret")
	c.Assert(got, qt.Not(qt.Contains), "mysql-secret")
	c.Assert(got, qt.Not(qt.Contains), "mysql-token")
	c.Assert(got, qt.Not(qt.Contains), "p%40ss")
	c.Assert(got, qt.Not(qt.Contains), "t%2Fok")
	c.Assert(got, qt.Contains, "password=redacted")
}

// A password with a reserved character is found whole, where the parts of a URL
// net/url reads would stop short of it.
func TestSanitize_RedactsAPasswordWithReservedCharacters(t *testing.T) {
	tests := []struct {
		name   string
		url    string
		secret string
	}{
		{name: "a / in a DSN's password", url: "mysql://app:s3cret/pw@tcp(db:3306)/shop", secret: "s3cret/pw"},
		// #nosec G101 -- fixture URL with a made-up password, not a credential
		{name: "an @ and a / in a DSN's password", url: "mysql://app:s3c@ret/pw@tcp(db:3306)/shop", secret: "s3c@ret/pw"},
		{name: "a # in a URL's password", url: "postgres://app:s3cret#pw@db/shop", secret: "s3cret#pw"},
		{name: "a ? in a URL's password", url: "postgres://app:s3cret?pw@db/shop", secret: "s3cret?pw"},
		{name: "a token an engine names", url: "libsql://db?authToken=s3cret%2Fpw", secret: "s3cret/pw"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := secretdisplay.Sanitize("the loader said "+test.secret, []string{"DATABASE_URL=" + test.url}, nil)

			c.Assert(got, qt.Equals, "the loader said redacted")
		})
	}
}

// A short secret is taken out where it stands as a credential, and the words
// that hold its characters stay whole.
func TestSanitize_KeepsTheWordsOfAShortSecret(t *testing.T) {
	c := qt.New(t)

	got := secretdisplay.Sanitize(
		"ptah: execute as ptah failed: password=x url=postgres://ptah:ptah@db/ptah",
		[]string{"DATABASE_PASSWORD=x", "DATABASE_URL=postgres://ptah:ptah@db/ptah"},
		nil,
	)

	c.Assert(got, qt.Equals, "ptah: execute as ptah failed: password=redacted url=postgres://ptah:redacted@db/ptah")
}

// A secret of five characters or more is taken out wherever a program writes
// it, glued to an option or an escape too, and a short one where it stands as a
// credential.
func TestSanitize_RedactsASecretAProgramWrites(t *testing.T) {
	tests := []struct {
		name string
		env  []string
		args []string
		text string
		want string
	}{
		{
			name: "after the MySQL client's -p",
			env:  []string{"DB_PASSWORD=Srv9Kq2Lm"},
			text: "+ : mysql -uapp -pSrv9Kq2Lm",
			want: "+ : mysql -uapp -predacted",
		},
		{
			name: "after an escape in a JSON line",
			env:  []string{"DB_PASSWORD=Srv9Kq2Lm"},
			text: `{"msg": "dump\nSrv9Kq2Lm"}`,
			want: `{"msg": "dump\nredacted"}`,
		},
		{
			name: "a variable whose name ends in PASSWORD",
			env:  []string{"PGPASSWORD=Tok3nValue9"},
			text: "dump -pTok3nValue9",
			want: "dump -predacted",
		},
		{
			name: "a short secret after a colon",
			env:  []string{"DB_PASSWORD=ab"},
			text: "password: ab",
			want: "password: redacted",
		},
		{
			name: "a short secret after a user a variable names",
			env:  []string{"PGUSER=app", "PGPASSWORD=ab"},
			text: "curl -u app:ab https://h/",
			want: "curl -u app:redacted https://h/",
		},
		{
			name: "a short secret after a user an option names",
			args: []string{"--user", "app", "--password=ab"},
			text: "curl -u app:ab https://h/",
			want: "curl -u app:redacted https://h/",
		},
		{
			name: "a short secret after the user of an address",
			env:  []string{"DATABASE_URL=postgres://app:ab@db/shop"},
			text: "localhost:5432:shop:app:ab",
			want: "localhost:5432:shop:app:redacted",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(secretdisplay.Sanitize(test.text, test.env, test.args), qt.Equals, test.want)
		})
	}
}

// A short secret after a ":" that no user name stands before is a port, and
// stays.
func TestSanitize_KeepsAPort(t *testing.T) {
	c := qt.New(t)
	text := "dial 127.0.0.1:1 (127.0.0.1) failed"

	got := secretdisplay.Sanitize(text, []string{"PGUSER=app", "PGPASSWORD=1"}, nil)

	c.Assert(got, qt.Equals, text)
}

// The working directories a shell keeps in PWD and OLDPWD are no secrets, so
// the paths a program writes keep them.
func TestSanitize_KeepsTheWorkingDirectory(t *testing.T) {
	c := qt.New(t)
	text := "read /work/shop/schema.sql after /work/old"

	got := secretdisplay.Sanitize(text, []string{"PWD=/work/shop", "OLDPWD=/work/old"}, nil)

	c.Assert(got, qt.Equals, text)
}

func TestSanitize_EscapesTerminalControls(t *testing.T) {
	c := qt.New(t)

	got := secretdisplay.Sanitize("safe\n\x1b[31mred\rtext\tend", nil, nil)

	c.Assert(got, qt.Equals, "safe\n\\x1b[31mred\\x0dtext\tend")
}

func TestSanitize_EscapesInvalidUTF8(t *testing.T) {
	c := qt.New(t)

	got := secretdisplay.Sanitize(string([]byte{'o', 'k', 0xff}), nil, nil)

	c.Assert(got, qt.Equals, `ok\xff`)
}

func TestSanitizeError_PreservesIdentityAndSanitizesText(t *testing.T) {
	c := qt.New(t)
	sentinel := errors.New("token=top-secret \x1b[31mfailed")

	got := secretdisplay.SanitizeError(
		sentinel,
		[]string{"API_TOKEN=top-secret"},
		nil,
	)

	c.Assert(got, qt.ErrorIs, sentinel)
	c.Assert(got.Error(), qt.Equals, `token=redacted \x1b[31mfailed`)
}

func TestSanitizeError_BoundsLongDiagnostics(t *testing.T) {
	c := qt.New(t)
	sentinel := errors.New("start:" + strings.Repeat("x", 3000) + ":end")

	got := secretdisplay.SanitizeError(sentinel, nil, nil)

	c.Assert(got, qt.ErrorIs, sentinel)
	c.Assert(got.Error(), qt.HasLen, 2000)
	c.Assert(strings.HasPrefix(got.Error(), "start:"), qt.IsTrue)
	c.Assert(got.Error(), qt.Contains, "...[truncated]...")
	c.Assert(strings.HasSuffix(got.Error(), ":end"), qt.IsTrue)
}

// A value the caller knows is a secret is redacted with no name or address
// beside it, where Sanitize reads a bare argument as a value like any other.
func TestSanitizeKnown_RedactsABareSecret(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		known []string
		want  string
	}{
		{name: "a token", text: "denied for Tok3nValueX9", known: []string{"Tok3nValueX9"}, want: "denied for redacted"},
		{
			name:  "the password of a known address",
			text:  "cannot convert \"Pa55wordX9\" to number",
			known: []string{"postgres://app:Pa55wordX9@db/shop"},
			want:  "cannot convert \"redacted\" to number",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := secretdisplay.SanitizeKnown(test.text, nil, []string{"Tok3nValueX9"}, test.known)

			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The control for the test above: the same bare value as an argument alone is
// not read as a secret, so the known values are what redacts it.
func TestSanitize_KeepsABareArgument(t *testing.T) {
	c := qt.New(t)

	got := secretdisplay.Sanitize("denied for Tok3nValueX9", nil, []string{"Tok3nValueX9"})

	c.Assert(got, qt.Equals, "denied for Tok3nValueX9")
}

func TestSanitizeErrorKnown_PreservesIdentityAndRedactsAKnownSecret(t *testing.T) {
	c := qt.New(t)
	sentinel := errors.New("denied for Tok3nValueX9")

	got := secretdisplay.SanitizeErrorKnown(sentinel, nil, nil, []string{"Tok3nValueX9"})

	c.Assert(got, qt.ErrorIs, sentinel)
	c.Assert(got, qt.ErrorMatches, "denied for redacted")
}
