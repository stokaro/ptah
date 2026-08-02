package secretdisplay_test

import (
	"errors"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/secretdisplay"
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
