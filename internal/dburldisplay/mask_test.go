package dburldisplay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/dburldisplay"
)

// A secret is taken out where it stands as a credential, whatever its length.
func TestMaskSecrets_TakesACredentialOfAnyLength(t *testing.T) {
	tests := []struct {
		name    string
		secrets []string
		text    string
		want    string
	}{
		{
			name:    "the user info of a URL",
			secrets: []string{"x"},
			text:    "connect to postgres://app:x@localhost/execute failed",
			want:    "connect to postgres://app:***@localhost/execute failed",
		},
		{
			name:    "the user info of a DSN",
			secrets: []string{"x"},
			text:    "cannot parse `app:x@tcp(localhost:3306)/execute`",
			want:    "cannot parse `app:***@tcp(localhost:3306)/execute`",
		},
		{
			name:    "a URL with no user name",
			secrets: []string{"x"},
			text:    "postgres://:x@localhost/execute",
			want:    "postgres://:***@localhost/execute",
		},
		{
			name:    "an escaped password in a quoted URL",
			secrets: []string{"p%40s"},
			text:    "connect to postgres://app:p%40s@localhost/shop",
			want:    "connect to postgres://app:***@localhost/shop",
		},
		{
			name:    "a password the text escapes",
			secrets: []string{"p@s"},
			text:    "connect to postgres://app:p%40s@localhost/shop",
			want:    "connect to postgres://app:***@localhost/shop",
		},
		{
			name:    "escapes in lower case",
			secrets: []string{"p/ss"},
			text:    "connect to postgres://app:p%2fss@localhost/shop",
			want:    "connect to postgres://app:***@localhost/shop",
		},
		{
			name:    "a space written as a plus",
			secrets: []string{"a b"},
			text:    "query password=a+b&sslmode=disable",
			want:    "query password=***&sslmode=disable",
		},
		{
			name:    "an escaped percent sign",
			secrets: []string{"%41"},
			text:    "postgres://app:%2541@localhost/shop and postgres://app:%41@localhost/shop",
			want:    "postgres://app:***@localhost/shop and postgres://app:***@localhost/shop",
		},
		{
			name:    "a keyword",
			secrets: []string{"x"},
			text:    "host=localhost password=x dbname=execute",
			want:    "host=localhost password=*** dbname=execute",
		},
		{
			name:    "a keyword in capitals at the end of the text",
			secrets: []string{"pw"},
			text:    "export PGPASSWORD=pw",
			want:    "export PGPASSWORD=***",
		},
		{
			name:    "a variable with an underscore",
			secrets: []string{"pw"},
			text:    "MYSQL_PWD=pw\n",
			want:    "MYSQL_PWD=***\n",
		},
		{
			name:    "a quoted keyword value",
			secrets: []string{"x"},
			text:    "options: Pwd='x' Database='execute'",
			want:    "options: Pwd='***' Database='execute'",
		},
		{
			name:    "an ODBC value in braces",
			secrets: []string{"x"},
			text:    "server=db;pwd={x};database=execute",
			want:    "server=db;pwd={***};database=execute",
		},
		{
			name:    "a dotted server option",
			secrets: []string{"k1"},
			text:    "-c app.jwt_secret=k1",
			want:    "-c app.jwt_secret=***",
		},
		{
			name:    "spaces around the equals sign",
			secrets: []string{"x"},
			text:    "password = x;",
			want:    "password = ***;",
		},
		{
			name:    "each place it stands",
			secrets: []string{"ptah"},
			text:    "ptah: postgres://ptah:ptah@localhost/ptah and password=ptah",
			want:    "ptah: postgres://ptah:***@localhost/ptah and password=***",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dburldisplay.MaskSecrets(test.text, test.secrets, "***"), qt.Equals, test.want)
		})
	}
}

// A secret of five characters or more is taken out where it stands as a word,
// each time it does.
func TestMaskSecrets_TakesALongSecretThatStandsAsAWord(t *testing.T) {
	tests := []struct {
		name    string
		secrets []string
		text    string
		want    string
	}{
		{
			name:    "repeated",
			secrets: []string{"Pa55wordX9"},
			text:    "login with Pa55wordX9 failed; retried Pa55wordX9",
			want:    "login with *** failed; retried ***",
		},
		{
			name:    "five characters, the shortest taken out",
			secrets: []string{"Xk9Lm"},
			text:    "lookup Xk9Lm: no such host",
			want:    "lookup ***: no such host",
		},
		{
			name:    "punctuation inside the secret",
			secrets: []string{"/tmp/Xk9Lm"},
			text:    "open /tmp/Xk9Lm: no such file",
			want:    "open ***: no such file",
		},
		{
			name:    "a value of a setting that is no secret",
			secrets: []string{"Xk9Lm"},
			text:    "database=Xk9Lm@localhost/shop",
			want:    "database=***@localhost/shop",
		},
		{
			name:    "the longer secret first",
			secrets: []string{"abcde", "abcde/fghij"},
			text:    "read abcde/fghij and abcde",
			want:    "read *** and ***",
		},
		{
			name:    "five characters wider than a byte",
			secrets: []string{"äöüßé"},
			text:    "login with äöüßé failed",
			want:    "login with *** failed",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dburldisplay.MaskSecrets(test.text, test.secrets, "***"), qt.Equals, test.want)
		})
	}
}

// Text that only holds the characters of a secret keeps them, so the message
// stays readable and shows nothing by what went missing.
func TestMaskSecrets_KeepsTextThatOnlyHoldsTheCharacters(t *testing.T) {
	tests := []struct {
		name    string
		secrets []string
		text    string
	}{
		{name: "a letter of a word", secrets: []string{"x"}, text: "failed to connect to `user=app database=execute`"},
		{name: "the product's name", secrets: []string{"ptah"}, text: "ptah: failed to connect to `user=ptah database=ptah`"},
		{name: "a short secret that stands as a word", secrets: []string{"YES"}, text: "Access denied (using password: YES)"},
		{name: "four characters wider than a byte", secrets: []string{"äöüß"}, text: "login with äöüß failed"},
		{name: "a long secret inside a longer word", secrets: []string{"Pa55wordX9"}, text: "database Pa55wordX9db"},
		{name: "a long secret before a digit", secrets: []string{"Pa55wordX9"}, text: "database Pa55wordX99"},
		{name: "a long secret after an underscore", secrets: []string{"Pa55wordX9"}, text: "database old_Pa55wordX9"},
		{name: "a keyword value the secret only begins", secrets: []string{"x"}, text: "password=xyz"},
		{name: "a keyword value the secret begins before punctuation", secrets: []string{"p@"}, text: "password=p@ss"},
		{name: "a setting that is no secret", secrets: []string{"x"}, text: "user=x database=x"},
		{name: "a colon without an at sign", secrets: []string{"x"}, text: "column a:x is ambiguous"},
		{name: "an escape that spells another character", secrets: []string{"x"}, text: "postgres://app:%79@localhost/shop"},
		{name: "an escape cut short", secrets: []string{"@"}, text: "postgres://app:%4@localhost/shop"},
		{name: "an empty secret", secrets: []string{""}, text: "connect to postgres://app:@localhost/shop"},
		{name: "no secrets", text: "password=x"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(dburldisplay.MaskSecrets(test.text, test.secrets, "***"), qt.Equals, test.text)
		})
	}
}
