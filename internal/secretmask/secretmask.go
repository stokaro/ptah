// Package secretmask takes secrets out of text that is about to be shown: a
// driver's error, a server's answer, a program's output, a configuration
// diagnostic or a provider's response body.
//
// It owns the one rule every such display applies, so that a value hidden on
// one line is not shown on the next: a secret of five characters or more is
// taken out wherever it occurs, and a shorter one only where it stands as a
// credential. A short value taken out of every word it occurs in garbles the
// text, and a reader who can tell which letters went learns the value.
//
// It knows no address grammar. internal/dburldisplay reads a database address
// for what it hides and passes the values here, and so do the callers that hold
// a secret of another kind: a role password, an API token, a sensitive
// variable of an atlas.hcl file.
//
// provider-sdk/internal/secretmask is a copy of this rule. The SDK is a module
// of its own, consumed here at a released version, so neither can import the
// other's package; the corpus both tests read,
// provider-sdk/internal/secretmask/testdata/rule.json, pins that they agree.
package secretmask

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// shortestFreeTextSecret is the length, in characters, below which a whole
// secret is taken out of a text only where it stands as a credential.
//
// A value this short is as likely to be a word of the text as the password:
// the product's name, an SQL keyword, a column called id. Taken out wherever it
// occurs, it garbles the text, and the reader who can tell which word went
// learns the password. A longer value is taken out wherever it occurs, because
// the text may be a program's output, and a program may write the password
// glued to an option or an escape, as `-pSecret1` or `\nSecret1`.
const shortestFreeTextSecret = 5

// shortestPiece is the length below which [MaskPieces] leaves a piece of a
// secret in a text: a character or two of a password is a word of most texts.
const shortestPiece = 3

// settingBefore is the end of text that names the setting a value follows: a
// name, an "=" or a ":", and the quote or brace a value may open with, as in
// `password=`, `Pwd='`, `PGPASSWORD=`, `app.jwt_secret=`, `password: ` or the
// JSON `"password": "`, escaped or not.
var settingBefore = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.-]*)\\?["']?\s*[=:]\s*\\?['"{]?$`)

// credentialBefore is the end of text that a credential follows on a command
// line or in SQL: the `-p` the MySQL client takes the password after,
// `--password` and a space or an "=", `Bearer `, and the quote that opens the
// value of `PASSWORD '...'`, PostgreSQL's escape string `PASSWORD E'...'` and
// `IDENTIFIED BY '...'`.
var credentialBefore = regexp.MustCompile(`(?i)(?:(?:^|[\s'"])-p|--password[\s=]\s*['"]?|\bbearer\s+|\b(?:password\s+e?|identified\s+by\s+)['"])$`)

// valueEnds holds the characters that end a setting's value in the text a
// driver or a program writes: a separator, the backslash of an escape, or the
// quote or bracket that closes the value or the text quoting it.
const valueEnds = " \t\r\n;&,:#\\'\"`})]"

// longestSetting bounds how far before a value [settingBefore] looks for the
// name of its setting.
const longestSetting = 64

// secretText is a value to take out of a text, and the length from which it is
// taken out outside a credential.
type secretText struct {
	value string
	floor int
	// whole is true for a whole secret, which from floor is taken out wherever
	// it occurs, and false for a piece of one, which from floor is taken out
	// where it stands as a word of its own. A piece is what a parser may have
	// cut out of the password, and as a word inside a longer one it is more
	// likely the text's own.
	whole bool
}

// Mask returns text with each occurrence of a secret of five characters or more
// replaced by replacement, and each occurrence of a shorter one where it stands
// as a credential.
//
// An occurrence stands as a credential where an address, a setting or a
// command keeps a secret, whatever its length:
//   - between the ":" or the "/" and the "@" of user info, as in
//     `app:...@host` and `app/...@host`;
//   - after one of users and a ":", and before white space or the end of the
//     text, as in curl's `-u app:...` and a line of a password file, where
//     the user name stands as a word;
//   - as the whole value of a setting whose name [IsSecretParameter] reports,
//     after an "=" or a ":": `password=...`, `PGPASSWORD=...`, `password:
//     ...`, `"password": "..."`;
//   - as the whole value after `-p` that follows white space, a quote or the
//     start of the text, after `--password`, after `Bearer`, and after
//     `PASSWORD` or `IDENTIFIED BY` and a single or a double quote, or
//     PostgreSQL's `E'` that opens an escape string.
//
// A shorter secret anywhere else, such as the "x" in "execute" for a password
// "x", or the "1" of the port in `127.0.0.1:1` for a password "1", is left as
// it is, so the text stays readable and shows nothing by what went missing.
//
// An occurrence is the secret as it is, or with any of its characters escaped
// as an address writes them: `p%40ss` spells `p@ss`, `%2f` and `%2F` spell "/",
// and a "+" spells a space. So the address a driver quotes loses the password
// the way the user wrote it.
//
// Longer secrets are tried first, so a secret that holds a shorter one is taken
// out whole. Empty secrets are ignored.
func Mask(text string, secrets, users []string, replacement string) string {
	return MaskPieces(text, secrets, nil, users, replacement)
}

// MaskPieces is [Mask] that also takes out pieces of a secret: each piece of
// three characters or more where it stands as a word of its own, or as a
// credential. A piece is part of a secret that a parser may have cut out of it
// and named on its own, as a driver that misread an address names a host or a
// database; as a word inside a longer one it is more likely the text's own. A
// shorter piece is left in the text.
func MaskPieces(text string, secrets, pieces, users []string, replacement string) string {
	values := make([]secretText, 0, len(secrets))
	for _, secret := range secrets {
		values = append(values, secretText{value: secret, floor: shortestFreeTextSecret, whole: true})
	}
	for _, piece := range pieces {
		if len(piece) >= shortestPiece {
			values = append(values, secretText{value: piece, floor: shortestPiece})
		}
	}
	return maskText(text, values, users, replacement)
}

// MasksAnywhere reports whether [Mask] takes secret out wherever it occurs,
// rather than only where it stands as a credential.
func MasksAnywhere(secret string) bool {
	return utf8.RuneCountInString(secret) >= shortestFreeTextSecret
}

// ContainsWord reports whether text holds word where no letter, digit or "_"
// runs on from it on either side, as [MaskPieces] reads a piece that stands as
// a word of its own. An empty word is in no text.
func ContainsWord(text, word string) bool {
	if word == "" {
		return false
	}
	for at := 0; at <= len(text)-len(word); {
		found := strings.Index(text[at:], word)
		if found < 0 {
			return false
		}
		start := at + found
		if standsAsWord(text, start, start+len(word)) {
			return true
		}
		at = start + 1
	}
	return false
}

// maskText is [Mask] with a floor for each value.
func maskText(text string, secrets []secretText, users []string, replacement string) string {
	secrets = slices.DeleteFunc(slices.Clone(secrets), func(secret secretText) bool { return secret.value == "" })
	slices.SortStableFunc(secrets, func(left, right secretText) int { return len(right.value) - len(left.value) })
	var out strings.Builder
	last := 0
	for at := 0; at < len(text); {
		width := maskedWidth(text, at, secrets, users)
		if width == 0 {
			at++
			continue
		}
		out.WriteString(text[last:at])
		out.WriteString(replacement)
		at += width
		last = at
	}
	if last == 0 {
		return text
	}
	out.WriteString(text[last:])
	return out.String()
}

// maskedWidth is the length of text, from at, that spells the first secret
// taken out there, or 0.
func maskedWidth(text string, at int, secrets []secretText, users []string) int {
	for _, secret := range secrets {
		width := spelled(text[at:], secret.value)
		if width < 0 {
			continue
		}
		end := at + width
		if standsAsCredential(text, at, end, users) ||
			utf8.RuneCountInString(secret.value) >= secret.floor && (secret.whole || standsAsWord(text, at, end)) {
			return width
		}
	}
	return 0
}

// spelled returns how many bytes at the start of text spell secret, each
// character as it is or escaped, or -1 when they do not.
func spelled(text, secret string) int {
	if secret == "" {
		return 0
	}
	if len(text) >= 3 && text[0] == '%' && unescapes(text[1:3], secret[0]) {
		if width := spelled(text[3:], secret[1:]); width >= 0 {
			return width + 3
		}
	}
	if text != "" && (text[0] == secret[0] || text[0] == '+' && secret[0] == ' ') {
		if width := spelled(text[1:], secret[1:]); width >= 0 {
			return width + 1
		}
	}
	return -1
}

// unescapes reports whether the two hexadecimal digits of an escape spell b.
func unescapes(digits string, b byte) bool {
	value, err := strconv.ParseUint(digits, 16, 8)
	return err == nil && byte(value) == b
}

// standsAsCredential reports whether text[start:end] is where an address, a
// setting or a command keeps a secret, as [Mask] lists them.
func standsAsCredential(text string, start, end int, users []string) bool {
	var before byte
	if start > 0 {
		before = text[start-1]
	}
	last := end == len(text)
	if (before == ':' || before == '/') && !last && text[end] == '@' {
		return true
	}
	if before == ':' && (last || unicode.IsSpace(rune(text[end]))) && followsUser(text, start-1, users) {
		return true
	}
	if !last && !strings.ContainsRune(valueEnds, rune(text[end])) {
		return false
	}
	window := text[max(0, start-longestSetting):start]
	if match := settingBefore.FindStringSubmatch(window); match != nil && IsSecretParameter(match[1], text[start:end]) {
		return true
	}
	return credentialBefore.MatchString(window)
}

// followsUser reports whether text[:colon] ends with one of users, and no word
// character runs on into the user name from before it. Without the user name a
// ":" says nothing: it ends a host before its port and a label before its
// value, as in `127.0.0.1:1` and `column a:x`.
func followsUser(text string, colon int, users []string) bool {
	return slices.ContainsFunc(users, func(user string) bool {
		return user != "" && strings.HasSuffix(text[:colon], user) && wordEnds(text, colon-len(user))
	})
}

// standsAsWord reports whether no word character runs on from text[start:end]
// on either side.
func standsAsWord(text string, start, end int) bool {
	return wordEnds(text, start) && wordEnds(text, end)
}

// wordEnds reports whether a word ends or begins at offset: a letter, a digit or
// an "_" does not stand on both sides of it.
func wordEnds(text string, offset int) bool {
	if offset == 0 || offset == len(text) {
		return true
	}
	before, _ := utf8.DecodeLastRuneInString(text[:offset])
	after, _ := utf8.DecodeRuneInString(text[offset:])
	return !wordRune(before) || !wordRune(after)
}

// wordRune reports a character that continues a word.
func wordRune(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}

// passwordParameterWords mark a parameter as a password wherever they stand in
// its name, whatever value it takes.
var passwordParameterWords = []string{"passhash", "passwd", "password", "pwd"}

// secretParameterWords mark a parameter as a secret wherever they stand in its
// name, unless its value is one of [flagValues].
var secretParameterWords = []string{"credential", "jwt", "key", "secret", "token"}

// flagValues are the spellings of a boolean a driver's flag takes, compared in
// any letter case. A flag says whether to do something, and holds no secret
// whatever its name holds.
var flagValues = []string{"0", "1", "false", "no", "off", "on", "true", "yes"}

// IsSecretParameter reports whether a query parameter, connection-string
// keyword or setting named key holds a secret, or says where one is, in value.
//
// The name is compared without case or punctuation, so `authToken`, `auth_token`
// and go-ora's `WALLET PASSWORD` read alike. A name that names a password
// counts whatever its value, and so does one ending in `pass`, as go-ora's `OS
// PASS` does; `passfile` names a file of passwords and does not. A name that
// holds `key` anywhere counts, as `sslkey`, `keyfile` and `encryption_key_b64`
// do, and so does one that holds `secret`, `token`, `jwt` or `credential`,
// unless value is a boolean: `allowPublicKeyRetrieval=true` and
// `_foreign_keys=false` are flags, and a value such as "false" taken for a
// secret would be taken out of every message. Otherwise it errs on the wide
// side: an engine's parameter a narrower list missed prints its value, and
// hiding a value that is no secret costs a word on a line.
func IsSecretParameter(key, value string) bool {
	name := comparableName(key)
	switch {
	case name == "oshash", strings.HasSuffix(name, "pass"), holdsAnyOf(name, passwordParameterWords):
		return true
	case slices.ContainsFunc(flagValues, func(flag string) bool { return strings.EqualFold(value, flag) }):
		return false
	default:
		return name == "sslcert" || holdsAnyOf(name, secretParameterWords)
	}
}

// holdsAnyOf reports whether name holds one of words.
func holdsAnyOf(name string, words []string) bool {
	return slices.ContainsFunc(words, func(word string) bool { return strings.Contains(name, word) })
}

// IsUserParameter reports whether key names the user a connection logs in as:
// libpq's `user`, SQL Server's `User Id` and `UID`, and an environment
// variable such as PGUSER or MYSQL_USER. The name is compared as
// [IsSecretParameter] compares it. [Mask] takes a short secret out after the
// user's name and a ":".
func IsUserParameter(key string) bool {
	name := comparableName(key)
	return name == "uid" || slices.ContainsFunc([]string{"user", "username", "userid"}, func(word string) bool { return strings.HasSuffix(name, word) })
}

// comparableName is key in lower case with everything but its letters and
// digits left out.
func comparableName(key string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, key)
}
