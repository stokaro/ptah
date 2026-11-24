package dburldisplay

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// shortestFreeTextSecret is the length, in characters, below which a whole
// hidden value is taken out of a message only where it stands as a credential.
//
// A value this short is as likely to be a word of the message as the
// password: the product's name, an SQL keyword, a column called id. Taken out
// wherever it occurs, it garbles the message, and the reader who can tell
// which word went learns the password. A longer value is taken out wherever it
// occurs, because the text may be a program's output, and a program may write
// the password glued to an option or an escape, as `-pSecret1` or `\nSecret1`.
const shortestFreeTextSecret = 5

// settingBefore is the end of text that names the setting a value follows: a
// name, an "=" or a ":", and the quote or brace a value may open with, as in
// `password=`, `Pwd='`, `PGPASSWORD=`, `app.jwt_secret=`, `password: ` or the
// JSON `"password": "`, escaped or not.
var settingBefore = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.-]*)\\?["']?\s*[=:]\s*\\?['"{]?$`)

// credentialBefore is the end of text that a credential follows on a command
// line or in SQL: the `-p` the MySQL client takes the password after,
// `--password` and a space or an "=", `Bearer `, and the quote that opens the
// value of `PASSWORD '...'` and `IDENTIFIED BY '...'`.
var credentialBefore = regexp.MustCompile(`(?i)(?:(?:^|[\s'"])-p|--password[\s=]\s*['"]?|\bbearer\s+|\b(?:password|identified\s+by)\s+['"])$`)

// valueEnds holds the characters that end a setting's value in the text a
// driver or a program writes: a separator, the backslash of an escape, or the
// quote or bracket that closes the value or the text quoting it.
const valueEnds = " \t\r\n;&,:#\\'\"`})]"

// longestSetting bounds how far before a value [settingBefore] looks for the
// name of its setting.
const longestSetting = 64

// secretText is a value to take out of a message, and the length from which it
// is taken out outside a credential.
type secretText struct {
	value string
	floor int
	// whole is true for a whole secret, which from floor is taken out wherever
	// it occurs, and false for a piece of one, which from floor is taken out
	// where it stands as a word of its own. A piece is what a parser may have
	// cut out of the password, and as a word inside a longer one it is more
	// likely the message's own text.
	whole bool
}

// MaskSecrets returns text with each occurrence of a secret of five characters
// or more replaced by replacement, and each occurrence of a shorter one where it
// stands as a credential.
//
// An occurrence stands as a credential where an address, a setting or a
// command keeps a secret, whatever its length:
//   - between the ":" or the "/" and the "@" of user info, as in
//     `app:...@host` and `app/...@host`;
//   - after a ":" and before white space or the end of the text, as in curl's
//     `-u app:...` and a line of a password file;
//   - as the whole value of a setting whose name [IsSecretParameter] reports,
//     after an "=" or a ":": `password=...`, `PGPASSWORD=...`, `password:
//     ...`, `"password": "..."`;
//   - as the whole value after `-p`, `--password`, `Bearer`, `PASSWORD '` and
//     `IDENTIFIED BY '`.
//
// A shorter secret anywhere else, such as the "x" in "execute" for a password
// "x", is left as it is, so the message stays readable and shows nothing by
// what went missing.
//
// An occurrence is the secret as it is, or with any of its characters escaped
// as an address writes them: `p%40ss` spells `p@ss`, `%2f` and `%2F` spell "/",
// and a "+" spells a space. So the address a driver quotes loses the password
// the way the user wrote it.
//
// Longer secrets are tried first, so a secret that holds a shorter one is taken
// out whole. Empty secrets are ignored.
func MaskSecrets(text string, secrets []string, replacement string) string {
	values := make([]secretText, 0, len(secrets))
	for _, secret := range secrets {
		values = append(values, secretText{value: secret, floor: shortestFreeTextSecret, whole: true})
	}
	return maskText(text, values, replacement)
}

// maskText is [MaskSecrets] with a floor for each value.
func maskText(text string, secrets []secretText, replacement string) string {
	secrets = slices.DeleteFunc(slices.Clone(secrets), func(secret secretText) bool { return secret.value == "" })
	slices.SortStableFunc(secrets, func(left, right secretText) int { return len(right.value) - len(left.value) })
	var out strings.Builder
	last := 0
	for at := 0; at < len(text); {
		width := maskedWidth(text, at, secrets)
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
func maskedWidth(text string, at int, secrets []secretText) int {
	for _, secret := range secrets {
		width := spelled(text[at:], secret.value)
		if width < 0 {
			continue
		}
		end := at + width
		if standsAsCredential(text, at, end) ||
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
// setting or a command keeps a secret, as [MaskSecrets] lists them.
func standsAsCredential(text string, start, end int) bool {
	var before byte
	if start > 0 {
		before = text[start-1]
	}
	last := end == len(text)
	if (before == ':' || before == '/') && !last && text[end] == '@' {
		return true
	}
	if before == ':' && (last || unicode.IsSpace(rune(text[end]))) {
		return true
	}
	if !last && !strings.ContainsRune(valueEnds, rune(text[end])) {
		return false
	}
	window := text[max(0, start-longestSetting):start]
	if match := settingBefore.FindStringSubmatch(window); match != nil && IsSecretParameter(match[1]) {
		return true
	}
	return credentialBefore.MatchString(window)
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
