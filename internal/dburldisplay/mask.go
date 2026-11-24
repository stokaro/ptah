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
// A password with no character a parser stops at is never misread, so a driver
// names it nowhere but in the address. Outside the address, a value this short
// is as likely to be a word of the message as the password: the product's name,
// an SQL keyword, a column called id. Taken out there, it garbles the message,
// and the reader who can tell which word went learns the password.
const shortestFreeTextSecret = 5

// settingBefore is the end of text that names the setting a value follows: a
// name, an "=", and the quote or brace a value may open with, as in
// `password=`, `Pwd='`, `PGPASSWORD=` or `app.jwt_secret=`.
var settingBefore = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.-]*)\s*=\s*['"{]?$`)

// valueEnds holds the characters that end a setting's value in the text a
// driver or a program writes: a separator, or the quote or bracket that closes
// the value or the text quoting it.
const valueEnds = " \t\r\n;&,'\"`})]"

// longestSetting bounds how far before a value [settingBefore] looks for the
// name of its setting.
const longestSetting = 64

// secretText is a value to take out of a message, and the length from which it
// is taken out where it stands as a word of its own.
type secretText struct {
	value string
	floor int
}

// MaskSecrets returns text with each occurrence of a secret replaced by
// replacement, where the occurrence stands as a credential or as a word.
//
// An occurrence stands as a credential between the ":" and the "@" of an
// address, or as the whole value of a setting whose name [IsSecretParameter]
// reports: `password=...`, `token=...`, `PGPASSWORD=...`. It is taken out there
// whatever its length. An occurrence stands as a word when no letter, digit or
// "_" runs on from it on either side; it is taken out there when the secret has
// at least five characters. Text that only holds the characters of a secret,
// such as the "x" in "execute" for a password "x", is left as it is, so the
// message stays readable and shows nothing by what went missing.
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
		values = append(values, secretText{value: secret, floor: shortestFreeTextSecret})
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
// standing there as a credential or a word, or 0.
func maskedWidth(text string, at int, secrets []secretText) int {
	for _, secret := range secrets {
		width := spelled(text[at:], secret.value)
		if width < 0 {
			continue
		}
		end := at + width
		if standsAsCredential(text, at, end) ||
			utf8.RuneCountInString(secret.value) >= secret.floor && standsAsWord(text, at, end) {
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

// standsAsCredential reports whether text[start:end] is where an address or a
// setting keeps a secret: between the ":" and the "@" of user info, or the whole
// value after the "=" of a secret's setting.
func standsAsCredential(text string, start, end int) bool {
	if start > 0 && text[start-1] == ':' && end < len(text) && text[end] == '@' {
		return true
	}
	if end < len(text) && !strings.ContainsRune(valueEnds, rune(text[end])) {
		return false
	}
	match := settingBefore.FindStringSubmatch(text[max(0, start-longestSetting):start])
	return match != nil && IsSecretParameter(match[1])
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
