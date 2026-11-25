// Package secretmask takes secrets out of text the conformance suite reports:
// a provider's reply, reason or message that quotes the address it was given.
//
// It is a copy of the host's rule, ptah.run/internal/secretmask: a secret of
// five characters or more is taken out wherever it occurs, and a shorter one
// only where it stands as a credential. A short value taken out of every word
// it occurs in garbles the text, and a reader who can tell which letters went
// learns the value.
//
// The rule is copied rather than shared because neither module can import the
// other's package: the SDK cannot import the host's internal packages, and the
// host builds against a released SDK, so a rule moved here would reach the
// host only after a release. testdata/rule.json is the corpus both copies are
// tested against; a change to the rule changes the corpus, and the copy that
// did not follow fails its test.
//
// One difference is deliberate: a secret's letters are compared here without
// regard to ASCII case, because the suite holds a provider to quoting no part
// of the address in any spelling, and an engine may fold an identifier's case
// in its messages.
package secretmask

import (
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"
)

// shortestFreeTextSecret is the length, in characters, below which a secret is
// taken out of a text only where it stands as a credential.
const shortestFreeTextSecret = 5

// settingBefore is the end of text that names the setting a value follows: a
// name, an "=" or a ":", and the quote or brace a value may open with, as in
// `password=`, `Pwd='`, `PGPASSWORD=`, `password: ` or the JSON
// `"password": "`, escaped or not.
var settingBefore = regexp.MustCompile(`([A-Za-z_][A-Za-z0-9_.-]*)\\?["']?\s*[=:]\s*\\?['"{]?$`)

// credentialBefore is the end of text that a credential follows on a command
// line or in SQL: the `-p` the MySQL client takes the password after,
// `--password` and a space or an "=", `Bearer `, and the quote that opens the
// value of `PASSWORD '...'`, PostgreSQL's escape string `PASSWORD E'...'` and
// `IDENTIFIED BY '...'`.
var credentialBefore = regexp.MustCompile(`(?i)(?:(?:^|[\s'"])-p|--password[\s=]\s*['"]?|\bbearer\s+|\b(?:password\s+e?|identified\s+by\s+)['"])$`)

// valueEnds holds the characters that end a setting's value: a separator, the
// backslash of an escape, or the quote or bracket that closes the value or the
// text quoting it.
const valueEnds = " \t\r\n;&,:#\\'\"`})]"

// longestSetting bounds how far before a value [settingBefore] looks for the
// name of its setting.
const longestSetting = 64

// Mask returns text with each occurrence of a secret of five characters or more
// replaced by replacement, and each occurrence of a shorter one where it stands
// as a credential: between the ":" or the "/" and the "@" of user info; after
// one of users and a ":", before white space or the end of the text; as the
// whole value of a setting [IsSecretParameter] names, after an "=" or a ":";
// and as the whole value after `-p`, `--password`, `Bearer`, and `PASSWORD` or
// `IDENTIFIED BY` and a quote.
//
// An occurrence is the secret in any ASCII letter case, with any of its
// characters escaped as an address writes them: `p%40ss` spells `p@ss`, and a
// "+" spells a space. Longer secrets are tried first, so a secret that holds a
// shorter one is taken out whole. Empty secrets are ignored.
func Mask(text string, secrets, users []string, replacement string) string {
	secrets = slices.DeleteFunc(slices.Clone(secrets), func(secret string) bool { return secret == "" })
	slices.SortStableFunc(secrets, func(left, right string) int { return len(right) - len(left) })
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
func maskedWidth(text string, at int, secrets, users []string) int {
	for _, secret := range secrets {
		width := spelled(text[at:], secret)
		if width < 0 {
			continue
		}
		if utf8.RuneCountInString(secret) >= shortestFreeTextSecret || standsAsCredential(text, at, at+width, users) {
			return width
		}
	}
	return 0
}

// spelled returns how many bytes at the start of text spell secret, each
// character as it is, in another ASCII case, or escaped, or -1 when they do not.
func spelled(text, secret string) int {
	if secret == "" {
		return 0
	}
	if len(text) >= 3 && text[0] == '%' && unescapes(text[1:3], secret[0]) {
		if width := spelled(text[3:], secret[1:]); width >= 0 {
			return width + 3
		}
	}
	if text != "" && (sameLetter(text[0], secret[0]) || text[0] == '+' && secret[0] == ' ') {
		if width := spelled(text[1:], secret[1:]); width >= 0 {
			return width + 1
		}
	}
	return -1
}

// sameLetter reports whether a and b are one byte in any ASCII letter case.
func sameLetter(a, b byte) bool {
	return a == b || lowerASCII(a) == lowerASCII(b)
}

// lowerASCII is b with an ASCII capital in lower case.
func lowerASCII(b byte) byte {
	if b >= 'A' && b <= 'Z' {
		return b + 'a' - 'A'
	}
	return b
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
	if match := settingBefore.FindStringSubmatch(window); match != nil && IsSecretParameter(match[1]) {
		return true
	}
	return credentialBefore.MatchString(window)
}

// followsUser reports whether text[:colon] ends with one of users, and no word
// character runs on into the user name from before it.
func followsUser(text string, colon int, users []string) bool {
	return slices.ContainsFunc(users, func(user string) bool {
		return user != "" && strings.HasSuffix(text[:colon], user) && wordEnds(text, colon-len(user))
	})
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

// secretParameterWords mark a parameter as a secret wherever they stand in its
// name.
var secretParameterWords = []string{"apikey", "credential", "jwt", "passhash", "passwd", "password", "pwd", "secret", "token"}

// IsSecretParameter reports a query parameter, connection-string keyword or
// setting that holds a secret or says where one is. The name is compared
// without case or punctuation, and a name ending in `key` or `pass` counts.
func IsSecretParameter(key string) bool {
	name := strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return unicode.ToLower(r)
		}
		return -1
	}, key)
	switch {
	case name == "sslcert", name == "oshash", strings.HasSuffix(name, "key"), strings.HasSuffix(name, "pass"):
		return true
	default:
		return slices.ContainsFunc(secretParameterWords, func(word string) bool { return strings.Contains(name, word) })
	}
}
