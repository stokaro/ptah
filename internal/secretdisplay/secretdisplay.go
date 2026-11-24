// Package secretdisplay prepares untrusted process output for safe display.
package secretdisplay

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dburldisplay"
)

// Sanitize redacts secrets discoverable from the effective environment and
// command arguments, then escapes terminal control characters. Newlines and
// tabs are preserved for readable diagnostics.
//
// A secret of five characters or more is redacted wherever it occurs, and a
// shorter one where it stands as a credential, as [dburldisplay.MaskSecrets]
// reads them: text that only holds a short secret's characters keeps them. The
// user names that a short secret is redacted after are those of the addresses
// in env and args, and the values of the variables and options that name a
// user, such as PGUSER and --user.
func Sanitize(text string, env, args []string) string {
	return SanitizeKnown(text, env, args, nil)
}

// SanitizeKnown is [Sanitize] with the secrets the caller holds. Each value in
// known is a secret of its own, redacted by the length rule [Sanitize] states
// whatever stands next to it: a value that is a secret only because the caller
// says so has no name or address beside it for env and args to show. A known
// value that is an address gives its password too, as an address in env or
// args does.
func SanitizeKnown(text string, env, args, known []string) string {
	return escapeControls(dburldisplay.MaskSecrets(text, secretValues(env, args, known), userNames(env, args), "redacted"))
}

// SanitizeError returns an error with display-safe text while preserving the
// original error for errors.Is and errors.As.
func SanitizeError(err error, env, args []string) error {
	return SanitizeErrorKnown(err, env, args, nil)
}

// SanitizeErrorKnown is [SanitizeError] with the secrets the caller holds, as
// [SanitizeKnown] reads them.
func SanitizeErrorKnown(err error, env, args, known []string) error {
	if err == nil {
		return nil
	}
	return displayError{
		err:  err,
		text: boundDisplay(SanitizeKnown(err.Error(), env, args, known)),
	}
}

type displayError struct {
	err  error
	text string
}

func (e displayError) Error() string { return e.text }
func (e displayError) Unwrap() error { return e.err }

func boundDisplay(text string) string {
	const (
		maxBytes = 2000
		marker   = "...[truncated]..."
	)
	if len(text) <= maxBytes {
		return text
	}

	prefixEnd := (maxBytes - len(marker)) / 2
	for prefixEnd > 0 && !utf8.RuneStart(text[prefixEnd]) {
		prefixEnd--
	}
	suffixStart := len(text) - (maxBytes - len(marker) - prefixEnd)
	for suffixStart < len(text) && !utf8.RuneStart(text[suffixStart]) {
		suffixStart++
	}
	return text[:prefixEnd] + marker + text[suffixStart:]
}

func secretValues(env, args, known []string) []string {
	values := make(map[string]struct{})
	for _, value := range known {
		addSecret(values, value)
		addURLSecrets(values, value)
	}
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if isSensitiveKey(key) {
			addSecret(values, value)
		}
		addURLSecrets(values, value)
	}
	for idx, arg := range args {
		key, value, hasValue := option(arg)
		if isSensitiveKey(key) {
			if hasValue {
				addSecret(values, value)
			} else if idx+1 < len(args) {
				addSecret(values, args[idx+1])
			}
		}
		addURLSecrets(values, arg)
	}

	secrets := make([]string, 0, len(values))
	for value := range values {
		secrets = append(secrets, value)
	}
	slices.SortFunc(secrets, func(left, right string) int {
		return len(right) - len(left)
	})
	return secrets
}

// userNames is each user name env and args give: the value of a variable or an
// option that names a user, and the user of each address among them.
func userNames(env, args []string) []string {
	var users []string
	for _, entry := range env {
		key, value, ok := strings.Cut(entry, "=")
		if !ok {
			continue
		}
		if dburldisplay.IsUserParameter(key) && value != "" {
			users = append(users, value)
		}
		users = append(users, addressUsers(value)...)
	}
	for idx, arg := range args {
		key, value, hasValue := option(arg)
		if strings.HasPrefix(arg, "--") && dburldisplay.IsUserParameter(key) {
			if hasValue && value != "" {
				users = append(users, value)
			} else if !hasValue && idx+1 < len(args) {
				users = append(users, args[idx+1])
			}
		}
		users = append(users, addressUsers(arg)...)
	}
	return users
}

// addressUsers is the user of raw where it is an address, which
// [addURLSecrets] reads the secrets of too.
func addressUsers(raw string) []string {
	if !strings.Contains(raw, "://") {
		return nil
	}
	return dburldisplay.Users(raw)
}

func option(arg string) (key, value string, hasValue bool) {
	arg = strings.TrimLeft(arg, "-")
	return strings.Cut(arg, "=")
}

// isSensitiveKey reports whether an environment variable or an option names
// a secret. A word of the name counts when it is one of the secret words, or
// ends in one that names nothing else when it ends a longer word, as
// PGPASSWORD ends in "password". PWD and OLDPWD name the working directories
// the shell keeps, and "pwd" ends no longer word for that reason: taken for a
// secret, the working directory would be taken out of every path a program
// writes.
func isSensitiveKey(key string) bool {
	if strings.EqualFold(key, "PWD") || strings.EqualFold(key, "OLDPWD") {
		return false
	}
	parts := strings.FieldsFunc(strings.ToLower(key), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	for _, part := range parts {
		switch part {
		case "credential", "credentials", "passwd", "password", "pwd", "secret", "token":
			return true
		}
		if slices.ContainsFunc([]string{"passwd", "password", "secret", "token"}, func(word string) bool { return strings.HasSuffix(part, word) }) {
			return true
		}
	}
	switch strings.Join(parts, "") {
	case "accesskey",
		"apikey",
		"awsaccesskeyid",
		"awssecretaccesskey",
		"awssessiontoken",
		"clientsecret",
		"idtoken",
		"privatekey",
		"refreshtoken",
		"sslkey",
		"sslpassword":
		return true
	default:
		return false
	}
}

func addURLSecrets(values map[string]struct{}, raw string) {
	if !strings.Contains(raw, "://") {
		return
	}
	for _, secret := range dburldisplay.Secrets(raw) {
		addSecret(values, secret)
	}
	parsed, err := atlasurl.Parse(raw)
	if err == nil && parsed.Scheme != "" && parsed.User != nil {
		if password, ok := parsed.User.Password(); ok {
			addSecret(values, password)
		}
	}
	addAuthorityPassword(values, raw)
	addQuerySecrets(values, raw)
}

func addAuthorityPassword(values map[string]struct{}, raw string) {
	_, authority, ok := strings.Cut(raw, "://")
	if !ok {
		return
	}
	authority, _, _ = strings.Cut(authority, "/")
	userInfo, _, ok := strings.Cut(authority, "@")
	if !ok {
		return
	}
	_, password, ok := strings.Cut(userInfo, ":")
	if !ok {
		return
	}
	addSecret(values, password)
	decoded, err := url.QueryUnescape(password)
	if err == nil {
		addSecret(values, decoded)
	}
}

func addQuerySecrets(values map[string]struct{}, raw string) {
	_, rawQuery, ok := strings.Cut(raw, "?")
	if !ok {
		return
	}
	rawQuery, _, _ = strings.Cut(rawQuery, "#")
	for pair := range strings.SplitSeq(rawQuery, "&") {
		rawKey, rawValue, _ := strings.Cut(pair, "=")
		key, err := url.QueryUnescape(rawKey)
		if err != nil {
			continue
		}
		if !isSensitiveKey(key) {
			continue
		}
		addSecret(values, rawValue)
		value, err := url.QueryUnescape(rawValue)
		if err == nil {
			addSecret(values, value)
		}
	}
}

func addSecret(values map[string]struct{}, value string) {
	if value != "" {
		values[value] = struct{}{}
	}
}

func escapeControls(text string) string {
	var out strings.Builder
	for len(text) > 0 {
		r, size := utf8.DecodeRuneInString(text)
		if r == utf8.RuneError && size == 1 {
			fmt.Fprintf(&out, `\x%02x`, text[0])
			text = text[1:]
			continue
		}
		text = text[size:]
		switch {
		case r == '\n' || r == '\t':
			out.WriteRune(r)
		case unicode.IsControl(r):
			if r <= 0xff {
				fmt.Fprintf(&out, `\x%02x`, r)
			} else {
				fmt.Fprintf(&out, `\u%04x`, r)
			}
		default:
			out.WriteRune(r)
		}
	}
	return out.String()
}
