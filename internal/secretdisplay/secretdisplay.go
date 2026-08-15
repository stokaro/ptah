// Package secretdisplay prepares untrusted process output for safe display.
package secretdisplay

import (
	"fmt"
	"net/url"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"

	"go.5x5.cz/ptah/internal/atlasurl"
)

// Sanitize redacts secrets discoverable from the effective environment and
// command arguments, then escapes terminal control characters. Newlines and
// tabs are preserved for readable diagnostics.
func Sanitize(text string, env, args []string) string {
	for _, secret := range secretValues(env, args) {
		text = strings.ReplaceAll(text, secret, "redacted")
	}
	return escapeControls(text)
}

// SanitizeError returns an error with display-safe text while preserving the
// original error for errors.Is and errors.As.
func SanitizeError(err error, env, args []string) error {
	if err == nil {
		return nil
	}
	return displayError{
		err:  err,
		text: boundDisplay(Sanitize(err.Error(), env, args)),
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

func secretValues(env, args []string) []string {
	values := make(map[string]struct{})
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

func option(arg string) (key, value string, hasValue bool) {
	arg = strings.TrimLeft(arg, "-")
	return strings.Cut(arg, "=")
}

func isSensitiveKey(key string) bool {
	parts := strings.FieldsFunc(strings.ToLower(key), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsDigit(r)
	})
	for _, part := range parts {
		switch part {
		case "credential", "credentials", "passwd", "password", "pwd", "secret", "token":
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
