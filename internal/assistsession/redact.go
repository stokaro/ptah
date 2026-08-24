package assistsession

import (
	"bytes"
	"encoding/json"
	"net/url"
	"strings"
)

// redactCredentials removes passwords from any connection URL inside a tool
// call's arguments.
//
// A tool argument is written to the session file verbatim, and one of Ptah's
// own tools takes a database URL as an argument. Without this, asking a model
// to read a live database puts the DSN -- password included -- into a file that
// outlives the process and that people are told to read later.
//
// The password is dropped rather than masked, and the username is kept: a
// session that could not say which account was used would lose the part of the
// URL worth reading back, and a mask is a string somebody eventually tries to
// use. This is the same shape internal/preflight uses when it has to show a URL.
//
// Only strings that parse as a URL carrying a password are touched. Prose is
// left exactly as it was, because a record that quietly rewrote the model's
// arguments would misreport what Ptah was asked to do.
func redactCredentials(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()

	var value any
	if err := decoder.Decode(&value); err != nil {
		// Not JSON Ptah can walk. It is what the model sent and it is kept as
		// it is: guessing at the shape of something unparsed is how a redaction
		// corrupts an argument it did not understand.
		return raw
	}

	cleaned, changed := redactValue(value)
	if !changed {
		return raw
	}
	encoded, err := json.Marshal(cleaned)
	if err != nil {
		return raw
	}
	return encoded
}

// redactValue walks a decoded JSON value, reporting whether it changed.
func redactValue(value any) (any, bool) {
	switch typed := value.(type) {
	case string:
		cleaned := redactURL(typed)
		return cleaned, cleaned != typed
	case []any:
		changed := false
		for i, element := range typed {
			cleaned, elementChanged := redactValue(element)
			typed[i] = cleaned
			changed = changed || elementChanged
		}
		return typed, changed
	case map[string]any:
		changed := false
		for key, element := range typed {
			cleaned, elementChanged := redactValue(element)
			typed[key] = cleaned
			changed = changed || elementChanged
		}
		return typed, changed
	}
	return value, false
}

// redactURL drops the password from one URL, and leaves anything else alone.
func redactURL(candidate string) string {
	if !strings.Contains(candidate, "@") {
		return candidate
	}
	parsed, err := url.Parse(candidate)
	if err != nil || parsed.Scheme == "" || parsed.User == nil {
		return candidate
	}
	if _, hasPassword := parsed.User.Password(); !hasPassword {
		return candidate
	}
	parsed.User = url.User(parsed.User.Username())
	return parsed.String()
}
