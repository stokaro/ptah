package conformance

import (
	"net/url"
	"slices"
	"strings"
)

// redacted stands in for a sensitive text in what the suite reports.
const redacted = "<redacted>"

// minimumSecret is the shortest query value the suite treats as a secret.
// A shorter one, such as a flag, would match ordinary text.
const minimumSecret = 4

// sensitiveTexts returns the texts no reply may quote and no report may
// show: the canary, the credential address, the live address, and the
// credentials in the live address, which are its password and the value of
// each query parameter whose name says it carries one. The longest come
// first, so that redacting a short one does not break a long one that
// contains it.
func sensitiveTexts(config Config) []string {
	texts := []string{Canary}
	if config.CredentialAddress != "" {
		texts = append(texts, config.CredentialAddress)
	}
	if config.Live != nil {
		texts = append(texts, config.Live.Address)
		texts = append(texts, addressSecrets(config.Live.Address)...)
	}
	slices.SortFunc(texts, func(a, b string) int { return len(b) - len(a) })
	return slices.Compact(texts)
}

// addressSecrets returns the password of an address and the values of its
// query parameters named for a credential.
func addressSecrets(address string) []string {
	parsed, err := url.Parse(address)
	if err != nil {
		return nil
	}
	var secrets []string
	if password, set := parsed.User.Password(); set && password != "" {
		secrets = append(secrets, password)
	}
	for name, values := range parsed.Query() {
		if !credentialName(name) {
			continue
		}
		for _, value := range values {
			if len(value) >= minimumSecret {
				secrets = append(secrets, value)
			}
		}
	}
	return secrets
}

func credentialName(name string) bool {
	name = strings.ToLower(name)
	for _, word := range []string{"password", "token", "secret", "credential", "key"} {
		if strings.Contains(name, word) {
			return true
		}
	}
	return false
}

// redact replaces every sensitive text in text.
func (r *run) redact(text string) string {
	for _, secret := range r.sensitive {
		text = replaceFold(text, secret, redacted)
	}
	return text
}

func containsFold(text, part string) bool {
	return part != "" && strings.Contains(strings.ToLower(text), strings.ToLower(part))
}

// replaceFold replaces every occurrence of part in text, compared without
// regard to ASCII letter case.
func replaceFold(text, part, with string) string {
	if part == "" {
		return text
	}
	lowerText, lowerPart := strings.ToLower(text), strings.ToLower(part)
	if len(lowerText) != len(text) || len(lowerPart) != len(part) {
		return strings.ReplaceAll(text, part, with)
	}
	var b strings.Builder
	for {
		at := strings.Index(lowerText, lowerPart)
		if at < 0 {
			b.WriteString(text)
			return b.String()
		}
		b.WriteString(text[:at])
		b.WriteString(with)
		text, lowerText = text[at+len(part):], lowerText[at+len(part):]
	}
}

// joinFailures writes the failures of one check, one per line.
func joinFailures(failures []string) string {
	return strings.Join(failures, "\n")
}
