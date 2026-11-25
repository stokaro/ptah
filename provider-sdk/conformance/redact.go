package conformance

import (
	"net/url"
	"slices"
	"strings"

	"ptah.run/provider-sdk/internal/secretmask"
)

// redacted stands in for a sensitive text in what the suite reports.
const redacted = "<redacted>"

// sensitiveTexts returns the texts no reply may quote and no report may
// show: the canary, the credential address, the live address, and the
// credentials in the live address, which are its password and the value of
// each query parameter whose name says it carries one.
func sensitiveTexts(config Config) []string {
	texts := []string{Canary}
	if config.CredentialAddress != "" {
		texts = append(texts, config.CredentialAddress)
	}
	if config.Live != nil {
		texts = append(texts, config.Live.Address)
		texts = append(texts, addressSecrets(config.Live.Address)...)
	}
	slices.Sort(texts)
	return slices.Compact(texts)
}

// sensitiveUsers returns the user names of the credential address and the
// live address, after which [secretmask.Mask] takes a short credential out.
func sensitiveUsers(config Config) []string {
	var users []string
	for _, address := range []string{config.CredentialAddress, liveAddress(config)} {
		if parsed, err := url.Parse(address); err == nil && parsed.User != nil {
			users = append(users, parsed.User.Username())
		}
	}
	return users
}

// liveAddress is the address of config's live database, or empty.
func liveAddress(config Config) string {
	if config.Live == nil {
		return ""
	}
	return config.Live.Address
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
		for _, value := range values {
			if secretmask.IsSecretParameter(name, value) {
				secrets = append(secrets, value)
			}
		}
	}
	return secrets
}

// redact takes every sensitive text out of text, by the rule the host masks
// its own output with: a text of five characters or more wherever it occurs,
// in any ASCII letter case, and a shorter one where it stands as a
// credential. A credential of a character or two taken out of every word
// would garble the detail and show the credential by the gaps.
//
// What [run.observeText] counts as quoting a sensitive text is what redact
// takes out, so a check that fails for a quoted credential reports a detail
// with it taken out, and one that passes reports the detail as it was sent.
func (r *run) redact(text string) string {
	return secretmask.Mask(text, r.sensitive, r.users, redacted)
}

// joinFailures writes the failures of one check, one per line.
func joinFailures(failures []string) string {
	return strings.Join(failures, "\n")
}
