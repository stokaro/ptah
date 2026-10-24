package providerconfig

import "fmt"

// origin says which selection rule produced an Installation. Verification
// wording follows it, because the remedy for a bad file differs: a manifest
// entry is corrected by the operator, and a stored file is replaced by running
// `ptah providers install`.
type origin int

const (
	originManifest origin = iota
	originStore
)

// label names the installation in a message.
func (o origin) label() string {
	if o == originStore {
		return "installed"
	}
	return "configured"
}

// untrusted explains a digest mismatch and what to do about it.
func (o origin) untrusted(engine string) string {
	if o == originStore {
		return fmt.Sprintf("The installed provider executable does not match the SHA-256 digest its install record holds; run `ptah providers install %s` to replace it. No other provider is tried.", engine)
	}
	return "The provider executable does not match its trusted SHA-256 checksum; verify its provenance before changing the manifest."
}

// restore explains how to bring back a missing or unreadable file.
func (o origin) restore(engine string) string {
	if o == originStore {
		return fmt.Sprintf("run `ptah providers install %s`", engine)
	}
	return "restore it or correct the trusted manifest"
}

// mismatch explains an engine declaration that differs from what was trusted.
func (o origin) mismatch() string {
	if o == originManifest {
		return "The executable's engine declarations differ from the trusted manifest."
	}
	return "The executable's engine declarations differ from the official catalog entries of the engines its install record names."
}
