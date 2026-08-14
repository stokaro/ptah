package dbcli

import "github.com/spf13/pflag"

// PlainHTTPFlagName is the CLI flag name that permits an unencrypted
// connection to an OCI registry.
const PlainHTTPFlagName = "plain-http"

// RegisterPlainHTTPFlag registers the flag that permits an unencrypted HTTP
// connection to an OCI registry.
//
// It is a shared registrar because the flag is not optional decoration on the
// commands that resolve `oci://`: without it there is no spelling of the
// command that reaches a local or in-cluster registry serving plain HTTP, and
// the failure an operator sees instead is a TLS handshake error naming neither
// the flag nor the fact that one exists. stokaro/ptah#928 item 1 was six
// commands in that state — `schema render`, `plan`, `apply`, `export`,
// `inspect` and `migrations generate` all resolved an `oci://` --schema-file
// and registered no flag — while seven sibling commands registered it under
// five different help strings.
//
// Both halves of that are what a registrar fixes. The flag-surface gate in
// cmd/migrations walks the built command tree and requires every command that
// resolves an `oci://` source to register this flag; sharing the help string
// means the census it prints is about presence rather than about wording.
func RegisterPlainHTTPFlag(flags *pflag.FlagSet, target *bool) {
	flags.BoolVar(target, PlainHTTPFlagName, false,
		"Allow an unencrypted HTTP connection to an explicitly trusted local OCI registry")
}

// RegisterPlainHTTPFlagValue registers the same flag for a command that reads
// its flags back out of the flag set with GetBool rather than binding them to
// a struct field.
//
// The discarded target is deliberate: pflag stores the parsed value on the
// Flag, so GetBool returns it whether or not anybody kept the pointer. Writing
// the registration this way rather than calling flags.Bool directly is what
// keeps the help string in one place — a second literal here is exactly how
// the five wordings this registrar replaced came to exist.
func RegisterPlainHTTPFlagValue(flags *pflag.FlagSet) {
	RegisterPlainHTTPFlag(flags, new(bool))
}
