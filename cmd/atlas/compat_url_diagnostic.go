package atlas

import (
	"errors"
	"fmt"
	"net/url"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlassource"
)

// This file owns the Atlas-compatible surface's `--url` error boundary: the
// words every compat verb uses when the database URL is missing, carries no
// scheme, or names a driver this build cannot open.
//
// Everything here is measured against the pinned community binary v1.3.0 on
// 2026-08-13, each exit status read from an unpiped invocation. Every message
// below lands on standard error, prefixed `Error: ` by the shared printer, with
// standard output empty and exit status 1. The measured contract, per verb:
//
//	verb             --url absent                     --url ""                        --url notadriver://x
//	migrate apply    required flag "url" not set      required flag "url" not set     unknown driver
//	migrate set      missing driver                   missing driver                  unknown driver
//	migrate status   missing driver                   missing driver                  unknown driver
//	schema apply     required flag(s) "url" not set   required flag(s) "url" not set  unknown driver
//	schema clean     required flag(s) "url" not set   missing driver                  unknown driver
//	schema inspect   required flag(s) "url" not set   missing scheme                  unknown driver
//
// Three separate facts are encoded there, and no single string reproduces them:
//
//   - The singular/plural split. `migrate apply` answers `required flag "url"
//     not set`; the three schema verbs answer `required flag(s) "url" not set`,
//     which is the wording cobra's own ValidateRequiredFlags produces. Emitting
//     one spelling everywhere matches three rows and un-matches the fourth,
//     which is why [atlasRequiredURLSpelling] is per-verb data rather than a
//     constant.
//
//   - The absent/empty split. `schema clean` and `schema inspect` refuse only
//     when the flag was never given: an explicitly empty value passes their
//     required check and travels on to a resolver. `migrate apply` and
//     `schema apply` refuse on the value, so an explicitly empty one answers
//     the same way an absent one does. That is the difference between asking
//     cobra's Changed bit and asking the string.
//
//   - `migrate set` and `migrate status` have no required check at all. An
//     absent `--url` is opened as the empty string and the client layer, not a
//     flag validator, is what answers. That is why [atlasDatabaseURLDiagnostic]
//     is a gate in front of the open rather than a translation of the open's
//     error: an adapter that refused an empty value early could never produce
//     the words those two verbs print.
//
// cobra's own MarkFlagRequired is deliberately NOT used for the plural rows.
// It tests pflag's Changed bit, which a URL supplied by atlas.hcl never sets in
// this build, because project configuration is resolved inside RunE and cobra
// validates before RunE reaches that resolution. Measured: with an atlas.hcl
// whose env carries a url, the pinned binary runs `schema inspect --env local`
// to exit 0 and gets past `schema clean --env local` to the community gate.
// Marking the flag required would refuse both, which deletes a capability the
// surface has (AGENTS.md compatibility rule (c)). The spelling is therefore
// reproduced as measured per-verb data applied where each verb's diagnostic is
// measured to land.
//
// None of this reaches native Ptah. `ptah schema inspect`, `ptah migrations
// status` and the rest keep `database URL is required`, which names the thing
// the caller has to change instead of reporting a driver problem. Matching is
// this surface's contract, not an improvement (AGENTS.md compatibility rule
// (b)), so the clearer wording stays where it is not a compatibility promise.

// atlasURLReference is the documentation pointer the pinned community binary
// appends to every diagnostic in this family. It is spelled once because the
// three messages below must agree on it byte for byte.
const atlasURLReference = ". See: https://atlasgo.io/url"

// atlasMissingDriverMessage is what the pinned binary prints when a database
// URL carries no scheme to select a driver with -- whether it is absent,
// empty, or a bare word. Measured identical for all three inputs.
const atlasMissingDriverMessage = "sql/sqlclient: missing driver" + atlasURLReference

// atlasMissingSchemeMessage is what the pinned binary prints when `schema
// inspect` is handed an empty `--url`. That verb's URL names a desired-state
// source rather than a connection, so it is answered by a different layer and
// carries no `sql/sqlclient:` prefix.
const atlasMissingSchemeMessage = "missing scheme" + atlasURLReference

// atlasRequiredURLSpelling is one verb's measured wording for a `--url` that
// was never given. The two spellings differ only in `(s)`, and which verb uses
// which is a measurement rather than a choice, so the value is carried to the
// refusal instead of being decided at it.
type atlasRequiredURLSpelling string

const (
	// atlasRequiredURLSingular is what `migrate apply` prints, on the pinned
	// binary, both for an absent `--url` and for an explicitly empty one.
	atlasRequiredURLSingular atlasRequiredURLSpelling = `required flag "url" not set`
	// atlasRequiredURLPlural is what `schema apply`, `schema clean` and
	// `schema inspect` print for an absent `--url`.
	atlasRequiredURLPlural atlasRequiredURLSpelling = `required flag(s) "url" not set`
)

// atlasRequiredURLError returns the refusal for a `--url` the verb requires
// before it opens anything.
func atlasRequiredURLError(spelling atlasRequiredURLSpelling) error {
	return errors.New(string(spelling))
}

// atlasUnknownDriverError returns the refusal for a URL whose scheme names no
// driver this build can open.
func atlasUnknownDriverError(scheme string) error {
	return fmt.Errorf("sql/sqlclient: unknown driver %q%s", scheme, atlasURLReference)
}

// atlasDatabaseURLDiagnostic reports how this surface refuses rawURL as a live
// database URL, or nil when the value is one the connector should be given a
// chance to open and report on itself.
//
// It is a gate placed immediately in front of the open, not a rewrite of the
// open's error, so that an absent `--url` -- which reaches it as the empty
// string -- is answered here. Its verdict is aligned with
// [dbschema.ConnectToDatabase] by construction: that connector refuses exactly
// the URLs with no scheme and the schemes [platform.NormalizeDialect] does not
// name, which are exactly the two branches below. Anything it would accept is
// passed through untouched, so a live connection failure still reports itself.
func atlasDatabaseURLDiagnostic(rawURL string) error {
	scheme, ok := atlasURLScheme(rawURL)
	if !ok {
		return errors.New(atlasMissingDriverMessage)
	}
	if platform.NormalizeDialect(scheme) != "" {
		return nil
	}
	return atlasUnknownDriverError(scheme)
}

// atlasDesiredStateURLDiagnostic reports how this surface refuses rawURL as
// `schema inspect --url`, whose value names a desired-state source and may
// legitimately be a schema file, a migration directory or an env:// reference
// rather than a connection.
//
// It re-words exactly one verdict of the shared resolver -- the scheme that
// names no source kind at all -- so that docker://, atlas:// and the reserved
// marker keep the refusals they were separately measured to deserve, and so a
// scheme added to [atlassource.Classify] later is recognized here without this
// file being edited.
//
// A bare local path is deliberately let through. The pinned binary answers it
// `missing scheme`; Ptah resolves it as a schema file, and refusing it would
// delete a capability rather than match one (AGENTS.md compatibility rule (c)).
// That retained divergence is named in docs/conformance.md.
func atlasDesiredStateURLDiagnostic(rawURL string) error {
	if strings.TrimSpace(rawURL) == "" {
		return errors.New(atlasMissingSchemeMessage)
	}
	_, err := atlassource.Classify(rawURL)
	if unsupported, ok := errors.AsType[*atlassource.UnsupportedSchemeError](err); ok {
		return atlasUnknownDriverError(unsupported.Scheme)
	}
	return nil
}

// atlasURLScheme reports the scheme rawURL selects a driver with.
//
// It reads the scheme the way the connector does -- through net/url, which
// lowercases it -- and falls back to the text before the first colon for the
// URLs net/url refuses to parse, of which the MySQL `@tcp(host:port)` DSN form
// is the one this surface actually receives. Without that fallback a MySQL DSN
// would be reported as having no scheme and refused in front of a connector
// that opens it.
func atlasURLScheme(rawURL string) (string, bool) {
	trimmed := strings.TrimSpace(rawURL)
	if trimmed == "" {
		return "", false
	}
	if parsed, err := url.Parse(trimmed); err == nil && parsed.Scheme != "" {
		return parsed.Scheme, true
	}
	scheme, _, found := strings.Cut(trimmed, ":")
	if !found || scheme == "" {
		return "", false
	}
	return strings.ToLower(scheme), true
}
