package atlas

import (
	"errors"
	"fmt"

	"go.5x5.cz/ptah/internal/atlasmigrate"
)

// This file holds the ONE place the compatibility surface turns a rejected
// migration-directory format into the diagnostic the pinned community binary
// v1.3.0 prints for it.
//
// It exists because the rejection has nine CE-comparable call sites and had
// one adapter.
// `unknown dir format "bogus"` is that binary's answer on `migrate new`,
// `hash`, `validate`, `lint`, `status`, `set`, `diff` and `import`, and on
// `migrate apply` under the `?format=` spelling — every path on which the
// pinned binary reaches directory-layout resolution. Ptah answered it on two
// of them
// (stokaro/ptah#1235 cell 9.8), because the adapter was a block inside the
// `migrate hash` / `migrate validate` wrapper rather than a property of the
// refusal itself. Each remaining verb wrapped the same typed sentinel with its
// own `atlas migrate <verb> <flag>: ` prefix and printed the semantic text.
//
// Every producer on those measured paths now calls [atlasDirFormatError]. A
// verb cannot reach this CE-comparable surface with the semantic wording
// unless it stops calling the helper, which is what the per-verb rows in
// migrate_dir_format_error_test.go are there to catch — one row per verb and
// per spelling, so reverting a single call site reddens exactly that verb.
// Fuller-surface commands such as `checkpoint`, `test`, `edit`, `rebase`, and
// `rm` deliberately stay outside this display adapter: the pinned binary does
// not reach a comparable layout refusal on those paths, and Ptah keeps its
// more useful semantic diagnostic there.

// atlasUnknownDirFormatDisplayError renders a rejected migration-directory
// format the way the pinned community binary v1.3.0 renders it.
//
// Unwrap keeps the semantic chain — the command, the spelling that carried the
// value, and [atlasmigrate.UnknownDirFormatError] with the list of accepted
// layouts — reachable to errors.Is and errors.As. Only the displayed text is
// adapted; nothing is discarded.
type atlasUnknownDirFormatDisplayError struct {
	value string
	err   error
}

func (e atlasUnknownDirFormatDisplayError) Error() string {
	return fmt.Sprintf("unknown dir format %q", e.value)
}

func (e atlasUnknownDirFormatDisplayError) Unwrap() error {
	return e.err
}

// atlasDirFormatError wraps the error a migration-directory format resolution
// returned, for the verb that ran it and the spelling that carried the value.
//
// It is the shared replacement for the `fmt.Errorf("atlas migrate %s %s: %w")`
// each call site used to write itself. The semantic chain is built exactly as
// before, so an error this does NOT recognize is textually unchanged; a
// rejected format value is additionally displayed as the community binary
// displays it.
//
// The value is read off the typed sentinel rather than off the spelling that
// carried it, because the two disagree: `--dir 'file://m?format=bogus'` is
// blamed on `--dir` while the value is `bogus`, and the binary prints the
// value alone.
func atlasDirFormatError(verb, spelling string, err error) error {
	prior := fmt.Errorf("atlas migrate %s %s: %w", verb, spelling, err)
	var unknownFormat *atlasmigrate.UnknownDirFormatError
	if !errors.As(err, &unknownFormat) {
		return prior
	}
	return atlasUnknownDirFormatDisplayError{value: unknownFormat.Value, err: prior}
}
