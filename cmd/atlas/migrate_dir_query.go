package atlas

import (
	"fmt"
	"net/url"

	"go.5x5.cz/ptah/internal/atlasmigrate"
)

// checkNativeAtlasDirQuery validates the query of an Atlas --dir migration
// directory URL for a verb that can only read a NATIVE Atlas directory.
//
// Every verb that registers --dir used to refuse a non-empty query outright.
// That refused two very different inputs with one message. Measured against the
// pinned community binary v1.3.0 on a hashed native Atlas directory,
// `?nonsense=1` exits 0 on apply, hash, validate, lint, status and set: an
// unrecognized key is dropped, and the directory reads as Atlas exactly as it
// would with no query at all (stokaro/ptah#1013 section 2). Ignoring it is
// therefore the matching behavior, and it is
// [atlasmigrate.ResolveApplyDirFormat] that does the ignoring, so the rule lives
// in one place for all eight verbs rather than being restated per verb.
//
// A `?format=` naming a foreign layout is a different input and stays refused
// here. The community binary honors it on these verbs — `migrate lint
// --dir 'file://gm?format=golang-migrate'` exits 0 where the same directory
// without the query exits 1 on a checksum mismatch, so it is functionally
// honored and not decoration — but converting a foreign directory for lint,
// status and set, and WRITING one for new and diff, is a larger change than
// accepting the query (stokaro/ptah#1013 section 1 and stokaro/ptah#1002 remain
// open). Refusing is the strict side: it never exits 0 where the community
// binary exits 1.
//
// An empty `?format=` value selects the native Atlas layout and so passes, which
// is what the community binary does with it too.
func checkNativeAtlasDirQuery(query url.Values) error {
	format, err := atlasmigrate.ResolveApplyDirFormat("", query)
	if err != nil {
		return err
	}
	if !atlasmigrate.ReadsNativeAtlasDir(format) {
		return fmt.Errorf(
			"Atlas accepts ?format=%s, but Ptah does not implement that directory format for this command yet",
			format,
		)
	}
	return nil
}
