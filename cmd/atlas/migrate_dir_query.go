package atlas

import (
	"fmt"
	"net/url"

	"go.5x5.cz/ptah/internal/atlasmigrate"
)

// checkWritingVerbDirQuery validates the `--dir` query for the two verbs that
// WRITE into the directory: `migrate new` and `migrate diff`.
//
// An unrecognized KEY is ignored, matching the community binary: measured on
// v1.3.0, `?nonsense=1` exits 0 and the directory reads as Atlas exactly as it
// would with no query at all. [atlasmigrate.ResolveApplyDirFormat] does the
// ignoring, so that rule lives in one place for every verb rather than being
// restated per verb.
//
// A `?format=` naming a foreign layout stays refused, and for these two verbs
// that is not the same call the reading verbs make. The reading verbs — hash,
// validate, lint, status, set — convert a foreign layout in memory and report
// on it, which is what #992, #1002 and #1133 built. Writing one is a different
// problem: measured against the pinned community binary, `migrate new --dir
// 'file://goosedir?format=goose'` refuses an unhashed Goose directory over
// GOOSE's own covered file set, so honoring the query here means computing that
// file set before writing, not reading the directory as Atlas. Ignoring the
// query would gate the wrong set and then write into it.
//
// Refusing is the strict side: it never exits 0 where the community binary
// exits 1. stokaro/ptah#1013 tracks closing the gap.
//
// An empty `?format=` value selects the native Atlas layout and passes, which is
// what the community binary does with it too.
func checkWritingVerbDirQuery(query url.Values) error {
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
