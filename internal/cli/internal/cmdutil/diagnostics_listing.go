package cmdutil

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/rundiag"
)

const diagnosticsListingAnnotation = "ptah.diagnostics_listing"

// SetDiagnosticsListing declares on a surface's root how the runs of that
// surface tell a reader to list the objects an aggregated warning counts. It
// is a property of the surface, like its exit code and its error prefix: the
// strict compatibility profile refuses the variable the other surfaces name,
// so it declares [rundiag.ListedOutsideStrictCompat], and a root that declares
// nothing gets [rundiag.ListedAtDebugLevel].
func SetDiagnosticsListing(cmd *cobra.Command, listing rundiag.Listing) {
	if cmd.Annotations == nil {
		cmd.Annotations = make(map[string]string)
	}
	cmd.Annotations[diagnosticsListingAnnotation] = string(listing)
}

// DiagnosticsListing returns the listing the nearest ancestor of cmd declares,
// or [rundiag.ListedAtDebugLevel] when none does.
func DiagnosticsListing(cmd *cobra.Command) rundiag.Listing {
	for current := cmd; current != nil; current = current.Parent() {
		if listing := current.Annotations[diagnosticsListingAnnotation]; listing != "" {
			return rundiag.Listing(listing)
		}
	}
	return rundiag.ListedAtDebugLevel
}
