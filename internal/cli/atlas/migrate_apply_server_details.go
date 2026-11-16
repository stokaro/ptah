package atlas

import (
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/migraterun"
	"ptah.run/internal/envbool"
)

// applyServerDetailsEnvVar asks `ptah-compat migrate apply` for the database
// server's details of a failed statement or commit, written after the error
// line: DETAIL, HINT, the error position and the objects the server names.
//
// The name is not invented here. Native `ptah migrations up` and `ptah
// migrations down` register --server-details, which the flag/environment twin
// convention (internal/cli/internal/cmdflags) binds to PTAH_SERVER_DETAILS, so
// one name means one thing across both binaries. `ptah-compat migrate down`
// forwards to the native verb and reads it through that binding, and reads it
// here under --format, where it runs the rollback itself.
//
// It is a variable rather than a flag because the compatibility surface
// registers no flag the community binary does not have.
const applyServerDetailsEnvVar = "PTAH_SERVER_DETAILS"

// applyServerDetails is the declaration of the variable. See
// [ptah.run/internal/envbool].
//
// It is [ptah.run/internal/envbool.Gated]: the details are output the
// community binary has no setting for, so strict compatibility refuses an
// enabled value rather than print text the oracle never prints.
var applyServerDetails = envbool.New(applyServerDetailsEnvVar, false, envbool.Gated)

// resolveAtlasApplyServerDetails reads the run's choice. A malformed value is
// refused before the run reads its directory or opens the database.
func resolveAtlasApplyServerDetails(cmd *cobra.Command) (migraterun.ServerDetails, error) {
	enabled, err := applyServerDetails.Resolve()
	if err != nil {
		return migraterun.ServerDetails{}, err
	}
	return migraterun.NewServerDetails(cmd.Context(), enabled), nil
}
