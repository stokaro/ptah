// Package testexternal answers whether a `.test.hcl` case may run its
// `external` step, so `ptah schema test`, `ptah migrations test` and the
// compatibility test verbs cannot answer the question differently.
package testexternal

import (
	"context"

	"ptah.run/internal/envbool"
	"ptah.run/internal/execpolicy"
	"ptah.run/migration/dbtest"
)

// externalStepOrigin names an external step for the program policy.
const externalStepOrigin = "a .test.hcl external step"

// Allowed reports whether this invocation authorizes an external test step.
//
// The binary's program policy answers it, the one ctx carries: ptah-compat
// runs the step as the pinned community binary does, and native ptah runs it
// with --allow-config-exec. PTAH_ALLOW_EXTERNAL_TEST_COMMAND is the older
// spelling, kept so a suite that sets it keeps working; it authorizes
// external steps and nothing else.
//
// Resolve it before the command does any work: a malformed value is a
// configuration error the operator already knows they changed, and letting it
// stay dormant until a run happens to reach an external step means every
// healthy run hides it.
func Allowed(ctx context.Context) (bool, error) {
	legacy, err := allowExternal.Resolve()
	if err != nil {
		return false, err
	}
	if legacy {
		return true, nil
	}
	return execpolicy.Authorize(ctx, externalStepOrigin, nil) == nil, nil
}

// allowExternal is the declaration of the older variable, made once, on the
// surface that owns it. See [ptah.run/internal/envbool].
//
// It is [ptah.run/internal/envbool.Retained]: it adds no capability the
// pinned community binary lacks, since that binary runs an `external` step
// with no authorization at all.
var allowExternal = envbool.New(dbtest.AllowExternalCommandsEnvVar, false, envbool.Retained)
