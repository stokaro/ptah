package projectconfig

import "go.5x5.cz/ptah/internal/envbool"

// IgnoreEnvSchemasEnvVar restores the treatment `env { schemas }` had before
// the attribute got a parser arm: the value is still decoded and still
// type-checked, but it selects nothing, and the schema universe stays whatever
// the connection can see.
//
// It exists because acting on the attribute REMOVES something Ptah emitted
// before. Measured on the pinned Atlas community binary v1.3.0 against a
// PostgreSQL database holding schemas `one`, `two` and `public`:
// `schemas = ["one"]` with `schema inspect --env local` describes `one` alone,
// and `schemas = ["nosuchschema"]` describes nothing at all and exits 0. Ptah
// described all three in both arms. Matching the binary is the default, and
// AGENTS.md ("Compatibility never removes a capability. Constitute it, do not
// discard it.") is why the realm-wide description stays reachable rather than
// being deleted.
//
// It is an environment variable and not a flag for the reason
// [go.5x5.cz/ptah/internal/atlashclrender.KeepAtlasRefusedBlocksEnvVar] gives:
// the conformance cli-surface tier asserts that `ptah-compat` registers exactly
// the flags the pinned binary registers, so a flag that binary does not have
// would break the promise the surface exists to keep.
//
// It governs the SELECTION only, never the refusal. A value the field cannot
// hold — `schemas = "one"` where a list belongs — is refused with this variable
// set exactly as it is without it, because the pinned binary refuses it and
// compatibility rule (a) does not have an opt-out.
const IgnoreEnvSchemasEnvVar = "PTAH_ATLAS_IGNORE_ENV_SCHEMAS"

// ignoreEnvSchemas is the declaration of the variable, made once, in the
// package that owns the atlas.hcl parse. See [go.5x5.cz/ptah/internal/envbool].
//
// It is [go.5x5.cz/ptah/internal/envbool.Gated], and the direction is what
// decides it. The pinned community binary HONORS `env { schemas }`: measured on
// v1.3.0 against a PostgreSQL database holding `one`, `two` and `public`,
// `schemas = ["one"]` describes `one` alone. A true value here restores the
// realm-wide description Ptah emitted before the attribute got a parser arm,
// which is strictly MORE than the binary describes -- so it is a capability the
// binary does not have, and strict mode, whose whole job is to be measurable
// against that binary, has to refuse it. Retaining it would let a conformance
// run describe schemas the oracle excluded and call the difference parity.
var ignoreEnvSchemas = envbool.New(IgnoreEnvSchemasEnvVar, false, envbool.Gated)

// ValidateAtlasEnvironmentVariables refuses a malformed value for every boolean
// `PTAH_*` variable that governs how this package evaluates an atlas.hcl. It
// reads the environment only; no file is opened and no document is parsed.
//
// It is the form for a caller that decides whether to parse AT ALL.
// [ParseAtlasFSCollectionWithOptions] resolves the same variable because it
// needs the value, and [LoadAtlasFileCollectionWithOptions] calls this before
// it looks at the file system; a caller that may reach neither still owes the
// refusal.
//
// cmd/atlas's compatibility adapter is exactly such a caller. It opens the
// project file itself and returns "no project" when the file is absent, so on
// that arm it reaches no entry point here. Measured on that adapter with
// `PTAH_ATLAS_IGNORE_ENV_SCHEMAS` exported empty, running
// `schema inspect --url sqlite://probe.db`: with an atlas.hcl in the working
// directory it exits 1 naming the variable, and with no atlas.hcl it exited 0
// and described the database. One environment, two answers, chosen by the
// presence of a file the variable says nothing about -- which is the defect the
// eager resolve exists to close, not a corner of it.
func ValidateAtlasEnvironmentVariables() error {
	_, err := ignoreEnvSchemas.Resolve()
	return err
}
