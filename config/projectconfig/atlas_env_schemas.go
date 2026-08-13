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
var ignoreEnvSchemas = envbool.New(IgnoreEnvSchemasEnvVar, false)
