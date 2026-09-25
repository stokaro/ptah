package devdocker

import "ptah.run/internal/envbool"

// DisposableServerEnvVar declares that the server a `--dev-url` names is the
// run's own, as a server a `docker://` URL starts is: a CI service container
// the job throws away, or a container running an image `docker://` cannot
// name. A dev database replay on such a server runs the statements whose effect
// reaches past the dev database, such as a `DO` block, a routine or a role.
//
// It is an environment variable and not a flag, so `ptah-compat` registers no
// flag the pinned community binary lacks, and one name means the same thing
// on both binaries.
const DisposableServerEnvVar = "PTAH_DEV_SERVER_DISPOSABLE"

// disposableServer is the declaration of the variable, made once, in the
// package that owns the record it feeds. See [ptah.run/internal/envbool].
//
// It is [ptah.run/internal/envbool.Retained]: the pinned community binary
// replays these statements on any dev URL, so the variable lifts a refusal
// that is Ptah's own and adds no Atlas capability for strict compatibility to
// withhold.
var disposableServer = envbool.New(DisposableServerEnvVar, false, envbool.Retained)

// DisposableServerDeclared reports whether the operator declared the dev server
// disposable. Unset declares nothing; an empty or unparsable value is a
// configuration error.
//
// Every command that replays a dev database resolves it before its early
// returns and hands the answer to [Resolve] as [Options.DeclaredDisposable], so
// a typo fails the first run rather than the one that would have replayed.
func DisposableServerDeclared() (bool, error) {
	return disposableServer.Resolve()
}
