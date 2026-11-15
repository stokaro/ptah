// Package replaytrust carries the operator's statement that they trust the SQL
// a run executes on its dev database: the migration files a replay runs, a
// desired-state SQL file loaded there, and the statements a plan rehearsal
// runs. Under it the replay guard in internal/devclean runs a statement it
// refuses only because it cannot confine the statement's effects to the dev
// database, such as a routine definition, a DO block or a CALL, with the
// privileges of the dev connection.
//
// Trust is not ownership. It claims nothing about the server: the run still
// cleans only the dev database it claimed, and never drops a role or any other
// server-level object because the trusted SQL created it. Ownership is
// internal/devdocker's record, which a server the operator declared disposable
// or a server the run provisioned enters; trust neither implies it nor is
// implied by it.
//
// Each binary resolves the setting when a command starts and attaches it to
// the command's context with [With]. internal/devdocker's Resolve, which every
// dev database consumer calls with that context, records the URL it hands back
// for [Trusted] until its release runs, so the guard reads the answer by the
// URL the dev connection was opened with, as it reads ownership.
package replaytrust

import (
	"context"
	"strings"
	"sync"

	"ptah.run/internal/envbool"
)

// EnvVar is the variable both binaries read. ptah-compat takes no flag for it,
// because the conformance cli-surface tier asserts that its flags are the
// pinned community binary's; native ptah binds it to [FlagName].
const EnvVar = "PTAH_TRUST_REPLAY_SQL"

// FlagName is the native flag. Its environment binding is [EnvVar]. ptah.yaml
// has no key for it: a project file arrives with the change under review, so a
// key there would let that change trust its own SQL against the dev URL a CI
// job supplies.
const FlagName = "trust-replay-sql"

// Remedy ends a refusal that trust would lift. It names the variable, which
// both binaries read, as the disposable server's remedy does.
const Remedy = "if you trust this SQL to run with the dev connection's privileges, set " + EnvVar + "=1"

// trustReplaySQL is the declaration of the variable, made once, in the package
// that owns it. See [ptah.run/internal/envbool].
//
// It is [envbool.Retained]: the pinned community binary replays every one of
// these statements on any dev URL without a guard, so a true value restores
// what that binary does and adds no Atlas capability for strict compatibility
// to withhold. Refusing it in strict mode would make strict mode stricter than
// the binary it measures.
var trustReplaySQL = envbool.New(EnvVar, false, envbool.Retained)

// Declared reports whether the operator set [EnvVar] to a true value. Unset
// declares nothing; an empty or unparsable value is a configuration error.
//
// A ptah-compat command that runs SQL on a dev database resolves it before its
// early returns, so a typo fails the first run rather than the one that would
// have replayed. Native ptah reads the flag instead, whose binding parses the
// variable by the same rule.
func Declared() (bool, error) {
	return trustReplaySQL.Resolve()
}

type contextKey struct{}

// With returns ctx carrying the operator's answer.
func With(ctx context.Context, trusted bool) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, trusted)
}

// From reports whether ctx carries trust. A context without an answer trusts
// nothing, so a path that forgot to carry one keeps the strict default.
func From(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	trusted, _ := ctx.Value(contextKey{}).(bool)
	return trusted
}

// record counts, per URL a dev connection opens, the runs in progress that
// trust the SQL they execute there. A URL is counted rather than stored once,
// because one command can resolve the same URL twice, one call inside the
// other, and the inner release must not end the outer one's trust.
var record = struct {
	sync.Mutex
	urls map[string]int
}{urls: make(map[string]int)}

// Record notes that the run trusts the SQL it executes through rawURL, trimmed
// of surrounding space, and returns the release that ends the note. The
// release is safe to call more than once.
func Record(rawURL string) func() {
	url := strings.TrimSpace(rawURL)
	record.Lock()
	record.urls[url]++
	record.Unlock()
	return sync.OnceFunc(func() {
		record.Lock()
		defer record.Unlock()
		record.urls[url]--
		if record.urls[url] <= 0 {
			delete(record.urls, url)
		}
	})
}

// Trusted reports whether a run in progress trusts the SQL it executes through
// rawURL, the URL a dev connection was opened with.
func Trusted(rawURL string) bool {
	record.Lock()
	defer record.Unlock()
	return record.urls[strings.TrimSpace(rawURL)] > 0
}
