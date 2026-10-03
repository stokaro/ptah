package atlas

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/config/projectconfig"
	"ptah.run/core/platform"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/connectgate"
	"ptah.run/internal/ydbgap"
)

// This file keeps every YDB database URL off the compatibility surface until
// the surface reaches YDB. YDB is a Ptah extension here, as no Atlas edition
// has a YDB driver, and the verbs of this surface do not reach it yet: the HCL
// a YDB schema inspects to, the Atlas-format revision table and the verbs' own
// checks are planned together. Each refusal names where the URL came from when
// that is known, and every one says it in the words of the same gap.
//
// The refusals hold whatever the policy. Strict mode refuses a URL flag
// earlier, in the words of the pinned binary, but a URL strict mode has not
// looked at must not reach a verb that cannot serve it either.

// atlasYDBURLFlags are the flags of this surface that carry a database URL.
// --db-url is `schema stats inspect`'s, which embeds the native verb.
var atlasYDBURLFlags = []string{"url", "dev-url", "from", "to", "db-url"}

// refuseAtlasYDBURLFlags refuses a ydb:// or ydbs:// URL in a URL flag, set on
// the command line or through the flag's PTAH_* variable.
func refuseAtlasYDBURLFlags(cmd *cobra.Command) error {
	for _, name := range atlasYDBURLFlags {
		flag := cmd.Flags().Lookup(name)
		if flag == nil || !flag.Changed {
			continue
		}
		values, err := atlasFlagStringValues(cmd, name, flag.Value.Type())
		if err != nil {
			return fmt.Errorf("read --%s: %w", name, err)
		}
		if slices.ContainsFunc(values, isAtlasYDBURL) {
			return fmt.Errorf("--%s names a YDB database: %s", name, ydbgap.Compatibility.Message())
		}
	}
	return nil
}

// refuseAtlasYDBProjectURLs refuses a project env whose url, dev or schema
// source names a YDB database. It runs on every env a project file loads, as
// soon as it is parsed, so a value written there or read into it through
// getenv is refused before any verb uses it.
func refuseAtlasYDBProjectURLs(path string, cfg projectconfig.Config) error {
	attributes := []struct {
		name   string
		values []string
	}{
		{name: "url", values: []string{cfg.DatabaseURL}},
		{name: "dev", values: []string{cfg.DevURL}},
		{name: "src", values: cfg.SchemaSources},
	}
	for _, attribute := range attributes {
		if slices.ContainsFunc(attribute.values, isAtlasYDBURL) {
			return fmt.Errorf("%s env %q: %s names a YDB database: %s",
				path, cfg.EnvName, attribute.name, ydbgap.Compatibility.Message())
		}
	}
	return nil
}

// refuseAtlasYDBConnection is the refusal every command of this surface puts
// on the context it runs under; see [installAtlasConnectGate].
func refuseAtlasYDBConnection(dialect string) error {
	if platform.NormalizeDialect(dialect) == platform.YDB {
		return errors.New(ydbgap.Compatibility.Message())
	}
	return nil
}

// installAtlasConnectGate makes every command in the tree run under a context
// that refuses a YDB connection. The flag and project-file refusals answer
// first and name the source; this one covers the paths they cannot see -- a
// native flag or variable a forwarded verb reads, a data source a project file
// connects to while it is parsed -- because dbschema asks it about every
// connection, whichever path the URL took.
//
// It runs before each command's own pre-run. cobra validates arguments
// before any pre-run, and that is where the forwarding and project-flag hooks
// refresh a command's context from the root's, so the refusal set here is the
// context the command's work and a forwarded native command run under.
func installAtlasConnectGate(cmd *cobra.Command) {
	preRunE := cmd.PreRunE
	preRun := cmd.PreRun
	cmd.PreRun = nil
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		cmd.SetContext(connectgate.With(contextOrBackground(cmd), refuseAtlasYDBConnection))
		switch {
		case preRunE != nil:
			return preRunE(cmd, args)
		case preRun != nil:
			preRun(cmd, args)
		}
		return nil
	}
	for _, child := range cmd.Commands() {
		installAtlasConnectGate(child)
	}
}

// contextOrBackground is the context cmd runs under, or the background one
// before cobra has given it any.
func contextOrBackground(cmd *cobra.Command) context.Context {
	if ctx := cmd.Context(); ctx != nil {
		return ctx
	}
	return context.Background()
}

// isAtlasYDBURL reports whether value is a ydb:// or ydbs:// URL.
func isAtlasYDBURL(value string) bool {
	scheme, ok := atlasURLScheme(value)
	return ok && platform.NormalizeDialect(scheme) == platform.YDB
}

// refuseAtlasYDBForwardedURLs refuses a YDB URL a forwarded verb would hand
// its native command: in the arguments it forwards, whether an Atlas flag,
// its variable, the project file or a native flag typed on this surface put it
// there, and in a PTAH_* variable the native command binds to a flag those
// arguments leave unset. It runs before the native command does, because a
// native verb with nothing to do -- a validation of an empty directory -- never
// connects, and would accept the URL in silence.
func refuseAtlasYDBForwardedURLs(verb atlasVerb, args []string) error {
	if err := refuseAtlasYDBArgs(args); err != nil {
		return err
	}
	if verb.factory == nil {
		return nil
	}
	target, _, err := verb.factory().Find(append(slices.Clone(verb.prefixArgs), args...))
	if err != nil {
		return nil
	}
	given := make(map[string]bool)
	for _, arg := range args {
		if name, _, _ := atlasForwardedFlag(arg); name != "" {
			given[name] = true
		}
	}
	var refusal error
	visit := func(flag *pflag.Flag) {
		envName, bound := cmdflags.EnvBindingName("PTAH", flag)
		if refusal != nil || !bound || given[flag.Name] {
			return
		}
		if value, set := os.LookupEnv(envName); set && isAtlasYDBURL(value) {
			refusal = fmt.Errorf("%s names a YDB database: %s", envName, ydbgap.Compatibility.Message())
		}
	}
	// The native flags a command defines itself. No forwarded target binds a
	// URL to a persistent flag, and a connection one carried would still meet
	// the refusal installAtlasConnectGate puts on the context.
	target.Flags().VisitAll(visit)
	return refusal
}

// refuseAtlasYDBArgs refuses a YDB URL anywhere in args, a flag's value or an
// argument of its own, naming the flag it is the value of.
func refuseAtlasYDBArgs(args []string) error {
	for i, arg := range args {
		name, value, inline := atlasForwardedFlag(arg)
		switch {
		case name != "" && !inline:
			continue
		case name == "" && i > 0:
			name, _, _ = atlasForwardedFlag(args[i-1])
		}
		if isAtlasYDBURL(value) {
			return fmt.Errorf("%s names a YDB database: %s", atlasForwardedSubject(name), ydbgap.Compatibility.Message())
		}
	}
	return nil
}

// atlasForwardedFlag reads one forwarded argument: a flag's name, and its
// value when it is written inline as --name=value. An argument that is not a
// flag comes back as its own value with no name.
func atlasForwardedFlag(arg string) (name, value string, inline bool) {
	flag, found := strings.CutPrefix(arg, "--")
	if !found {
		return "", arg, false
	}
	name, value, inline = strings.Cut(flag, "=")
	return name, value, inline
}

// atlasForwardedSubject names the flag a forwarded value belongs to, or the
// argument itself when it follows no flag.
func atlasForwardedSubject(flag string) string {
	if flag == "" {
		return "an argument"
	}
	return "--" + flag
}

// refuseAtlasYDBDirectURLs refuses a YDB URL on a branch of a forwarded verb
// that runs on this surface instead -- `migrate down --format`, `migrate
// validate` over a converted directory -- and resolves its URLs from its own
// flags, their variables and the project file.
func refuseAtlasYDBDirectURLs(databaseURL, devURL string) error {
	for _, field := range []struct{ flag, value string }{{"url", databaseURL}, {"dev-url", devURL}} {
		if isAtlasYDBURL(field.value) {
			return fmt.Errorf("--%s names a YDB database: %s", field.flag, ydbgap.Compatibility.Message())
		}
	}
	return nil
}
