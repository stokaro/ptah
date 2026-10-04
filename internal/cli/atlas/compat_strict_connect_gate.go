package atlas

import (
	"context"

	"github.com/spf13/cobra"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/connectgate"
)

// installAtlasStrictConnectGate makes every command in a strict tree run
// under a context that refuses a connection to a dialect the pinned community
// binary has no driver for, such as YDB.
//
// The strict URL checks answer first and in the binary's words: the URL
// flags, the project file's url, dev and src. They cannot see every path a URL
// takes -- a native flag or variable a forwarded verb reads, or a data source
// the project file connects to while it is parsed -- and dbschema asks this
// refusal about every connection, whichever path the URL took. The default
// policy installs nothing, because it keeps every dialect Ptah has.
//
// It runs before each command's own pre-run. cobra validates arguments before
// any pre-run, and that is where the forwarding and project-flag hooks refresh
// a command's context from the root's, so the refusal set here is the context
// the command's work and a forwarded native command run under.
func installAtlasStrictConnectGate(cmd *cobra.Command, policy atlascompatpolicy.Policy) {
	if !policy.IsStrictCE() {
		return
	}
	preRunE := cmd.PreRunE
	preRun := cmd.PreRun
	cmd.PreRun = nil
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		cmd.SetContext(connectgate.With(contextOrBackground(cmd), policy.ValidateDialect))
		switch {
		case preRunE != nil:
			return preRunE(cmd, args)
		case preRun != nil:
			preRun(cmd, args)
		}
		return nil
	}
	for _, child := range cmd.Commands() {
		installAtlasStrictConnectGate(child, policy)
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
