package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"ptah.run/cmd/internal/banner"
	"ptah.run/internal/buildinfo"
	"ptah.run/internal/ptahls"
)

// usageExitCode is the status ptah-ls exits with for any usage error. It is
// the code flag.ExitOnError already uses for an unknown flag, and it matches
// the native ptah binary's exit code for an unknown command, so this binary
// has exactly one usage-error code rather than one per rejection site.
const usageExitCode = 2

// versionCommand is the positional spelling of a version query. Accepting it
// here is what makes `version` the one spelling that answers on all three
// Ptah binaries (ptah, ptah-compat, ptah-ls); see stokaro/ptah#1064.
const versionCommand = "version"

func main() {
	flags := flag.NewFlagSet("ptah-ls", flag.ExitOnError)
	showVersion := flags.Bool("version", false, "print version information")
	flags.SetOutput(os.Stderr)
	// flag.ExitOnError: an unrecognized flag prints usage and exits
	// usageExitCode from inside Parse.
	flags.Parse(os.Args[1:])

	// Leftover positionals used to be dropped on the floor, which turned
	// `ptah-ls version` into an unannounced launch of the stdio language
	// server: exit 0 with zero bytes written when stdin was already at EOF,
	// and a process that never returns when it was not. Every positional is
	// now either the version query or a usage error.
	args := flags.Args()
	versionRequested := *showVersion
	if len(args) == 1 && args[0] == versionCommand {
		versionRequested = true
		args = nil
	}
	if len(args) > 0 {
		// Name the argument that is actually wrong. Reporting args[0] tells a
		// reader `ptah-ls version extra` has an unknown argument "version" --
		// the one word that IS supported -- and sends them looking in the wrong
		// place. When the first positional is the version query, the surplus
		// starts after it.
		surplus := args
		if args[0] == versionCommand {
			surplus = args[1:]
		}
		fmt.Fprintf(os.Stderr, "ptah-ls: unexpected positional arguments %q\n", surplus)
		flags.Usage()
		os.Exit(usageExitCode)
	}

	info := buildinfo.Resolve()
	if versionRequested {
		buildinfo.Write(os.Stdout, info)
		return
	}

	// The banner goes to stderr, never to stdout, and this is the one binary
	// where that is structural rather than a preference: stdout IS the
	// language-server protocol stream, and a client reading a framed message
	// off it would be handed ASCII art. Stderr is where this binary already
	// writes everything a person reads -- flags.SetOutput above, and every
	// diagnostic below -- so it is also the consistent choice.
	//
	// A client pipes stderr into its log, so the writer gate keeps it out of
	// there too; what is left is a person who started the server by hand and
	// is now looking at a process that appears to do nothing.
	banner.Print(os.Stderr, "ptah-ls", info.Version)

	opts := ptahls.ServerOptions{Version: info.Version}
	if err := ptahls.RunWithOptions(context.Background(), os.Stdin, os.Stdout, opts); err != nil {
		fmt.Fprintf(os.Stderr, "ptah-ls: %v\n", err)
		os.Exit(1)
	}
}
