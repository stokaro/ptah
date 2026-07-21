package main

import (
	"context"
	"flag"
	"fmt"
	"os"

	"github.com/stokaro/ptah/cmd/internal/buildinfo"
	"github.com/stokaro/ptah/internal/ptahls"
)

func main() {
	flags := flag.NewFlagSet("ptah-ls", flag.ExitOnError)
	showVersion := flags.Bool("version", false, "print version information")
	flags.SetOutput(os.Stderr)
	flags.Parse(os.Args[1:])

	info := buildinfo.Resolve()
	if *showVersion {
		fmt.Fprintf(os.Stdout, "Version: %s\n", info.Version)
		fmt.Fprintf(os.Stdout, "Commit: %s\n", info.Commit)
		fmt.Fprintf(os.Stdout, "Date: %s\n", info.Date)
		fmt.Fprintf(os.Stdout, "Go: %s\n", info.Go)
		fmt.Fprintf(os.Stdout, "Platform: %s\n", info.Platform)
		return
	}

	opts := ptahls.ServerOptions{Version: info.Version}
	if err := ptahls.RunWithOptions(context.Background(), os.Stdin, os.Stdout, opts); err != nil {
		fmt.Fprintf(os.Stderr, "ptah-ls: %v\n", err)
		os.Exit(1)
	}
}
