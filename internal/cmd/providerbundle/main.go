// Command providerbundle stages the pinned official providers for a release
// build. GoReleaser runs it as a before hook, and the archives, the container
// image and the Homebrew cask then pick the staged files up, so every channel
// ships the executable the pin in this Ptah release names.
//
// Usage:
//
//	go run ./internal/cmd/providerbundle --out dist-providers
//	go run ./internal/cmd/providerbundle --out dist-providers --base-url https://mirror.example/ydb/v0.1.1
//
// The layout written is <out>/<os>_<arch>/providers/<provider-id>/<version>/<executable>.
// Each archive is checked against the pinned archive digest and the executable
// against the pinned executable digest; a mismatch, a missing platform or an
// unreachable archive stops the build. The command resolves no latest version.
//
// With --verify-archives <dir> it instead opens the host archive of every
// platform in dir (the release archives GoReleaser wrote) and fails unless each
// holds the pinned executable at the bundled path with the pinned digest.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/spf13/pflag"

	"ptah.run/internal/providerbundle"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "providerbundle:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout *os.File) error {
	flags := pflag.NewFlagSet("providerbundle", pflag.ContinueOnError)
	out := flags.String("out", "", "Staging root; one directory per platform is created below it")
	verify := flags.String("verify-archives", "", "Check that the host archive of every platform in this directory contains the pinned provider at the bundled path, then exit")
	baseURL := flags.String("base-url", "", "Directory to fetch the pinned archives from instead of the official release; digests still decide")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *verify != "" {
		verified, err := providerbundle.VerifyArchives(*verify, nil, nil)
		if err != nil {
			return err
		}
		for _, archive := range verified {
			fmt.Fprintln(stdout, "verified", archive)
		}
		return nil
	}
	if *out == "" {
		return errors.New("--out is required")
	}
	staged, err := providerbundle.Stage(ctx, providerbundle.Options{Out: *out, BaseURL: *baseURL})
	if err != nil {
		return err
	}
	for _, item := range staged {
		fmt.Fprintf(stdout, "%s %s %s %s\n", item.EngineID, item.Version, item.Platform, item.SHA256)
	}
	return nil
}
