// Command quickstart runs the commands the quick-start pages publish and
// checks them against the output those pages show.
//
// It holds no copy of the commands. Every step it runs was read out of a page
// that opts in with `quickstart: true`, so a page edited without running its
// own commands turns this red, and a step deleted from a page stops being
// covered on the same pull request that deleted it.
//
//	quickstart list
//	quickstart run
//	quickstart run --shell powershell
//	quickstart run --docs-dir some/tree --ptah bin/ptah
package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/internal/exeext"
	"go.5x5.cz/ptah/internal/quickstart"
)

// defaultDocsDir is where the site's pages live. The runner reads the published
// pages, not a fixture, unless a caller points it somewhere else.
const defaultDocsDir = "docs/site/src/content/docs"

func main() {
	if err := newCommand().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "quickstart: %v\n", err)
		os.Exit(1)
	}
}

type options struct {
	docsDir string
	shell   string
	ptah    string
	keep    bool
}

func newCommand() *cobra.Command {
	root := &cobra.Command{
		Use:           "quickstart",
		Short:         "Run the commands the quick-start pages publish",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	root.AddCommand(newListCommand(), newScriptCommand(), newRunCommand())
	return root
}

func (o *options) bind(cmd *cobra.Command) {
	cmd.Flags().StringVar(&o.docsDir, "docs-dir", defaultDocsDir, "Documentation tree to read the pages from")
	cmd.Flags().StringVar(&o.shell, "shell", "auto",
		"Shell whose steps to read: bash, powershell, or auto for the one this operating system runs")
}

func (o *options) resolveShell() (quickstart.Shell, error) {
	switch strings.ToLower(o.shell) {
	case "auto":
		if runtime.GOOS == "windows" {
			return quickstart.PowerShell, nil
		}
		return quickstart.Bash, nil
	case string(quickstart.Bash):
		return quickstart.Bash, nil
	case string(quickstart.PowerShell):
		return quickstart.PowerShell, nil
	default:
		return "", fmt.Errorf("unknown --shell %q; use bash, powershell, or auto", o.shell)
	}
}

func newListCommand() *cobra.Command {
	options := &options{}
	cmd := &cobra.Command{
		Use:   "list",
		Short: "Print the pages, steps, and assertions a run would cover",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			shell, err := options.resolveShell()
			if err != nil {
				return err
			}
			pages, err := quickstart.Discover(options.docsDir)
			if err != nil {
				return err
			}
			for _, page := range pages {
				program, ok := page.Program(shell)
				if !ok {
					fmt.Fprintf(cmd.OutOrStdout(), "%s: no %s steps\n", page.Path, shell)
					continue
				}
				fmt.Fprintf(cmd.OutOrStdout(), "%s: %d %s step(s), %d assertion(s)\n",
					page.Path, program.Steps(), shell, program.Expectations())
			}
			return quickstart.CheckFloors(pages, shell, quickstart.DefaultFloors())
		},
	}
	options.bind(cmd)
	return cmd
}

// newScriptCommand prints what a run would execute.
//
// A red Windows leg is read by someone who cannot reproduce it on the machine
// in front of them, so the generated script has to be readable without one.
func newScriptCommand() *cobra.Command {
	options := &options{}
	cmd := &cobra.Command{
		Use:   "script",
		Short: "Print the script a run would execute, without running it",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			shell, err := options.resolveShell()
			if err != nil {
				return err
			}
			pages, err := quickstart.Discover(options.docsDir)
			if err != nil {
				return err
			}
			for _, page := range pages {
				program, ok := page.Program(shell)
				if !ok {
					continue
				}
				script, renderErr := quickstart.RenderScript(program)
				if renderErr != nil {
					return renderErr
				}
				fmt.Fprintf(cmd.OutOrStdout(), "# %s (%s)\n%s\n", page.Path, shell, script)
			}
			return quickstart.CheckFloors(pages, shell, quickstart.DefaultFloors())
		},
	}
	options.bind(cmd)
	return cmd
}

func newRunCommand() *cobra.Command {
	options := &options{}
	cmd := &cobra.Command{
		Use:   "run",
		Short: "Run every published quick start and check its output blocks",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return run(cmd.Context(), cmd, options)
		},
	}
	options.bind(cmd)
	cmd.Flags().StringVar(&options.ptah, "ptah", "",
		"Path to a ptah binary to put on PATH; empty builds ./cmd/ptah from this tree")
	cmd.Flags().BoolVar(&options.keep, "keep", false, "Leave each throwaway working directory behind")
	return cmd
}

func run(ctx context.Context, cmd *cobra.Command, options *options) error {
	shell, err := options.resolveShell()
	if err != nil {
		return err
	}

	pages, err := quickstart.Discover(options.docsDir)
	if err != nil {
		return err
	}
	if err := quickstart.CheckFloors(pages, shell, quickstart.DefaultFloors()); err != nil {
		return err
	}

	ptahDir, cleanup, err := resolvePtah(ctx, options.ptah)
	if err != nil {
		return err
	}
	defer cleanup()

	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "quickstart: %d page(s), %s steps, ptah from %s\n", len(pages), shell, ptahDir)

	failed := 0
	for _, page := range pages {
		result, runErr := quickstart.Run(ctx, page, shell, quickstart.Options{PtahDir: ptahDir, Keep: options.keep})
		if runErr != nil {
			return runErr
		}
		fmt.Fprint(out, quickstart.FormatResult(result))
		if !result.OK() {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%d of %d published quick start(s) did not run as the page says", failed, len(pages))
	}
	fmt.Fprintf(out, "quickstart: OK (%d page(s) ran exactly as published)\n", len(pages))
	return nil
}

// resolvePtah returns the directory to put in front of PATH, so the `ptah` the
// pages spell is this tree's binary.
//
// Building it here rather than in the workflow keeps the .exe suffix in one
// place -- internal/exeext -- instead of in a shell conditional per job.
func resolvePtah(ctx context.Context, given string) (dir string, cleanup func(), err error) {
	if given != "" {
		absolute, absErr := filepath.Abs(given)
		if absErr != nil {
			return "", nil, absErr
		}
		if _, statErr := os.Stat(absolute); statErr != nil {
			return "", nil, fmt.Errorf("--ptah %s: %w", given, statErr)
		}
		return filepath.Dir(absolute), func() {}, nil
	}

	tempDir, err := os.MkdirTemp("", "ptah-quickstart-bin-")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { _ = os.RemoveAll(tempDir) }

	target := filepath.Join(tempDir, "ptah"+exeext.Suffix)
	build := exec.CommandContext(ctx, "go", "build", "-o", target, "./cmd/ptah")
	build.Stdout, build.Stderr = os.Stderr, os.Stderr
	if buildErr := build.Run(); buildErr != nil {
		cleanup()
		return "", nil, fmt.Errorf("building ./cmd/ptah: %w", buildErr)
	}
	return tempDir, cleanup, nil
}
