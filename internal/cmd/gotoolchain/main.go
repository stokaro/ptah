// Command gotoolchain checks that the Go toolchain is declared once, in the
// root go.mod, and derived everywhere else.
//
// The rules are internal/gotoolchain, where they are tested against fixtures
// rather than only against this repository (stokaro/ptah#2511).
package main

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"go.5x5.cz/ptah/internal/gotoolchain"
)

func main() {
	root, err := repositoryRoot()
	if err != nil {
		fmt.Fprintln(os.Stderr, "go toolchain check:", err)
		os.Exit(1)
	}
	report, err := gotoolchain.Run(root)
	if err != nil {
		fmt.Fprintln(os.Stderr, "go toolchain check:", err)
		os.Exit(1)
	}
	for _, finding := range report.Findings {
		// The GitHub annotation form, so the message lands on the offending
		// line in the pull request diff rather than only in the job log.
		fmt.Fprintln(os.Stderr, finding)
	}
	if !report.OK() {
		os.Exit(1)
	}
	fmt.Printf(
		"go toolchain check: go.mod declares toolchain %s; scanned %d setup-go steps, all deriving (%d of which forward a composite action input)\n",
		report.Toolchain, report.Steps, report.Forwarding)
}

func repositoryRoot() (string, error) {
	out, err := exec.Command("git", "rev-parse", "--show-toplevel").Output()
	if err != nil {
		return "", fmt.Errorf("git rev-parse --show-toplevel: %w", err)
	}
	return strings.TrimSpace(string(out)), nil
}
