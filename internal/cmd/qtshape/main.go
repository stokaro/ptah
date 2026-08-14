// Package main reads a list of Go test files on standard input and reports every
// violation of Ptah's quicktest shape rules.
//
// The file list is supplied rather than discovered. scripts/check-quicktest-shape.sh
// builds it with git, for the reason recorded in scripts/lib/select-test-files.sh:
// a filesystem walk descends into the linked git worktrees parked under this
// repository and reports other checkouts' tests as this one's.
package main

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"go.5x5.cz/ptah/internal/qtshape"
)

const (
	exitOK       = 0
	exitFindings = 1
	exitBroken   = 2
)

func main() {
	os.Exit(run(os.Stdin, os.Stdout, os.Stderr))
}

// run is separated from main so the exit codes themselves are testable; a gate
// whose failure exit code was never executed is a gate nobody has seen fail.
func run(stdin io.Reader, stdout, stderr io.Writer) int {
	paths, err := readPaths(stdin)
	if err != nil {
		fmt.Fprintf(stderr, "qtshape: reading file list: %s\n", err)
		return exitBroken
	}

	// An empty selection, an unreadable file and a parse error all land here and
	// all exit 2. None of them is a clean tree, and none of them may be reported
	// as one.
	findings, scanned, err := qtshape.ScanFiles(paths)
	if err != nil {
		fmt.Fprintf(stderr, "qtshape: %s\n", err)
		return exitBroken
	}

	// Printed on every run, pass or fail. The count is the only thing that
	// distinguishes "clean" from "looked at almost nothing and called it clean".
	fmt.Fprintf(stderr, "qtshape: scanned %d test files\n", scanned)

	for _, f := range findings {
		fmt.Fprintln(stdout, f.String())
	}

	if len(findings) > 0 {
		fmt.Fprintf(stderr, "qtshape: %d violations in %d files; see AGENTS.md, \"Assert Through A *qt.C\" and \"Subtests Use t.Run With A Fresh qt.New\"\n",
			len(findings), countFiles(findings))
		return exitFindings
	}

	return exitOK
}

// readPaths reads one path per line, ignoring blank lines.
func readPaths(stdin io.Reader) ([]string, error) {
	var paths []string
	scanner := bufio.NewScanner(stdin)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}
		paths = append(paths, line)
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	return paths, nil
}

// countFiles counts distinct paths among the findings.
func countFiles(findings []qtshape.Finding) int {
	seen := make(map[string]struct{}, len(findings))
	for _, f := range findings {
		seen[f.Path] = struct{}{}
	}
	return len(seen)
}
