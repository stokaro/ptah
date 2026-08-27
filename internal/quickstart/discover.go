package quickstart

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// Floors are the counts below which the runner refuses to report success.
//
// They exist for the reason check-documented-install.sh states about its own:
// a run that discovered nothing reports exactly what a run that checked
// everything reports. A page renamed out of the tree, a frontmatter key
// dropped, or an extractor that stopped matching would all be silent without
// them.
type Floors struct {
	// Pages is the smallest number of opted-in pages the tree may hold.
	Pages int
	// Steps is the smallest number of commands one page must publish for the
	// shell being run.
	Steps int
	// Expectations is the smallest number of output blocks one page must
	// assert for that shell.
	Expectations int
}

// DefaultFloors are the counts the documented quick starts clear with margin.
func DefaultFloors() Floors { return Floors{Pages: 2, Steps: 6, Expectations: 4} }

// Discover reads every documentation page under root and returns the ones that
// opt in, sorted by path.
func Discover(root string) ([]*Page, error) {
	paths, err := documentationFiles(root)
	if err != nil {
		return nil, err
	}

	var pages []*Page
	for _, path := range paths {
		source, readErr := os.ReadFile(path) // #nosec G304 -- the path comes from walking the caller's own documentation root.
		if readErr != nil {
			return nil, readErr
		}
		page, extractErr := Extract(path, source)
		if extractErr != nil {
			return nil, extractErr
		}
		if page != nil {
			pages = append(pages, page)
		}
	}
	sort.Slice(pages, func(i, j int) bool { return pages[i].Path < pages[j].Path })
	return pages, nil
}

// documentationFiles collects the pages to read, and reads none of them.
//
// Collecting first and opening afterwards keeps every filesystem operation out
// of the walk callback, where a path can change under the walker between the
// entry being reported and the file being opened.
func documentationFiles(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		extension := strings.ToLower(filepath.Ext(path))
		if extension != ".md" && extension != ".mdx" {
			return nil
		}
		// ToSlash, because this string is the page's IDENTITY, not a handle:
		// pages are ordered by it and every diagnostic prints it. Left native,
		// `\` sorts before `/` and the discovery order, the floors' messages
		// and the runner's output all differ between Windows and everywhere
		// else. Windows accepts a forward slash in a file path, so opening it
		// afterwards is unaffected.
		paths = append(paths, filepath.ToSlash(path))
		return nil
	})
	return paths, err
}

// CheckFloors fails when the discovery found less than the runner promises to
// check, and says which count came up short.
func CheckFloors(pages []*Page, shell Shell, floors Floors) error {
	var problems []string
	if len(pages) < floors.Pages {
		problems = append(problems, fmt.Sprintf(
			"found %d page(s) with %s: true in their frontmatter, expected at least %d",
			len(pages), optInKey, floors.Pages))
	}
	for _, page := range pages {
		program, ok := page.Program(shell)
		if !ok {
			problems = append(problems, fmt.Sprintf(
				"%s publishes no %s steps; it carries %s",
				page.Path, shell, describeShells(page.ShellsPresent())))
			continue
		}
		if steps := program.Steps(); steps < floors.Steps {
			problems = append(problems, fmt.Sprintf(
				"%s publishes %d %s step(s), expected at least %d",
				page.Path, steps, shell, floors.Steps))
		}
		if asserted := program.Expectations(); asserted < floors.Expectations {
			problems = append(problems, fmt.Sprintf(
				"%s asserts %d %s output block(s), expected at least %d",
				page.Path, asserted, shell, floors.Expectations))
		}
	}
	if len(problems) == 0 {
		return nil
	}
	return errors.New("quickstart: " + strings.Join(problems, "\nquickstart: "))
}

func describeShells(shells []Shell) string {
	if len(shells) == 0 {
		return "no steps at all"
	}
	names := make([]string, 0, len(shells))
	for _, shell := range shells {
		names = append(names, string(shell))
	}
	return "steps for " + strings.Join(names, " and ")
}
