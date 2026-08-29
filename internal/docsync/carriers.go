package docsync

import (
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

// Carriers reports, for each marker, the tracked documents that contain it.
//
// A block copied into a second page goes stale with nothing looking at it: the
// gate reads the page it was told about and the copy drifts. So a marker is
// required to appear in exactly the file its target names.
//
// The marker is matched ANCHORED TO ITS OWN LINE, not as a substring. A page
// that DOCUMENTS a marker -- docs/site/CONTENT_INVENTORY.md says where each
// generated block comes from -- is not a second carrier of the block, and a
// check that could not tell the two apart would force the documentation to
// misspell the thing it documents.
//
// git rather than a filesystem walk, for the reason scripts/check-test-style.sh
// already documents: a walk descends into linked worktrees parked under this
// one and reports files belonging to a different checkout.
func Carriers(root, marker string) ([]string, error) {
	cmd := exec.Command("git", "-C", root, "grep", "-l", "-E", "-e", "^"+regexp.QuoteMeta(marker)+"$", "--", "*.md", "*.mdx")
	out, err := cmd.Output()
	if err != nil {
		// git grep exits 1 for "no match", which is a legitimate answer here
		// and becomes the "carries no markers" refusal one level up.
		var exit *exec.ExitError
		if errors.As(err, &exit) && exit.ExitCode() == 1 {
			return nil, nil
		}
		return nil, fmt.Errorf("git grep for %s: %w", marker, err)
	}
	var files []string
	for line := range strings.SplitSeq(strings.TrimSpace(string(out)), "\n") {
		if line != "" {
			files = append(files, line)
		}
	}
	sort.Strings(files)
	return files, nil
}

// CheckCarriers reports every marker that appears somewhere the targets do not
// claim.
func CheckCarriers(root string, targets []Target) ([]string, error) {
	claimed := make(map[string][]string)
	for _, target := range targets {
		if target.WholeFile() {
			continue
		}
		claimed[target.Begin] = append(claimed[target.Begin], target.Path)
	}
	var problems []string
	for marker, paths := range claimed {
		carriers, err := Carriers(root, marker)
		if err != nil {
			return nil, err
		}
		sort.Strings(paths)
		if strings.Join(carriers, ",") != strings.Join(unique(paths), ",") {
			problems = append(problems, fmt.Sprintf(
				"%s is carried by %v; the targets claim %v",
				marker, carriers, unique(paths)))
		}
	}
	sort.Strings(problems)
	return problems, nil
}

func unique(values []string) []string {
	seen := make(map[string]bool)
	var out []string
	for _, v := range values {
		if !seen[v] {
			seen[v] = true
			out = append(out, v)
		}
	}
	sort.Strings(out)
	return out
}
