package featureinventory

import (
	"fmt"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"

	yaml "go.yaml.in/yaml/v3"
)

// OwnsKey is the frontmatter key by which a documentation page claims the
// features it documents.
//
// The direction is the whole design. The closed attempt asked a gate to find
// the page that documents a feature, computed identifying tokens and accepted a
// substring hit for any of them, so a page passed without documenting the
// feature it was credited with. There is no threshold that repairs that: "does
// this page explain this feature" is not machine-decidable. Asking instead
// which features a page CLAIMS needs a read, and every comparison downstream of
// it is string equality on a derived identifier.
//
// What survives is a claim, and the column is named claimed_by for that reason.
// A page may claim a feature it does not document and the gate will not know;
// what it can no longer do is have that claim raise a floor and lock itself in,
// because the floor is a constant in source rather than a number this file
// carries forward.
const OwnsKey = "owns"

// PageClaim is one documentation page and the feature identifiers it claims.
type PageClaim struct {
	// Path is the page's repository path, and it is what a row's claimed_by
	// field carries: the file the claim was read from, never a value somebody
	// typed into the inventory.
	Path string
	// Owns are the identifiers the page's frontmatter names, in file order.
	Owns []string
}

// DocumentationRoot is the site content root the pages are enumerated under.
const DocumentationRoot = "docs/site/src/content/docs"

// ReadClaims returns every tracked documentation page and what it claims.
//
// The page list comes from `git ls-files` rather than from a filesystem walk,
// for the reason docs/site/scripts/lib/docroutes.mjs already gives: the site
// publishes what the repository tracks, and a walk additionally descends into
// build output and into linked worktrees parked under this one.
func ReadClaims(repoRoot string) ([]PageClaim, error) {
	command := exec.Command("git", "-C", repoRoot, "ls-files", "-z", "--", DocumentationRoot)
	out, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("listing %s: %w", DocumentationRoot, err)
	}

	var claims []PageClaim
	for name := range strings.SplitSeq(strings.TrimRight(string(out), "\x00"), "\x00") {
		if extension := strings.ToLower(path.Ext(name)); extension != ".md" && extension != ".mdx" {
			continue
		}
		source, readErr := os.ReadFile(path.Join(repoRoot, name)) // #nosec G304 -- the path comes from git's own list of tracked files.
		if readErr != nil {
			return nil, readErr
		}
		owns, ownsErr := Owns(source)
		if ownsErr != nil {
			return nil, fmt.Errorf("%s: %w", name, ownsErr)
		}
		if len(owns) == 0 {
			continue
		}
		claims = append(claims, PageClaim{Path: name, Owns: owns})
	}
	sort.Slice(claims, func(i, j int) bool { return claims[i].Path < claims[j].Path })
	return claims, nil
}

// Owns reads one page's claimed identifiers out of its frontmatter.
//
// The frontmatter is handed to a YAML parser rather than matched with a
// pattern, so the key is read the way the site reads it and a list written in
// either YAML spelling means the same thing here as it does there.
func Owns(source []byte) ([]string, error) {
	front, ok := frontmatter(source)
	if !ok {
		return nil, nil
	}
	var page struct {
		Owns []string `yaml:"owns"`
	}
	if err := yaml.Unmarshal([]byte(front), &page); err != nil {
		return nil, fmt.Errorf("reading frontmatter: %w", err)
	}
	return page.Owns, nil
}

// frontmatter returns the page's leading YAML block, and whether it has one.
func frontmatter(source []byte) (string, bool) {
	lines := strings.Split(strings.ReplaceAll(string(source), "\r\n", "\n"), "\n")
	if len(lines) == 0 || strings.TrimSpace(lines[0]) != "---" {
		return "", false
	}
	for i := 1; i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) == "---" {
			return strings.Join(lines[1:i], "\n"), true
		}
	}
	return "", false
}

// applyClaims joins the pages' claims onto the rows and returns how many rows
// ended up claimed.
//
// Two refusals, both by string equality and both naming everything the reader
// needs: an identifier no row carries, and an identifier two pages claim.
func applyClaims(rows []Row, claims []PageClaim) (int, []Problem) {
	index := make(map[string]int, len(rows))
	for i, row := range rows {
		index[row.ID] = i
	}

	claimant := make(map[string]string, len(rows))
	var problems []Problem
	for _, claim := range claims {
		for _, id := range claim.Owns {
			position, known := index[id]
			if !known {
				problems = append(problems, Problem{RuleUnknownClaim, fmt.Sprintf(
					"%s claims %q in its %s: frontmatter, and the derivation produces no such feature",
					claim.Path, id, OwnsKey)})
				continue
			}
			if first, taken := claimant[id]; taken {
				problems = append(problems, Problem{RuleDuplicateClaim, fmt.Sprintf(
					"%s and %s both claim %q; one feature is claimed by at most one page",
					first, claim.Path, id)})
				continue
			}
			claimant[id] = claim.Path
			rows[position].ClaimedBy = &claim.Path
		}
	}

	return len(claimant), problems
}
