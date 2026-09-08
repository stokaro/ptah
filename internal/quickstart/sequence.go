package quickstart

import (
	"fmt"
	"path/filepath"
	"sort"
	"strings"
)

// Sequences orders the discovered pages into the journeys a reader follows.
//
// A page that declares no continuation, and that no page continues to, is a
// journey of one and runs exactly as it did before. A declared continuation
// joins pages into one program that runs in ONE working directory, in order,
// with the steps numbered straight through.
//
// That single directory is the whole point. Two pages can each be internally
// correct while the move between them is impossible: stokaro/ptah#2995 is the
// measured case, where the quick start deleted its working directory and then
// linked to a continuation that opened by assuming it. Running each page in its
// own fresh directory can never see that, and neither can a link checker.
//
// A page may name several continuations, because a quick start really does
// branch: the reader picks one. Each branch becomes its own journey and runs
// the shared prefix again, so every transition is measured rather than the
// first one standing in for the rest.
//
// Journeys are returned as ordinary pages, so every caller -- Run, the
// reporter, the floors -- keeps working. What a joined page adds is that each
// step remembers which page published it, so a failure names the page a reader
// would be on rather than the head of the journey.
func Sequences(pages []*Page) ([]*Page, error) {
	byPath := make(map[string]*Page, len(pages))
	for _, page := range pages {
		byPath[key(page)] = page
	}

	next, err := resolveContinuations(pages, byPath)
	if err != nil {
		return nil, err
	}
	continued := make(map[string]bool)
	for _, targets := range next {
		for _, target := range targets {
			continued[key(target)] = true
		}
	}

	var out []*Page
	for _, page := range pages {
		if continued[key(page)] {
			// Reached from its predecessor rather than started from.
			continue
		}
		for _, chain := range walk(page, next) {
			out = append(out, join(chain))
		}
	}
	if err := everyPageIsReached(pages, out); err != nil {
		return nil, err
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Path < out[j].Path })
	return out, nil
}

// everyPageIsReached refuses a page that no journey runs.
//
// The walk below refuses a cycle it can reach, and a cycle with no page outside
// it has no head, so nothing reaches it at all: both pages are marked as
// continued-to, neither starts a journey, and they leave the run without a
// word. That is the shape this whole gate exists to refuse -- a page nobody
// executes reads exactly like a page that passed.
func everyPageIsReached(pages, journeys []*Page) error {
	reached := make(map[string]bool, len(pages))
	for _, journey := range journeys {
		for _, shell := range journey.ShellsPresent() {
			program, ok := journey.Program(shell)
			if !ok {
				continue
			}
			for _, action := range program.Actions {
				reached[action.Page] = true
			}
		}
		reached[journey.Path] = true
	}
	var lost []string
	for _, page := range pages {
		if !reached[page.Path] {
			lost = append(lost, page.Path)
		}
	}
	if len(lost) == 0 {
		return nil
	}
	sort.Strings(lost)
	return fmt.Errorf(
		"%s: no journey reaches these pages, so nothing would run them;"+
			" %s leads back on itself", strings.Join(lost, ", "), continuesKey)
}

// key is the map key a page is found under, so a Windows path and a documented
// one name the same page.
func key(page *Page) string { return filepath.ToSlash(page.Path) }

// resolveContinuations turns every declared continuation into the page it
// names, refusing a declaration nothing can act on.
//
// A target that is not opted in is refused rather than skipped: a page saying
// where a reader goes next, pointing at a page this runner cannot execute, is
// a journey nobody measures, and dropping it silently is the gap this gate
// exists to close.
func resolveContinuations(pages []*Page, byPath map[string]*Page) (map[string][]*Page, error) {
	next := make(map[string][]*Page)
	claimedBy := make(map[string]string)
	for _, page := range pages {
		for _, declared := range splitContinuations(page.Continues) {
			// Resolved against the declaring page's own directory, the way the
			// link a reader clicks is written: a sibling is named by its file
			// name and a page in another section by a relative path.
			resolved := filepath.ToSlash(filepath.Join(filepath.Dir(page.Path), declared))
			target, ok := byPath[resolved]
			if !ok {
				return nil, fmt.Errorf(
					"%s: %s names %q, which is not a page this runner reads:"+
						" give the documentation-root-relative path of a page carrying %s: true",
					page.Path, continuesKey, declared, optInKey)
			}
			if target == page {
				return nil, fmt.Errorf("%s: %s names the page itself", page.Path, continuesKey)
			}
			if first, taken := claimedBy[key(target)]; taken {
				return nil, fmt.Errorf(
					"%s and %s both continue to %s; a page has one predecessor",
					first, page.Path, declared)
			}
			claimedBy[key(target)] = page.Path
			next[key(page)] = append(next[key(page)], target)
		}
	}
	return next, nil
}

// splitContinuations reads the one-line list a page writes, and returns nothing
// for a page that declares none.
func splitContinuations(declared string) []string {
	var out []string
	for field := range strings.SplitSeq(declared, ",") {
		if trimmed := strings.TrimSpace(field); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

// walk returns every path from one head to a page that continues nowhere.
//
// The recursion terminates without a cycle guard, and the one-predecessor rule
// above is why: entering a cycle needs an edge into it from outside, and the
// cycle's own edge already claims that page. So a cycle is always a component
// with no head, which this is never called on -- and a guard here could not
// fire. everyPageIsReached is what reports such a component, by noticing that
// nothing runs its pages.
func walk(head *Page, next map[string][]*Page) [][]*Page {
	following := next[key(head)]
	if len(following) == 0 {
		return [][]*Page{{head}}
	}
	var chains [][]*Page
	for _, target := range following {
		for _, tail := range walk(target, next) {
			chains = append(chains, append([]*Page{head}, tail...))
		}
	}
	return chains
}

// join folds a chain into one page per shell, renumbering the steps so the
// sentinels stay unique across the whole journey.
//
// A chain of one is returned unchanged, so a page that declares nothing is the
// same object it was, running the same script it ran.
func join(chain []*Page) *Page {
	if len(chain) == 1 {
		return chain[0]
	}
	head := chain[0]
	joined := &Page{Path: SequenceName(chain), Title: head.Title, Programs: make(map[Shell]*Program)}
	for _, shell := range Shells() {
		program := &Program{Shell: shell}
		number := 0
		carries := false
		for _, page := range chain {
			part, ok := page.Program(shell)
			if !ok {
				continue
			}
			carries = true
			for _, action := range part.Actions {
				action.Page = page.Path
				if action.Kind == ActionStep {
					number++
					action.Number = number
				}
				program.Actions = append(program.Actions, action)
			}
		}
		if carries {
			joined.Programs[shell] = program
		}
	}
	return joined
}

// SequenceName renders a journey for a report: the pages in order, joined by
// the arrow a reader follows.
func SequenceName(chain []*Page) string {
	names := make([]string, 0, len(chain))
	for _, page := range chain {
		names = append(names, page.Path)
	}
	return strings.Join(names, " -> ")
}
