// Package cmdref turns the cobra trees the two Ptah binaries ship into the
// generated half of the command reference.
//
// It measures a `*cobra.Command` and nothing else. It opens no Markdown, reads
// no Go source, parses no invocation string and starts no process: every fact
// it prints came from a command or a flag in this process, through the one
// traversal in internal/agentsurface. That is the whole difference between a
// reference and a document about a reference, and it is why the pages it feeds
// cannot name a verb the binary does not have or omit one it does.
//
// # Hidden commands and hidden flags are rendered, and marked
//
// Both appear in the output, each with a column saying that `--help` does not
// list it. Hiding is not privacy here, it is presentation, and three
// measurements say so. Under PTAH_ATLAS_STRICT_COMPAT=1 hiding is the entire
// mechanism by which 22 command paths are refused, so a reference that skipped
// hidden commands would print an empty strict-mode table. Six hidden native
// flags -- `--atlas-project-config` and `--atlas-project-var` on
// `migrations down`, `migrations lint` and `migrations status` -- carry
// `PTAH_ATLAS_PROJECT_CONFIG` and `PTAH_ATLAS_PROJECT_VAR` bindings, and a flag
// an operator can set from the environment is not private under any reading.
// The two hidden compatibility flags, `migrate diff --dry-run` and
// `schema apply --file`, are accepted Atlas spellings that parse and act.
//
// The failure a reference must not have is telling a reader that something the
// binary accepts does not exist. Omitting these would be that failure, spelled
// eight times.
package cmdref

import (
	"fmt"
	"strings"

	"ptah.run/internal/agentsurface"
)

// Availability says what PTAH_ATLAS_STRICT_COMPAT=1 does to one path of the
// Atlas-compatible tree.
//
// The four answers are distinguishable in the tree, which is why they are four
// rather than one "unavailable": a command still registered but hidden is
// refused by its own gate, and a command no longer registered is answered by
// whichever ancestor cobra reaches instead.
type Availability string

const (
	// Available is a path strict mode leaves registered and listed.
	Available Availability = "available"
	// Refused is a path strict mode keeps registered and hides, so its own
	// gate answers and names it.
	Refused Availability = "refused"
	// RefusedByGroup is a path below a refused group. The group's gate answers
	// and names THE GROUP, never the path that was invoked.
	RefusedByGroup Availability = "refused by its group"
	// AbsorbedByGroup is a path strict mode unregisters below a group cobra
	// will run. The group runs in its place and prints its own help.
	AbsorbedByGroup Availability = "absorbed by the group"
	// UnknownCommand is a path whose nearest registered ancestor cannot run,
	// so cobra reports the first segment it could not resolve.
	UnknownCommand Availability = "unknown command"
)

// Exit is the process status a bare invocation of such a path answers with.
//
// It is a property of the class rather than a value read out of the tree, and
// it is asserted against the built binary by
// TestStrictCompatProcessAnswersEachRemovedClass in
// integration/atlascmd/strict_compat_process_test.go. AbsorbedByGroup is the
// one that matters: it exits 0, so a caller testing only the status cannot
// tell it from the verb having run.
func (a Availability) Exit() int {
	if a == Available || a == AbsorbedByGroup {
		return 0
	}
	return 1
}

// Stream is where that answer is written, and it follows Exit for the same
// measured reason: the group whose help runs in a verb's place writes to
// standard output and writes nothing to standard error.
func (a Availability) Stream() string {
	if a == Available || a == AbsorbedByGroup {
		return "stdout"
	}
	return "stderr"
}

// Path is one command of the Atlas-compatible tree under strict mode.
type Path struct {
	// Name is the command line spelling without the program name.
	Name string
	// Summary is the command's own one-line description in the FULL tree,
	// because a gated command carries none in the strict one.
	Summary string
	// Availability is what strict mode does to this path.
	Availability Availability
	// Answers is the path the answer names, which is not always Name: a
	// refused group names itself for every child, and an unresolvable segment
	// is reported alone. A script grepping the diagnostic for the verb it
	// invoked finds nothing in three of the four classes.
	Answers string
}

// Classify joins the full and strict compatibility trees into one verdict per
// path of the full tree.
//
// The join is by command path rather than by pointer because the two trees are
// separately constructed, and by the full tree's paths because a path strict
// mode adds would be a policy that grants rather than removes -- which the
// caller refuses rather than renders, since nothing in this repository has ever
// produced one.
func Classify(full, strict []agentsurface.Node) []Path {
	registered := make(map[string]agentsurface.Node, len(strict))
	for _, node := range strict {
		registered[node.Name] = node
	}

	paths := make([]Path, 0, len(full))
	for _, node := range full {
		path := Path{Name: node.Name, Summary: node.Summary, Answers: node.Name}
		switch present, found := registered[node.Name]; {
		case found && !present.Hidden:
			path.Availability = Available
		case found:
			path.Availability = Refused
		default:
			ancestor, unresolved := nearest(node.Name, registered)
			path.Answers = ancestor.Name
			switch {
			case ancestor.Hidden:
				path.Availability = RefusedByGroup
			case ancestor.Runnable:
				path.Availability = AbsorbedByGroup
			default:
				path.Availability = UnknownCommand
				path.Answers = unresolved
			}
		}
		paths = append(paths, path)
	}
	return paths
}

// nearest walks up from a path cobra cannot resolve to the deepest ancestor
// that is registered, and returns with it the shortest prefix that is not.
//
// The zero Node stands for the root, which is registered by existing and is
// not runnable -- that non-runnability is what puts `script` in the
// unknown-command class rather than the absorbed one.
func nearest(name string, registered map[string]agentsurface.Node) (agentsurface.Node, string) {
	segments := strings.Split(name, " ")
	for depth := len(segments) - 1; depth > 0; depth-- {
		if node, found := registered[strings.Join(segments[:depth], " ")]; found {
			return node, strings.Join(segments[:depth+1], " ")
		}
	}
	return agentsurface.Node{}, segments[0]
}

// Removed lists the paths strict mode takes away, in the full tree's order.
func Removed(paths []Path) []Path {
	removed := make([]Path, 0, len(paths))
	for _, path := range paths {
		if path.Availability != Available {
			removed = append(removed, path)
		}
	}
	return removed
}

// errEmpty is the refusal every renderer shares: a walk that reached nothing
// must stop here rather than print an empty table for a gate to compare
// against another empty table.
func errEmpty(what string) error {
	return fmt.Errorf("cmdref: %s is empty; refusing to render a reference that names nothing", what)
}
