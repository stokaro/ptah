package featureinventory

import (
	"fmt"
	"strings"
)

// The generated command reference, and the markers bounding it.
//
// It is a file of its own rather than a section of docs/feature-inventory.md,
// because the two are maintained in opposite ways: the inventory is written by
// a person and checked against the trees, and this is rendered from the trees
// and checked against itself. Keeping a generated block inside a hand-written
// register invites a `--write` run to reformat somebody's prose.
const (
	CommandReferencePath = "docs/command-reference.md"
	ReferenceBegin       = "<!-- BEGIN GENERATED COMMAND REFERENCE -->"
	ReferenceEnd         = "<!-- END GENERATED COMMAND REFERENCE -->"
)

// StrictPresence says what PTAH_ATLAS_STRICT_COMPAT=1 does to one compat path.
type StrictPresence string

const (
	// StrictNotApplicable is a native path. The selector is a ptah-compat
	// contract and does not reach the native tree.
	StrictNotApplicable StrictPresence = "not applicable"
	// StrictRegistered is a path the strict profile still offers.
	StrictRegistered StrictPresence = "registered"
	// StrictGated is a path the strict profile keeps registered and hidden so
	// that invoking it produces a named abort instead of `unknown command`.
	StrictGated StrictPresence = "gated"
	// StrictAbsent is a path the strict profile does not register at all.
	StrictAbsent StrictPresence = "not registered"
)

// StrictOf reports what strict mode does to one command.
func (c *Census) StrictOf(cmd Command) StrictPresence {
	if cmd.Tree != TreeCompat {
		return StrictNotApplicable
	}
	strict, ok := c.Lookup(TreeCompatStrict, cmd.Path)
	if !ok {
		return StrictAbsent
	}
	if strict.Hidden {
		return StrictGated
	}
	return StrictRegistered
}

// ReferenceBlock renders every command path of every shipped tree.
//
// Two trees are rendered as rows and the third as a column, because the strict
// profile is not a separate product: it is the same executable answering
// differently, and 25 of its paths are the same paths. A document that rendered
// it as a third block would repeat 25 rows and still not say which 28 went
// missing.
//
// The block is generated because a hand-written command table goes stale in one
// direction only, and silently. docs/site/src/content/docs/reference/
// native-commands.md describes itself as the complete verb table for the native
// tree, names 77 paths, and contains the string `inference` zero times -- a
// namespace with ten verbs.
func (c *Census) ReferenceBlock() string {
	var out strings.Builder
	out.WriteString("| Path | Tree | Kind | Strict mode | Summary |\n")
	out.WriteString("| --- | --- | --- | --- | --- |\n")
	for _, tree := range []Tree{TreeNative, TreeCompat} {
		for _, cmd := range c.OfTree(tree) {
			kind := "verb"
			if !cmd.Leaf {
				kind = "group"
			}
			if cmd.Path == "" {
				kind = "root"
			}
			if cmd.Hidden {
				kind += ", hidden"
			}
			fmt.Fprintf(&out, "| `%s` | `%s` | %s | %s | %s |\n",
				cmd.Qualified(), tree.Launcher(), kind, c.StrictOf(cmd), cell(cmd.Short))
		}
	}
	return out.String()
}

// ReferenceCounts is the one-line summary a gate prints, so a run that measured
// a shorter tree than the last one says so out loud instead of reporting the
// same success.
func (c *Census) ReferenceCounts() string {
	parts := make([]string, 0, 3)
	for _, tree := range Trees() {
		commands := c.OfTree(tree)
		visible := 0
		leaves := 0
		for _, cmd := range commands {
			if !cmd.Hidden {
				visible++
			}
			if cmd.Leaf {
				leaves++
			}
		}
		parts = append(parts, fmt.Sprintf("%s %d paths (%d visible, %d leaves)", tree, len(commands), visible, leaves))
	}
	return strings.Join(parts, "; ")
}

// cell folds a summary into one table cell.
func cell(text string) string {
	text = strings.ReplaceAll(strings.TrimSpace(text), "|", `\|`)
	text = strings.Join(strings.Fields(text), " ")
	if text == "" {
		return "—"
	}
	return text
}

// ExtractBlock reads the generated block out of a document.
func ExtractBlock(body string) (string, error) {
	_, after, found := strings.Cut(body, ReferenceBegin+"\n")
	if !found {
		return "", fmt.Errorf("featureinventory: %s carries no %s marker; a gate that compares nothing to nothing reports success at exactly the moment it stopped working", CommandReferencePath, ReferenceBegin)
	}
	block, _, found := strings.Cut(after, ReferenceEnd)
	if !found {
		return "", fmt.Errorf("featureinventory: %s carries no %s marker", CommandReferencePath, ReferenceEnd)
	}
	if strings.TrimSpace(block) == "" {
		return "", fmt.Errorf("featureinventory: the generated block in %s is empty", CommandReferencePath)
	}
	return block, nil
}

// ReplaceBlock returns the document with the generated block rewritten.
func ReplaceBlock(body, block string) (string, error) {
	head, after, found := strings.Cut(body, ReferenceBegin+"\n")
	if !found {
		return "", fmt.Errorf("featureinventory: %s carries no %s marker", CommandReferencePath, ReferenceBegin)
	}
	_, tail, found := strings.Cut(after, ReferenceEnd)
	if !found {
		return "", fmt.Errorf("featureinventory: %s carries no %s marker", CommandReferencePath, ReferenceEnd)
	}
	return head + ReferenceBegin + "\n" + block + ReferenceEnd + tail, nil
}
