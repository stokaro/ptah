package protobufrender

import (
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// SplitPolicy controls how many files one export writes.
type SplitPolicy string

const (
	// SplitNone writes exactly one file, at --out. It is the default because the
	// generated file is the compatibility state: a schema that already has a
	// single-file baseline must keep it until the caller asks otherwise.
	SplitNone SplitPolicy = "none"
	// SplitTable writes one file per exported table, named after the message it
	// holds, next to the --out file. Every generated enum stays in the --out
	// file, which the table files import, because one enum is routinely shared
	// by columns of several tables and protobuf has no way to declare it twice.
	SplitTable SplitPolicy = "table"
)

// MovePolicy controls what happens when a type that a previous export already
// pinned would be written to a different file.
type MovePolicy string

const (
	// MoveError refuses the export. It is the default because the file a type
	// lives in is where its field numbers are recorded: treating a move as a
	// removal plus an addition restarts that message's numbering at 1 and
	// collides with every number its consumers still hold.
	MoveError MovePolicy = "error"
	// MoveRelocate carries the type's pinned numbering into its new file.
	MoveRelocate MovePolicy = "relocate"
)

// defaultAnchorName is the anchor's base name when --out is empty. --out is
// required by the CLI, so this only guards direct library use.
const defaultAnchorName = "schema.proto"

// generatedFileNamePattern is buf lint's FILE_LOWER_SNAKE_CASE. A derived file
// name that misses it is still written, exactly as a sanitized message name is:
// Ptah does not own the caller's lint policy.
var generatedFileNamePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*\.proto$`)

// anchorName is the base name of the file --out points at.
func anchorName(outPath string) string {
	if outPath == "" {
		return defaultAnchorName
	}
	return filepath.Base(outPath)
}

// packageDir is the directory a package requires relative to the buf module
// root, which is what buf lint's PACKAGE_DIRECTORY_MATCH checks.
func packageDir(pkg string) string {
	return strings.ReplaceAll(pkg, ".", "/")
}

// logicalPath is the path handed to the protobuf compiler, and the path a
// generated file imports a sibling by. Using the package directory keeps
// compiler diagnostics meaningful even when --out lives somewhere else, and
// makes the emitted import resolve from the buf module root.
func logicalPath(pkgDir, base string) string {
	if pkgDir == "" {
		return base
	}
	return pkgDir + "/" + base
}

// splitFileName is the file a message is written to under SplitTable. It is
// derived from the message name rather than the table name so that two tables
// disambiguated into distinct messages stay in distinct files.
func splitFileName(message string) string {
	return strings.ToLower(bufSnakeCase(message)) + ".proto"
}

// assignFiles maps every desired message to the base name of the file that will
// hold it. Enums are never assigned here: they always live in the anchor.
func (b *builder) assignFiles(desired desiredShape, anchor string) (map[string]string, error) {
	homes := make(map[string]string, len(desired.messages))
	if b.opts.Split != SplitTable {
		for _, dm := range desired.messages {
			homes[dm.Name] = anchor
		}
		return homes, nil
	}

	claimed := make(map[string][]string)
	for _, dm := range desired.messages {
		name := splitFileName(dm.Name)
		claimed[name] = append(claimed[name], dm.Name)
		homes[dm.Name] = name
	}

	names := make([]string, 0, len(claimed))
	for name := range claimed {
		names = append(names, name)
	}
	sort.Strings(names)

	var collisions []string
	for _, name := range names {
		owners := claimed[name]
		sort.Strings(owners)
		// EqualFold, not ==: on a case-insensitive filesystem "product.proto"
		// and a --out named "Product.Proto" are one file, and writing both would
		// silently leave whichever was written last.
		if strings.EqualFold(name, anchor) {
			return nil, fmt.Errorf(
				"message(s) %s would be written to %q, which is the file --out already names and which holds the generated enums; point --out at a different file name",
				strings.Join(quoteAll(owners), ", "), name)
		}
		if len(owners) > 1 {
			collisions = append(collisions, fmt.Sprintf("%s: %s", name, strings.Join(quoteAll(owners), ", ")))
		}
	}
	if len(collisions) > 0 {
		return nil, fmt.Errorf(
			"messages map to the same protobuf file name: %s; set a distinct schema= on one of them or exclude it with --exclude-tables",
			strings.Join(collisions, "; "))
	}

	for _, name := range names {
		if !generatedFileNamePattern.MatchString(name) {
			b.warn(name, fmt.Sprintf(
				"generated file %q is not lower_snake_case.proto; buf lint STANDARD reports FILE_LOWER_SNAKE_CASE for it", name))
		}
	}
	return homes, nil
}

func quoteAll(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, fmt.Sprintf("%q", value))
	}
	return out
}

// typeMove is one type whose file changed between the previous export and this
// one.
type typeMove struct {
	Kind string
	Name string
	From string
	To   string
}

// checkMoves refuses, or reports, every type that would change files. It runs
// before any numbering is reconciled, so a refused move leaves the whole
// baseline untouched.
//
// The refusal exists because a move is invisible in the generated output: the
// type simply appears in another file. Handled as a removal plus an addition it
// silently restarts the message's field numbers at 1, which is exactly the
// incompatibility the single-file baseline was there to prevent.
func (b *builder) checkMoves(homes map[string]string, prev *previousSet, anchor string) error {
	if prev == nil {
		return nil
	}

	var moves []typeMove
	for name, to := range homes {
		if from, ok := prev.MessageFile[name]; ok && from != to {
			moves = append(moves, typeMove{Kind: "message", Name: name, From: from, To: to})
		}
	}
	for _, en := range b.enums {
		if from, ok := prev.EnumFile[en.Name]; ok && from != anchor {
			moves = append(moves, typeMove{Kind: "enum", Name: en.Name, From: from, To: anchor})
		}
	}
	if len(moves) == 0 {
		return nil
	}
	sort.Slice(moves, func(i, j int) bool { return moves[i].Name < moves[j].Name })

	if b.opts.OnTypeMove == MoveRelocate {
		for _, move := range moves {
			b.warn(move.Name, fmt.Sprintf(
				"%s %q moved from %q to %q; its pinned numbering was carried into the new file",
				move.Kind, move.Name, move.From, move.To))
		}
		return nil
	}

	parts := make([]string, 0, len(moves))
	for _, move := range moves {
		parts = append(parts, fmt.Sprintf("%s %q from %q to %q", move.Kind, move.Name, move.From, move.To))
	}
	return fmt.Errorf(
		"types would move between files: %s; a move is indistinguishable from a removal plus an addition, which restarts field numbering at 1, so pass --proto-on-type-move=relocate to carry the pinned numbering into the new file",
		strings.Join(parts, "; "))
}

// group distributes reconciled types over the files that will be written. A
// desired type goes to its assigned file; a type retained only for its
// reservations stays in the file that already held it, so a tombstone never
// moves and never has to be reported as a move.
func (b *builder) group(messages []message, enums []enum, homes map[string]string, prev *previousSet, anchor string) []file {
	pkgDir := packageDir(b.opts.Package)

	byName := map[string]*file{anchor: {Name: anchor, Anchor: true}}
	order := []string{anchor}
	at := func(name string) *file {
		if existing, ok := byName[name]; ok {
			return existing
		}
		created := &file{Name: name}
		byName[name] = created
		order = append(order, name)
		return created
	}

	for _, msg := range messages {
		home := anchor
		if assigned, ok := homes[msg.Name]; ok {
			home = assigned
		} else if prev != nil {
			if previous, ok := prev.MessageFile[msg.Name]; ok {
				home = previous
			}
		}
		target := at(home)
		target.Messages = append(target.Messages, msg)
		target.Imports = append(target.Imports, b.messageImports[msg.Name]...)
		// A generated enum only ever lives in the anchor, so a message that
		// names one and sits elsewhere is the single source of a cross-file
		// import.
		if home != anchor && b.referencesEnum(msg) {
			target.Imports = append(target.Imports, logicalPath(pkgDir, anchor))
		}
	}
	byName[anchor].Enums = append(byName[anchor].Enums, enums...)

	sort.Strings(order[1:])
	out := make([]file, 0, len(order))
	var siblings []string
	for _, name := range order {
		current := byName[name]
		if !current.Anchor && len(current.Messages) == 0 && len(current.Enums) == 0 {
			continue
		}
		current.Package = b.opts.Package
		current.GoPackage = b.opts.GoPackage
		current.Imports = dedupe(current.Imports)
		current.sortForOutput()
		if !current.Anchor {
			siblings = append(siblings, name)
		}
		out = append(out, *current)
	}
	out[0].Siblings = siblings
	return out
}

// referencesEnum reports whether a message names a generated enum, which is the
// only reason a table file ever imports the anchor.
func (b *builder) referencesEnum(msg message) bool {
	for _, fld := range msg.Fields {
		if b.enumNames[fld.Type] {
			return true
		}
	}
	return false
}

func dedupe(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := make(map[string]bool, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	return out
}

// removedFiles lists the files a previous export wrote that this one does not,
// so the caller can delete them. Every name comes from the previous set, which
// was validated in full, so nothing Ptah did not write can appear here.
func removedFiles(prev *previousSet, written []file) []string {
	if prev == nil {
		return nil
	}
	keep := make(map[string]bool, len(written))
	for _, f := range written {
		keep[f.Name] = true
	}
	var removed []string
	for _, name := range prev.Files {
		if !keep[name] {
			removed = append(removed, name)
		}
	}
	sort.Strings(removed)
	return removed
}
