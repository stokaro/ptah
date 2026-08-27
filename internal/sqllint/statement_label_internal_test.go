package sqllint

// White-box testing required: the guard asks statementLabelForName about a node
// type read from disk, and constructing every node kind to ask through the
// exported path would be a second, hand-written list of exactly the types this
// exists to guard.

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestStatementLabel_EveryNodeKindWouldReadAsSQL guards the derivation against
// the node type nobody has added yet.
//
// The label comes from the Go type name, which is right for most of core/ast
// and wrong wherever the node is not named after the statement. A future
// `FooNode` would report that Ptah "does not model FOO statements yet" -- a
// finding naming a keyword that is not SQL, in the rule whose whole job is to
// be honest about what was not analyzed.
//
// The list of types is read from core/ast on disk rather than written here, for
// the reason such lists exist: a hand-written copy is the thing it would be
// guarding.
func TestStatementLabel_EveryNodeKindWouldReadAsSQL(t *testing.T) {
	c := qt.New(t)

	// Verbs a statement label may open with. Short and stable on purpose: this
	// is the vocabulary of SQL statements, not of this package.
	verbs := []string{
		"CREATE", "ALTER", "DROP", "COMMENT", "TRUNCATE", "GRANT", "REVOKE",
		"REFRESH", "DO", "RAW", "SET", "UPSERT", "EXTENDED PROPERTY",
	}
	// Node types that are parts of a statement rather than statements, so the
	// linter never answers one, and the routine nodes the rule labels by hand.
	notStatements := map[string]string{
		"ColumnNode":           "a column inside CREATE TABLE",
		"ConstraintNode":       "a constraint inside CREATE TABLE",
		"MySQLRoutineNode":     "labeled by an explicit case, from its own Kind",
		"OpaqueRoutineNode":    "labeled by an explicit case, from its own Kind",
		"PostgresDoBlockNode":  "labeled by an explicit case",
		"PostgresRoutineNode":  "labeled by an explicit case, from its own Kind",
		"SQLServerRoutineNode": "labeled by an explicit case, from its own Kind",
	}

	// Kinds a rule analyzes never reach the default arm, so they are never
	// labeled. Read from the production map rather than listed again here.
	analyzed := analyzedKindNames()

	names := nodeTypeNames(c, filepath.Join("..", "..", "core", "ast"))
	c.Assert(len(names) > 30, qt.IsTrue, qt.Commentf("found %d node types, expected the whole package", len(names)))

	for _, name := range names {
		assertLabelReadsAsSQL(c, name, notStatements, analyzed, verbs)
	}
}

// analyzedKindNames is the production set of analyzed kinds, by bare type name.
func analyzedKindNames() map[string]struct{} {
	names := make(map[string]struct{})
	for kind := range analyzedStatementKinds {
		name := kind.String()
		names[name[strings.LastIndex(name, ".")+1:]] = struct{}{}
	}
	return names
}

// assertLabelReadsAsSQL checks one node type, skipping the two kinds that never
// reach the label: a part of a statement, and a kind a rule analyzes.
func assertLabelReadsAsSQL(c *qt.C, name string, notStatements map[string]string, analyzed map[string]struct{}, verbs []string) {
	if _, exempt := notStatements[name]; exempt {
		return
	}
	if _, ok := analyzed[name]; ok {
		return
	}
	label := statementLabelForName(name)
	c.Assert(opensWithVerb(label, verbs), qt.IsTrue,
		qt.Commentf("%s labels a finding %q, which does not open with a SQL verb; "+
			"add it to statementLabelOverrides or to the exempt list with a reason", name, label))
}

// nodeTypeNames reads every `type XxxNode struct` declared in a package.
func nodeTypeNames(c *qt.C, dir string) []string {
	entries, err := os.ReadDir(dir)
	c.Assert(err, qt.IsNil)

	fset := token.NewFileSet()
	var names []string
	for _, entry := range entries {
		names = append(names, nodeTypeNamesInFile(c, fset, dir, entry.Name())...)
	}
	return names
}

// nodeTypeNamesInFile reads one Go file, skipping anything that is not
// production source.
func nodeTypeNamesInFile(c *qt.C, fset *token.FileSet, dir, name string) []string {
	if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
		return nil
	}
	file, err := parser.ParseFile(fset, filepath.Join(dir, name), nil, 0)
	c.Assert(err, qt.IsNil)

	var names []string
	for _, decl := range file.Decls {
		names = append(names, structNodeNames(decl)...)
	}
	return names
}

// structNodeNames lists the `Node`-suffixed struct types one declaration holds.
func structNodeNames(decl ast.Decl) []string {
	generic, ok := decl.(*ast.GenDecl)
	if !ok || generic.Tok != token.TYPE {
		return nil
	}
	var names []string
	for _, spec := range generic.Specs {
		typeSpec, ok := spec.(*ast.TypeSpec)
		if !ok || !strings.HasSuffix(typeSpec.Name.Name, "Node") {
			continue
		}
		if _, isStruct := typeSpec.Type.(*ast.StructType); !isStruct {
			continue
		}
		names = append(names, typeSpec.Name.Name)
	}
	return names
}

// opensWithVerb reports whether a label starts with one of the SQL verbs.
func opensWithVerb(label string, verbs []string) bool {
	for _, verb := range verbs {
		if label == verb || strings.HasPrefix(label, verb+" ") {
			return true
		}
	}
	return false
}
