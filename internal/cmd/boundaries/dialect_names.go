package main

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/constant"
	"go/token"
	"go/types"
	"maps"
	"os/exec"
	"path"
	"slices"
	"strings"

	"golang.org/x/tools/go/packages"

	"ptah.run/core/platform"
)

// dialectNameRule is the identifier of the rule that counts dialect identity
// in shared packages.
const dialectNameRule = "shared-packages-name-dialects"

// dialectDeclaration is the package that declares dialect identity: the
// canonical name constants, the alias table NormalizeDialect folds, and the
// family predicates. It is the one place allowed to name every engine, so its
// own references are not counted, and a call to one of its family predicates
// is counted where it is made instead.
const dialectDeclaration = "core/platform"

// dialectNameRoots start the shared set of stokaro/ptah#4204: code that a new
// database engine should not have to edit. They are the neutral layer and the
// shared SQL reader the owner rules already hold, and the library tree a
// caller imports: core, migration, dbschema and catalog. sharedPackageSet adds
// what they link and says what it leaves out.
var dialectNameRoots = slices.Concat(neutralRoots, sqlReaderRoots, []string{
	"core", "migration", "dbschema", "catalog",
})

// requiredDialectNamePackages must be in the shared set for the rule to mean
// anything. A rename that took one out of the set would otherwise lower the
// count with no change to the code.
var requiredDialectNamePackages = []string{
	"core/platform/capability", "core/platform/identifier", "core/query", "core/sqlutil",
	"dbschema", "migration/migrator", "migration/planner", "migration/schemadiff/internal/compare",
	"internal/parser", "internal/sqlschema", "internal/schemaprep",
}

// dialectNameFloor is the fewest shared packages the rule may measure. The set
// comes from roots and exclusions, and a root that stopped matching, or an
// exclusion that started matching everything, would report zero findings and
// read as success. The figure is a source constant rather than a field of the
// baseline, so a regenerated baseline cannot lower it.
const dialectNameFloor = 150

// dialectIdentity is what the rule recognizes as naming a dialect, read out of
// the declaring package rather than written here.
type dialectIdentity struct {
	// constants are the declaring package's canonical name constants: the
	// exported string constants whose value NormalizeDialect folds onto itself.
	constants map[types.Object]bool
	// spellings are every accepted spelling of every built-in target.
	spellings map[string]bool
	// predicates are the declaring package's exported functions whose body
	// names a canonical constant, such as IsPostgresFamily.
	predicates map[types.Object]bool
}

// deriveDialectIdentity reads dialect identity out of the declaring package.
func deriveDialectIdentity(loaded []*packages.Package) (dialectIdentity, error) {
	index := slices.IndexFunc(loaded, func(pkg *packages.Package) bool {
		return relative(pkg.PkgPath) == dialectDeclaration
	})
	if index < 0 {
		return dialectIdentity{}, fmt.Errorf("dialect declaration %q was not loaded", dialectDeclaration)
	}
	declaration := loaded[index]
	identity := dialectIdentity{
		constants:  make(map[types.Object]bool),
		spellings:  make(map[string]bool),
		predicates: make(map[types.Object]bool),
	}
	for _, spelling := range platform.DialectSpellings() {
		identity.spellings[spelling] = true
	}
	scope := declaration.Types.Scope()
	for _, name := range scope.Names() {
		object, ok := scope.Lookup(name).(*types.Const)
		if !ok || !object.Exported() || object.Val().Kind() != constant.String {
			continue
		}
		value := constant.StringVal(object.Val())
		if platform.NormalizeDialect(value) == value {
			identity.constants[object] = true
		}
	}
	// Every built-in target has one canonical constant. A target without one
	// means the derivation stopped recognizing the declaration, and the
	// target's every use would go uncounted.
	named := make(map[string]bool)
	for object := range identity.constants {
		named[constant.StringVal(object.(*types.Const).Val())] = true
	}
	for spelling := range identity.spellings {
		if canonical := platform.NormalizeDialect(spelling); !named[canonical] {
			return dialectIdentity{}, fmt.Errorf("dialect %q has no canonical constant in %q", canonical, dialectDeclaration)
		}
	}
	for _, file := range declaration.Syntax {
		for _, decl := range file.Decls {
			function, ok := decl.(*ast.FuncDecl)
			if !ok || function.Recv != nil || !function.Name.IsExported() || function.Body == nil {
				continue
			}
			if namesConstant(declaration.TypesInfo, function.Body, identity.constants) {
				identity.predicates[declaration.TypesInfo.Defs[function.Name]] = true
			}
		}
	}
	return identity, nil
}

func namesConstant(info *types.Info, node ast.Node, constants map[types.Object]bool) bool {
	found := false
	ast.Inspect(node, func(n ast.Node) bool {
		if ident, ok := n.(*ast.Ident); ok && constants[info.Uses[ident]] {
			found = true
		}
		return !found
	})
	return found
}

// trackedPackageDirs reports the module-relative directories that hold a
// tracked non-test Go file. The shared set is chosen from these, never from a
// filesystem walk, which would descend into the linked worktrees parked under
// the repository and measure another branch's packages as this one's.
func trackedPackageDirs(root string) (map[string]bool, error) {
	command := exec.Command("git", "ls-files", "-z", "--", "*.go")
	command.Dir = root
	var stderr bytes.Buffer
	command.Stderr = &stderr
	out, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("git ls-files: %w: %s", err, strings.TrimSpace(stderr.String()))
	}
	dirs := make(map[string]bool)
	for name := range strings.SplitSeq(string(out), "\x00") {
		if name == "" || strings.HasSuffix(name, "_test.go") {
			continue
		}
		dirs[path.Dir(name)] = true
	}
	return dirs, nil
}

// engineAbbreviations are the stems package names use for an engine that
// NormalizeDialect does not accept as a spelling: internal/pgname,
// internal/oratypesource and internal/yqlparse each serve one engine.
var engineAbbreviations = []string{"pg", "ora", "yql"}

// sharedPackageSet chooses the shared set from the tracked packages.
//
// A package is shared when it sits at or under a root, or when a shared
// package links it through a chain of imports that passes only through shared
// packages, and none of these exclusions applies:
//
//   - it belongs to an owner family: dialect/<name>, feature/<name> or
//     engine/builtin and everything below it;
//   - it is the declaring package itself;
//   - a segment of its path names an engine: the segment is, or begins with,
//     a spelling NormalizeDialect accepts, the name of a dialect owner, or an
//     abbreviation above. A name ending in "db" counts without it as well, as
//     the spelling table already writes cockroach beside cockroachdb, so
//     internal/timescale belongs to the timescaledb owner. That is what makes
//     internal/dbschema/postgres a reader for one target,
//     internal/planner/dialects/mysql a planner for one, and
//     internal/mysqlname, internal/pgname or internal/chtype a helper for one.
//
// An excluded package is not followed, so a helper that only a per-engine
// package links is that engine's, not shared.
//
// The prefix test reads "ch" as ClickHouse, which is also how ordinary words
// begin. No package a shared package links begins with it today except the
// ClickHouse helpers; internal/chronologyguard is the nearest, and is linked
// by none.
func sharedPackageSet(loaded []*packages.Package, tracked map[string]bool, spellings map[string]bool) map[string]bool {
	names := slices.Concat(slices.Collect(maps.Keys(spellings)), engineAbbreviations)
	for _, pkg := range loaded {
		if family, owned := ownerFamily(relative(pkg.PkgPath)); owned && strings.HasPrefix(family, "dialect/") {
			names = append(names, strings.TrimPrefix(family, "dialect/"))
		}
	}
	stems := slices.Clone(names)
	for _, name := range names {
		if trimmed, found := strings.CutSuffix(name, "db"); found && len(trimmed) > 3 {
			stems = append(stems, trimmed)
		}
	}

	eligible := func(rel string) bool {
		if rel == "" || !tracked[rel] || rel == dialectDeclaration {
			return false
		}
		if _, owned := ownerFamily(rel); owned {
			return false
		}
		return !slices.ContainsFunc(strings.Split(rel, "/"), func(segment string) bool {
			return slices.ContainsFunc(stems, func(stem string) bool { return strings.HasPrefix(segment, stem) })
		})
	}
	shared := make(map[string]bool)
	var visit func(*packages.Package)
	visit = func(pkg *packages.Package) {
		rel := relative(pkg.PkgPath)
		if shared[rel] || !eligible(rel) {
			return
		}
		shared[rel] = true
		for _, imported := range pkg.Imports {
			visit(imported)
		}
	}
	for _, pkg := range loaded {
		rel := relative(pkg.PkgPath)
		if slices.ContainsFunc(dialectNameRoots, func(root string) bool {
			return rel == root || strings.HasPrefix(rel, root+"/")
		}) {
			visit(pkg)
		}
	}
	return shared
}

// dialectNameFindings reports each place a shared package names a dialect.
//
// Three shapes count, each resolved by the type checker:
//
//   - a use of a canonical name constant, such as platform.Postgres, in any
//     position: a comparison, a switch case, a map key or an argument;
//   - an operand of == or != , or a case of an expression switch, whose
//     constant value is a dialect spelling, such as "postgres" or a local
//     constant equal to it. A use of a canonical constant there is counted
//     once, as a use;
//   - a call to a family predicate of the declaring package, such as
//     platform.IsPostgresFamily, so that replacing a comparison with the
//     predicate does not read as progress.
//
// A spelling outside a comparison, such as a log message or a map key written
// as a literal, is not counted, since nothing tells a name from a word there.
func dialectNameFindings(pkg *packages.Package, self string, identity dialectIdentity) []finding {
	info := pkg.TypesInfo
	found := make([]finding, 0)
	report := func(node ast.Node, detail string) {
		found = append(found, finding{Package: self, Detail: detail, Position: pkg.Fset.Position(node.Pos()).String()})
	}
	isConstantUse := func(expr ast.Expr) bool {
		switch e := ast.Unparen(expr).(type) {
		case *ast.Ident:
			return identity.constants[info.Uses[e]]
		case *ast.SelectorExpr:
			return identity.constants[info.Uses[e.Sel]]
		default:
			return false
		}
	}
	spelling := func(expr ast.Expr) (string, bool) {
		if isConstantUse(expr) {
			return "", false
		}
		value := info.Types[expr].Value
		if value == nil || value.Kind() != constant.String {
			return "", false
		}
		text := constant.StringVal(value)
		return text, identity.spellings[text]
	}
	for _, file := range pkg.Syntax {
		ast.Inspect(file, func(node ast.Node) bool {
			switch n := node.(type) {
			case *ast.Ident:
				object := info.Uses[n]
				if identity.constants[object] {
					report(n, "names platform."+object.Name())
				}
				if identity.predicates[object] {
					report(n, "calls platform."+object.Name())
				}
			case *ast.BinaryExpr:
				if n.Op != token.EQL && n.Op != token.NEQ {
					return true
				}
				for _, operand := range []ast.Expr{n.X, n.Y} {
					if text, ok := spelling(operand); ok {
						report(operand, fmt.Sprintf("compares with %q", text))
					}
				}
			case *ast.SwitchStmt:
				if n.Tag == nil {
					return true
				}
				for _, statement := range n.Body.List {
					for _, expr := range statement.(*ast.CaseClause).List {
						if text, ok := spelling(expr); ok {
							report(expr, fmt.Sprintf("switches on %q", text))
						}
					}
				}
			}
			return true
		})
	}
	return found
}
