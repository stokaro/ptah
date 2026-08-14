package qtshape

import (
	"go/ast"
	"go/token"
	"strconv"
)

// This file answers one question for the rules in qtshape.go: at the position an
// identifier is written, which declaration does it name?
//
// Both halves of the gate used to answer that question by spelling, and both
// were wrong in the same direction for a different reason.
//
//   - R1 reported any selector whose receiver was written `qt`. A test file that
//     does not import quicktest at all and declares its own `qt` — or aliases an
//     unrelated package to it — was reported for calls that cannot reach the
//     prohibited package-level functions, so a repository-wide CI gate refused
//     correct code.
//   - R2 and R3 recognized a subtest callback only where it was written inline,
//     and R2's index of func(*qt.C) callbacks was file-wide and keyed on the name
//     alone. `callback := func(t *testing.T) { c.Assert(...) }` followed by
//     `t.Run(name, callback)` escaped both rules while producing exactly the
//     parent-FailNow failure R3 exists to prevent.
//   - R2's receiver and R3's inherited names were then still decided by
//     membership in a set of names, which is the same mistake one level down.
//     An inner `c := runner{}` was reported as the checker the enclosing
//     function happens to call `c`, and a `c := qt.New(t)` written halfway down
//     a closure retroactively excused every assertion above it.
//
// The resolution here is lexical and positional because Go's scoping is: a name
// declared inside a block is what that name means from the end of its
// declaration to the end of that block, and before that point the name still
// means whatever the enclosing block gave it. A rule that reduces a block to a
// set of declared names cannot tell those two apart.

// defaultBindingNames gives the name an unaliased import of each package these
// rules are about binds.
//
// Only these two are listed. This analysis parses one file at a time and cannot
// read another package's clause, so it never guesses a binding name it has not
// been told: an import this map does not cover and that carries no alias binds
// nothing here, which can only cost a finding and can never invent one.
var defaultBindingNames = map[string]string{
	QuicktestImportPath: "quicktest",
	testingImportPath:   "testing",
}

// span is the half-open source range of one lexical block.
type span struct {
	start token.Pos
	end   token.Pos
}

// contains reports whether pos lies inside the block.
func (s span) contains(pos token.Pos) bool {
	return pos >= s.start && pos < s.end
}

// width is how much source the block covers. The narrowest block containing a
// position is the innermost one, and the innermost declaration is the one a name
// resolves to.
func (s span) width() int {
	return int(s.end - s.start)
}

// nameDecl is one declaration of a name together with the region of source in
// which that declaration is what the name means.
type nameDecl struct {
	// from is the first position at which the name denotes this declaration.
	// For a block-local variable that is the end of its declaration; for a
	// parameter or a package-level name it is the start of the scope.
	from token.Pos
	// scope is the block the declaration belongs to.
	scope span
	// spec is set when the name is an imported package.
	spec *ast.ImportSpec
	// sig is set when the name denotes a function, however it was declared.
	sig *ast.FuncType
	// lit is set when the name was bound to a function literal, which is the
	// only case in which this package can look inside the callback's body.
	lit *ast.FuncLit
	// typ is the type written in the declaration, and value its initializer,
	// when it has them. They are kept unclassified because deciding whether a
	// type is *qt.C is itself a name resolution through this table, so it
	// cannot run until the table is built.
	typ   ast.Expr
	value ast.Expr
	// handed is set when the name is bound by a signature -- a parameter or a
	// method receiver -- rather than by a statement inside the body. R3 asks it
	// because a parameter is the only thing a function is given by its caller:
	// every other local declaration holds whatever the body put in it, which may
	// well be the enclosing test's TB.
	handed bool
	// checker and tb are filled in by classifyBindings once the file's imports
	// are resolved: the name denotes a *qt.C, or a testing.T, B, F or TB.
	checker bool
	tb      bool
}

// declaredWithin reports whether this declaration is written inside a region of
// source. For a closure that is the difference between a name the closure owns
// and one it reaches out of itself for, which is the whole of R3.
func (d *nameDecl) declaredWithin(s span) bool {
	return s.contains(d.from)
}

// bindings is one file's lexical name table.
type bindings struct {
	file  span
	decls map[string][]*nameDecl
}

// newBindings builds the name table for a parsed file.
func newBindings(file *ast.File) *bindings {
	b := &bindings{
		file:  span{start: file.Pos(), end: file.End()},
		decls: map[string][]*nameDecl{},
	}

	for _, spec := range file.Imports {
		name := importBindingName(spec)
		if name == "" {
			continue
		}
		b.declare(name, &nameDecl{from: b.file.start, scope: b.file, spec: spec})
	}

	for _, decl := range file.Decls {
		b.declarePackageLevel(decl)
	}

	b.walk(file)
	b.forgetReassigned(file)

	return b
}

// lookup returns the declaration an identifier of this name at this position
// resolves to: the one whose block is innermost among those that contain the
// position and already had the name in scope there.
func (b *bindings) lookup(name string, pos token.Pos) (*nameDecl, bool) {
	var best *nameDecl
	for _, d := range b.decls[name] {
		if !d.scope.contains(pos) || pos < d.from {
			continue
		}
		if best == nil || closer(d, best) {
			best = d
		}
	}

	return best, best != nil
}

// each visits every declaration in the table, in no particular order.
func (b *bindings) each(visit func(d *nameDecl)) {
	for _, decls := range b.decls {
		for _, d := range decls {
			visit(d)
		}
	}
}

// closer reports whether d is a nearer declaration than best: a narrower block
// wins, and within one block the later declaration wins.
func closer(d, best *nameDecl) bool {
	if d.scope.width() != best.scope.width() {
		return d.scope.width() < best.scope.width()
	}
	return d.from > best.from
}

// resolvesToImport reports whether the identifier names the given import at the
// position it is written, rather than something else spelled the same.
func (b *bindings) resolvesToImport(ident *ast.Ident, spec *ast.ImportSpec) bool {
	d, ok := b.lookup(ident.Name, ident.Pos())
	return ok && d.spec == spec && spec != nil
}

// declare records one declaration.
func (b *bindings) declare(name string, decl *nameDecl) {
	if name == "_" {
		return
	}
	b.decls[name] = append(b.decls[name], decl)
}

// declarePackageLevel records the names a top-level declaration binds. They are
// visible across the whole file regardless of where they are written, which is
// why they are recorded before the positional walk rather than during it.
func (b *bindings) declarePackageLevel(decl ast.Decl) {
	switch typed := decl.(type) {
	case *ast.FuncDecl:
		if typed.Recv != nil {
			return
		}
		b.declare(typed.Name.Name, &nameDecl{from: b.file.start, scope: b.file, sig: typed.Type})
	case *ast.GenDecl:
		for _, spec := range typed.Specs {
			b.declarePackageSpec(spec)
		}
	}
}

// declarePackageSpec records one spec of a top-level var, const or type block.
func (b *bindings) declarePackageSpec(spec ast.Spec) {
	switch typed := spec.(type) {
	case *ast.ValueSpec:
		for i, name := range typed.Names {
			value := valueAt(typed.Values, i, len(typed.Names))
			b.declare(name.Name, &nameDecl{
				from:  b.file.start,
				scope: b.file,
				sig:   declaredSignature(typed.Type, value),
				lit:   literalOf(value),
				typ:   typed.Type,
				value: value,
			})
		}
	case *ast.TypeSpec:
		b.declare(typed.Name.Name, &nameDecl{from: b.file.start, scope: b.file})
	}
}

// walk records every declaration inside a function body, carrying the block each
// one belongs to.
//
// ast.Inspect calls the visitor a second time with nil once a node's children
// are done, which is what makes a scope stack possible without hand-writing a
// traversal for every node type.
func (b *bindings) walk(file *ast.File) {
	spans := []span{b.file}
	var opened []bool

	ast.Inspect(file, func(node ast.Node) bool {
		if node == nil {
			last := len(opened) - 1
			if opened[last] {
				spans = spans[:len(spans)-1]
			}
			opened = opened[:last]
			return true
		}

		inner, opens := blockOf(node)
		if opens {
			spans = append(spans, inner)
		}
		opened = append(opened, opens)

		b.declareIn(node, spans[len(spans)-1])
		return true
	})
}

// blockOf reports the lexical block a node opens, if it opens one.
//
// Parameters belong to the function's block and a range variable belongs to the
// range statement's, so the block is pushed before the node's own declarations
// are recorded and both land in the right place.
func blockOf(node ast.Node) (span, bool) {
	switch node.(type) {
	case *ast.FuncDecl, *ast.FuncLit, *ast.BlockStmt,
		*ast.IfStmt, *ast.ForStmt, *ast.RangeStmt,
		*ast.SwitchStmt, *ast.TypeSwitchStmt, *ast.SelectStmt,
		*ast.CaseClause, *ast.CommClause:
		return span{start: node.Pos(), end: node.End()}, true
	}

	return span{}, false
}

// declareIn records the names a node binds in the block it belongs to.
func (b *bindings) declareIn(node ast.Node, sc span) {
	switch typed := node.(type) {
	case *ast.FuncDecl:
		b.declareFields(typed.Recv, sc, handedIn)
		b.declareFields(typed.Type.Params, sc, handedIn)
		b.declareFields(typed.Type.Results, sc, declaredLocally)
	case *ast.FuncLit:
		b.declareFields(typed.Type.Params, sc, handedIn)
		b.declareFields(typed.Type.Results, sc, declaredLocally)
	case *ast.AssignStmt:
		b.declareAssign(typed, sc)
	case *ast.ValueSpec:
		b.declareValueSpec(typed, sc)
	case *ast.TypeSpec:
		b.declareTypeSpec(typed, sc)
	case *ast.RangeStmt:
		b.declareRange(typed, sc)
	}
}

// handedIn and declaredLocally name the two kinds of signature field for
// declareFields, so its call sites say which one they are recording. A named
// result is written in the signature but is not something the caller supplies,
// so it is declared locally.
const (
	handedIn        = true
	declaredLocally = false
)

// declareFields records a signature's parameter, result or receiver names.
func (b *bindings) declareFields(list *ast.FieldList, sc span, handed bool) {
	if list == nil {
		return
	}
	for _, field := range list.List {
		for _, name := range field.Names {
			b.declare(name.Name, &nameDecl{
				from:   sc.start,
				scope:  sc,
				sig:    functionType(field.Type),
				typ:    field.Type,
				handed: handed,
			})
		}
	}
}

// declareAssign records a short variable declaration.
func (b *bindings) declareAssign(assign *ast.AssignStmt, sc span) {
	if assign.Tok != token.DEFINE {
		return
	}
	for i, lhs := range assign.Lhs {
		ident, ok := lhs.(*ast.Ident)
		if !ok {
			continue
		}
		value := valueAt(assign.Rhs, i, len(assign.Lhs))
		b.declare(ident.Name, &nameDecl{
			from:  assign.End(),
			scope: sc,
			sig:   declaredSignature(nil, value),
			lit:   literalOf(value),
			value: value,
		})
	}
}

// declareValueSpec records a var or const declaration written inside a function.
// The top-level form is recorded by declarePackageSpec, which gives it the file
// scope it actually has.
func (b *bindings) declareValueSpec(spec *ast.ValueSpec, sc span) {
	if sc == b.file {
		return
	}
	for i, name := range spec.Names {
		value := valueAt(spec.Values, i, len(spec.Names))
		b.declare(name.Name, &nameDecl{
			from:  spec.End(),
			scope: sc,
			sig:   declaredSignature(spec.Type, value),
			lit:   literalOf(value),
			typ:   spec.Type,
			value: value,
		})
	}
}

// declareTypeSpec records a type declaration written inside a function.
func (b *bindings) declareTypeSpec(spec *ast.TypeSpec, sc span) {
	if sc == b.file {
		return
	}
	b.declare(spec.Name.Name, &nameDecl{from: spec.Name.Pos(), scope: sc})
}

// declareRange records the variables a range clause binds.
func (b *bindings) declareRange(stmt *ast.RangeStmt, sc span) {
	if stmt.Tok != token.DEFINE {
		return
	}
	for _, expr := range []ast.Expr{stmt.Key, stmt.Value} {
		ident, ok := expr.(*ast.Ident)
		if !ok {
			continue
		}
		b.declare(ident.Name, &nameDecl{from: sc.start, scope: sc})
	}
}

// forgetReassigned drops the function value remembered for any name that is also
// assigned to. A callback rebound after its declaration has no single known
// value, and reporting the first literal at a call site that may receive a
// different one is a guess this gate does not make.
func (b *bindings) forgetReassigned(file *ast.File) {
	ast.Inspect(file, func(node ast.Node) bool {
		assign, ok := node.(*ast.AssignStmt)
		if !ok || assign.Tok == token.DEFINE {
			return true
		}
		for _, lhs := range assign.Lhs {
			ident, ok := lhs.(*ast.Ident)
			if !ok {
				continue
			}
			b.forget(ident)
		}
		return true
	})
}

// forget clears the remembered function value of every declaration of a name
// whose block contains the assignment.
func (b *bindings) forget(ident *ast.Ident) {
	for _, d := range b.decls[ident.Name] {
		if !d.scope.contains(ident.Pos()) {
			continue
		}
		d.sig = nil
		d.lit = nil
	}
}

// importBindingName is the name an import spec binds, or "" when it binds
// nothing a selector can name: a blank import, a dot import, or an unaliased
// import of a package this analysis has not been told the name of.
func importBindingName(spec *ast.ImportSpec) string {
	if spec.Name != nil {
		if spec.Name.Name == "_" || spec.Name.Name == "." {
			return ""
		}
		return spec.Name.Name
	}

	path, err := strconv.Unquote(spec.Path.Value)
	if err != nil {
		return ""
	}

	return defaultBindingNames[path]
}

// valueAt returns the i-th initializer of a declaration binding count names, or
// nil when the values do not line up one to one with the names.
func valueAt(values []ast.Expr, i, count int) ast.Expr {
	if len(values) != count || i >= len(values) {
		return nil
	}
	return values[i]
}

// declaredSignature is the function type a declaration gives a name, taken from
// the written type when there is one and from the initializer otherwise.
func declaredSignature(declared ast.Expr, value ast.Expr) *ast.FuncType {
	if sig := functionType(declared); sig != nil {
		return sig
	}
	if lit := literalOf(value); lit != nil {
		return lit.Type
	}

	return nil
}

// functionType narrows an expression to a function type, or nil.
func functionType(expr ast.Expr) *ast.FuncType {
	sig, ok := expr.(*ast.FuncType)
	if !ok {
		return nil
	}
	return sig
}

// literalOf narrows an expression to a function literal, or nil.
func literalOf(expr ast.Expr) *ast.FuncLit {
	lit, ok := expr.(*ast.FuncLit)
	if !ok {
		return nil
	}
	return lit
}
