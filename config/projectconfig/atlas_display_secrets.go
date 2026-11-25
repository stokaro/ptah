package projectconfig

import (
	"math/big"
	"net/url"
	"os"
	"slices"
	"strconv"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/secretdisplay"
	"ptah.run/internal/secretmask"
)

// HCL quotes the argument of a call that fails, so an atlas.hcl error can
// carry a password that no variable declares sensitive: `url =
// "postgres://app:${tonumber(var.pw)}@host/db"` fails with `cannot convert
// "<the password>" to number`. The value never becomes an address, so no
// address in the error shows it as a secret, and the masking has to be told.
//
// It is told what reaches a secret place of an address the file writes. The
// expressions that may write one are found before anything is evaluated: each
// template, and each call to one of [addressBuilders]. Once every env instance
// is expanded, each is read as an address: the text it writes out, and a slot
// for each value it puts in. A local it names is read through its expression,
// down to the attribute or the element the traversal names, so
// `local.db.password` reads the password item of the object `local.db`
// declares and nothing else of it. Each slot's place is read once, with the
// other slots in place, and each value the slot holds in a context it
// evaluates in is read for what that place keeps secret. A value whose place
// cannot be read may be a secret: the file's own errors are masked against
// it, and the rest of the command is not, since a value that is not a secret
// would lose its text in every error that names it.
//
// A place cannot be read where a call the reading does not follow writes the
// address around a value and fails, because the error is raised exactly where
// the call's result would have said which part each value fills. Rather than
// name more functions to follow, the reading takes such an address as unread: an
// expression that writes "://" through such a call, and does not evaluate,
// gives every value it holds as one that may be a secret, and so does a slot
// that does not evaluate and writes text of its own beside the value it takes,
// as `join("", concat(["password="], [var.pw]))` does in a query. The user name
// and the host such a call writes are masked in the file's errors with the
// password; a slot that converts one value, as `tonumber(var.port)` does, is
// read by where it stands.

// addressBuilders are the functions whose call may write an address. A
// template is read beside them.
var addressBuilders = []string{"format", "join", "replace", "urlqueryset", "urluserinfo"}

// addressCandidates returns each expression in body that may write an
// address: a template, and a call to one of [addressBuilders].
func addressCandidates(body *hclsyntax.Body) []hclsyntax.Expression {
	var candidates []hclsyntax.Expression
	_ = hclsyntax.VisitAll(body, func(node hclsyntax.Node) hcl.Diagnostics {
		switch node := node.(type) {
		case *hclsyntax.TemplateExpr:
			candidates = append(candidates, node)
		case *hclsyntax.FunctionCallExpr:
			if slices.Contains(addressBuilders, node.Name) {
				candidates = append(candidates, node)
			}
		}
		return nil
	})
	return candidates
}

// addressPiece is text an address writes out, or the expression of a value it
// puts in.
type addressPiece struct {
	text string
	slot hclsyntax.Expression
	// query is true where text is the name of a query parameter and its "=":
	// the address writes "?" before it where the text before it holds none, and
	// "&" where it does.
	query bool
}

// addressTemplate is an address the file writes, as its pieces in order.
type addressTemplate struct {
	pieces []addressPiece
	// whole is true where the pieces are the slots alone, each in wholePlace:
	// the password argument of urluserinfo(), a secret, and the value arguments
	// of a call whose address cannot be read, in a place that is unknown.
	whole      bool
	wholePlace slotPlace
	// contexts, where set, are the only contexts the slots are evaluated in,
	// and secrets are what the address holds secret in its text: the address
	// takes its "://" from a value one context gives a slot, written in as
	// text.
	contexts []*hcl.EvalContext
	secrets  []string
}

// slotPlace is the part of an address a slot fills.
type slotPlace int

const (
	// placeOther is any part that keeps no secret of its own.
	placeOther slotPlace = iota
	// placeSecret is a part the address keeps secret: the password, or a
	// query parameter whose name says it holds a secret.
	placeSecret
	// placeUserInfo is a user info with no ":" outside the slot, which the
	// slot may fill with a user name and a password.
	placeUserInfo
	// placeUnknown is a part that cannot be read, which may keep a secret.
	placeUnknown
)

// slotProbe and otherSlot stand for values while a slot's place is read. They
// are digits, so each reads as a port, a host or a password alike and changes
// nothing about how the address parses.
const (
	slotProbe = "8642097531"
	otherSlot = "1357924680"
)

// written returns the address with value in the piece at slot, and others in
// each other slot.
func (t addressTemplate) written(slot int, value, others string) string {
	var address strings.Builder
	for i, piece := range t.pieces {
		switch {
		case piece.slot == nil:
			if piece.query {
				address.WriteString(querySeparator(address.String()))
			}
			address.WriteString(piece.text)
		case i == slot:
			address.WriteString(value)
		default:
			address.WriteString(others)
		}
	}
	return address.String()
}

// querySeparator returns what an address that writes address before a query
// parameter writes between them.
func querySeparator(address string) string {
	if strings.Contains(address, "?") {
		return "&"
	}
	return "?"
}

// place returns the part of the address the piece at slot fills. The place of
// a slot of a whole template is its wholePlace, and an address whose own text
// holds a probe value cannot be read, so the place of each slot of it is
// unknown.
func (t addressTemplate) place(slot int) slotPlace {
	if t.whole {
		return t.wholePlace
	}
	text := t.written(-1, "", "")
	if strings.Contains(text, slotProbe) || strings.Contains(text, otherSlot) {
		return placeUnknown
	}
	probe := t.written(slot, slotProbe, otherSlot)
	for _, secret := range dburldisplay.Secrets(probe) {
		if strings.Contains(secret, slotProbe) {
			return placeSecret
		}
	}
	if userInfo, ok := addressUserInfo(probe); ok && strings.Contains(userInfo, slotProbe) && !strings.Contains(userInfo, ":") {
		return placeUserInfo
	}
	return placeOther
}

// addressUserInfo returns the user info of address: the text between its
// "://" and the last "@" before the first "/", "?" or "#" that follows. It
// reports false when the address has no user info.
func addressUserInfo(address string) (string, bool) {
	_, rest, found := strings.Cut(address, "://")
	if !found {
		return "", false
	}
	if end := strings.IndexAny(rest, "/?#"); end >= 0 {
		rest = rest[:end]
	}
	at := strings.LastIndex(rest, "@")
	if at < 0 {
		return "", false
	}
	return rest[:at], true
}

// addressTemplates returns each address the file writes and puts a value
// into, with the locals it names read through their expressions. A candidate
// that writes no "://" of its own is read once for each value a slot of it
// gives that writes one; see [atlasParser.givenAddressTemplates].
func (p atlasParser) addressTemplates() []addressTemplate {
	var templates []addressTemplate
	for _, candidate := range p.addressCandidates {
		if call, ok := candidate.(*hclsyntax.FunctionCallExpr); ok {
			if slots, place, whole := wholeArguments(call); whole {
				pieces := make([]addressPiece, 0, len(slots))
				for _, slot := range slots {
					pieces = append(pieces, addressPiece{slot: slot})
				}
				templates = append(templates, addressTemplate{pieces: pieces, whole: true, wholePlace: place})
				continue
			}
		}
		pieces := p.addressPieces(candidate, make(map[string]struct{}))
		if template, ok := newAddressTemplate(pieces); ok {
			templates = append(templates, template)
			continue
		}
		given := p.givenAddressTemplates(pieces)
		if len(given) == 0 {
			given = p.unreadAddressTemplates(candidate, pieces)
		}
		templates = append(templates, given...)
	}
	return templates
}

// unreadAddressTemplates returns pieces as an address whose every slot is in a
// place that is unknown, when the candidate writes "://" somewhere the pieces
// do not show and does not evaluate: a join() over a concat(), a flatten() or
// a local list, or a template whose address a failing call was to give. The
// candidate's value would have said what the address is, and the error is
// raised where it has none.
func (p atlasParser) unreadAddressTemplates(candidate hclsyntax.Expression, pieces []addressPiece) []addressTemplate {
	contexts := p.contextsOf(candidate)
	if p.evaluatesIn(candidate, contexts) || !p.writesScheme(candidate, contexts) {
		return nil
	}
	var slots []addressPiece
	for _, piece := range pieces {
		if piece.slot != nil {
			slots = append(slots, piece)
		}
	}
	if len(slots) == 0 {
		return nil
	}
	return []addressTemplate{{pieces: slots, whole: true, wholePlace: placeUnknown}}
}

// writesScheme reports whether expr may write the "://" of an address: a
// literal string it holds, or one of the part of a local it names, writes one,
// or a string a part of it gives, or any string a value it names holds, such as
// an element of a list variable, holds one.
func (p atlasParser) writesScheme(expr hclsyntax.Expression, contexts []*hcl.EvalContext) bool {
	literals, values := p.valueParts(expr, contexts, make(map[string]struct{}))
	texts := slices.Concat(literals, p.expressionStrings(expr, contexts, make(map[string]struct{})))
	for _, value := range values {
		for _, ctx := range contexts {
			if evaluated, diags := value.Value(ctx); !diags.HasErrors() {
				appendStringLeaves(&texts, evaluated)
			}
		}
	}
	return slices.ContainsFunc(texts, func(text string) bool { return strings.Contains(text, "://") })
}

// writesBeside reports whether expr may write text of its own beside the value
// it takes, so that the part of the address that value fills cannot be read
// from where expr stands: it is made of more than one part, or of one value
// that holds more than one string, such as a list or an object.
func (p atlasParser) writesBeside(expr hclsyntax.Expression, contexts []*hcl.EvalContext) bool {
	literals, values := p.valueParts(expr, contexts, make(map[string]struct{}))
	switch {
	case len(literals)+len(values) > 1:
		return true
	case len(values) == 1:
		return slices.ContainsFunc(contexts, func(ctx *hcl.EvalContext) bool {
			value, diags := values[0].Value(ctx)
			return !diags.HasErrors() && value.IsWhollyKnown() && !value.IsNull() && value.CanIterateElements()
		})
	default:
		return false
	}
}

// valueParts returns the parts the string expr writes is made of: the text of
// each literal string with text it holds, and each value it names. A local it
// names is read through its expression, as [atlasParser.traversalStrings]
// reads it: as one value where the traversal steps past what the expression
// writes out and the local evaluates, and as the parts of the part it reaches
// otherwise. The key of an object is a name and no part.
func (p atlasParser) valueParts(expr hclsyntax.Expression, contexts []*hcl.EvalContext, seen map[string]struct{}) (literals []string, values []*hclsyntax.ScopeTraversalExpr) {
	keys := objectKeyNodes(expr)
	_ = hclsyntax.VisitAll(expr, func(node hclsyntax.Node) hcl.Diagnostics {
		if _, key := keys[node]; key {
			return nil
		}
		switch node := node.(type) {
		case *hclsyntax.LiteralValueExpr:
			if node.Val.Type() == cty.String && node.Val.IsKnown() && !node.Val.IsNull() && node.Val.AsString() != "" {
				literals = append(literals, node.Val.AsString())
			}
		case *hclsyntax.ScopeTraversalExpr:
			reached, name, resolved := p.localExpression(node.Traversal, seen)
			if reached == nil || (!resolved && p.evaluatesIn(node, contexts)) {
				values = append(values, node)
				return nil
			}
			innerLiterals, innerValues := p.valueParts(reached, p.contextsOf(reached), withSeen(seen, name))
			literals = append(literals, innerLiterals...)
			values = append(values, innerValues...)
		}
		return nil
	})
	return literals, values
}

// givenAddressTemplates returns pieces as an address once for each context a
// slot of them evaluates in to a string that writes "://": that string is
// written in as text, and the other slots are evaluated in that context alone.
// `"${var.base}&password=${var.pw}"` takes its address from var.base this way.
// What the string holds secret is the template's own secret, as for a value
// that carries a whole user info and host.
func (p atlasParser) givenAddressTemplates(pieces []addressPiece) []addressTemplate {
	var templates []addressTemplate
	for i, piece := range pieces {
		if piece.slot == nil {
			continue
		}
		for _, ctx := range p.contextsOf(piece.slot) {
			value, diags := piece.slot.Value(ctx)
			if diags.HasErrors() || !value.IsWhollyKnown() || value.IsNull() || value.Type() != cty.String {
				continue
			}
			text := value.AsString()
			if !strings.Contains(text, "://") {
				continue
			}
			given := addressTemplate{pieces: slices.Clone(pieces), contexts: []*hcl.EvalContext{ctx}}
			given.pieces[i] = addressPiece{text: text}
			for _, secret := range dburldisplay.Secrets(given.written(-1, "", otherSlot)) {
				if strings.Contains(text, secret) {
					given.secrets = append(given.secrets, secret)
				}
			}
			templates = append(templates, given)
		}
	}
	return templates
}

// newAddressTemplate returns pieces as an address. It reports false when the
// pieces put in no value or write no "://".
func newAddressTemplate(pieces []addressPiece) (addressTemplate, bool) {
	template := addressTemplate{pieces: pieces}
	hasSlot := slices.ContainsFunc(pieces, func(piece addressPiece) bool { return piece.slot != nil })
	if !hasSlot || !strings.Contains(template.written(-1, "", ""), "://") {
		return addressTemplate{}, false
	}
	return template, true
}

// wholeArguments returns the arguments of call that each take one place of an
// address whole, because the call does not write the address out, and that
// place: the password argument of urluserinfo() is a secret; the value of
// urlqueryset() under a key the file does not write out, and each argument of
// a format() whose format writes "://" but takes an argument by index or a
// width from an argument, take a place that is unknown, since the key or the
// verb that would say which is unknown.
func wholeArguments(call *hclsyntax.FunctionCallExpr) ([]hclsyntax.Expression, slotPlace, bool) {
	switch call.Name {
	case "urluserinfo":
		return call.Args[min(2, len(call.Args)):], placeSecret, len(call.Args) == 3
	case "urlqueryset":
		_, literalKey := literalArgument(call.Args, 1)
		return call.Args[min(2, len(call.Args)):], placeUnknown, len(call.Args) == 3 && !literalKey
	case "format":
		format, literal := literalArgument(call.Args, 0)
		if !literal || !strings.Contains(format, "://") {
			return nil, placeOther, false
		}
		_, readable := formatPieces(format, call.Args[1:], func(arg hclsyntax.Expression) []addressPiece {
			return []addressPiece{{slot: arg}}
		})
		return call.Args[1:], placeUnknown, !readable
	default:
		return nil, placeOther, false
	}
}

// literalArgument returns the text of args[index] when the file writes it out.
func literalArgument(args []hclsyntax.Expression, index int) (string, bool) {
	if index >= len(args) {
		return "", false
	}
	return literalString(args[index])
}

// addressPieces returns the pieces of the string expr writes: the text of a
// literal, the pieces of each part of a template and of each call to one of
// [addressBuilders] that can be read, and the pieces of the expression of a
// local it names. Anything else is one slot. seen holds the locals already
// read through, so a local that names itself is a slot.
func (p atlasParser) addressPieces(expr hclsyntax.Expression, seen map[string]struct{}) []addressPiece {
	switch node := expr.(type) {
	case *hclsyntax.TemplateWrapExpr:
		return p.addressPieces(node.Wrapped, seen)
	case *hclsyntax.ParenthesesExpr:
		return p.addressPieces(node.Expression, seen)
	case *hclsyntax.TemplateExpr:
		var pieces []addressPiece
		for _, part := range node.Parts {
			pieces = append(pieces, p.addressPieces(part, seen)...)
		}
		return pieces
	case *hclsyntax.ScopeTraversalExpr:
		if reached, name, resolved := p.localExpression(node.Traversal, seen); resolved {
			return p.addressPieces(reached, withSeen(seen, name))
		}
	case *hclsyntax.FunctionCallExpr:
		if pieces, ok := p.callPieces(node, seen); ok {
			return pieces
		}
	}
	if text, ok := literalString(expr); ok {
		return []addressPiece{{text: text}}
	}
	return []addressPiece{{slot: expr}}
}

// callPieces returns the pieces of the string call writes, when it is a call
// to one of [addressBuilders] that can be read: format() with a format the
// file writes out and no verb that takes an argument by index or a width from
// an argument, join() with a separator and lists the file writes out,
// replace() with a string and a substring the file writes out, and
// urlqueryset() with a key the file writes out.
func (p atlasParser) callPieces(call *hclsyntax.FunctionCallExpr, seen map[string]struct{}) ([]addressPiece, bool) {
	pieceOf := func(arg hclsyntax.Expression) []addressPiece { return p.addressPieces(arg, seen) }
	switch call.Name {
	case "format":
		format, ok := literalArgument(call.Args, 0)
		if !ok {
			return nil, false
		}
		return formatPieces(format, call.Args[1:], pieceOf)
	case "join":
		return joinPieces(call.Args, pieceOf)
	case "replace":
		return replacePieces(call.Args, pieceOf)
	case "urlqueryset":
		return queryPieces(call.Args, pieceOf)
	default:
		return nil, false
	}
}

// formatPieces writes format with each verb as the pieces of the argument it
// takes, in order: an argument a plain %s takes keeps its own pieces, and any
// other verb makes its argument one slot. It reports false when a verb takes
// an argument by index, takes its width or precision from an argument, or
// takes an argument the call does not pass.
func formatPieces(format string, args []hclsyntax.Expression, pieceOf func(hclsyntax.Expression) []addressPiece) ([]addressPiece, bool) {
	var pieces []addressPiece
	var text strings.Builder
	next := 0
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			text.WriteByte(format[i])
			continue
		}
		verb := i + 1
		for verb < len(format) && strings.IndexByte("+-# 0123456789.", format[verb]) >= 0 {
			verb++
		}
		switch {
		case verb < len(format) && format[verb] == '%':
			text.WriteByte('%')
		case verb >= len(format) || format[verb] == '[' || format[verb] == '*' || next == len(args):
			return nil, false
		default:
			pieces = append(pieces, addressPiece{text: text.String()})
			text.Reset()
			if verb == i+1 && format[verb] == 's' {
				pieces = append(pieces, pieceOf(args[next])...)
			} else {
				pieces = append(pieces, addressPiece{slot: args[next]})
			}
			next++
		}
		i = verb
	}
	return append(pieces, addressPiece{text: text.String()}), true
}

// joinPieces writes the elements of the lists join() takes, with the
// separator between them. It reports false when the separator or a list is
// not written out in the file.
func joinPieces(args []hclsyntax.Expression, pieceOf func(hclsyntax.Expression) []addressPiece) ([]addressPiece, bool) {
	separator, ok := literalArgument(args, 0)
	if !ok || len(args) < 2 {
		return nil, false
	}
	var pieces []addressPiece
	for _, list := range args[1:] {
		tuple, isTuple := list.(*hclsyntax.TupleConsExpr)
		if !isTuple {
			return nil, false
		}
		for _, element := range tuple.Exprs {
			if len(pieces) > 0 {
				pieces = append(pieces, addressPiece{text: separator})
			}
			pieces = append(pieces, pieceOf(element)...)
		}
	}
	return pieces, true
}

// replacePieces writes the string replace() takes with the pieces of its
// replacement in place of each occurrence of the substring in the text the
// string writes out. It reports false when the substring is not written out
// in the file or is empty.
func replacePieces(args []hclsyntax.Expression, pieceOf func(hclsyntax.Expression) []addressPiece) ([]addressPiece, bool) {
	substring, ok := literalArgument(args, 1)
	if !ok || substring == "" || len(args) != 3 {
		return nil, false
	}
	replacement := pieceOf(args[2])
	var pieces []addressPiece
	for _, piece := range pieceOf(args[0]) {
		if piece.slot != nil {
			pieces = append(pieces, piece)
			continue
		}
		for i, part := range strings.Split(piece.text, substring) {
			if i > 0 {
				pieces = append(pieces, replacement...)
			}
			// The separator a query parameter's name writes goes before
			// its first part.
			pieces = append(pieces, addressPiece{text: part, query: piece.query && i == 0})
		}
	}
	return pieces, true
}

// queryPieces writes the address urlqueryset() takes with the key and the
// pieces of the value as a query parameter after it. It reports false when
// the key is not written out in the file.
func queryPieces(args []hclsyntax.Expression, pieceOf func(hclsyntax.Expression) []addressPiece) ([]addressPiece, bool) {
	key, ok := literalArgument(args, 1)
	if !ok || len(args) != 3 {
		return nil, false
	}
	pieces := append(pieceOf(args[0]), addressPiece{text: url.QueryEscape(key) + "=", query: true})
	return append(pieces, pieceOf(args[2])...), true
}

// literalString returns the text of expr when the file writes it out.
func literalString(expr hclsyntax.Expression) (string, bool) {
	switch expr := expr.(type) {
	case *hclsyntax.LiteralValueExpr:
		if expr.Val.Type() != cty.String || !expr.Val.IsKnown() || expr.Val.IsNull() {
			return "", false
		}
		return expr.Val.AsString(), true
	case *hclsyntax.TemplateExpr:
		if !expr.IsStringLiteral() {
			return "", false
		}
		return literalString(expr.Parts[0])
	default:
		return "", false
	}
}

// localExpressions returns the expression of each local the locals blocks of
// body declare, by name.
func localExpressions(body *hclsyntax.Body) map[string]hclsyntax.Expression {
	locals := make(map[string]hclsyntax.Expression)
	for _, block := range body.Blocks {
		if block.Type != "locals" {
			continue
		}
		for name, attr := range block.Body.Attributes {
			locals[name] = attr.Expr
		}
	}
	return locals
}

// localExpression returns the part of a local's expression that traversal
// names: the item of an object for an attribute or a key the file writes out,
// and the element of a tuple for an index. It returns the part reached and
// false where a step names something the expression does not write out, and
// nil when traversal names no local or a local seen holds.
func (p atlasParser) localExpression(traversal hcl.Traversal, seen map[string]struct{}) (hclsyntax.Expression, string, bool) {
	if len(traversal) < 2 || traversal.RootName() != "local" {
		return nil, "", false
	}
	attr, ok := traversal[1].(hcl.TraverseAttr)
	if !ok {
		return nil, "", false
	}
	expr, declared := p.locals[attr.Name]
	if _, done := seen[attr.Name]; !declared || done {
		return nil, "", false
	}
	for _, step := range traversal[2:] {
		next, found := expressionStep(expr, step)
		if !found {
			return expr, attr.Name, false
		}
		expr = next
	}
	return expr, attr.Name, true
}

// expressionStep returns the part of expr that step names: the value of the
// object item whose key is the attribute or the string index step names, or
// the element of the tuple at the number index it names.
func expressionStep(expr hclsyntax.Expression, step hcl.Traverser) (hclsyntax.Expression, bool) {
	for {
		switch node := expr.(type) {
		case *hclsyntax.ParenthesesExpr:
			expr = node.Expression
			continue
		case *hclsyntax.TemplateWrapExpr:
			expr = node.Wrapped
			continue
		}
		break
	}
	var key cty.Value
	switch step := step.(type) {
	case hcl.TraverseAttr:
		key = cty.StringVal(step.Name)
	case hcl.TraverseIndex:
		key = step.Key
	default:
		return nil, false
	}
	if !key.IsKnown() || key.IsNull() {
		return nil, false
	}
	switch node := expr.(type) {
	case *hclsyntax.ObjectConsExpr:
		return objectItem(node, key)
	case *hclsyntax.TupleConsExpr:
		return tupleElement(node, key)
	default:
		return nil, false
	}
}

// objectItem returns the value of the item of object whose key the file
// writes out as key.
func objectItem(object *hclsyntax.ObjectConsExpr, key cty.Value) (hclsyntax.Expression, bool) {
	if key.Type() != cty.String {
		return nil, false
	}
	for _, item := range object.Items {
		name, diags := item.KeyExpr.Value(nil)
		if !diags.HasErrors() && name.Type() == cty.String && name.IsKnown() && !name.IsNull() && name.AsString() == key.AsString() {
			return item.ValueExpr, true
		}
	}
	return nil, false
}

// tupleElement returns the element of tuple at the number index key.
func tupleElement(tuple *hclsyntax.TupleConsExpr, key cty.Value) (hclsyntax.Expression, bool) {
	if key.Type() != cty.Number {
		return nil, false
	}
	index, accuracy := key.AsBigFloat().Int64()
	if accuracy != big.Exact || index < 0 || index >= int64(len(tuple.Exprs)) {
		return nil, false
	}
	return tuple.Exprs[index], true
}

// withSeen returns seen and name, leaving seen as it is.
func withSeen(seen map[string]struct{}, name string) map[string]struct{} {
	next := make(map[string]struct{}, len(seen)+1)
	for key := range seen {
		next[key] = struct{}{}
	}
	next[name] = struct{}{}
	return next
}

// addressSecrets returns what reaches a secret place of an address the file
// writes, and apart from it what may reach one: what the address holds secret
// in its own text, and what each slot gives; see [atlasParser.slotSecrets].
func (p atlasParser) addressSecrets() (secret, possible []string) {
	for _, template := range p.addressTemplates() {
		secret = append(secret, template.secrets...)
		for i, piece := range template.pieces {
			if piece.slot == nil {
				continue
			}
			sure, maybe := p.slotSecrets(template, i)
			secret = append(secret, sure...)
			possible = append(possible, maybe...)
		}
	}
	return secret, possible
}

// slotSecrets returns what the values the slot at i of template may hold give,
// by the slot's place, and apart from it what they may give: itself whole in
// the password or a secret query parameter; in a user info the slot fills
// whole, the text after its first ":", and itself whole as a value that may be
// a secret where it has none and the slot does not evaluate, since then which
// part it fills is unknown; in a place that is unknown, itself whole as a
// value that may be a secret; and anywhere else, what [dburldisplay.Secrets]
// reads as a secret of the address with the value written in and the value
// holds, as a value that carries a whole user info and host does, unless the
// slot writes text beside the value and does not evaluate, which makes the
// place each value fills unknown.
func (p atlasParser) slotSecrets(template addressTemplate, i int) (secret, possible []string) {
	slot := template.pieces[i].slot
	contexts := template.contexts
	if contexts == nil {
		contexts = p.contextsOf(slot)
	}
	values := p.expressionStrings(slot, contexts, make(map[string]struct{}))
	switch template.place(i) {
	case placeSecret:
		return values, nil
	case placeUnknown:
		return nil, values
	case placeUserInfo:
		passwords, wholes := userInfoSecrets(values)
		if len(wholes) > 0 && !p.evaluatesIn(slot, contexts) {
			return passwords, wholes
		}
		return passwords, nil
	default:
		if p.writesBeside(slot, contexts) && !p.evaluatesIn(slot, contexts) {
			return nil, values
		}
		for _, value := range values {
			for _, found := range dburldisplay.Secrets(template.written(i, value, otherSlot)) {
				if strings.Contains(value, found) {
					secret = append(secret, found)
				}
			}
		}
		return secret, nil
	}
}

// userInfoSecrets returns what values a slot that fills a user info whole may
// hold give: the password after the first ":" of a value, and apart from them
// each value with no ":", which may be a secret where the slot does not
// evaluate, since then which part it fills is unknown.
func userInfoSecrets(values []string) (passwords, wholes []string) {
	for _, value := range values {
		if _, password, hasPassword := strings.Cut(value, ":"); hasPassword {
			passwords = append(passwords, password)
			continue
		}
		wholes = append(wholes, value)
	}
	return passwords, wholes
}

// expressionStrings returns the strings expr and the parts of it evaluate to
// under each of contexts, the --var values of each variable it names, and the
// strings of the part of each local it names that the traversal reaches. A
// literal gives nothing, since the file shows it already: `getenv("PW")` gives
// the variable's value, not "PW". The key of an object gives nothing either:
// it names a value and is not one.
func (p atlasParser) expressionStrings(expr hclsyntax.Expression, contexts []*hcl.EvalContext, seen map[string]struct{}) []string {
	keys := objectKeyNodes(expr)
	var values []string
	_ = hclsyntax.VisitAll(expr, func(node hclsyntax.Node) hcl.Diagnostics {
		part, ok := node.(hclsyntax.Expression)
		if _, key := keys[node]; !ok || key {
			return nil
		}
		if _, literal := part.(*hclsyntax.LiteralValueExpr); literal {
			return nil
		}
		if _, literal := literalString(part); literal {
			return nil
		}
		if traversal, ok := part.(*hclsyntax.ScopeTraversalExpr); ok {
			values = append(values, p.traversalStrings(traversal, contexts, seen)...)
		}
		for _, ctx := range contexts {
			if value, diags := part.Value(ctx); !diags.HasErrors() && value.Type() == cty.String {
				appendStringLeaves(&values, value)
			}
		}
		return nil
	})
	return values
}

// objectKeyNodes returns each node of the object keys expr holds.
func objectKeyNodes(expr hclsyntax.Expression) map[hclsyntax.Node]struct{} {
	keys := make(map[hclsyntax.Node]struct{})
	_ = hclsyntax.VisitAll(expr, func(node hclsyntax.Node) hcl.Diagnostics {
		if key, ok := node.(*hclsyntax.ObjectConsKeyExpr); ok {
			_ = hclsyntax.VisitAll(key, func(inner hclsyntax.Node) hcl.Diagnostics {
				keys[inner] = struct{}{}
				return nil
			})
		}
		return nil
	})
	return keys
}

// traversalStrings returns the --var values of the variable traversal names,
// and the strings of the part of the local it names that the traversal
// reaches. A traversal into a local that steps past what the local's
// expression writes out reads the local's evaluated value, under contexts;
// when that does not evaluate either, it reads the strings of the part it
// reached.
func (p atlasParser) traversalStrings(traversal *hclsyntax.ScopeTraversalExpr, contexts []*hcl.EvalContext, seen map[string]struct{}) []string {
	switch traversal.Traversal.RootName() {
	case "var":
		attr, ok := traversal.Traversal[min(1, len(traversal.Traversal)-1)].(hcl.TraverseAttr)
		value, given := p.varOverride[attr.Name]
		var values []string
		if ok && given {
			appendStringLeaves(&values, value)
		}
		return values
	case "local":
		reached, name, resolved := p.localExpression(traversal.Traversal, seen)
		if reached == nil || (!resolved && p.evaluatesIn(traversal, contexts)) {
			return nil
		}
		return p.expressionStrings(reached, p.contextsOf(reached), withSeen(seen, name))
	default:
		return nil
	}
}

// appendStringLeaves appends each string value holds, itself or as an element
// of a collection at any depth. A key is not appended.
func appendStringLeaves(target *[]string, value cty.Value) {
	if value.IsNull() || !value.IsWhollyKnown() {
		return
	}
	if value.Type() == cty.String {
		if raw := value.AsString(); raw != "" {
			*target = append(*target, raw)
		}
		return
	}
	if !value.CanIterateElements() {
		return
	}
	for iterator := value.ElementIterator(); iterator.Next(); {
		_, element := iterator.Element()
		appendStringLeaves(target, element)
	}
}

// instanceContext is the context of one env instance a for_each expanded,
// with the range of the env block that declares the instance.
type instanceContext struct {
	block hcl.Range
	ctx   *hcl.EvalContext
}

// contextsOf returns the contexts expr evaluates in: the parser's own, and the
// context of each instance of the for_each env block that holds expr, so a
// value read through `each` is found when an error is masked after the
// instance is gone. An expression outside such a block is evaluated in the
// parser's context alone, however many instances the file expands.
func (p atlasParser) contextsOf(expr hclsyntax.Expression) []*hcl.EvalContext {
	contexts := []*hcl.EvalContext{p.ctx}
	if p.instanceContexts == nil {
		return contexts
	}
	at := expr.Range()
	for _, instance := range *p.instanceContexts {
		if instance.block.Filename == at.Filename && instance.block.ContainsOffset(at.Start.Byte) {
			contexts = append(contexts, instance.ctx)
		}
	}
	return contexts
}

// evaluatesIn reports whether expr evaluates to a wholly known value in any of
// contexts.
func (p atlasParser) evaluatesIn(expr hclsyntax.Expression, contexts []*hcl.EvalContext) bool {
	return slices.ContainsFunc(contexts, func(ctx *hcl.EvalContext) bool {
		value, diags := expr.Value(ctx)
		return !diags.HasErrors() && value.IsWhollyKnown()
	})
}

// heldSecrets are the values the parser holds secret beyond what the
// environment and the --var flags show.
type heldSecrets struct {
	// secret are the values the file declares sensitive, which include what a
	// token data source minted, and what reaches a secret place of an address
	// the file writes.
	secret []string
	// possible are the values that may reach one: a value whose place in an
	// address is unknown.
	possible []string
}

// heldSecrets returns the values the parser holds secret, each once.
func (p atlasParser) heldSecrets() heldSecrets {
	var held heldSecrets
	if p.sensitiveValues != nil {
		held.secret = append(held.secret, *p.sensitiveValues...)
	}
	secret, possible := p.addressSecrets()
	held.secret = compactStrings(append(held.secret, secret...))
	held.possible = compactStrings(possible)
	return held
}

// display returns the values an atlas.hcl error is masked against: both kinds,
// since HCL quotes the argument of a call that fails whatever place the
// argument takes.
func (h heldSecrets) display() []string {
	return append(slices.Clip(h.secret), h.possible...)
}

// compactStrings returns values sorted, each once.
func compactStrings(values []string) []string {
	slices.Sort(values)
	return slices.Compact(values)
}

// displaySecrets returns the values an atlas.hcl error is masked against
// beyond what the environment and the --var flags show.
func (p atlasParser) displaySecrets() []string {
	return p.heldSecrets().display()
}

// withProgramSecrets gives secrets to each external_schema program configs
// run, an env's own and each part of a composite_schema, so the program's
// diagnostics are masked against them. Every program shares the one slice,
// clipped so an append to it copies.
func withProgramSecrets(configs []Config, secrets []string) {
	secrets = slices.Clip(secrets)
	for i := range configs {
		if configs[i].ExternalSchema.Origin == AtlasFileName {
			configs[i].ExternalSchema.Secrets = secrets
		}
		for _, composite := range configs[i].compositeSchemas {
			for _, part := range composite.Parts {
				if part.ExternalSchema != nil {
					part.ExternalSchema.Secrets = secrets
				}
			}
		}
	}
}

// varArguments returns each --var value as the `name=value` an atlas.hcl error
// is masked against, so a value that is an address loses its password, and a
// value whose name says it is a secret is masked as one.
func (p atlasParser) varArguments() []string {
	var arguments []string
	for name, value := range p.varOverride {
		var values []string
		appendStringLeaves(&values, value)
		for _, text := range values {
			arguments = append(arguments, name+"="+text)
		}
	}
	return arguments
}

// maskError returns err with each of secrets, each secret the environment and
// the --var flags show, taken out of its text, and err itself when the text
// holds none. The result unwraps to err.
func (p atlasParser) maskError(err error, secrets []string) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	masked := secretdisplay.SanitizeKnown(maskQuotedValues(text, secrets), os.Environ(), p.varArguments(), secrets)
	if masked == text {
		return err
	}
	return maskedError{text: masked, err: err}
}

// maskQuotedValues takes each of values too short for [secretmask.Mask] to take
// out wherever it occurs out of text where an HCL diagnostic quotes it whole, as
// a function that refused its argument does: `cannot convert "x" to number`. The
// quotes are HCL's, so what stands between them is the value the call was given
// rather than a word of the message, and its place in the message says nothing
// of its length.
func maskQuotedValues(text string, values []string) string {
	for _, value := range values {
		if value != "" && !secretmask.MasksAnywhere(value) {
			text = strings.ReplaceAll(text, strconv.Quote(value), `"redacted"`)
		}
	}
	return text
}

// maskedError is an error whose text the parser masked.
type maskedError struct {
	text string
	err  error
}

func (e maskedError) Error() string { return e.text }
func (e maskedError) Unwrap() error { return e.err }
