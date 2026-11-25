package projectconfig

import (
	"math/big"
	"net/url"
	"os"
	"slices"
	"strings"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclsyntax"
	"github.com/zclconf/go-cty/cty"

	"ptah.run/internal/dburldisplay"
	"ptah.run/internal/secretdisplay"
)

// HCL quotes the argument of a call that fails, so an atlas.hcl error can
// carry a password that no variable declares sensitive: `url =
// "postgres://app:${tonumber(var.pw)}@host/db"` fails with `cannot convert
// "<the password>" to number`. The value never becomes an address, so no
// address in the error shows it as a secret, and the masking has to be told.
//
// It is told what reaches a secret place of an address the file writes. The
// expressions that may write one are found before anything is evaluated: each
// template, and each call to one of [addressBuilders]. When an error is masked,
// each is read as an address: the text it writes out, and a slot for each
// value it puts in. A local it names is read through its expression, down to
// the attribute or the element the traversal names, so `local.db.password`
// reads the password item of the object `local.db` declares and nothing else
// of it. Each slot's place is read once, with the other slots in place, and
// each value the slot may hold is read for what that place keeps secret.

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
}

// addressTemplate is an address the file writes, as its pieces in order.
type addressTemplate struct {
	pieces []addressPiece
	// whole is true where each value is a secret whole and the pieces are the
	// slots alone: the password argument of urluserinfo(), and the value
	// arguments of a call whose address cannot be read.
	whole bool
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
			address.WriteString(piece.text)
		case i == slot:
			address.WriteString(value)
		default:
			address.WriteString(others)
		}
	}
	return address.String()
}

// place returns the part of the address the piece at slot fills. An address
// whose own text holds a probe value cannot be read this way, and every slot
// of it counts as a secret.
func (t addressTemplate) place(slot int) slotPlace {
	text := t.written(-1, "", "")
	if strings.Contains(text, slotProbe) || strings.Contains(text, otherSlot) {
		return placeSecret
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
// into, with the locals it names read through their expressions.
func (p atlasParser) addressTemplates() []addressTemplate {
	var templates []addressTemplate
	for _, candidate := range p.addressCandidates {
		if call, ok := candidate.(*hclsyntax.FunctionCallExpr); ok {
			if slots, whole := wholeSecretArguments(call); whole {
				pieces := make([]addressPiece, 0, len(slots))
				for _, slot := range slots {
					pieces = append(pieces, addressPiece{slot: slot})
				}
				templates = append(templates, addressTemplate{pieces: pieces, whole: true})
				continue
			}
		}
		if template, ok := newAddressTemplate(p.addressPieces(candidate, make(map[string]struct{}))); ok {
			templates = append(templates, template)
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

// wholeSecretArguments returns the arguments of call that are each a secret
// whole, because the call puts them in a secret place of an address it does
// not write out: the password argument of urluserinfo(), the value of
// urlqueryset() under a key the file does not write out, and each argument of
// a format() whose format writes "://" but takes an argument by index or a
// width from an argument, which leaves unknown which argument a verb takes.
func wholeSecretArguments(call *hclsyntax.FunctionCallExpr) ([]hclsyntax.Expression, bool) {
	switch call.Name {
	case "urluserinfo":
		return call.Args[min(2, len(call.Args)):], len(call.Args) == 3
	case "urlqueryset":
		_, literalKey := literalArgument(call.Args, 1)
		return call.Args[min(2, len(call.Args)):], len(call.Args) == 3 && !literalKey
	case "format":
		format, literal := literalArgument(call.Args, 0)
		if !literal || !strings.Contains(format, "://") {
			return nil, false
		}
		_, readable := formatPieces(format, call.Args[1:], func(arg hclsyntax.Expression) []addressPiece {
			return []addressPiece{{slot: arg}}
		})
		return call.Args[1:], !readable
	default:
		return nil, false
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
			pieces = append(pieces, addressPiece{text: part})
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
	pieces := pieceOf(args[0])
	separator := "?"
	if strings.Contains(addressTemplate{pieces: pieces}.written(-1, "", ""), "?") {
		separator = "&"
	}
	pieces = append(pieces, addressPiece{text: separator + url.QueryEscape(key) + "="})
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
// writes. Each value a slot may hold gives, by the slot's place: itself whole
// in the password or a secret query parameter; in a user info the slot fills
// whole, the text after its first ":", or itself whole where it has none and
// the slot does not evaluate, since then which part it fills is unknown; and
// anywhere else, what [dburldisplay.Secrets] reads as a secret of the address
// with the value written in and the value holds, as a value that carries a
// whole user info and host does. A slot of a template with no address gives
// each value whole.
func (p atlasParser) addressSecrets() []string {
	var secrets []string
	for _, template := range p.addressTemplates() {
		for i, piece := range template.pieces {
			if piece.slot == nil {
				continue
			}
			values := p.expressionStrings(piece.slot, make(map[string]struct{}))
			place := placeSecret
			if !template.whole {
				place = template.place(i)
			}
			switch place {
			case placeSecret:
				secrets = append(secrets, values...)
			case placeUserInfo:
				secrets = append(secrets, userInfoSecrets(values, p.evaluates(piece.slot))...)
			default:
				for _, value := range values {
					for _, secret := range dburldisplay.Secrets(template.written(i, value, otherSlot)) {
						if strings.Contains(value, secret) {
							secrets = append(secrets, secret)
						}
					}
				}
			}
		}
	}
	return secrets
}

// userInfoSecrets returns the secrets of values a slot that fills a user info
// whole may hold: the password after the first ":" of a value, and a value
// with no ":" whole when the slot does not evaluate.
func userInfoSecrets(values []string, evaluates bool) []string {
	var secrets []string
	for _, value := range values {
		_, password, hasPassword := strings.Cut(value, ":")
		switch {
		case hasPassword:
			secrets = append(secrets, password)
		case !evaluates:
			secrets = append(secrets, value)
		}
	}
	return secrets
}

// expressionStrings returns the strings expr and the parts of it evaluate to
// under each context of the parser, the --var values of each variable it
// names, and the strings of the part of each local it names that the
// traversal reaches. A literal gives nothing, since the file shows it
// already: `getenv("PW")` gives the variable's value, not "PW". The key of an
// object gives nothing either: it names a value and is not one.
func (p atlasParser) expressionStrings(expr hclsyntax.Expression, seen map[string]struct{}) []string {
	keys := objectKeyNodes(expr)
	contexts := p.evalContexts()
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
			values = append(values, p.traversalStrings(traversal, seen)...)
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
// expression writes out reads the local's evaluated value; when that does not
// evaluate either, it reads the strings of the part it reached.
func (p atlasParser) traversalStrings(traversal *hclsyntax.ScopeTraversalExpr, seen map[string]struct{}) []string {
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
		if reached == nil || (!resolved && p.evaluates(traversal)) {
			return nil
		}
		return p.expressionStrings(reached, withSeen(seen, name))
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

// evalContexts returns the contexts the parser evaluates in: its own, and the
// context of each env instance a for_each expanded, so a value read through
// `each` is found when an error is masked after the instance is gone.
func (p atlasParser) evalContexts() []*hcl.EvalContext {
	contexts := []*hcl.EvalContext{p.ctx}
	if p.instanceContexts != nil {
		contexts = append(contexts, *p.instanceContexts...)
	}
	return contexts
}

// evaluates reports whether expr evaluates to a wholly known value in any
// context of the parser.
func (p atlasParser) evaluates(expr hclsyntax.Expression) bool {
	return slices.ContainsFunc(p.evalContexts(), func(ctx *hcl.EvalContext) bool {
		value, diags := expr.Value(ctx)
		return !diags.HasErrors() && value.IsWhollyKnown()
	})
}

// displaySecrets returns the values an atlas.hcl error is masked against
// beyond what the environment and the --var flags show: the values the parser
// holds sensitive, which include what a token data source minted, and what
// reaches a secret place of an address the file writes.
func (p atlasParser) displaySecrets() []string {
	var secrets []string
	if p.sensitiveValues != nil {
		secrets = append(secrets, *p.sensitiveValues...)
	}
	return append(secrets, p.addressSecrets()...)
}

// varArguments returns each --var value as the `name=value` an atlas.hcl error
// is masked against, so a value that is an address loses its password, and a
// value whose name says it is a secret is masked as one.
func (p atlasParser) varArguments() []string {
	var arguments []string
	for name, value := range p.varOverride {
		var values []string
		appendSensitiveStrings(&values, value)
		for _, text := range values {
			arguments = append(arguments, name+"="+text)
		}
	}
	return arguments
}

// maskError returns err with each secret of the parser taken out of its text,
// and err itself when the text holds none. The result unwraps to err.
func (p atlasParser) maskError(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	masked := secretdisplay.SanitizeKnown(text, os.Environ(), p.varArguments(), p.displaySecrets())
	if masked == text {
		return err
	}
	return maskedError{text: masked, err: err}
}

// maskedError is an error whose text the parser masked.
type maskedError struct {
	text string
	err  error
}

func (e maskedError) Error() string { return e.text }
func (e maskedError) Unwrap() error { return e.err }
