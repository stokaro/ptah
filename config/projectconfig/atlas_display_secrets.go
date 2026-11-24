package projectconfig

import (
	"fmt"
	"os"
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
// It is told what reaches the password of an address the file writes. The
// places are read from the file before anything is evaluated: a template, a
// format() call whose format writes "://", and the password argument of
// urluserinfo(). The values are read when an error is masked, from each part
// of the expression at such a place: what the part evaluates to, the --var
// values of a variable it names, and the same of the expression of a local it
// names.

// addressTemplate is an address the file writes, with each value put into it
// written as a marker.
type addressTemplate struct {
	// address is the text of the address, each value put into it written as
	// its marker. It is empty where each value is a secret whole: the
	// password argument of urluserinfo(), and the arguments of a format()
	// whose verbs cannot be matched to them.
	address string
	// slots are the expressions of the values put into the address, in the
	// order of their markers.
	slots []hclsyntax.Expression
}

// slotMarker is the text a value stands as in an address. It is letters and
// digits only, so it changes nothing about how the address reads, and its
// index ends in a letter, so no marker holds another.
func slotMarker(index int) string {
	return fmt.Sprintf("ptahslot%dx", index)
}

// addressTemplates returns each place in body that writes an address and puts
// a value into it.
func addressTemplates(body *hclsyntax.Body) []addressTemplate {
	var templates []addressTemplate
	_ = hclsyntax.VisitAll(body, func(node hclsyntax.Node) hcl.Diagnostics {
		var found addressTemplate
		var ok bool
		switch node := node.(type) {
		case *hclsyntax.TemplateExpr:
			found, ok = templateAddress(node)
		case *hclsyntax.FunctionCallExpr:
			found, ok = callAddress(node)
		}
		if ok {
			templates = append(templates, found)
		}
		return nil
	})
	return templates
}

// templateAddress reads template as an address. It reports false when the
// template puts in no value or writes no "://".
func templateAddress(template *hclsyntax.TemplateExpr) (addressTemplate, bool) {
	var address strings.Builder
	var slots []hclsyntax.Expression
	for _, part := range template.Parts {
		if text, ok := literalString(part); ok {
			address.WriteString(text)
			continue
		}
		address.WriteString(slotMarker(len(slots)))
		slots = append(slots, part)
	}
	return newAddressTemplate(address.String(), slots)
}

// callAddress reads the address a call writes: the format of a format() call
// with each verb as the argument it takes, or the password argument of
// urluserinfo(), which is a secret whatever address it joins. A format() whose
// format writes "://" but takes an argument by index or a width from an
// argument leaves unknown which argument a verb takes, so each of its
// arguments counts as a secret. It reports false for any other call, and for a
// format() whose format is not a literal.
func callAddress(call *hclsyntax.FunctionCallExpr) (addressTemplate, bool) {
	switch {
	case call.Name == "urluserinfo" && len(call.Args) == 3:
		return addressTemplate{slots: call.Args[2:]}, true
	case call.Name == "format" && len(call.Args) > 1:
		format, ok := literalString(call.Args[0])
		if !ok || !strings.Contains(format, "://") {
			return addressTemplate{}, false
		}
		if found, ok := formatAddress(format, call.Args[1:]); ok {
			return found, true
		}
		return addressTemplate{slots: call.Args[1:]}, true
	default:
		return addressTemplate{}, false
	}
}

// formatAddress writes format with each verb as the marker of the argument
// it takes, in order. It reports false when a verb takes an argument by
// index, takes its width or precision from an argument, or takes an argument
// the call does not pass.
func formatAddress(format string, args []hclsyntax.Expression) (addressTemplate, bool) {
	var address strings.Builder
	var slots []hclsyntax.Expression
	for i := 0; i < len(format); i++ {
		if format[i] != '%' {
			address.WriteByte(format[i])
			continue
		}
		verb := i + 1
		for verb < len(format) && strings.IndexByte("+-# 0123456789.", format[verb]) >= 0 {
			verb++
		}
		switch {
		case verb < len(format) && format[verb] == '%':
			address.WriteByte('%')
		case verb >= len(format) || format[verb] == '[' || format[verb] == '*' || len(slots) == len(args):
			return addressTemplate{}, false
		default:
			address.WriteString(slotMarker(len(slots)))
			slots = append(slots, args[len(slots)])
		}
		i = verb
	}
	return newAddressTemplate(address.String(), slots)
}

// newAddressTemplate returns address with its slots. It reports false when
// address puts in no value or is no address.
func newAddressTemplate(address string, slots []hclsyntax.Expression) (addressTemplate, bool) {
	if len(slots) == 0 || !strings.Contains(address, "://") {
		return addressTemplate{}, false
	}
	return addressTemplate{address: address, slots: slots}, true
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

// addressSecrets returns what reaches the password of an address the file
// writes. Each value a slot may hold is written into the address in place of
// the slot's marker, and gives what [dburldisplay.Secrets] reads as a secret
// of the result and the value holds: the whole value in the password, and
// only the password of a value that holds `app:password` or a whole user info
// and host. A slot of a template with no address gives each value whole.
func (p atlasParser) addressSecrets() []string {
	var secrets []string
	for _, template := range p.addressTemplates {
		for i, slot := range template.slots {
			for _, value := range p.expressionStrings(slot, make(map[string]struct{})) {
				if template.address == "" {
					secrets = append(secrets, value)
					continue
				}
				written := strings.Replace(template.address, slotMarker(i), value, 1)
				for _, secret := range dburldisplay.Secrets(written) {
					if strings.Contains(value, secret) {
						secrets = append(secrets, secret)
					}
				}
			}
		}
	}
	return secrets
}

// expressionStrings returns each string expr and the parts of it evaluate to
// under the parser's context, the --var values of each variable it names, and
// the same of the expression of each local it names that seen does not hold.
// A part that does not evaluate gives nothing, and a literal gives nothing,
// since the file shows it already: `getenv("PW")` gives the variable's value,
// not "PW".
func (p atlasParser) expressionStrings(expr hclsyntax.Expression, seen map[string]struct{}) []string {
	var values []string
	_ = hclsyntax.VisitAll(expr, func(node hclsyntax.Node) hcl.Diagnostics {
		part, ok := node.(hclsyntax.Expression)
		if !ok {
			return nil
		}
		if _, literal := literalString(part); literal {
			return nil
		}
		if traversal, ok := part.(*hclsyntax.ScopeTraversalExpr); ok {
			values = append(values, p.traversalStrings(traversal.Traversal, seen)...)
		}
		if value, diags := part.Value(p.ctx); !diags.HasErrors() {
			appendSensitiveStrings(&values, value)
		}
		return nil
	})
	return values
}

// traversalStrings returns the --var values of the variable traversal names,
// and the strings of the expression of the local it names.
func (p atlasParser) traversalStrings(traversal hcl.Traversal, seen map[string]struct{}) []string {
	if len(traversal) < 2 {
		return nil
	}
	attr, ok := traversal[1].(hcl.TraverseAttr)
	if !ok {
		return nil
	}
	switch traversal.RootName() {
	case "var":
		var values []string
		if value, ok := p.varOverride[attr.Name]; ok {
			appendSensitiveStrings(&values, value)
		}
		return values
	case "local":
		expr, declared := p.locals[attr.Name]
		if _, done := seen[attr.Name]; !declared || done {
			return nil
		}
		seen[attr.Name] = struct{}{}
		return p.expressionStrings(expr, seen)
	default:
		return nil
	}
}

// displaySecrets returns the values an atlas.hcl error is masked against
// beyond what the environment and the --var flags show: the values the parser
// holds sensitive, which include what a token data source minted, and what
// reaches the password of an address the file writes.
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
