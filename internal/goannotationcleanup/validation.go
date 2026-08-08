package goannotationcleanup

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/annotationmeta"
	"go.5x5.cz/ptah/internal/annotationparse"
)

// ValidateParsed proves that every planned removal came from a supported source
// attachment and that directives the parser can collapse or skip produced the
// corresponding schema object. Callers must run this against the same captured
// source view before destructive cleanup.
func (p *Plan) ValidateParsed(database *goschema.Database) error {
	var validationErrors []error
	for _, removal := range p.removals() {
		if err := annotationAttachmentError(removal); err != nil {
			validationErrors = append(validationErrors, err)
			continue
		}
		if !annotationRepresented(removal, database) {
			validationErrors = append(validationErrors, fmt.Errorf(
				"%w: %s:%d: //%s did not produce a schema object",
				ErrUnexportedAnnotation,
				removal.annotation.Path,
				removal.annotation.Line,
				removal.annotation.Directive,
			))
		}
	}
	return errors.Join(validationErrors...)
}

func (p *Plan) validateAttachments() error {
	var validationErrors []error
	for _, removal := range p.removals() {
		if err := annotationAttachmentError(removal); err != nil {
			validationErrors = append(validationErrors, err)
		}
	}
	return errors.Join(validationErrors...)
}

func (p *Plan) removals() []removedLine {
	var removals []removedLine
	for _, change := range p.changes {
		removals = append(removals, change.removed...)
	}
	return removals
}

func annotationAttachmentError(removal removedLine) error {
	directive, ok := annotationmeta.Lookup(removal.annotation.Directive)
	if !ok || !annotationmeta.AllowsScope(directive, removal.scope) {
		return annotationScopeError(removal, directive)
	}
	if removal.annotation.Directive == "ptah:schema:field" && !removal.namedField {
		return fmt.Errorf(
			"%w: %s:%d: //%s requires a named field",
			ErrUnexportedAnnotation,
			removal.annotation.Path,
			removal.annotation.Line,
			removal.annotation.Directive,
		)
	}
	return nil
}

func annotationScopeError(removal removedLine, directive annotationmeta.Directive) error {
	allowed := make([]string, len(directive.Scopes))
	for i, scope := range directive.Scopes {
		allowed[i] = string(scope)
	}
	return fmt.Errorf(
		"%w: %s:%d: //%s has %s scope; expected %s scope",
		ErrUnexportedAnnotation,
		removal.annotation.Path,
		removal.annotation.Line,
		removal.annotation.Directive,
		removal.scope,
		strings.Join(allowed, " or "),
	)
}

func annotationRepresented(removal removedLine, database *goschema.Database) bool {
	if database == nil {
		return false
	}
	switch removal.annotation.Directive {
	case "ptah:schema:enum":
		return slices.ContainsFunc(database.Enums, func(enum goschema.Enum) bool {
			return enum.Name == removal.values["name"] &&
				slices.Equal(enum.Values, splitAnnotationList(removal.values["values"]))
		})
	case "ptah:schema:rls:policy":
		return slices.ContainsFunc(database.RLSPolicies, func(policy goschema.RLSPolicy) bool {
			return policy.Name == removal.values["name"] &&
				policy.Table == removal.values["table"] &&
				policy.PolicyFor == removal.values["for"] &&
				policy.ToRoles == removal.values["to"] &&
				policy.UsingExpression == removal.values["using"] &&
				policy.WithCheckExpression == removal.values["with_check"] &&
				policy.Comment == removal.values["comment"]
		})
	case "ptah:schema:rls:enable":
		return slices.ContainsFunc(database.RLSEnabledTables, func(table goschema.RLSEnabledTable) bool {
			return table.Table == removal.values["table"] && table.Comment == removal.values["comment"]
		})
	default:
		return true
	}
}

func splitAnnotationList(value string) []string {
	parts := strings.Split(value, ",")
	values := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			values = append(values, value)
		}
	}
	return values
}

func annotationAttributes(comment string) ([]string, map[string]string) {
	annotations := annotationparse.Scan(comment)
	if len(annotations) == 0 {
		return nil, nil
	}
	attributes := make([]string, len(annotations[0].Attributes))
	values := make(map[string]string, len(annotations[0].Attributes))
	for i, attribute := range annotations[0].Attributes {
		attributes[i] = attribute.Name
		values[attribute.Name] = attribute.DecodedValue
	}
	return attributes, values
}
