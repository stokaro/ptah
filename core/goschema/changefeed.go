package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"slices"

	ptahast "ptah.run/core/ast"
	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbchangefeed"
)

// pendingChangefeed is a changefeed annotation waiting for the table it
// belongs to, which the file may declare after it.
type pendingChangefeed struct {
	structName string
	table      string
	spec       ptahast.ChangefeedSpec
	ctx        annotationErrorContext
}

// pendingConsumer is a consumer annotation waiting for its changefeed.
type pendingConsumer struct {
	structName string
	table      string
	changefeed string
	consumer   ptahast.TopicConsumerSpec
	ctx        annotationErrorContext
}

// parseChangefeedComment reads a YDB changefeed declaration. The changefeed
// belongs to the table the struct maps to, or to the table its `table`
// attribute names, and is attached to it once the whole file is read.
func (s *schemaParseState) parseChangefeedComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:changefeed", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	spec, err := ydbchangefeed.ParseDeclaration(kv)
	if err != nil {
		return changefeedAttributeError(ctx, "ptah:schema:changefeed", err)
	}
	s.changefeeds = append(s.changefeeds, pendingChangefeed{
		structName: structName,
		table:      kv[ydbchangefeed.AttributeTable],
		spec:       spec,
		ctx:        ctx,
	})
	return nil
}

// parseChangefeedConsumerComment reads a consumer of a YDB changefeed's
// topic, attached to its changefeed once the whole file is read.
func (s *schemaParseState) parseChangefeedConsumerComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//ptah:schema:changefeed:consumer", structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	consumer, err := ydbchangefeed.ParseConsumer(kv)
	if err != nil {
		return changefeedAttributeError(ctx, "ptah:schema:changefeed:consumer", err)
	}
	s.consumers = append(s.consumers, pendingConsumer{
		structName: structName,
		table:      kv[ydbchangefeed.AttributeTable],
		changefeed: kv[ydbchangefeed.AttributeChangefeed],
		consumer:   consumer,
		ctx:        ctx,
	})
	return nil
}

// changefeedAttributeError reports a value a changefeed or consumer
// declaration cannot carry, naming the attribute.
func changefeedAttributeError(ctx annotationErrorContext, directive string, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbchangefeed.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	return parseErr
}

// attachChangefeeds gives each table of the file the changefeeds and the
// consumers declared for it. A changefeed whose table the file does not
// declare is refused rather than dropped, because a stream nobody gets is a
// declaration with no effect.
func (s *schemaParseState) attachChangefeeds() error {
	for _, pending := range s.changefeeds {
		index, err := s.changefeedTable(pending.structName, pending.table, pending.ctx, "ptah:schema:changefeed")
		if err != nil {
			return err
		}
		table := &s.tableDirectives[index]
		if slices.ContainsFunc(table.Changefeeds, func(have ptahast.ChangefeedSpec) bool {
			return have.Name == pending.spec.Name
		}) {
			return s.changefeedPlacementError(pending.ctx, "ptah:schema:changefeed",
				fmt.Sprintf("table %q declares changefeed %q twice", table.Name, pending.spec.Name))
		}
		table.Changefeeds = append(table.Changefeeds, pending.spec)
	}
	for _, pending := range s.consumers {
		index, err := s.changefeedTable(pending.structName, pending.table, pending.ctx, "ptah:schema:changefeed:consumer")
		if err != nil {
			return err
		}
		table := &s.tableDirectives[index]
		feed := slices.IndexFunc(table.Changefeeds, func(have ptahast.ChangefeedSpec) bool {
			return have.Name == pending.changefeed
		})
		if feed < 0 {
			return s.changefeedPlacementError(pending.ctx, "ptah:schema:changefeed:consumer",
				fmt.Sprintf("table %q declares no changefeed %q for consumer %q", table.Name, pending.changefeed,
					pending.consumer.Name))
		}
		table.Changefeeds[feed].Consumers = append(table.Changefeeds[feed].Consumers, pending.consumer)
	}
	return nil
}

// changefeedTable finds the table a changefeed annotation belongs to: the one
// its table attribute names, or the one its struct maps to.
func (s *schemaParseState) changefeedTable(structName, table string, ctx annotationErrorContext, directive string) (int, error) {
	schemaName, tableName := tableDirectiveName("", table)
	index := slices.IndexFunc(s.tableDirectives, func(declared schemamodel.Table) bool {
		if table == "" {
			return declared.StructName == structName
		}
		return declared.Name == tableName && (schemaName == "" || declared.Schema == schemaName)
	})
	if index >= 0 {
		return index, nil
	}
	if table == "" {
		return -1, s.changefeedPlacementError(ctx, directive,
			fmt.Sprintf("struct %s maps to no table in this file; name the table with the table attribute", structName))
	}
	return -1, s.changefeedPlacementError(ctx, directive,
		fmt.Sprintf("table %q is not declared in this file, and a changefeed is declared beside its table", table))
}

// changefeedPlacementError reports an annotation whose table or changefeed
// cannot be found.
func (s *schemaParseState) changefeedPlacementError(ctx annotationErrorContext, directive, reason string) error {
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%s on %s at %s", reason, ctx.directive, ctx.location),
	}
}
