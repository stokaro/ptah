package goschema

import (
	"cmp"
	"errors"
	"fmt"
	"go/ast"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/ydbchangefeed"
)

// pendingChangefeed is a changefeed annotation waiting for the table it
// belongs to, which the file may declare after it.
type pendingChangefeed struct {
	structName string
	table      string
	spec       ydbschema.ChangefeedSpec
	ctx        annotationErrorContext
}

// pendingConsumer is a consumer annotation waiting for its changefeed.
type pendingConsumer struct {
	structName string
	table      string
	changefeed string
	consumer   ydbtopic.ConsumerSpec
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
	consumer, err := ydbtopic.ParseConsumer(kv)
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
	if declared, ok := errors.AsType[*ydbtopic.DeclarationError](err); ok {
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
		s.featureObjects, err = s.featureObjects.With(ydbschema.DesiredObject(table.Schema, table.Name, pending.spec))
		if errors.Is(err, schemaext.ErrDuplicate) {
			return s.placementError(pending.ctx, "ptah:schema:changefeed", fmt.Sprintf("table %q declares changefeed %q twice", table.Name, pending.spec.Name))
		}
		if err != nil {
			return err
		}
	}
	for _, pending := range s.consumers {
		index, err := s.changefeedTable(pending.structName, pending.table, pending.ctx, "ptah:schema:changefeed:consumer")
		if err != nil {
			return err
		}
		table := &s.tableDirectives[index]
		ref := ydbschema.ChangefeedRef(table.Schema, table.Name, pending.changefeed)
		object, found, err := s.featureObjects.Get(ref)
		if err != nil {
			return err
		}
		if !found {
			return s.placementError(pending.ctx, "ptah:schema:changefeed:consumer",
				fmt.Sprintf("table %q declares no changefeed %q for consumer %q", table.Name, pending.changefeed, pending.consumer.Name))
		}
		value, ok := object.Value.(*ydbschema.DesiredChangefeed)
		if !ok {
			return fmt.Errorf("%w: changefeed consumer requires a desired changefeed", schemaext.ErrInvalidValue)
		}
		value.Spec.Consumers = append(value.Spec.Consumers, pending.consumer)
		s.featureObjects, err = s.featureObjects.Replace(object)
		if err != nil {
			return err
		}
	}
	var err error
	s.featureCoverage, err = sourceCoverage(s.featureLimits)
	return err
}

// changefeedTable finds the table a changefeed annotation belongs to: the one
// its table attribute names, or the one its struct maps to.
func (s *schemaParseState) changefeedTable(structName, table string, ctx annotationErrorContext, directive string) (int, error) {
	return s.ownerTable(structName, table, ctx, directive, "a changefeed")
}

// ownerTable finds the table an annotation declaring one of the table's parts
// belongs to: the one its table attribute names, or the one its struct maps
// to. object names the part, as the refusal reads it.
//
// A table attribute that names no schema matches a table of that name in any
// schema of the file, and is refused when the file declares it in more than
// one: attaching the part to whichever came first would partition, or stream
// from, a table the author may not have meant.
func (s *schemaParseState) ownerTable(structName, table string, ctx annotationErrorContext, directive, object string) (int, error) {
	if table == "" {
		index := slices.IndexFunc(s.tableDirectives, func(declared schemamodel.Table) bool { return declared.StructName == structName })
		if index >= 0 {
			return index, nil
		}
		return -1, s.placementError(ctx, directive,
			fmt.Sprintf("struct %s maps to no table in this file; name the table with the table attribute", structName))
	}
	schemaName, tableName := tableDirectiveName("", table)
	var matches []int
	for i, declared := range s.tableDirectives {
		if declared.Name == tableName && (schemaName == "" || declared.Schema == schemaName) {
			matches = append(matches, i)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return -1, s.placementError(ctx, directive,
			fmt.Sprintf("table %q is not declared in this file, and %s is declared beside its table", table, object))
	}
	schemas := make([]string, 0, len(matches))
	for _, index := range matches {
		schemas = append(schemas, strconv.Quote(cmp.Or(s.tableDirectives[index].Schema, "(default)")))
	}
	return -1, s.placementError(ctx, directive,
		fmt.Sprintf("table %q is declared in schemas %s; name the schema in the table attribute", table, strings.Join(schemas, " and ")))
}

// placementError reports an annotation that cannot be placed: one whose table
// or changefeed cannot be found, or that declares a part its table already
// has.
func (s *schemaParseState) placementError(ctx annotationErrorContext, directive, reason string) error {
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%s on %s at %s", reason, ctx.directive, ctx.location),
	}
}
