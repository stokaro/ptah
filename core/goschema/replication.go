package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"slices"
	"strings"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/dialect/ydb/ydbreplication"
)

// pendingReplication is a replication annotation waiting for the items the
// same file may declare after it. It becomes a feature object once the file is
// read.
type pendingReplication struct {
	schema     string
	name       string
	structName string
	spec       ydbreplication.ReplicationSpec
	ctx        annotationErrorContext
}

// pendingReplicationItem is an item annotation waiting for the replication it
// belongs to, which the same file may declare after it.
type pendingReplicationItem struct {
	schema      string
	replication string
	item        ydbreplication.Item
	ctx         annotationErrorContext
}

// parseAsyncReplicationComment reads a YDB async replication declaration.
//
// There is no dialect scope here, for the reason a synonym has none: a
// replication is a YDB object and nothing else, and every other target
// refuses one.
func (s *schemaParseState) parseAsyncReplicationComment(comment *ast.Comment, structName string) error {
	const directive = "ptah:schema:async_replication"
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+directive, structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	spec, err := ydbreplication.ParseReplication(kv)
	if err != nil {
		return replicationAttributeError(ctx, directive, err)
	}
	s.asyncReplications = append(s.asyncReplications, pendingReplication{
		schema:     strings.Trim(strings.TrimSpace(kv[ydbreplication.AttributeSchema]), "/"),
		name:       strings.TrimSpace(kv[ydbreplication.AttributeName]),
		structName: structName,
		spec:       spec,
		ctx:        ctx,
	})
	return nil
}

// parseAsyncReplicationItemComment reads one replicated table, attached to its
// replication once the whole file is read.
func (s *schemaParseState) parseAsyncReplicationItemComment(comment *ast.Comment, structName string) error {
	const directive = "ptah:schema:async_replication:item"
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+directive, structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	item, err := ydbreplication.ParseItem(kv)
	if err != nil {
		return replicationAttributeError(ctx, directive, err)
	}
	s.replicationItems = append(s.replicationItems, pendingReplicationItem{
		schema:      strings.Trim(strings.TrimSpace(kv[ydbreplication.AttributeSchema]), "/"),
		replication: strings.TrimSpace(kv[ydbreplication.AttributeReplication]),
		item:        item,
		ctx:         ctx,
	})
	return nil
}

// parseTransferComment reads a YDB transfer declaration.
func (s *schemaParseState) parseTransferComment(comment *ast.Comment, structName string) error {
	const directive = "ptah:schema:transfer"
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+directive, structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	spec, err := ydbreplication.ParseTransfer(kv)
	if err != nil {
		return replicationAttributeError(ctx, directive, err)
	}
	objects, err := ydbreplication.DeclareTransfer(s.featureObjects, kv[ydbreplication.AttributeSchema],
		kv[ydbreplication.AttributeName], structName, spec)
	if err != nil {
		return replicationAttributeError(ctx, directive, err)
	}
	s.featureObjects = objects
	return nil
}

// attachReplicationItems gives each replication of the file the items
// declared for it, then declares each replication as a feature object. An
// item of a replication the file does not declare is refused rather than
// dropped, because a table nobody replicates is a declaration with no effect,
// and so is a second item creating one replica path, where one table can
// stand. A replication left with no item is refused too: YDB takes none
// without a FOR clause.
func (s *schemaParseState) attachReplicationItems() error {
	for _, pending := range s.replicationItems {
		index := slices.IndexFunc(s.asyncReplications, func(replication pendingReplication) bool {
			return replication.name == pending.replication && replication.schema == pending.schema
		})
		if index < 0 {
			return replicationPlacementError(pending.ctx, fmt.Sprintf(
				"the file declares no async replication %q for the item replicating %q",
				ydbreplication.Display(pending.schema, pending.replication), pending.item.Source))
		}
		replication := &s.asyncReplications[index]
		if slices.ContainsFunc(replication.spec.Items, func(have ydbreplication.Item) bool {
			return strings.Trim(have.Target, "/") == strings.Trim(pending.item.Target, "/")
		}) {
			return replicationPlacementError(pending.ctx, fmt.Sprintf(
				"async replication %q declares two items with target %q",
				ydbreplication.Display(replication.schema, replication.name), pending.item.Target))
		}
		replication.spec.Items = append(replication.spec.Items, pending.item)
	}
	for _, replication := range s.asyncReplications {
		if len(replication.spec.Items) == 0 {
			return &ptaherr.ParseError{
				File:      s.filename,
				Directive: "ptah:schema:async_replication",
				Err:       ptaherr.ErrMissingRequiredAttribute,
				Message: fmt.Sprintf("async replication %q declares no item; declare the tables it replicates "+
					"with //ptah:schema:async_replication:item in the same file",
					ydbreplication.Display(replication.schema, replication.name)),
			}
		}
		objects, err := ydbreplication.DeclareReplication(s.featureObjects, replication.schema, replication.name,
			replication.structName, replication.spec)
		if err != nil {
			return replicationAttributeError(replication.ctx, "ptah:schema:async_replication", err)
		}
		s.featureObjects = objects
	}
	return nil
}

// replicationAttributeError reports a value a replication, item or transfer
// declaration cannot carry, naming the attribute. The error wraps
// [ptaherr.ErrInvalidAttributeValue] and the owner's own error, so an object
// declared twice still matches [schemaext.ErrDuplicate], as it does in YAML,
// in YQL and across merged files.
func replicationAttributeError(ctx annotationErrorContext, directive string, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       fmt.Errorf("%w: %w", ptaherr.ErrInvalidAttributeValue, err),
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbreplication.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
	}
	if _, ok := errors.AsType[*ydbreplication.DuplicateError](err); ok {
		parseErr.Attribute = ydbreplication.AttributeName
	}
	return parseErr
}

// replicationPlacementError reports an item annotation that names no
// replication of its file, or a target twice.
func replicationPlacementError(ctx annotationErrorContext, reason string) error {
	return &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: "ptah:schema:async_replication:item",
		Attribute: ydbreplication.AttributeReplication,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%s on %s at %s", reason, ctx.directive, ctx.location),
	}
}
