package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"slices"
	"strings"

	ptahast "ptah.run/core/ast"
	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
)

// pendingReplicationItem is an item annotation waiting for the replication it
// belongs to, which the same file may declare after it.
type pendingReplicationItem struct {
	schema      string
	replication string
	item        ptahast.AsyncReplicationItem
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
	s.asyncReplications = append(s.asyncReplications, schemamodel.AsyncReplication{
		StructName: structName,
		Name:       strings.TrimSpace(kv[ydbreplication.AttributeName]),
		Schema:     strings.Trim(strings.TrimSpace(kv[ydbreplication.AttributeSchema]), "/"),
		Spec:       spec,
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
	s.transfers = append(s.transfers, schemamodel.Transfer{
		StructName: structName,
		Name:       strings.TrimSpace(kv[ydbreplication.AttributeName]),
		Schema:     strings.Trim(strings.TrimSpace(kv[ydbreplication.AttributeSchema]), "/"),
		Spec:       spec,
	})
	return nil
}

// attachReplicationItems gives each replication of the file the items
// declared for it. An item of a replication the file does not declare is
// refused rather than dropped, because a table nobody replicates is a
// declaration with no effect, and so is a second item creating one replica
// path, where one table can stand. A replication left with no item is refused
// too: YDB takes none without a FOR clause.
func (s *schemaParseState) attachReplicationItems() error {
	for _, pending := range s.replicationItems {
		index := slices.IndexFunc(s.asyncReplications, func(replication schemamodel.AsyncReplication) bool {
			return replication.Name == pending.replication && replication.Schema == pending.schema
		})
		if index < 0 {
			return replicationPlacementError(pending.ctx, fmt.Sprintf(
				"the file declares no async replication %q for the item replicating %q",
				schemamodel.AsyncReplication{Name: pending.replication, Schema: pending.schema}.QualifiedName(),
				pending.item.Source))
		}
		replication := &s.asyncReplications[index]
		if slices.ContainsFunc(replication.Spec.Items, func(have ptahast.AsyncReplicationItem) bool {
			return strings.Trim(have.Target, "/") == strings.Trim(pending.item.Target, "/")
		}) {
			return replicationPlacementError(pending.ctx, fmt.Sprintf(
				"async replication %q declares two items with target %q",
				replication.QualifiedName(), pending.item.Target))
		}
		replication.Spec.Items = append(replication.Spec.Items, pending.item)
	}
	for _, replication := range s.asyncReplications {
		if len(replication.Spec.Items) == 0 {
			return &ptaherr.ParseError{
				File:      s.filename,
				Directive: "ptah:schema:async_replication",
				Err:       ptaherr.ErrMissingRequiredAttribute,
				Message: fmt.Sprintf("async replication %q declares no item; declare the tables it replicates "+
					"with //ptah:schema:async_replication:item in the same file", replication.QualifiedName()),
			}
		}
	}
	return nil
}

// replicationAttributeError reports a value a replication, item or transfer
// declaration cannot carry, naming the attribute.
func replicationAttributeError(ctx annotationErrorContext, directive string, err error) error {
	parseErr := &ptaherr.ParseError{
		File:      ctx.file,
		Line:      ctx.line,
		Directive: directive,
		Err:       ptaherr.ErrInvalidAttributeValue,
		Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
	}
	if declared, ok := errors.AsType[*ydbreplication.DeclarationError](err); ok {
		parseErr.Attribute = declared.Attribute
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
