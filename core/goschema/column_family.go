package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"slices"

	ptahast "ptah.run/core/ast"
	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/internal/ydbfamily"
)

// pendingColumnFamily is a YDB column family annotation waiting for the table
// it belongs to, which the file may declare after it.
type pendingColumnFamily struct {
	structName string
	table      string
	spec       ptahast.YDBColumnFamilySpec
	ctx        annotationErrorContext
}

// columnFamilyDirective is the annotation that declares a YDB column family.
const columnFamilyDirective = "ptah:schema:columnfamily"

// parseColumnFamilyComment reads a YDB column family declaration. The family
// belongs to the table the struct maps to, or to the table its `table`
// attribute names, and is attached to it once the whole file is read.
func (s *schemaParseState) parseColumnFamilyComment(comment *ast.Comment, structName string) error {
	kv := parseutils.ParseKeyValueComment(comment.Text)
	ctx := s.annotationContext(comment, "//"+columnFamilyDirective, structName)
	if err := validateAttributes(kv, ctx); err != nil {
		return err
	}
	if err := requireAttributes(kv, ctx); err != nil {
		return err
	}
	spec, err := ydbfamily.ParseDeclaration(kv)
	if err != nil {
		parseErr := &ptaherr.ParseError{
			File:      ctx.file,
			Line:      ctx.line,
			Directive: columnFamilyDirective,
			Err:       ptaherr.ErrInvalidAttributeValue,
			Message:   fmt.Sprintf("%v on %s at %s", err, ctx.directive, ctx.location),
		}
		if declared, ok := errors.AsType[*ydbfamily.DeclarationError](err); ok {
			parseErr.Attribute = declared.Attribute
		}
		return parseErr
	}
	s.columnFamilies = append(s.columnFamilies, pendingColumnFamily{
		structName: structName,
		table:      kv[ydbfamily.AttributeTable],
		spec:       spec,
		ctx:        ctx,
	})
	return nil
}

// attachColumnFamilies gives each table of the file the column families
// declared for it. A family whose table the file does not declare is refused
// rather than dropped, and so is a second family of one name. Whether the
// columns a family names exist is the renderer's question, asked of the whole
// table.
func (s *schemaParseState) attachColumnFamilies() error {
	for _, pending := range s.columnFamilies {
		index, err := s.ownerTable(pending.structName, pending.table, pending.ctx, columnFamilyDirective, "a column family")
		if err != nil {
			return err
		}
		table := &s.tableDirectives[index]
		if slices.ContainsFunc(table.YDBColumnFamilies, func(have ptahast.YDBColumnFamilySpec) bool {
			return have.Name == pending.spec.Name
		}) {
			return s.placementError(pending.ctx, columnFamilyDirective,
				fmt.Sprintf("table %q declares column family %q twice", table.Name, pending.spec.Name))
		}
		table.YDBColumnFamilies = append(table.YDBColumnFamilies, pending.spec)
	}
	return nil
}
