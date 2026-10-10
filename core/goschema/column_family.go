package goschema

import (
	"errors"
	"fmt"
	"go/ast"
	"slices"
	"strings"

	"ptah.run/core/goschema/internal/parseutils"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/ydbfamily"
)

// pendingColumnFamily is a YDB column family annotation waiting for the table
// it belongs to, which the file may declare after it.
type pendingColumnFamily struct {
	structName string
	table      string
	spec       ydbschema.ColumnFamily
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
// declared for it, as the YDB owner's facet. A family whose table the file
// does not declare is refused rather than dropped, and so are a second family
// of one name and a column two families name. Whether the columns a family
// names exist is the renderer's question, asked of the whole table.
func (s *schemaParseState) attachColumnFamilies() error {
	declared := make(map[int][]ydbschema.ColumnFamily)
	var order []int
	for _, pending := range s.columnFamilies {
		index, err := s.ownerTable(pending.structName, pending.table, pending.ctx, columnFamilyDirective, "a column family")
		if err != nil {
			return err
		}
		table := &s.tableDirectives[index]
		if slices.ContainsFunc(declared[index], func(have ydbschema.ColumnFamily) bool { return have.Name == pending.spec.Name }) {
			return s.placementError(pending.ctx, columnFamilyDirective,
				fmt.Sprintf("table %q declares column family %q twice", table.Name, pending.spec.Name))
		}
		if _, seen := declared[index]; !seen {
			order = append(order, index)
		}
		declared[index] = append(declared[index], pending.spec)
		if err := ydbschema.ValidateDesiredColumnFamilies(&ydbschema.DesiredColumnFamilies{Families: declared[index]}); err != nil {
			if invalid, ok := errors.AsType[*schemaext.InvalidModelError](err); ok {
				reason := strings.TrimPrefix(invalid.Message, schemaext.ErrInvalidValue.Error()+": ")
				return s.placementError(pending.ctx, columnFamilyDirective, fmt.Sprintf("table %q: %s", table.Name, reason))
			}
			return err
		}
	}
	for _, index := range order {
		table := &s.tableDirectives[index]
		facets, err := table.Facets.With(&ydbschema.DesiredColumnFamilies{Families: declared[index]})
		if err != nil {
			return err
		}
		table.Facets = facets
	}
	return nil
}
