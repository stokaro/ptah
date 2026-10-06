package sqlschema

import (
	"fmt"
	"path"
	"strconv"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbsequence"
)

func alterYDBSequence(database *schemamodel.Database, document *Document, node *ast.AlterSequenceNode) error {
	if node.Schema != "" || node.AsType != "" || node.MinValue != nil || node.MaxValue != nil || node.Cache != nil || node.Cycle != nil || node.OwnedBy != "" || node.Comment != "" || (node.Start == nil && node.Increment == nil) {
		return fmt.Errorf("%w: desired YDB sequence settings accept only START and INCREMENT", ErrUnmodeledStatement)
	}
	if !strings.HasPrefix(node.Name, "/") || node.Name != strings.TrimSpace(node.Name) {
		return fmt.Errorf("%w: a Serial sequence needs its absolute database path", ErrUnmodeledStatement)
	}
	relative, err := relativeYDBSourcePath(node.Name, document.YDBDatabasePath)
	if err != nil {
		return err
	}
	field := declaredYDBSequence(database, document, relative)
	if field == nil {
		return fmt.Errorf("%w: ALTER SEQUENCE must name the sequence of a Serial column declared earlier in this desired schema", ErrUnmodeledStatement)
	}
	start, increment := field.IdentityStart, field.IdentityIncrement
	if node.Start != nil {
		start = strconv.FormatInt(*node.Start, 10)
	}
	if node.Increment != nil {
		increment = strconv.FormatInt(*node.Increment, 10)
	}
	if _, err := ydbsequence.Parse(start, increment); err != nil {
		return fmt.Errorf("%w: %w", ErrUnmodeledStatement, err)
	}
	field.IdentityStart, field.IdentityIncrement = start, increment
	return nil
}

// Resolve the table through the normal source identity rules, then use the
// shared implicit-sequence name. A matching suffix in another database must
// never bind to a local column.
func declaredYDBSequence(database *schemamodel.Database, document *Document, relative string) *schemamodel.Field {
	target, found := findAlterTarget(database, document.base, sqlident.Quote(platform.YDB, path.Dir(relative)), platform.YDB)
	if !found {
		return nil
	}
	for _, source := range target.databases {
		for i := range source.Fields {
			field := &source.Fields[i]
			if field.StructName == target.structName && ydbsequence.Name(field.Name) == path.Base(relative) && ydbsequence.DeclaresSerialColumn(field.Type, field.AutoInc, field.IdentityGeneration) {
				return field
			}
		}
	}
	return nil
}
