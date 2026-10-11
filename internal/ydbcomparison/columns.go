package ydbcomparison

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/ydbsequence"
	"ptah.run/internal/ydbtype"
)

// Columns compares YDB column definitions and reads back the defaults a YDB
// catalog reports.
type Columns struct{}

// TypeChange compares a YDB column's type. An integer that increments, on
// either side, is read as the Serial type the renderer writes for it: a
// declared BIGINT with auto_increment is a BigSerial, and so is a column the
// other side reports as an incrementing Int64. See comparableTypes for how
// the two spellings are read.
func (Columns) TypeChange(desired schemamodel.Field, current catalog.Column, currentType string) string {
	declared, held := desired.Type, currentType
	if desired.AutoInc || desired.IdentityGeneration != "" {
		declared = serialOf(declared)
	}
	if current.IsAutoIncrement {
		held = serialOf(held)
	}
	desiredType, heldType := comparableTypes(declared, held)
	if desiredType == heldType {
		return ""
	}
	return fmt.Sprintf("%s -> %s", heldType, desiredType)
}

// DefaultChange compares a YDB column's default as YQL literals. YDB stores a
// default as a typed value, and the reader reports it as the literal
// ydbtype.Literal writes for it; the declaration is written through the same
// function in the type the column has, so `5`, `+5` and `05` on an Int8
// column all compare as `5t`, and a declared TIMESTAMP default compares in the
// Timestamp64 or Timestamp spelling the server built. A Serial column takes
// its value from its sequence, so the reader reports no default for one and
// there is nothing to compare.
func (Columns) DefaultChange(desired schemamodel.Field, current catalog.Column, declared, held string) string {
	if strings.TrimSpace(declared) == "" && held == "" {
		return ""
	}
	if declaredDefault(desired, current.RawType(), declared) == held {
		return ""
	}
	return fmt.Sprintf("%s -> %s", held, declared)
}

// AlwaysNotNull reports a column the YDB renderer writes as a Serial type,
// which it writes NOT NULL whatever the field says: a Serial column always
// holds a value from its sequence. Without this a Serial column declared
// without not_null would plan DROP NOT NULL against the column Ptah built, on
// every run.
func (Columns) AlwaysNotNull(desired schemamodel.Field) bool {
	return ydbsequence.DeclaresSerialColumn(desired.Type, desired.AutoInc, desired.IdentityGeneration)
}

// SequenceChanges reports a difference in the start or the increment of a
// Serial column's sequence, under the attribute names a declaration spells
// them with. It compares only where both sides are Serial: a column that is
// Serial on one side only has a type difference, which says more.
//
// Each side is read through [ydbsequence.Canonical], so an omitted setting is
// the 1 a sequence nobody altered has, and `0100` is 100. Both settings are
// compared and reported on their own, while the plan writes both whatever
// changed, so a value is never left to whatever the server keeps.
func (Columns) SequenceChanges(desired schemamodel.Field, current catalog.Column) map[string]string {
	declaredSerial := desired.AutoInc || desired.IdentityGeneration != "" || ydbtype.DeclaresSerial(desired.Type)
	if !declaredSerial || !current.IsAutoIncrement {
		return nil
	}
	var changes map[string]string
	for _, setting := range []struct{ key, declared, current string }{
		{"identity_start", desired.IdentityStart, current.IdentityStart},
		{"identity_increment", desired.IdentityIncrement, current.IdentityIncrement},
	} {
		declared, held := ydbsequence.Canonical(setting.declared), ydbsequence.Canonical(setting.current)
		if declared != held {
			if changes == nil {
				changes = make(map[string]string, 2)
			}
			changes[setting.key] = held + " -> " + declared
		}
	}
	return changes
}

// DefaultValue returns the value a YDB default was written from. YDB stores
// every default as a value and reports it as the typed YQL literal
// internal/ydbtype writes (`5t`, `'x'u`, `Timestamp('...')`); a column read
// back into a declaration carries that value, so a renderer writes the same
// literal again. A default that is no such literal is not a value.
func (Columns) DefaultValue(reported string) (string, bool) {
	return ydbtype.LiteralValue(reported)
}

// declaredDefault writes a declared default as the literal the YDB renderer
// writes for it: in the catalog's type where the declaration lands on that
// type, and in the declaration's own type otherwise. A NULL default writes
// nothing, as the renderer writes none. An expression, and a value the type
// cannot hold, are compared as written.
func declaredDefault(desired schemamodel.Field, catalogType, declared string) string {
	if strings.TrimSpace(desired.Default) == "" && strings.TrimSpace(desired.DefaultExpr) != "" {
		return declared
	}
	value, isNull := ydbtype.DeclaredValue(declared)
	if isNull {
		return ""
	}
	columnType, _ := comparableTypes(desired.Type, catalogType)
	literal, err := ydbtype.Literal(columnType, value, capability.Capabilities{
		capability.SmallIntegerDefaults: true,
		capability.DocumentTypeDefaults: true,
	})
	if err != nil {
		return declared
	}
	return literal
}

// serialOf answers the Serial type an incrementing integer of columnType is
// written as, and columnType itself where YDB has no Serial for it.
func serialOf(columnType string) string {
	renderings := ydbtype.Renderings(columnType)
	if len(renderings) == 0 {
		return columnType
	}
	if serial, ok := ydbtype.SerialFor(renderings[0]); ok {
		return serial
	}
	return columnType
}

// comparableTypes reads both sides through the YDB type map, the one the
// renderer writes from, and calls them equal when any type the declaration can
// land on is a type the other side can be. A declared TIMESTAMP is Timestamp64
// on a line with the wide types and Timestamp on one without, and a table
// built on either answers the declaration. A declared VARCHAR(255) is Utf8,
// whose length YDB does not keep, so the length is not a difference here; the
// render reports it as dropped instead.
//
// The current side goes through the map too because it is not always a
// catalog: a file-to-file comparison hands it a declaration, VARCHAR(255)
// where a server would say Utf8. A catalog spelling maps to itself, since the
// map reads YDB's own names case-sensitively -- Int8 is YDB's 8-bit integer,
// INT8 is SQL's 64-bit one. The full spelling is compared, Decimal(10,2)
// against Decimal(12,2) included, so no width check runs after this.
func comparableTypes(declared, current string) (desiredType, currentType string) {
	want := ydbtype.Renderings(declared)
	have := ydbtype.Renderings(current)
	if len(have) == 0 {
		have = []string{strings.TrimSpace(current)}
	}
	for _, rendering := range want {
		for _, candidate := range have {
			if strings.EqualFold(rendering, candidate) {
				return candidate, candidate
			}
		}
	}
	if len(want) == 0 {
		return strings.TrimSpace(declared), have[0]
	}
	return want[0], have[0]
}
