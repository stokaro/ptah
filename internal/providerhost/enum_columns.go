package providerhost

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/platform/descriptor"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
	protocol "ptah.run/provider-sdk/protocol/experimental"
	"ptah.run/provider-sdk/provider"
)

// enumColumns builds every enum column of desired in the form t's engine
// states (Engine.enum_columns): a type that lists the values, or a column of
// the stated type that holds them under its CHECK, with no enum type left in
// the schema, so the provider receives an ordinary column and a comparison
// sees the column a read reports. Under REFUSED it refuses an enum column by
// its table and column, and a declared enum by its name. An engine that
// states no form keeps the enum a type of its own, and desired is returned
// as it is. desired is not modified.
//
// The comparison's lowering, the validation and the schema render each call
// it, so a plan, a validation and a render see one column.
func (t *target) enumColumns(desired *schemamodel.Database) (*schemamodel.Database, error) {
	columns := t.engine.GetEnumColumns()
	if desired == nil || columns == nil || !t.offers(provider.EnumColumnsFeature) {
		return desired, nil
	}
	var form schemaprep.EnumForm
	switch columns.GetForm() {
	case protocol.EnumColumns_VALUES_TYPE:
		form = schemaprep.EnumForm{Style: descriptor.EnumColumnType}
	case protocol.EnumColumns_CHECK:
		form = schemaprep.EnumForm{Style: descriptor.EnumCheck, CheckType: columns.GetCheckType()}
	case protocol.EnumColumns_REFUSED:
		if err := refusedEnums(t.engine.GetId(), desired); err != nil {
			return nil, err
		}
		return desired, nil
	default:
		return nil, fmt.Errorf("%w: engine %s states no enum form", provider.ErrProtocol, t.engine.GetId())
	}
	if len(desired.Enums) == 0 && !hasEnumColumn(desired) {
		return desired, nil
	}
	lowered := *desired
	lowered.Fields = schemaprep.LowerEnumColumns(desired.Fields, desired.Enums, form)
	lowered.Enums = nil
	return &lowered, nil
}

// hasEnumColumn reports whether desired declares a column typed ENUM with
// its values.
func hasEnumColumn(desired *schemamodel.Database) bool {
	for _, field := range desired.Fields {
		if values, _ := schemaprep.EnumColumnValues(field, desired.Enums); len(values) != 0 {
			return true
		}
	}
	return false
}

// refusedEnums refuses the first enum column of desired, by its table and
// column, and otherwise the first enum desired declares, for an engine that
// builds no enum. It returns nil for a schema with neither.
func refusedEnums(engine string, desired *schemamodel.Database) error {
	for _, field := range desired.Fields {
		values, _ := schemaprep.EnumColumnValues(field, desired.Enums)
		if len(values) == 0 {
			continue
		}
		table := field.StructName
		for _, candidate := range desired.Tables {
			if candidate.StructName == field.StructName {
				table = candidate.QualifiedName()
				break
			}
		}
		return fmt.Errorf("%w: column %q of table %q is an enum column with the values %s, and the %s engine builds no enum: type the column with a type %s has",
			ptaherr.ErrUnsupportedFeature, field.Name, table, quotedValues(values), engine, engine)
	}
	if len(desired.Enums) != 0 {
		return fmt.Errorf("%w: the schema declares the enum %q, and the %s engine builds no enum", ptaherr.ErrUnsupportedFeature,
			desired.Enums[0].QualifiedName(), engine)
	}
	return nil
}

// quotedValues writes values as a parenthesized list of quoted strings.
func quotedValues(values []string) string {
	quoted := make([]string, len(values))
	for i, value := range values {
		quoted[i] = strconv.Quote(value)
	}
	return "(" + strings.Join(quoted, ", ") + ")"
}
