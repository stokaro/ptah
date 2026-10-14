// Package ydbdata is the YDB owner's managed-data services: the core/dataext
// contracts YDB's descriptor asks for. YQL types every literal and converts
// few of them, so a value is written in its column's YDB type and the rows a
// comparison pairs are typed first. engine/builtin registers the services on
// the YDB target, and the shared packages receive them through
// engine.Runtime.DataServices. The package links no database driver, which is
// what lets the rendering runtime link it; YDB's seed service runs through the
// driver and sits with the YDB connection, in internal/dbschema/ydb.
package ydbdata

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/dataext"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbtype"
)

// Services returns the YDB owner's data services.
func Services() dataext.Services {
	return dataext.Services{Literals: Literals{}, Rows: Rows{}}
}

// Literals writes every value in its column's YDB type through
// ydbtype.ValueLiteral, the map the YDB renderer writes the column with. YDB
// converts almost no literal to a column's type, so without the type there is
// no literal to write, and the column is refused rather than given the type
// its Go value suggests.
type Literals struct{}

var _ dataext.Literals = Literals{}

// Literal writes value for a column of ydbType. It handles every value.
func (Literals) Literal(ydbType string, value any) (string, bool, error) {
	if strings.TrimSpace(ydbType) == "" {
		return "", true, errors.New("YDB stores a literal only in its column's own type, " +
			"and the diff names no type for this column")
	}
	literal, err := ydbtype.ValueLiteral(ydbType, value)
	return literal, true, err
}

// Rows types a YDB table's rows: each column's YDB type, and the declared and
// live rows with every typed value in one canonical Go form.
type Rows struct{}

var _ dataext.Rows = Rows{}

// TypeRows answers, for a YDB table, the YDB type of each column, and the
// declared and live rows with every typed value in one canonical Go form.
//
// YQL types every literal, so the diff writes each value in its column's YDB
// type, and that type is the live table's where the column exists: YDB cannot
// change a column's type, so what the table holds is what a statement writes
// into. A column the table does not have yet takes the type its declaration
// lands on for this target's capabilities, which is the type the schema stage
// of the same plan creates it with.
//
// The rows are compared in canonical form (see ydbtype.CanonicalValue):
// declared as `12.50` or as a moment in UTC, a value pairs with the 12.5 a
// Decimal reads back and with the local moment the driver returns, so a
// converged row plans nothing. A declared value its column cannot hold is an
// error naming the column and the type.
func (Rows) TypeRows(
	caps capability.Capabilities,
	liveTable *catalog.Table,
	declaredTypes map[string]string,
	desired, live []map[string]any,
) (dataext.TypedRows, error) {
	columnTypes := make(map[string]string, len(declaredTypes))
	if liveTable != nil {
		for _, column := range liveTable.Columns {
			columnTypes[column.Name] = column.DataType
		}
	}
	for column, declared := range declaredTypes {
		if _, isLive := columnTypes[column]; isLive {
			continue
		}
		mapping, err := ydbtype.Map(declared, caps)
		if err != nil {
			return dataext.TypedRows{}, fmt.Errorf("column %q: %w", column, err)
		}
		columnTypes[column] = mapping.Type
	}
	typedDesired, err := ydbtype.CanonicalRows(columnTypes, desired)
	if err != nil {
		return dataext.TypedRows{}, fmt.Errorf("declared %w", err)
	}
	typedLive, err := ydbtype.CanonicalRows(columnTypes, live)
	if err != nil {
		return dataext.TypedRows{}, fmt.Errorf("live %w", err)
	}
	return dataext.TypedRows{ColumnTypes: columnTypes, Desired: typedDesired, Live: typedLive}, nil
}
