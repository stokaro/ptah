// Package oracledata is the Oracle owner's managed-data service: the
// core/dataext contract Oracle's descriptor asks for. A declaration writes a
// moment as text, and Oracle refuses text for a column that holds a moment, so
// migration/datadiff asks this package which declared types those are and
// writes the moment the text names. engine/builtin registers the service on
// the Oracle target.
package oracledata

import (
	"ptah.run/core/dataext"
	"ptah.run/internal/oracletype"
)

// Services returns the Oracle owner's data services.
func Services() dataext.Services {
	return dataext.Services{Columns: Columns{}}
}

// Columns answers which declared types Oracle stores as a moment.
type Columns struct{}

var _ dataext.Columns = Columns{}

// HoldsMoment reports whether a column declared as declaredType is created as
// a DATE or a TIMESTAMP. Which columns those are is the type map's answer
// (oracletype.Map) rather than a second list, so a literal follows the type
// the column was created with. A bare string is read through
// NLS_TIMESTAMP_FORMAT, DD-MON-RR by default, and refused with ORA-01843.
func (Columns) HoldsMoment(declaredType string) bool {
	return oracletype.IsDatetime(declaredType)
}
