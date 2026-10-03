// Package ydbgap names the parts of Ptah that accept YDB by name and do not
// implement it yet, so that each one refuses with the same words and points
// to the plan that adds it.
//
// YDB is a dialect name before every layer behind it exists: stokaro/ptah#4015
// adds them in phases. A layer reached with the name and nothing behind it
// would otherwise answer with its default arm, which is another dialect's
// behavior or an error about an empty driver name. Each such layer refuses
// through [Layer.Message] instead, and the phase that implements a layer
// removes its constant together with every refusal that names it.
//
// The renderer and the planner exist. The object families they do not carry
// yet -- comments, views, access control, table settings -- are layers here
// too, because a declaration of one reaches the renderer by name and has to be
// refused there rather than rendered as something else.
package ydbgap

import "fmt"

// Plan is the issue that plans YDB support and owns every gap named here.
const Plan = "stokaro/ptah#4015"

// Layer is one part of Ptah that YDB does not reach yet.
type Layer int

// The layers YDB does not reach yet. Each answers [Layer.Phase] with the phase
// of [Plan] that implements it.
const (
	// SchemaFiles is reading a YQL file as a desired schema.
	SchemaFiles Layer = iota + 1
	// Connecting is opening a YDB server, and with it every reader, writer
	// and migrator that needs one.
	Connecting
	// QueryBuilding is the query builder writing YQL.
	QueryBuilding
	// DataChanges is writing rows: an upsert, a data diff, a seed.
	DataChanges
	// Linting is `ptah sql lint` and `ptah migrations lint` over YQL.
	Linting
	// CreatingDatabases is making a new YDB database for a scratch or dev
	// run. SQL cannot create one; the alternative is designed with the dev
	// database work.
	CreatingDatabases
	// Comments is storing a comment on a table, a column or an index. YQL has
	// no COMMENT statement; the comments family stores them as table
	// attributes through the scheme API.
	Comments
	// Views is creating, replacing and dropping a view.
	Views
	// AccessControl is users, groups, membership and permissions.
	AccessControl
	// TableSettings is a table's YDB settings: TTL, partitioning, column
	// families and changefeeds.
	TableSettings
)

// work is what a layer does, as a refusal prints it.
func (l Layer) work() string {
	switch l {
	case SchemaFiles:
		return "reading a YDB schema file"
	case Connecting:
		return "connecting to a YDB server"
	case QueryBuilding:
		return "building a YQL query"
	case DataChanges:
		return "writing YDB rows"
	case Linting:
		return "linting YQL for YDB"
	case CreatingDatabases:
		return "creating a YDB database"
	case Comments:
		return "storing a comment on a YDB object"
	case Views:
		return "managing YDB views"
	case AccessControl:
		return "managing YDB users, groups and permissions"
	case TableSettings:
		return "setting YDB table options (TTL, partitioning, column families, changefeeds)"
	default:
		return "this YDB operation"
	}
}

// Phase is the phase of [Plan] that implements the layer, or 0 for a value
// that names no layer.
func (l Layer) Phase() int {
	switch l {
	case SchemaFiles:
		return 2
	case Connecting:
		return 4
	case QueryBuilding, DataChanges:
		return 7
	case Linting:
		return 8
	case CreatingDatabases:
		return 9
	case Comments, Views, AccessControl, TableSettings:
		return 10
	default:
		return 0
	}
}

// Message is the refusal a layer reports: what is not implemented, and where
// the work is planned. It names YDB, so a caller does not repeat the dialect.
func (l Layer) Message() string {
	return fmt.Sprintf("%s is not implemented yet (%s, phase %d)", l.work(), Plan, l.Phase())
}
