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
// The renderer, the planner, the connection, the schema reader, the schema
// writer and the versioned migrator exist. The object families they do not
// carry yet -- comments, views, access control, table settings, the index kinds
// beyond global ones -- are layers here too, because a declaration of one
// reaches the renderer by name, and a database holding one reaches the reader,
// and each has to be refused there rather than handled as something else. So
// are the commands that connect and then need a layer that does not exist yet:
// data changes, the compatibility surface and the surfaces planned last.
package ydbgap

import "fmt"

// Plan is the issue that plans YDB support and owns every gap named here.
const Plan = "stokaro/ptah#4015"

// Layer is one part of Ptah that YDB does not reach yet.
type Layer int

// The layers YDB does not reach yet. Each answers [Layer.Phase] with the phase
// of [Plan] that implements it.
const (
	// SchemaFiles is reading a YQL file as a desired schema: a parser for
	// YQL's CREATE TABLE, whose key, index and option clauses no other
	// dialect has. It lands with the object families, because each family
	// brings the clauses the parser has to read.
	SchemaFiles Layer = iota + 1
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
	// DevDatabases is a YDB database a run claims, resets and replays into as
	// its dev or shadow database.
	DevDatabases
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
	// IndexFamilies is the index kinds beyond a row table's global indexes:
	// vector, full-text and JSON indexes, and a column table's local ones.
	IndexFamilies
	// Compatibility is a YDB URL on the ptah-compat surface.
	Compatibility
	// OtherSurfaces is the commands planned after the compatibility surface:
	// Go struct generation from a database, schema security analysis and the
	// other surfaces that read more than the schema reader describes.
	OtherSurfaces
)

// work is what a layer does, as a refusal prints it.
func (l Layer) work() string {
	switch l {
	case SchemaFiles:
		return "reading a YDB schema file"
	case QueryBuilding:
		return "building a YQL query"
	case DataChanges:
		return "writing YDB rows"
	case Linting:
		return "linting YQL for YDB"
	case CreatingDatabases:
		return "creating a YDB database"
	case DevDatabases:
		return "using a YDB database as a dev or shadow database"
	case Comments:
		return "storing a comment on a YDB object"
	case Views:
		return "managing YDB views"
	case AccessControl:
		return "managing YDB users, groups and permissions"
	case TableSettings:
		return "setting YDB table options (TTL, partitioning, column families, changefeeds)"
	case IndexFamilies:
		return "reading or creating a YDB vector, full-text, JSON or column-table index"
	case Compatibility:
		return "using a YDB database through ptah-compat"
	case OtherSurfaces:
		return "running this command against YDB"
	default:
		return "this YDB operation"
	}
}

// Phase is the phase of [Plan] that implements the layer, or 0 for a value
// that names no layer.
func (l Layer) Phase() int {
	switch l {
	case QueryBuilding, DataChanges:
		return 7
	case Linting:
		return 8
	case CreatingDatabases, DevDatabases:
		return 9
	case SchemaFiles, Comments, Views, AccessControl, TableSettings, IndexFamilies:
		return 10
	case Compatibility:
		return 11
	case OtherSurfaces:
		return 12
	default:
		return 0
	}
}

// Message is the refusal a layer reports: what is not implemented, and where
// the work is planned. It names YDB, so a caller does not repeat the dialect.
func (l Layer) Message() string {
	return fmt.Sprintf("%s is not implemented yet (%s, phase %d)", l.work(), Plan, l.Phase())
}
