package atlasschema

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/schemaselection"
)

// The kinds of [PlanScope].
const (
	// PlanScopeServer is a MySQL or MariaDB URL that names no database: the
	// whole server.
	PlanScopeServer = "server"
	// PlanScopeRealm is a URL that pins no schema where the dialect has
	// several, such as a PostgreSQL URL without search_path: every schema of
	// the database.
	PlanScopeRealm = "realm"
	// PlanScopeSchema is a URL that pins one schema, or for MySQL and
	// MariaDB names one database.
	PlanScopeSchema = "schema"
)

// PlanScope is what the connection URL of a saved plan covered when the plan
// was computed. It is recorded for the refusal a stale plan gives, and decides
// nothing: a plan whose source fingerprint matches the target applies whatever
// the target URL covers. The Atlas `.plan.hcl` format has no place for it, so a
// plan read back from that format carries none.
type PlanScope struct {
	// Kind is [PlanScopeServer], [PlanScopeRealm] or [PlanScopeSchema].
	Kind string `json:"kind"`
	// Name is the schema a [PlanScopeSchema] URL pins, which on MySQL and
	// MariaDB is the database it names.
	Name string `json:"name,omitempty"`
}

// planScopeOf is the scope of a connection.
func planScopeOf(info catalog.ServerInfo) PlanScope {
	switch {
	case info.WholeServer:
		return PlanScope{Kind: PlanScopeServer}
	case schemaselection.Realm(info.Dialect, info.URL, info.Schema):
		return PlanScope{Kind: PlanScopeRealm}
	default:
		return PlanScope{Kind: PlanScopeSchema, Name: info.Schema}
	}
}

// describe names the scope for a person, in the words of dialect: a schema of
// a dialect whose schema is a database is called a database.
func (s PlanScope) describe(dialect string) string {
	switch s.Kind {
	case PlanScopeServer:
		return "the whole server"
	case PlanScopeRealm:
		return "every schema of the database"
	default:
		noun := "schema"
		if described(dialect).Schemas.Selection == descriptor.SchemaSelectionDatabase {
			noun = "database"
		}
		return fmt.Sprintf("%s %q", noun, s.Name)
	}
}
