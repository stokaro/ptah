package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgrole"
)

// readRoleSettings reads the configuration defaults the server keeps for the
// roles the read found, described or not, and records that the read knows
// them all: a role without the facet holds none.
//
// pg_db_role_setting keeps three kinds of row, and only one is modeled: a
// role's defaults in every database (setdatabase 0). A default stored for one
// role in this database (ALTER ROLE ... IN DATABASE) and one stored for every
// role (ALTER ROLE ALL, or ALTER DATABASE ... SET) apply to sessions here
// too, so each is named as an unmodeled object rather than dropped. Rows for
// other databases do not apply to this one and are not read.
//
// Only PostgreSQL takes the query: CockroachDB and YugabyteDB keep role
// settings in catalogs this reader does not promise, and no owner of theirs
// models them.
func (r *Reader) readRoleSettings(ctx context.Context, database *catalog.Database) error {
	if r.dialect != platform.Postgres {
		return nil
	}
	const query = `
		SELECT COALESCE(r.rolname, ''), s.setdatabase <> 0, COALESCE(d.datname, ''),
		       COALESCE(array_to_json(s.setconfig)::text, '[]')
		FROM pg_catalog.pg_db_role_setting s
		LEFT JOIN pg_catalog.pg_roles r ON r.oid = s.setrole
		LEFT JOIN pg_catalog.pg_database d ON d.oid = s.setdatabase
		WHERE s.setdatabase = 0 OR d.datname = current_database()
		ORDER BY 1, 3`
	roles := make(map[string]*catalog.Role, len(database.Roles)+len(database.RolesOutOfScope))
	for _, list := range [][]catalog.Role{database.Roles, database.RolesOutOfScope} {
		for i := range list {
			roles[list[i].Name] = &list[i]
		}
	}
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("read role settings: %w", err)
	}
	defer rows.Close()
	held := make(map[string][]pgrole.Setting)
	for rows.Next() {
		var (
			role, databaseName, config string
			inDatabase                 bool
		)
		if err := rows.Scan(&role, &inDatabase, &databaseName, &config); err != nil {
			return fmt.Errorf("read role settings: %w", err)
		}
		// A row of a role the read did not find is skipped before it is
		// parsed, so a value this reader cannot parse there fails nothing.
		if _, found := roles[role]; role != "" && !found {
			continue
		}
		settings, err := parseRoleConfig(config)
		if err != nil {
			return fmt.Errorf("read role settings of %q: %w", role, err)
		}
		if role != "" && !inDatabase {
			held[role] = append(held[role], settings...)
			continue
		}
		database.UnmodeledObjects = append(database.UnmodeledObjects, unmodeledRoleSettings(role, databaseName, settings)...)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("read role settings: %w", err)
	}
	for name, settings := range held {
		role := roles[name]
		if role.Facets, err = pgrole.WithObservedSettings(role.Facets, settings); err != nil {
			return fmt.Errorf("read role settings of %q: %w", name, err)
		}
	}
	known, err := pgrole.SettingsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return err
	}
	database.FeatureCoverage, err = database.FeatureCoverage.Combine(known)
	return err
}

// parseRoleConfig reads setconfig, which array_to_json spells as a JSON array
// of `name=value` strings, the value as the server keeps it.
func parseRoleConfig(config string) ([]pgrole.Setting, error) {
	var entries []string
	if err := json.Unmarshal([]byte(config), &entries); err != nil {
		return nil, fmt.Errorf("setconfig %q: %w", config, err)
	}
	settings := make([]pgrole.Setting, 0, len(entries))
	for _, entry := range entries {
		name, value, found := strings.Cut(entry, "=")
		if !found || name == "" {
			return nil, fmt.Errorf("setconfig entry %q is not name=value", entry)
		}
		settings = append(settings, pgrole.Setting{Name: name, Value: value})
	}
	return settings, nil
}

// unmodeledRoleSettings names each setting of a row the model does not
// describe: a role's default in this database, or a default for every role.
func unmodeledRoleSettings(role, databaseName string, settings []pgrole.Setting) []catalog.UnmodeledObject {
	objects := make([]catalog.UnmodeledObject, 0, len(settings))
	for _, setting := range settings {
		object := catalog.UnmodeledObject{Kind: "role setting"}
		switch {
		case role == "" && databaseName == "":
			object.Name = setting.Name + " for every role"
			object.Reason = "ALTER ROLE ALL SET stores a default for every role, which Ptah does not model"
		case role == "":
			object.Name = setting.Name + " for every role in database " + databaseName
			object.Reason = "ALTER DATABASE SET and ALTER ROLE ALL IN DATABASE store a default for every role in one database, which Ptah does not model"
		default:
			object.Name = setting.Name + " for role " + role + " in database " + databaseName
			object.Reason = "ALTER ROLE IN DATABASE stores a default for one database; Ptah models the defaults a role holds in every database"
		}
		objects = append(objects, object)
	}
	return objects
}
