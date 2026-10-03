// Package pgsnapshot records the catalog state of a PostgreSQL dev database
// at its starting point, and plans the statements that return the database
// there after a run.
//
// A dev database an atlas.hcl docker block provisions starts from the state
// the image and the block's baseline leave (stokaro/ptah#4056). That state is
// the environment the run works in: the Supabase image's auth, storage and
// realtime schemas, their tables and functions, and the grants on them. A run
// adds to it, in the schemas it creates and inside the ones that were there --
// a trigger on auth.users is the common case -- and every reset removes what
// the run added and puts back every grant the run changed, so the next run
// starts from the same state.
//
// [Read] records the state as the identities of the objects in user schemas,
// the columns of their tables, the privileges on them, and the settings a run
// can change in place: types, defaults, nullability, row security, trigger
// states, owners, comments and definitions. [Plan] compares a later read with
// it. It drops each object and column the record does not hold, in an order
// the server accepts, restores each setting it can, and grants or revokes
// each privilege that differs. What it cannot return is named rather than
// hidden: an object the record holds and the later read does not
// ([Missing]), which no statement can create again, and a setting no
// statement restores, such as a column's type or an index's definition
// ([Changed]).
//
// Row data is not part of the record: a reset returns the schema, not the
// rows a migration inserted into a table the starting point held.
package pgsnapshot

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/internal/aclitem"
	"ptah.run/internal/pgdefaultacl"
)

// Querier is the part of a database handle a read uses. A transaction, a pool
// and a dbschema.DatabaseConnection all satisfy it.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Object is one object in a user schema, or a schema or an extension.
type Object struct {
	// Catalog is the system catalog the object's row lives in, such as
	// pg_class or pg_trigger. With OID it identifies the object.
	Catalog string
	// OID is the object's row in Catalog.
	OID uint32
	// Kind names the object in a message: "table", "trigger", "schema".
	Kind string
	// Name is the object as a message names it, qualified.
	Name string
	// Drop is the statement that removes the object, and everything that
	// depends on it, if it still exists.
	Drop string
	// Order is where the drop runs: lower first. A trigger, a policy, a rule
	// and a constraint go before the relations, routines and types, and those
	// before extensions and schemas.
	Order int
}

// key identifies an object across two reads.
func (o Object) key() objectKey {
	return objectKey{catalog: o.Catalog, oid: o.OID}
}

type objectKey struct {
	catalog string
	oid     uint32
}

// Column is one column of a table in a user schema.
type Column struct {
	// Relation is the table's pg_class OID.
	Relation uint32
	// Number is the column's attnum.
	Number int16
	// Name is the column as a message names it, qualified by its table.
	Name string
	// Drop is the statement that removes the column.
	Drop string
}

type columnKey struct {
	relation uint32
	number   int16
}

func (c Column) key() columnKey {
	return columnKey{relation: c.Relation, number: c.Number}
}

// Privileges is the access list of one object, or of one column.
type Privileges struct {
	// Catalog and OID identify the object, as for [Object].
	Catalog string
	OID     uint32
	// Column is the column's attnum for a column's privileges, and zero for
	// the object's own.
	Column int16
	// Target is what a GRANT names: `TABLE "s"."t"`, `SCHEMA "s"`,
	// `FUNCTION s.f(integer)`.
	Target string
	// ColumnName is the quoted column a GRANT names in parentheses, and empty
	// for the object's own privileges.
	ColumnName string
	// ACL is the list, the built-in default when the catalog holds none.
	ACL []aclitem.Item
}

type privilegesKey struct {
	catalog string
	oid     uint32
	column  int16
}

func (p Privileges) key() privilegesKey {
	return privilegesKey{catalog: p.Catalog, oid: p.OID, column: p.Column}
}

// Snapshot is a dev database's catalog at one moment: its starting point, or
// the state a run left.
type Snapshot struct {
	Objects    []Object
	Columns    []Column
	Privileges []Privileges
	// Schemas are the user schemas, sorted.
	Schemas []string
	// DefaultPrivileges are the pg_default_acl rows set in Schemas and the
	// global ones, as [pgdefaultacl.ReadRows] and
	// [pgdefaultacl.ReadGlobalRows] read them.
	DefaultPrivileges []pgdefaultacl.Row
	// Settings are the properties of the objects and columns a run can change
	// in place.
	Settings []Setting
}

// Setting is one property of an object or a column that a run can change
// without creating or dropping anything: a column's type, default or
// nullability, a table's row security and options, a trigger's firing state,
// an owner, a comment, or a definition.
type Setting struct {
	// Catalog and OID identify the object, as for [Object].
	Catalog string
	OID     uint32
	// Column is the column's attnum for a column's setting, and zero
	// otherwise.
	Column int16
	// Aspect names the property: "type", "default", "not null", "row
	// security", "options", "state", "owner", "comment" or "definition".
	Aspect string
	// Name is the object or column as a message names it.
	Name string
	// Value is the property as the catalog reports it.
	Value string
	// Restore is the statement that sets the property to Value, and empty
	// where a reset cannot: a column's type, an index's definition, a
	// policy's expressions. [Changed] names such a property when it differs.
	Restore string
}

type settingKey struct {
	catalog string
	oid     uint32
	column  int16
	aspect  string
}

func (s Setting) key() settingKey {
	return settingKey{catalog: s.Catalog, oid: s.OID, column: s.Column, aspect: s.Aspect}
}

// Read records q's database. It reads the user schemas -- every schema but
// the server's own -- with the objects in them that a run can create: tables,
// views, materialized views, foreign tables, sequences, indexes, routines,
// types, collations, extended statistics and text search configurations and
// dictionaries, and on tables the triggers, policies, rules, constraints and
// columns. It reads the extensions, the large objects, the privileges on
// schemas, relations, columns, routines and types, and the [Setting]s of each
// object and column. An object an extension owns is left out: the extension
// stands for it.
func Read(ctx context.Context, q Querier) (Snapshot, error) {
	var snapshot Snapshot
	var err error
	if snapshot.Objects, err = readObjects(ctx, q); err != nil {
		return Snapshot{}, err
	}
	if snapshot.Columns, err = readColumns(ctx, q); err != nil {
		return Snapshot{}, err
	}
	if snapshot.Privileges, err = readPrivileges(ctx, q); err != nil {
		return Snapshot{}, err
	}
	if snapshot.Settings, err = readSettings(ctx, q); err != nil {
		return Snapshot{}, err
	}
	for _, object := range snapshot.Objects {
		if object.Catalog == "pg_namespace" {
			snapshot.Schemas = append(snapshot.Schemas, strings.Trim(object.Name, `"`))
		}
	}
	slices.Sort(snapshot.Schemas)
	scoped, err := pgdefaultacl.ReadRows(ctx, q, snapshot.Schemas)
	if err != nil {
		return Snapshot{}, err
	}
	global, err := pgdefaultacl.ReadGlobalRows(ctx, q)
	if err != nil {
		return Snapshot{}, err
	}
	snapshot.DefaultPrivileges = slices.Concat(global, scoped)
	return snapshot, nil
}

// userNamespace is the predicate for a user schema on the alias n.
const userNamespace = `n.nspname NOT LIKE 'pg\_%' ESCAPE '\' AND n.nspname <> 'information_schema'`

// notExtensionMember is the predicate for an object no extension owns, for a
// catalog and an OID expression.
func notExtensionMember(catalog, oid string) string {
	return `NOT EXISTS (SELECT 1 FROM pg_depend d WHERE d.classid = '` + catalog +
		`'::regclass AND d.objid = ` + oid + ` AND d.deptype = 'e')`
}

// objectsQuery lists the objects [Read] records, one row each: the catalog,
// the OID, the kind, the name, the drop statement and its order.
var objectsQuery = `
	SELECT 'pg_namespace', n.oid, 'schema', format('%I', n.nspname),
		format('DROP SCHEMA IF EXISTS %I CASCADE', n.nspname), 90
	FROM pg_namespace n
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_namespace", "n.oid") + `

	UNION ALL
	SELECT 'pg_extension', e.oid, 'extension', format('%I', e.extname),
		format('DROP EXTENSION IF EXISTS %I CASCADE', e.extname), 80
	FROM pg_extension e

	UNION ALL
	SELECT 'pg_class', c.oid,
		CASE c.relkind WHEN 'v' THEN 'view' WHEN 'm' THEN 'materialized view' WHEN 'f' THEN 'foreign table'
			WHEN 'S' THEN 'sequence' WHEN 'i' THEN 'index' WHEN 'I' THEN 'index' ELSE 'table' END,
		format('%I.%I', n.nspname, c.relname),
		format('DROP %s IF EXISTS %I.%I CASCADE',
			CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW' WHEN 'f' THEN 'FOREIGN TABLE'
				WHEN 'S' THEN 'SEQUENCE' WHEN 'i' THEN 'INDEX' WHEN 'I' THEN 'INDEX' ELSE 'TABLE' END,
			n.nspname, c.relname),
		CASE c.relkind WHEN 'i' THEN 30 WHEN 'I' THEN 30 WHEN 'v' THEN 40 WHEN 'm' THEN 40 WHEN 'S' THEN 60 ELSE 50 END
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + `
	  AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S', 'i', 'I')
	  -- A partition's index goes with its parent's, which the server refuses
	  -- to drop on its own. A partition is a table like any other.
	  AND NOT (c.relispartition AND c.relkind IN ('i', 'I'))
	  AND ` + notExtensionMember("pg_class", "c.oid") + `
	  -- An index a constraint stands for goes with the constraint, and a
	  -- sequence a column owns goes with the column. An index depends on its
	  -- columns the same way an owned sequence does, so the second test names
	  -- the sequence.
	  AND NOT EXISTS (SELECT 1 FROM pg_constraint con WHERE con.conindid = c.oid AND con.contype IN ('p', 'u', 'x'))
	  AND NOT (c.relkind = 'S' AND EXISTS (
		SELECT 1 FROM pg_depend d
		WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
		  AND d.refobjsubid > 0 AND d.deptype IN ('a', 'i')
	  ))

	UNION ALL
	SELECT 'pg_proc', p.oid,
		CASE p.prokind WHEN 'p' THEN 'procedure' WHEN 'a' THEN 'aggregate' ELSE 'function' END,
		p.oid::regprocedure::text,
		format('DROP %s IF EXISTS %s CASCADE',
			CASE p.prokind WHEN 'p' THEN 'PROCEDURE' WHEN 'a' THEN 'AGGREGATE' ELSE 'FUNCTION' END,
			p.oid::regprocedure),
		70
	FROM pg_proc p
	JOIN pg_namespace n ON n.oid = p.pronamespace
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_proc", "p.oid") + `

	UNION ALL
	SELECT 'pg_type', t.oid, CASE t.typtype WHEN 'd' THEN 'domain' ELSE 'type' END,
		format('%I.%I', n.nspname, t.typname),
		format('DROP %s IF EXISTS %I.%I CASCADE', CASE t.typtype WHEN 'd' THEN 'DOMAIN' ELSE 'TYPE' END,
			n.nspname, t.typname),
		75
	FROM pg_type t
	JOIN pg_namespace n ON n.oid = t.typnamespace
	LEFT JOIN pg_class c ON c.oid = t.typrelid
	WHERE ` + userNamespace + `
	  AND (t.typtype IN ('e', 'd', 'r') OR (t.typtype = 'c' AND c.relkind = 'c'))
	  AND ` + notExtensionMember("pg_type", "t.oid") + `

	UNION ALL
	SELECT 'pg_collation', co.oid, 'collation', format('%I.%I', n.nspname, co.collname),
		format('DROP COLLATION IF EXISTS %I.%I CASCADE', n.nspname, co.collname), 76
	FROM pg_collation co
	JOIN pg_namespace n ON n.oid = co.collnamespace
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_collation", "co.oid") + `

	UNION ALL
	SELECT 'pg_statistic_ext', s.oid, 'statistics', format('%I.%I', n.nspname, s.stxname),
		format('DROP STATISTICS IF EXISTS %I.%I', n.nspname, s.stxname), 25
	FROM pg_statistic_ext s
	JOIN pg_namespace n ON n.oid = s.stxnamespace
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_statistic_ext", "s.oid") + `

	UNION ALL
	SELECT 'pg_ts_config', cfg.oid, 'text search configuration', format('%I.%I', n.nspname, cfg.cfgname),
		format('DROP TEXT SEARCH CONFIGURATION IF EXISTS %I.%I CASCADE', n.nspname, cfg.cfgname), 77
	FROM pg_ts_config cfg
	JOIN pg_namespace n ON n.oid = cfg.cfgnamespace
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_ts_config", "cfg.oid") + `

	UNION ALL
	SELECT 'pg_ts_dict', dict.oid, 'text search dictionary', format('%I.%I', n.nspname, dict.dictname),
		format('DROP TEXT SEARCH DICTIONARY IF EXISTS %I.%I CASCADE', n.nspname, dict.dictname), 78
	FROM pg_ts_dict dict
	JOIN pg_namespace n ON n.oid = dict.dictnamespace
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_ts_dict", "dict.oid") + `

	UNION ALL
	SELECT 'pg_trigger', tg.oid, 'trigger', format('%I on %s', tg.tgname, tg.tgrelid::regclass),
		format('DROP TRIGGER IF EXISTS %I ON %s', tg.tgname, tg.tgrelid::regclass), 10
	FROM pg_trigger tg
	JOIN pg_class c ON c.oid = tg.tgrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + ` AND NOT tg.tgisinternal AND tg.tgparentid = 0
	  AND ` + notExtensionMember("pg_trigger", "tg.oid") + `

	UNION ALL
	SELECT 'pg_policy', pol.oid, 'policy', format('%I on %s', pol.polname, pol.polrelid::regclass),
		format('DROP POLICY IF EXISTS %I ON %s', pol.polname, pol.polrelid::regclass), 10
	FROM pg_policy pol
	JOIN pg_class c ON c.oid = pol.polrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + `

	UNION ALL
	SELECT 'pg_rewrite', r.oid, 'rule', format('%I on %s', r.rulename, r.ev_class::regclass),
		format('DROP RULE IF EXISTS %I ON %s', r.rulename, r.ev_class::regclass), 10
	FROM pg_rewrite r
	JOIN pg_class c ON c.oid = r.ev_class
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + ` AND r.rulename <> '_RETURN'
	  AND ` + notExtensionMember("pg_rewrite", "r.oid") + `

	UNION ALL
	SELECT 'pg_constraint', con.oid, 'constraint', format('%I on %s', con.conname, con.conrelid::regclass),
		format('ALTER TABLE %s DROP CONSTRAINT IF EXISTS %I CASCADE', con.conrelid::regclass, con.conname), 20
	FROM pg_constraint con
	JOIN pg_class c ON c.oid = con.conrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + ` AND con.conislocal AND con.conparentid = 0 AND NOT c.relispartition
	  -- PostgreSQL 18 records a column's NOT NULL as a constraint, which the
	  -- server refuses to drop while a primary key holds the column. It is
	  -- the column's "not null" setting here, on every version.
	  AND con.contype <> 'n'
	  AND ` + notExtensionMember("pg_constraint", "con.oid") + `

	UNION ALL
	SELECT 'pg_constraint', con.oid, 'constraint', format('%I on %s', con.conname, con.contypid::regtype),
		format('ALTER DOMAIN %s DROP CONSTRAINT IF EXISTS %I CASCADE', con.contypid::regtype, con.conname), 20
	FROM pg_constraint con
	JOIN pg_type t ON t.oid = con.contypid
	JOIN pg_namespace n ON n.oid = t.typnamespace
	WHERE ` + userNamespace + ` AND con.contypid <> 0
	  AND ` + notExtensionMember("pg_constraint", "con.oid") + `

	UNION ALL
	SELECT 'pg_largeobject_metadata', lo.oid, 'large object', lo.oid::text,
		format('SELECT lo_unlink(%s)', lo.oid), 85
	FROM pg_largeobject_metadata lo
`

func readObjects(ctx context.Context, q Querier) ([]Object, error) {
	rows, err := q.QueryContext(ctx, objectsQuery)
	if err != nil {
		return nil, fmt.Errorf("read the dev database's objects: %w", err)
	}
	defer rows.Close()
	var objects []Object
	for rows.Next() {
		var object Object
		if err := rows.Scan(&object.Catalog, &object.OID, &object.Kind, &object.Name, &object.Drop, &object.Order); err != nil {
			return nil, fmt.Errorf("scan the dev database's objects: %w", err)
		}
		objects = append(objects, object)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the dev database's objects: %w", err)
	}
	return objects, nil
}

// columnsQuery lists the columns of the tables in user schemas: the table's
// own columns, not ones a parent or a partitioned table gives it.
var columnsQuery = `
	SELECT a.attrelid, a.attnum, format('%s.%I', a.attrelid::regclass, a.attname),
		format('ALTER TABLE %s DROP COLUMN IF EXISTS %I CASCADE', a.attrelid::regclass, a.attname)
	FROM pg_attribute a
	JOIN pg_class c ON c.oid = a.attrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + `
	  AND c.relkind IN ('r', 'p', 'f') AND NOT c.relispartition
	  AND a.attnum > 0 AND NOT a.attisdropped AND a.attislocal AND a.attinhcount = 0
	  AND ` + notExtensionMember("pg_class", "c.oid")

func readColumns(ctx context.Context, q Querier) ([]Column, error) {
	rows, err := q.QueryContext(ctx, columnsQuery)
	if err != nil {
		return nil, fmt.Errorf("read the dev database's columns: %w", err)
	}
	defer rows.Close()
	var columns []Column
	for rows.Next() {
		var column Column
		if err := rows.Scan(&column.Relation, &column.Number, &column.Name, &column.Drop); err != nil {
			return nil, fmt.Errorf("scan the dev database's columns: %w", err)
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the dev database's columns: %w", err)
	}
	return columns, nil
}

// privilegesQuery lists the access list of every schema, relation, column,
// routine and type in user schemas, the built-in default standing in for a
// list the catalog does not hold. A column with no list of its own holds none.
var privilegesQuery = `
	SELECT 'pg_namespace', n.oid, 0::int2, format('SCHEMA %I', n.nspname), '',
		COALESCE(array_to_json(n.nspacl), array_to_json(acldefault('n'::"char", n.nspowner)))::text
	FROM pg_namespace n
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_namespace", "n.oid") + `

	UNION ALL
	SELECT 'pg_class', c.oid, 0::int2,
		format('%s %s', CASE c.relkind WHEN 'S' THEN 'SEQUENCE' ELSE 'TABLE' END, c.oid::regclass), '',
		COALESCE(array_to_json(c.relacl),
			array_to_json(acldefault((CASE c.relkind WHEN 'S' THEN 's' ELSE 'r' END)::"char", c.relowner)))::text
	FROM pg_class c
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + ` AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
	  AND ` + notExtensionMember("pg_class", "c.oid") + `

	UNION ALL
	SELECT 'pg_class', c.oid, a.attnum, format('TABLE %s', c.oid::regclass), format('%I', a.attname),
		COALESCE(array_to_json(a.attacl), '[]'::json)::text
	FROM pg_attribute a
	JOIN pg_class c ON c.oid = a.attrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + ` AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
	  AND a.attnum > 0 AND NOT a.attisdropped
	  AND ` + notExtensionMember("pg_class", "c.oid") + `

	UNION ALL
	SELECT 'pg_proc', p.oid, 0::int2,
		format('%s %s', CASE p.prokind WHEN 'p' THEN 'PROCEDURE' ELSE 'FUNCTION' END, p.oid::regprocedure), '',
		COALESCE(array_to_json(p.proacl), array_to_json(acldefault('f'::"char", p.proowner)))::text
	FROM pg_proc p
	JOIN pg_namespace n ON n.oid = p.pronamespace
	WHERE ` + userNamespace + ` AND p.prokind <> 'a' AND ` + notExtensionMember("pg_proc", "p.oid") + `

	UNION ALL
	SELECT 'pg_type', t.oid, 0::int2,
		format('%s %s', CASE t.typtype WHEN 'd' THEN 'DOMAIN' ELSE 'TYPE' END, t.oid::regtype), '',
		COALESCE(array_to_json(t.typacl), array_to_json(acldefault('T'::"char", t.typowner)))::text
	FROM pg_type t
	JOIN pg_namespace n ON n.oid = t.typnamespace
	LEFT JOIN pg_class c ON c.oid = t.typrelid
	WHERE ` + userNamespace + `
	  AND (t.typtype IN ('e', 'd', 'r') OR (t.typtype = 'c' AND c.relkind = 'c'))
	  AND ` + notExtensionMember("pg_type", "t.oid") + `
`

func readPrivileges(ctx context.Context, q Querier) ([]Privileges, error) {
	rows, err := q.QueryContext(ctx, privilegesQuery)
	if err != nil {
		return nil, fmt.Errorf("read the dev database's privileges: %w", err)
	}
	defer rows.Close()
	var all []Privileges
	for rows.Next() {
		var privileges Privileges
		var acl string
		if err := rows.Scan(&privileges.Catalog, &privileges.OID, &privileges.Column, &privileges.Target,
			&privileges.ColumnName, &acl); err != nil {
			return nil, fmt.Errorf("scan the dev database's privileges: %w", err)
		}
		if privileges.ACL, err = aclitem.ParseJSON(acl); err != nil {
			return nil, fmt.Errorf("read the privileges on %s: %w", privileges.Target, err)
		}
		all = append(all, privileges)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the dev database's privileges: %w", err)
	}
	return all, nil
}

// relationKeyword is the keyword a statement names a relation on the alias c
// with.
const relationKeyword = `CASE c.relkind WHEN 'v' THEN 'VIEW' WHEN 'm' THEN 'MATERIALIZED VIEW'
	WHEN 'f' THEN 'FOREIGN TABLE' WHEN 'S' THEN 'SEQUENCE' WHEN 'i' THEN 'INDEX' WHEN 'I' THEN 'INDEX'
	ELSE 'TABLE' END`

// commentKeyword is the keyword COMMENT ON names an object with, for the type
// pg_identify_object reports on the alias i. It is NULL for a type the reset
// does not comment on.
const commentKeyword = `CASE i.type WHEN 'table' THEN 'TABLE' WHEN 'view' THEN 'VIEW'
	WHEN 'materialized view' THEN 'MATERIALIZED VIEW' WHEN 'foreign table' THEN 'FOREIGN TABLE'
	WHEN 'sequence' THEN 'SEQUENCE' WHEN 'index' THEN 'INDEX' WHEN 'function' THEN 'FUNCTION'
	WHEN 'procedure' THEN 'PROCEDURE' WHEN 'aggregate' THEN 'AGGREGATE' WHEN 'type' THEN 'TYPE'
	WHEN 'composite type' THEN 'TYPE' WHEN 'domain' THEN 'DOMAIN' WHEN 'schema' THEN 'SCHEMA'
	WHEN 'extension' THEN 'EXTENSION' WHEN 'collation' THEN 'COLLATION' WHEN 'statistics object' THEN 'STATISTICS'
	WHEN 'text search configuration' THEN 'TEXT SEARCH CONFIGURATION'
	WHEN 'text search dictionary' THEN 'TEXT SEARCH DICTIONARY' WHEN 'trigger' THEN 'TRIGGER'
	WHEN 'policy' THEN 'POLICY' WHEN 'rule' THEN 'RULE' WHEN 'table constraint' THEN 'CONSTRAINT' END`

// settingsQuery lists the settings [Read] records: the catalog, the OID, the
// column, the aspect, the name, the value and the statement that restores it.
// It covers every object and column in user schemas, so a setting of an
// object a later read holds and start does not is there too, and is ignored.
var settingsQuery = `
	WITH relations AS (
		SELECT c.oid, c.relkind, c.relowner, c.relrowsecurity, c.relforcerowsecurity, c.reloptions,
			c.relreplident, c.relpersistence, c.relispartition, ` + relationKeyword + ` AS keyword,
			c.relkind = 'S' AND EXISTS (
				SELECT 1 FROM pg_depend d
				WHERE d.classid = 'pg_class'::regclass AND d.objid = c.oid
				  AND d.refobjsubid > 0 AND d.deptype IN ('a', 'i')
			) AS owned
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE ` + userNamespace + `
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S', 'i', 'I')
		  AND ` + notExtensionMember("pg_class", "c.oid") + `
	),
	columns AS (
		SELECT a.attrelid, a.attnum, a.attname, a.atttypid, a.atttypmod, a.attcollation, a.attidentity,
			a.attgenerated, a.attnotnull, pg_get_expr(d.adbin, d.adrelid) AS expression, r.keyword,
			format('column %s.%I', a.attrelid::regclass, a.attname) AS label
		FROM pg_attribute a
		JOIN relations r ON r.oid = a.attrelid
		LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum
		WHERE r.relkind IN ('r', 'p', 'f') AND NOT r.relispartition
		  AND a.attnum > 0 AND NOT a.attisdropped AND a.attislocal AND a.attinhcount = 0
	),
	routines AS (
		SELECT p.oid, p.prokind, p.proowner
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_proc", "p.oid") + `
	),
	types AS (
		SELECT t.oid, t.typtype, t.typowner, t.typbasetype, t.typtypmod, t.typnotnull, t.typdefault, t.typrelid,
			CASE t.typtype WHEN 'd' THEN 'DOMAIN' ELSE 'TYPE' END AS keyword
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		LEFT JOIN pg_class c ON c.oid = t.typrelid
		WHERE ` + userNamespace + `
		  AND (t.typtype IN ('e', 'd', 'r') OR (t.typtype = 'c' AND c.relkind = 'c'))
		  AND ` + notExtensionMember("pg_type", "t.oid") + `
	),
	triggers AS (
		SELECT tg.oid, tg.tgname, tg.tgrelid, tg.tgenabled
		FROM pg_trigger tg
		JOIN pg_class c ON c.oid = tg.tgrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE ` + userNamespace + ` AND NOT tg.tgisinternal AND tg.tgparentid = 0
		  AND ` + notExtensionMember("pg_trigger", "tg.oid") + `
	),
	recorded AS (
		SELECT objects.catalog, objects.oid
		FROM (` + objectsQuery + `) AS objects(catalog, oid, kind, name, drop_statement, drop_order)
		-- A large object's comment is filed under pg_largeobject.
		WHERE objects.catalog <> 'pg_largeobject_metadata'
	)
	SELECT 'pg_class', attrelid, attnum, 'type', label,
		concat_ws(' ', format_type(atttypid, atttypmod),
			(SELECT 'collate ' || co.collname FROM pg_collation co WHERE co.oid = attcollation),
			CASE WHEN attidentity IN ('a', 'd') THEN 'identity ' || attidentity::text END,
			CASE WHEN attgenerated IN ('s', 'v') THEN 'generated ' || attgenerated::text || ' ' || expression END),
		''
	FROM columns

	UNION ALL
	SELECT 'pg_class', attrelid, attnum, 'default', label, COALESCE(expression, ''),
		CASE WHEN expression IS NULL
			THEN format('ALTER %s %s ALTER COLUMN %I DROP DEFAULT', keyword, attrelid::regclass, attname)
			ELSE format('ALTER %s %s ALTER COLUMN %I SET DEFAULT %s', keyword, attrelid::regclass, attname, expression)
		END
	FROM columns
	WHERE attidentity NOT IN ('a', 'd') AND attgenerated NOT IN ('s', 'v')

	UNION ALL
	SELECT 'pg_class', attrelid, attnum, 'not null', label, attnotnull::text,
		format('ALTER %s %s ALTER COLUMN %I %s NOT NULL', keyword, attrelid::regclass, attname,
			CASE WHEN attnotnull THEN 'SET' ELSE 'DROP' END)
	FROM columns

	UNION ALL
	SELECT 'pg_class', oid, 0::int2, 'row security', format('table %s', oid::regclass),
		concat_ws(' ', relrowsecurity, relforcerowsecurity),
		format('ALTER TABLE %s %s ROW LEVEL SECURITY, %s ROW LEVEL SECURITY', oid::regclass,
			CASE WHEN relrowsecurity THEN 'ENABLE' ELSE 'DISABLE' END,
			CASE WHEN relforcerowsecurity THEN 'FORCE' ELSE 'NO FORCE' END)
	FROM relations
	WHERE relkind IN ('r', 'p')

	UNION ALL
	SELECT 'pg_class', oid, 0::int2, 'options', format('%s %s', lower(keyword), oid::regclass),
		concat_ws(' ', reloptions::text, 'replica identity ' || relreplident::text,
			'persistence ' || relpersistence::text),
		''
	FROM relations

	UNION ALL
	SELECT 'pg_class', oid, 0::int2, 'owner', format('%s %s', lower(keyword), oid::regclass),
		pg_get_userbyid(relowner),
		format('ALTER %s %s OWNER TO %I', keyword, oid::regclass, pg_get_userbyid(relowner))
	FROM relations
	-- An index and a sequence a column owns follow their table's owner.
	WHERE relkind NOT IN ('i', 'I') AND NOT owned

	UNION ALL
	SELECT 'pg_class', oid, 0::int2, 'definition', format('view %s', oid::regclass), pg_get_viewdef(oid),
		format('CREATE OR REPLACE VIEW %s%s AS %s', oid::regclass,
			COALESCE(' WITH (' || array_to_string(reloptions, ', ') || ')', ''),
			rtrim(pg_get_viewdef(oid), ';'))
	FROM relations
	WHERE relkind = 'v'

	UNION ALL
	SELECT 'pg_class', oid, 0::int2, 'definition', format('materialized view %s', oid::regclass),
		pg_get_viewdef(oid), ''
	FROM relations
	WHERE relkind = 'm'

	UNION ALL
	SELECT 'pg_class', oid, 0::int2, 'definition', format('index %s', oid::regclass), pg_get_indexdef(oid), ''
	FROM relations
	WHERE relkind IN ('i', 'I')

	UNION ALL
	SELECT 'pg_class', s.seqrelid, 0::int2, 'definition', format('sequence %s', s.seqrelid::regclass),
		concat_ws(' ', s.seqtypid::regtype, s.seqstart, s.seqincrement, s.seqmax, s.seqmin, s.seqcache, s.seqcycle),
		''
	FROM pg_sequence s
	JOIN relations r ON r.oid = s.seqrelid

	UNION ALL
	SELECT 'pg_proc', oid, 0::int2, 'definition',
		format('%s %s', CASE prokind WHEN 'p' THEN 'procedure' ELSE 'function' END, oid::regprocedure),
		pg_get_functiondef(oid), pg_get_functiondef(oid)
	FROM routines
	WHERE prokind <> 'a'

	UNION ALL
	SELECT 'pg_proc', oid, 0::int2, 'owner', format('routine %s', oid::regprocedure), pg_get_userbyid(proowner),
		format('ALTER ROUTINE %s OWNER TO %I', oid::regprocedure, pg_get_userbyid(proowner))
	FROM routines

	UNION ALL
	SELECT 'pg_type', types.oid, 0::int2, 'definition', format('%s %s', lower(keyword), types.oid::regtype),
		COALESCE(CASE typtype
			WHEN 'e' THEN (
				SELECT string_agg(e.enumlabel, ',' ORDER BY e.enumsortorder)
				FROM pg_enum e WHERE e.enumtypid = types.oid)
			WHEN 'd' THEN concat_ws(' ', format_type(typbasetype, typtypmod), typnotnull, typdefault)
			WHEN 'r' THEN (
				SELECT concat_ws(' ', rg.rngsubtype::regtype, rg.rngcollation, rg.rngsubopc, rg.rngcanonical,
					rg.rngsubdiff)
				FROM pg_range rg WHERE rg.rngtypid = types.oid)
			ELSE (
				SELECT string_agg(format('%I %s', a.attname, format_type(a.atttypid, a.atttypmod)), ','
					ORDER BY a.attnum)
				FROM pg_attribute a WHERE a.attrelid = types.typrelid AND a.attnum > 0 AND NOT a.attisdropped)
		END, ''),
		''
	FROM types

	UNION ALL
	SELECT 'pg_type', oid, 0::int2, 'owner', format('%s %s', lower(keyword), oid::regtype),
		pg_get_userbyid(typowner), format('ALTER %s %s OWNER TO %I', keyword, oid::regtype, pg_get_userbyid(typowner))
	FROM types

	UNION ALL
	SELECT 'pg_namespace', n.oid, 0::int2, 'owner', format('schema %I', n.nspname), pg_get_userbyid(n.nspowner),
		format('ALTER SCHEMA %I OWNER TO %I', n.nspname, pg_get_userbyid(n.nspowner))
	FROM pg_namespace n
	WHERE ` + userNamespace + ` AND ` + notExtensionMember("pg_namespace", "n.oid") + `

	UNION ALL
	SELECT 'pg_trigger', oid, 0::int2, 'state', format('trigger %I on %s', tgname, tgrelid::regclass),
		tgenabled::text,
		format('ALTER TABLE %s %s TRIGGER %I', tgrelid::regclass,
			CASE tgenabled WHEN 'D' THEN 'DISABLE' WHEN 'R' THEN 'ENABLE REPLICA' WHEN 'A' THEN 'ENABLE ALWAYS'
				ELSE 'ENABLE' END,
			tgname)
	FROM triggers

	UNION ALL
	SELECT 'pg_trigger', oid, 0::int2, 'definition', format('trigger %I on %s', tgname, tgrelid::regclass),
		pg_get_triggerdef(oid), ''
	FROM triggers

	UNION ALL
	SELECT 'pg_policy', pol.oid, 0::int2, 'definition', format('policy %I on %s', pol.polname, pol.polrelid::regclass),
		concat_ws(' ', pol.polcmd::text, pol.polpermissive, pol.polroles::regrole[]::text,
			pg_get_expr(pol.polqual, pol.polrelid), pg_get_expr(pol.polwithcheck, pol.polrelid)),
		''
	FROM pg_policy pol
	JOIN pg_class c ON c.oid = pol.polrelid
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + `

	UNION ALL
	SELECT 'pg_constraint', con.oid, 0::int2, 'definition',
		format('constraint %I on %s', con.conname,
			CASE WHEN con.conrelid <> 0 THEN con.conrelid::regclass::text ELSE con.contypid::regtype::text END),
		pg_get_constraintdef(con.oid), ''
	FROM pg_constraint con
	LEFT JOIN pg_class c ON c.oid = con.conrelid
	LEFT JOIN pg_type t ON t.oid = con.contypid
	JOIN pg_namespace n ON n.oid = COALESCE(c.relnamespace, t.typnamespace)
	WHERE ` + userNamespace + ` AND con.contype <> 'n'
	  AND ` + notExtensionMember("pg_constraint", "con.oid") + `

	UNION ALL
	SELECT 'pg_rewrite', r.oid, 0::int2, 'definition', format('rule %I on %s', r.rulename, r.ev_class::regclass),
		pg_get_ruledef(r.oid), ''
	FROM pg_rewrite r
	JOIN pg_class c ON c.oid = r.ev_class
	JOIN pg_namespace n ON n.oid = c.relnamespace
	WHERE ` + userNamespace + ` AND r.rulename <> '_RETURN'
	  AND ` + notExtensionMember("pg_rewrite", "r.oid") + `

	UNION ALL
	SELECT recorded.catalog, recorded.oid, 0::int2, 'comment', concat_ws(' ', i.type, i.identity),
		COALESCE(d.description, ''),
		CASE WHEN k.keyword IS NULL THEN ''
			ELSE format('COMMENT ON %s %s IS %s', k.keyword, i.identity, COALESCE(quote_literal(d.description), 'NULL'))
		END
	FROM recorded
	CROSS JOIN LATERAL pg_identify_object(recorded.catalog::regclass, recorded.oid, 0) i
	CROSS JOIN LATERAL (SELECT ` + commentKeyword + ` AS keyword) k
	LEFT JOIN pg_description d
		ON d.classoid = recorded.catalog::regclass AND d.objoid = recorded.oid AND d.objsubid = 0

	UNION ALL
	SELECT 'pg_class', a.attrelid, a.attnum, 'comment', format('column %s.%I', a.attrelid::regclass, a.attname),
		COALESCE(d.description, ''),
		format('COMMENT ON COLUMN %s.%I IS %s', a.attrelid::regclass, a.attname,
			COALESCE(quote_literal(d.description), 'NULL'))
	FROM pg_attribute a
	JOIN relations r ON r.oid = a.attrelid
	LEFT JOIN pg_description d
		ON d.classoid = 'pg_class'::regclass AND d.objoid = a.attrelid AND d.objsubid = a.attnum
	WHERE r.relkind IN ('r', 'p', 'v', 'm', 'f') AND a.attnum > 0 AND NOT a.attisdropped
`

func readSettings(ctx context.Context, q Querier) ([]Setting, error) {
	rows, err := q.QueryContext(ctx, settingsQuery)
	if err != nil {
		return nil, fmt.Errorf("read the dev database's settings: %w", err)
	}
	defer rows.Close()
	var settings []Setting
	for rows.Next() {
		var setting Setting
		if err := rows.Scan(&setting.Catalog, &setting.OID, &setting.Column, &setting.Aspect, &setting.Name,
			&setting.Value, &setting.Restore); err != nil {
			return nil, fmt.Errorf("scan the dev database's settings: %w", err)
		}
		settings = append(settings, setting)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("read the dev database's settings: %w", err)
	}
	return settings, nil
}

// Missing names each object and column start holds that current does not, in
// order: what a run dropped from the starting point.
func Missing(start, current Snapshot) []string {
	held := make(map[objectKey]bool, len(current.Objects))
	for _, object := range current.Objects {
		held[object.key()] = true
	}
	heldColumns := make(map[columnKey]bool, len(current.Columns))
	for _, column := range current.Columns {
		heldColumns[column.key()] = true
	}
	var missing []string
	for _, object := range start.Objects {
		if !held[object.key()] {
			missing = append(missing, object.Kind+" "+object.Name)
		}
	}
	for _, column := range start.Columns {
		if !heldColumns[column.key()] {
			missing = append(missing, "column "+column.Name)
		}
	}
	slices.Sort(missing)
	return missing
}

// Changed names each setting start holds that current reports with another
// value, in order: what a run changed in place in the starting point. A
// setting of an object current lacks is [Missing]'s.
func Changed(start, current Snapshot) []string {
	held := make(map[settingKey]Setting, len(current.Settings))
	for _, setting := range current.Settings {
		held[setting.key()] = setting
	}
	var changed []string
	for _, want := range start.Settings {
		if have, ok := held[want.key()]; ok && have.Value != want.Value {
			changed = append(changed, want.Name+" "+want.Aspect)
		}
	}
	slices.Sort(changed)
	return changed
}

// Plan returns the statements that return current to start, in the order they
// have to run:
//
//   - the drop of each object current holds and start does not, by
//     [Object.Order], the column drops after the trigger, policy, rule and
//     constraint drops and before the relation drops;
//   - the statement that restores each setting both hold that differs and
//     that a reset can restore: routine and view definitions first, then
//     column defaults and nullability, row security, trigger states, owners
//     and comments;
//   - for each privilege list both hold that differs, a REVOKE ALL and a GRANT
//     of what start holds, for each grantee whose privileges differ;
//   - the statements that return the default privileges start records to what
//     they were.
//
// A column of a table start does not hold goes with its table. What start
// holds and current does not is [Missing]'s, and a setting no statement can
// restore is [Changed]'s; Plan leaves both.
func Plan(start, current Snapshot) []string {
	inStart := make(map[objectKey]bool, len(start.Objects))
	for _, object := range start.Objects {
		inStart[object.key()] = true
	}
	added := make([]Object, 0)
	for _, object := range current.Objects {
		if !inStart[object.key()] {
			added = append(added, object)
		}
	}
	inStartColumns := make(map[columnKey]bool, len(start.Columns))
	for _, column := range start.Columns {
		inStartColumns[column.key()] = true
	}
	var addedColumns []Column
	for _, column := range current.Columns {
		if inStart[objectKey{catalog: "pg_class", oid: column.Relation}] && !inStartColumns[column.key()] {
			addedColumns = append(addedColumns, column)
		}
	}
	slices.SortStableFunc(added, func(a, b Object) int {
		return cmp.Or(cmp.Compare(a.Order, b.Order), strings.Compare(a.Name, b.Name))
	})
	slices.SortFunc(addedColumns, func(a, b Column) int { return strings.Compare(a.Name, b.Name) })

	var statements []string
	columnsDone := false
	for _, object := range added {
		if !columnsDone && object.Order > 20 {
			for _, column := range addedColumns {
				statements = append(statements, column.Drop)
			}
			columnsDone = true
		}
		statements = append(statements, object.Drop)
	}
	if !columnsDone {
		for _, column := range addedColumns {
			statements = append(statements, column.Drop)
		}
	}
	statements = append(statements, restoreStatements(start, current)...)
	statements = append(statements, privilegeStatements(start, current)...)
	// A default set in a schema the run created goes with the schema, which
	// is dropped by then; the rest return to what start holds.
	var kept []pgdefaultacl.Row
	for _, row := range current.DefaultPrivileges {
		if row.Schema == "" || slices.Contains(start.Schemas, row.Schema) {
			kept = append(kept, row)
		}
	}
	for _, statement := range pgdefaultacl.Reconcile(start.DefaultPrivileges, kept) {
		statements = append(statements, statement.Statement)
	}
	return statements
}

// restoreRank orders the statements that restore settings. A definition goes
// first, a routine's before a view's, since a view or a default can call a
// routine; the rest do not depend on each other.
var restoreRank = map[string]int{
	"definition":   0,
	"default":      1,
	"not null":     2,
	"row security": 3,
	"state":        4,
	"owner":        5,
	"comment":      6,
}

// restoreStatements returns the statement that restores each setting current
// holds with a value other than start's, where one exists.
func restoreStatements(start, current Snapshot) []string {
	held := make(map[settingKey]Setting, len(current.Settings))
	for _, setting := range current.Settings {
		held[setting.key()] = setting
	}
	var restore []Setting
	for _, want := range start.Settings {
		if have, ok := held[want.key()]; ok && have.Value != want.Value && want.Restore != "" {
			restore = append(restore, want)
		}
	}
	slices.SortFunc(restore, func(a, b Setting) int {
		return cmp.Or(
			cmp.Compare(restoreRank[a.Aspect], restoreRank[b.Aspect]),
			cmp.Compare(catalogRank(a.Catalog), catalogRank(b.Catalog)),
			strings.Compare(a.Name, b.Name),
		)
	})
	statements := make([]string, 0, len(restore))
	for _, setting := range restore {
		statements = append(statements, setting.Restore)
	}
	return statements
}

func catalogRank(catalog string) int {
	if catalog == "pg_proc" {
		return 0
	}
	return 1
}

// privilegeStatements returns the REVOKE and GRANT statements that return
// each privilege list current holds to the one start holds.
func privilegeStatements(start, current Snapshot) []string {
	held := make(map[privilegesKey]Privileges, len(current.Privileges))
	for _, privileges := range current.Privileges {
		held[privileges.key()] = privileges
	}
	var statements []string
	for _, want := range start.Privileges {
		have, ok := held[want.key()]
		if !ok {
			continue
		}
		statements = append(statements, reconcileList(want, granted(want.ACL), granted(have.ACL))...)
	}
	return statements
}

// granted indexes an access list by grantee and privilege, recording the
// grant option. Who granted a privilege is not compared: a statement the
// reset runs records the role that runs it as the grantor, which is not a
// difference in what the grantee can do.
func granted(items []aclitem.Item) map[string]map[string]bool {
	held := make(map[string]map[string]bool, len(items))
	for _, item := range items {
		if held[item.Grantee] == nil {
			held[item.Grantee] = make(map[string]bool, len(item.Privileges))
		}
		for _, privilege := range item.Privileges {
			held[item.Grantee][privilege.Name] = held[item.Grantee][privilege.Name] || privilege.Grantable
		}
	}
	return held
}

// reconcileList returns the statements that take one privilege list from have
// to want, grantee by grantee, touching only the grantees that differ.
func reconcileList(target Privileges, want, have map[string]map[string]bool) []string {
	grantees := slices.Sorted(maps.Keys(union(want, have)))
	var statements []string
	for _, grantee := range grantees {
		if maps.Equal(want[grantee], have[grantee]) {
			continue
		}
		on := target.Target
		columns := ""
		if target.ColumnName != "" {
			columns = " (" + target.ColumnName + ")"
		}
		to := pgdefaultacl.Public
		if grantee != "" {
			to = quoteIdent(grantee)
		}
		if len(have[grantee]) > 0 {
			statements = append(statements, "REVOKE ALL PRIVILEGES"+columns+" ON "+on+" FROM "+to)
		}
		var plain, grantable []string
		for _, name := range slices.Sorted(maps.Keys(want[grantee])) {
			if want[grantee][name] {
				grantable = append(grantable, name)
			} else {
				plain = append(plain, name)
			}
		}
		if len(plain) > 0 {
			statements = append(statements, "GRANT "+privilegeList(plain, columns)+" ON "+on+" TO "+to)
		}
		if len(grantable) > 0 {
			statements = append(statements, "GRANT "+privilegeList(grantable, columns)+" ON "+on+" TO "+to+" WITH GRANT OPTION")
		}
	}
	return statements
}

// privilegeList spells privileges for a GRANT, each followed by columns when
// the list is a column's.
func privilegeList(names []string, columns string) string {
	spelled := make([]string, len(names))
	for i, name := range names {
		spelled[i] = name + columns
	}
	return strings.Join(spelled, ", ")
}

func union[V any](a, b map[string]V) map[string]struct{} {
	keys := make(map[string]struct{}, len(a)+len(b))
	for key := range a {
		keys[key] = struct{}{}
	}
	for key := range b {
		keys[key] = struct{}{}
	}
	return keys
}

func quoteIdent(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}
