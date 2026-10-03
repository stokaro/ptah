package pgdefaultacl

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/internal/aclitem"
)

// Row is one row of pg_default_acl: what new objects of one class that one
// grantor creates receive, in one schema or, for a global row, in every
// schema.
//
// A dev database keeps the rows it held when it was claimed, and a reset
// returns the rows to them with [Reconcile]. Rows a database holds are read
// with [ReadRows].
type Row struct {
	// Schema is the schema the row is set in, or the empty string for a
	// global row.
	Schema string
	// Grantor is the role whose new objects the row applies to.
	Grantor string
	// ObjectType is the keyword a statement writes, as [ObjectTypeSQL] names
	// the row's object class.
	ObjectType string
	// ACL is the row's list. A global row's list holds the built-in default
	// too, since the server stores the whole list.
	ACL []aclitem.Item
	// Builtin is what a global row's list is when the row does not exist: the
	// built-in default of its object class for its grantor. It is empty for a
	// row set in a schema, which adds to the global default and starts from
	// nothing.
	Builtin []aclitem.Item
}

// ReadRows returns the rows set in schemas, of the four classes IN SCHEMA can
// name, ordered by schema, grantor and object type. They are the rows a
// cleanup of those schemas revokes, so [Reconcile] over them covers what the
// cleanup would otherwise take away. [ReadGlobalRows] reads the global ones.
//
// The caller decides whether the server has pg_default_acl at all, as for
// [ReadRevokes]. CockroachDB answers the relation in a shape that cannot be
// read back this way (see [ReadGlobalFromShow]), so it is not asked.
func ReadRows(ctx context.Context, q Querier, schemas []string) ([]Row, error) {
	if len(schemas) == 0 {
		return nil, nil
	}
	args := make([]any, len(schemas))
	for i, schema := range schemas {
		args[i] = schema
	}
	return readRows(ctx, q, `
		SELECT
			n.nspname,
			`+GrantorSQL("d")+` AS grantor,
			`+ObjectTypeSQL("d")+` AS object_type,
			`+ListSQL("d")+` AS acl,
			'[]' AS builtin
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		WHERE `+schemaPredicate(len(schemas))+`
		AND `+ScopedClassesSQL("d"), args...)
}

// ReadGlobalRows returns the global rows a declaration can name, with the
// built-in default of each, ordered by grantor and object type. They are the
// rows a realm cleanup returns to the built-in default; see [ReadRows] for
// what the caller decides.
func ReadGlobalRows(ctx context.Context, q Querier) ([]Row, error) {
	return readRows(ctx, q, `
		SELECT
			'',
			pg_get_userbyid(d.defaclrole) AS grantor,
			`+ObjectTypeSQL("d")+` AS object_type,
			`+ListSQL("d")+` AS acl,
			`+BuiltinListSQL("d")+` AS builtin
		FROM pg_default_acl d
		WHERE `+GlobalSQL("d"))
}

func readRows(ctx context.Context, q Querier, query string, args ...any) ([]Row, error) {
	result, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query default privileges: %w", err)
	}
	defer result.Close()

	var rows []Row
	for result.Next() {
		var row Row
		var acl, builtin string
		if err := result.Scan(&row.Schema, &row.Grantor, &row.ObjectType, &acl, &builtin); err != nil {
			return nil, fmt.Errorf("failed to scan default privileges: %w", err)
		}
		if row.ACL, err = aclitem.ParseJSON(acl); err != nil {
			return nil, fmt.Errorf("failed to read the default privileges %s set on %s%s: %w",
				grantorLabel(row.Grantor), row.ObjectType, schemaLabel(row.Schema), err)
		}
		if row.Builtin, err = aclitem.ParseJSON(builtin); err != nil {
			return nil, fmt.Errorf("failed to read the built-in default privileges of %s on %s: %w",
				grantorLabel(row.Grantor), row.ObjectType, err)
		}
		rows = append(rows, row)
	}
	if err := result.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate default privileges: %w", err)
	}
	slices.SortFunc(rows, func(a, b Row) int {
		return compareRowKeys(a.key(), b.key())
	})
	return rows, nil
}

func schemaLabel(schema string) string {
	if schema == "" {
		return ""
	}
	return " in schema " + schema
}

// Reconcile returns the statements that turn current into baseline, in the
// order they have to run: for each row and each grantee whose privileges
// differ, a REVOKE ALL of what the grantee holds, then a GRANT of what the
// baseline gives it, the grantable privileges in a GRANT of their own WITH
// GRANT OPTION. A grantee whose privileges are the same on both sides gets no
// statement, so a row the run did not change is not touched, and nor is a
// grantor the connecting role could not set defaults for.
//
// A row missing on one side is compared as the server would have it: a row set
// in a schema as an empty list, and a global row as its built-in default,
// which the other side's row carries. So a global row the run created is
// returned to the built-in default, and one the run reset to it is set again.
//
// Both sides must cover the same rows: what [ReadRows] answers for one set of
// schemas, with or without what [ReadGlobalRows] answers. A row outside them
// on one side only would be revoked or recreated.
func Reconcile(baseline, current []Row) []Revoke {
	wanted := indexRows(baseline)
	held := indexRows(current)
	keys := slices.SortedFunc(maps.Keys(unionKeys(wanted, held)), compareRowKeys)

	var statements []Revoke
	for _, key := range keys {
		want, have := wanted[key], held[key]
		builtin := want.Builtin
		if len(builtin) == 0 {
			builtin = have.Builtin
		}
		wantList, haveList := want.ACL, have.ACL
		if key.schema == "" {
			if _, ok := wanted[key]; !ok {
				wantList = builtin
			}
			if _, ok := held[key]; !ok {
				haveList = builtin
			}
		}
		statements = append(statements, reconcileRow(key, heldPrivileges(wantList), heldPrivileges(haveList))...)
	}
	return statements
}

// reconcileRow is [Reconcile] for one row.
func reconcileRow(key rowKey, want, have map[string]map[string]bool) []Revoke {
	grantees := slices.Sorted(maps.Keys(unionKeys(want, have)))
	class := classCodes[key.objectType]
	var statements []Revoke
	for _, grantee := range grantees {
		if maps.Equal(want[grantee], have[grantee]) {
			continue
		}
		entry := Revoke{Schema: key.schema, Grantor: key.grantor, Class: class, Grantee: grantee}
		if len(have[grantee]) > 0 {
			revoke := entry
			revoke.Statement = defaultPrivilegeStatement(key, "REVOKE ALL PRIVILEGES ON "+key.objectType+" FROM", grantee)
			statements = append(statements, revoke)
		}
		var plain, grantable []string
		for _, name := range slices.Sorted(maps.Keys(want[grantee])) {
			if want[grantee][name] {
				grantable = append(grantable, name)
			} else {
				plain = append(plain, name)
			}
		}
		for _, grant := range []struct {
			names  []string
			suffix string
		}{{plain, ""}, {grantable, " WITH GRANT OPTION"}} {
			if len(grant.names) == 0 {
				continue
			}
			statement := entry
			statement.Statement = defaultPrivilegeStatement(key,
				"GRANT "+strings.Join(grant.names, ", ")+" ON "+key.objectType+" TO", grantee) + grant.suffix
			statements = append(statements, statement)
		}
	}
	return statements
}

// defaultPrivilegeStatement spells an ALTER DEFAULT PRIVILEGES for one row and
// one grantee: the grantor clause, IN SCHEMA for a row set in a schema, the
// action, then the grantee, PUBLIC for the empty one.
func defaultPrivilegeStatement(key rowKey, action, grantee string) string {
	grantorClause := "FOR ALL ROLES"
	if key.grantor != "" {
		grantorClause = "FOR ROLE " + quote(key.grantor)
	}
	schemaClause := ""
	if key.schema != "" {
		schemaClause = " IN SCHEMA " + quote(key.schema)
	}
	granteeClause := Public
	if grantee != "" {
		granteeClause = quote(grantee)
	}
	return "ALTER DEFAULT PRIVILEGES " + grantorClause + schemaClause + " " + action + " " + granteeClause
}

// rowKey is what identifies a pg_default_acl row: the schema, empty for a
// global row, the grantor and the object class.
type rowKey struct {
	schema     string
	grantor    string
	objectType string
}

func (r Row) key() rowKey {
	return rowKey{schema: r.Schema, grantor: r.Grantor, objectType: r.ObjectType}
}

func compareRowKeys(a, b rowKey) int {
	return cmp.Or(
		strings.Compare(a.schema, b.schema),
		strings.Compare(a.grantor, b.grantor),
		strings.Compare(a.objectType, b.objectType),
	)
}

func indexRows(rows []Row) map[rowKey]Row {
	index := make(map[rowKey]Row, len(rows))
	for _, row := range rows {
		index[row.key()] = row
	}
	return index
}

func unionKeys[K comparable, V any](a, b map[K]V) map[K]struct{} {
	union := make(map[K]struct{}, len(a)+len(b))
	for key := range a {
		union[key] = struct{}{}
	}
	for key := range b {
		union[key] = struct{}{}
	}
	return union
}
