package schemamodel_test

import (
	"reflect"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
)

// scopedKind describes one object kind that can carry a dialect scope: how to
// put one scoped instance into a database, and how to count what survived a
// projection.
type scopedKind struct {
	// name is the Database field the kind lives in, so a failure names the
	// field a reader can go and look at.
	name string
	// declare puts exactly one object of this kind, scoped to scope, into db.
	declare func(db *schemamodel.Database, scope []string)
	// count reports how many objects of this kind db holds.
	count func(db *schemamodel.Database) int
}

func scopedKinds() []scopedKind {
	return []scopedKind{
		{
			name: "Extensions",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Extensions = []schemamodel.Extension{{Name: "pgcrypto", Dialects: scope}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Extensions) },
		},
		{
			name: "Functions",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Functions = []schemamodel.Function{{
					StructName: "Fn", Name: "tenant_id", Returns: "TEXT",
					Language: "plpgsql", Body: "BEGIN RETURN 'x'; END;", Dialects: scope,
				}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Functions) },
		},
		{
			name: "Sequences",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Sequences = []schemamodel.Sequence{{StructName: "Seq", Name: "order_seq", Dialects: scope}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Sequences) },
		},
		{
			name: "Domains",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Domains = []schemamodel.Domain{{StructName: "Dom", Name: "email_t", BaseType: "TEXT", Dialects: scope}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Domains) },
		},
		{
			name: "CompositeTypes",
			declare: func(db *schemamodel.Database, scope []string) {
				db.CompositeTypes = []schemamodel.CompositeType{{
					StructName: "Comp", Name: "address",
					Fields:   []schemamodel.CompositeField{{Name: "city", Type: "TEXT"}},
					Dialects: scope,
				}}
			},
			count: func(db *schemamodel.Database) int { return len(db.CompositeTypes) },
		},
		{
			name: "Ranges",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Ranges = []schemamodel.Range{{StructName: "Rng", Name: "floatrange", Subtype: "float8", Dialects: scope}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Ranges) },
		},
		{
			name: "Views",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Views = []schemamodel.View{{StructName: "V", Name: "active", Body: "SELECT 1", Dialects: scope}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Views) },
		},
		{
			name: "MaterializedViews",
			declare: func(db *schemamodel.Database, scope []string) {
				db.MaterializedViews = []schemamodel.MaterializedView{{
					StructName: "MV", Name: "stats", Body: "SELECT 1",
					Dialects: scope,
				}}
			},
			count: func(db *schemamodel.Database) int { return len(db.MaterializedViews) },
		},
		{
			name: "Triggers",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Triggers = []schemamodel.Trigger{{
					StructName: "T", Name: "touch", Table: "tenants",
					Timing: "BEFORE", Event: "UPDATE", Body: "RETURN NEW;", Dialects: scope,
				}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Triggers) },
		},
		{
			name: "RLSPolicies",
			declare: func(db *schemamodel.Database, scope []string) {
				db.RLSPolicies = []schemamodel.RLSPolicy{{
					StructName: "P", Name: "isolation", Table: "tenants",
					PolicyFor: "ALL", UsingExpression: "true", Dialects: scope,
				}}
			},
			count: func(db *schemamodel.Database) int { return len(db.RLSPolicies) },
		},
		{
			name: "RLSEnabledTables",
			declare: func(db *schemamodel.Database, scope []string) {
				db.RLSEnabledTables = []schemamodel.RLSEnabledTable{{StructName: "E", Table: "tenants", Dialects: scope}}
			},
			count: func(db *schemamodel.Database) int { return len(db.RLSEnabledTables) },
		},
		{
			name: "Roles",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Roles = []schemamodel.Role{{StructName: "R", Name: "app_reader", Dialects: scope}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Roles) },
		},
		{
			name: "Grants",
			declare: func(db *schemamodel.Database, scope []string) {
				db.Grants = []schemamodel.Grant{{
					StructName: "G", Role: "app_reader",
					Privileges: []string{"SELECT"}, OnTable: "tenants", Dialects: scope,
				}}
			},
			count: func(db *schemamodel.Database) int { return len(db.Grants) },
		},
	}
}

// TestScopeToDialect_EveryScopableKindIsProjected walks every object kind that
// accepts a scope and proves the projection both ways for each one: present on
// the dialect the declaration names, absent on the dialect it does not.
//
// Both directions are asserted per kind on purpose. A projection that dropped
// everything would pass a test that only checked absence, and one that dropped
// nothing would pass a test that only checked presence.
func TestScopeToDialect_EveryScopableKindIsProjected(t *testing.T) {
	for _, kind := range scopedKinds() {
		t.Run(kind.name, func(t *testing.T) {
			c := qt.New(t)

			db := &schemamodel.Database{}
			kind.declare(db, []string{"postgres"})

			c.Assert(kind.count(schemamodel.ScopeToDialect(db, "postgres")), qt.Equals, 1)
			c.Assert(kind.count(schemamodel.ScopeToDialect(db, "mysql")), qt.Equals, 0)
			c.Assert(kind.count(schemamodel.ScopeToDialect(db, "postgresql")), qt.Equals, 1)
		})
	}
}

// TestScopeToDialect_ScopableKindsCoverEveryDeclaredScopeField is the guard
// against the next kind being added to the type and forgotten in the
// projection.
//
// A `Dialects` field on a schema object is a promise that the object can be
// scoped away. If a future kind gains the field but no projection line, the
// annotation would parse, the JSON Schema would advertise the attribute, and
// the object would still reach every target -- the exact silence this feature
// exists to remove, reintroduced one object kind at a time. Reflection is what
// makes that impossible to do by accident: the table above must name every
// Database field whose element type declares the promise.
func TestScopeToDialect_ScopableKindsCoverEveryDeclaredScopeField(t *testing.T) {
	c := qt.New(t)

	covered := make([]string, 0, len(scopedKinds()))
	for _, kind := range scopedKinds() {
		covered = append(covered, kind.name)
	}

	c.Assert(databaseFieldsDeclaringScope(), qt.DeepEquals, covered)
}

// TestScopeToDialect_AnUnscopedSchemaIsUnchanged holds the compatibility half:
// a schema written before the attribute existed reaches every target exactly as
// it did, so the projection can only narrow and never widen.
func TestScopeToDialect_AnUnscopedSchemaIsUnchanged(t *testing.T) {
	for _, kind := range scopedKinds() {
		t.Run(kind.name, func(t *testing.T) {
			c := qt.New(t)

			db := &schemamodel.Database{}
			kind.declare(db, nil)

			for _, dialect := range []string{"postgres", "mysql", "mariadb", "sqlite", "clickhouse", "sqlserver"} {
				c.Assert(kind.count(schemamodel.ScopeToDialect(db, dialect)), qt.Equals, 1)
			}
		})
	}
}

// TestScopeToDialect_KeepsWhatTheScopeDoesNotName proves the projection is
// surgical: a scoped object leaving does not take an unscoped neighbor or the
// table structure with it.
func TestScopeToDialect_KeepsWhatTheScopeDoesNotName(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Tenant", Name: "tenants"}},
		Fields: []schemamodel.Field{{StructName: "Tenant", Name: "id", Type: "INTEGER", Primary: true}},
		Functions: []schemamodel.Function{
			{StructName: "Scoped", Name: "pg_only", Returns: "TEXT", Language: "plpgsql", Body: "BEGIN RETURN 'x'; END;", Dialects: []string{"postgres"}},
			{StructName: "Shared", Name: "everywhere", Returns: "TEXT", Language: "sql", Body: "SELECT 'x'"},
		},
	}

	projected := schemamodel.ScopeToDialect(db, "mysql")

	c.Assert(projected.Tables, qt.HasLen, 1)
	c.Assert(projected.Fields, qt.HasLen, 1)
	c.Assert(projected.Functions, qt.HasLen, 1)
	c.Assert(projected.Functions[0].Name, qt.Equals, "everywhere")
}

// TestScopeToDialect_DoesNotMutateTheCallersSchema pins that the projection is
// a copy. Both seams project the same desired state -- the renderer and the
// comparator -- and `ptah schema render` with no --dialect projects it nine
// times in a row. A projection that filtered in place would leave the second
// target rendering what the first one had left of the schema.
func TestScopeToDialect_DoesNotMutateTheCallersSchema(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Functions: []schemamodel.Function{{
			StructName: "Scoped", Name: "pg_only", Returns: "TEXT",
			Language: "plpgsql", Body: "BEGIN RETURN 'x'; END;", Dialects: []string{"postgres"},
		}},
	}

	c.Assert(schemamodel.ScopeToDialect(db, "mysql").Functions, qt.HasLen, 0)
	c.Assert(db.Functions, qt.HasLen, 1)
	c.Assert(schemamodel.ScopeToDialect(db, "postgres").Functions, qt.HasLen, 1)
}

// TestOmissionsForDialect_NamesWhatLeftAndWhyItLeft covers the report the
// commands print. An absent object is indistinguishable from one that was never
// declared, so the projection alone cannot tell an operator anything.
func TestOmissionsForDialect_NamesWhatLeftAndWhyItLeft(t *testing.T) {
	db := &schemamodel.Database{
		Extensions: []schemamodel.Extension{{Name: "pgcrypto", Dialects: []string{"postgres"}}},
		Functions: []schemamodel.Function{
			{StructName: "Scoped", Name: "pg_only", Dialects: []string{"cockroachdb", "postgres"}},
			{StructName: "Shared", Name: "everywhere"},
		},
	}

	tests := []struct {
		name    string
		dialect string
		want    []schemamodel.ScopedObject
	}{
		{
			name:    "the excluded target is told what it is not getting",
			dialect: "mysql",
			want: []schemamodel.ScopedObject{
				{Kind: "extension", Name: "pgcrypto", Dialects: []string{"postgres"}},
				{Kind: "function", Name: "pg_only", Dialects: []string{"cockroachdb", "postgres"}},
			},
		},
		{
			name:    "a named target is told nothing, because nothing left",
			dialect: "postgres",
			want:    nil,
		},
		{
			name:    "a partially named target hears only about what it lost",
			dialect: "cockroachdb",
			want: []schemamodel.ScopedObject{
				{Kind: "extension", Name: "pgcrypto", Dialects: []string{"postgres"}},
			},
		},
		{
			name:    "a target that names no platform hears nothing, because nothing is projected",
			dialect: "not-a-database",
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(schemamodel.OmissionsForDialect(db, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestScopedObjects_ReportsEveryScopeRegardlessOfTarget covers the exporter's
// question, which has no dialect in it: what scopes exist at all.
func TestScopedObjects_ReportsEveryScopeRegardlessOfTarget(t *testing.T) {
	c := qt.New(t)

	db := &schemamodel.Database{
		Roles: []schemamodel.Role{
			{StructName: "R", Name: "app_reader", Dialects: []string{"postgres"}},
			{StructName: "S", Name: "unscoped"},
		},
	}

	c.Assert(schemamodel.ScopedObjects(db), qt.DeepEquals, []schemamodel.ScopedObject{
		{Kind: "role", Name: "app_reader", Dialects: []string{"postgres"}},
	})
}

// databaseFieldsDeclaringScope names every [schemamodel.Database] field whose
// element type declares a Dialects scope, in declaration order.
func databaseFieldsDeclaringScope() []string {
	databaseType := reflect.TypeFor[schemamodel.Database]()
	names := make([]string, 0, databaseType.NumField())
	for field := range databaseType.Fields() {
		names = append(names, map[bool][]string{true: {field.Name}}[declaresScope(field.Type)]...)
	}
	return names
}

// declaresScope reports whether fieldType is a slice of structs carrying the
// Dialects scope field.
func declaresScope(fieldType reflect.Type) bool {
	sliceOfStruct := fieldType.Kind() == reflect.Slice && fieldType.Elem().Kind() == reflect.Struct
	probe := map[bool]func() bool{
		true: func() bool {
			_, found := fieldType.Elem().FieldByName("Dialects")
			return found
		},
		false: func() bool { return false },
	}
	return probe[sliceOfStruct]()
}
