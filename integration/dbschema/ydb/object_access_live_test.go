//go:build integration

package ydb_test

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// objectAccessSchema is the directory the scheme object permission tests
// write into.
const objectAccessSchema = "ptah_ydb_object_access"

var objectAccessSchemas = []string{objectAccessSchema}

// The variable the declared secrets take their values from.
const objectAccessSecretEnv = "PTAH_SECRET_LIVE_OBJECT_ACCESS" // #nosec G101 -- a variable name, not a credential

// objectAccessNames are the principals one run creates: a user the grants
// name, and a group that may list the directory. Users and groups belong to
// the database, so each run takes names of its own.
type objectAccessNames struct {
	user, group string
}

func newObjectAccessNames(c *qt.C) objectAccessNames {
	c.Helper()
	raw := make([]byte, 4)
	_, err := rand.Read(raw)
	c.Assert(err, qt.IsNil)
	suffix := hex.EncodeToString(raw)
	return objectAccessNames{user: "ptahobju" + suffix, group: "ptahobjg" + suffix}
}

// removeObjectAccess drops the test directory with everything in it, which
// takes each object's permission entries with it, and then the run's
// principals. DROP USER leaves a principal's entries behind, so the
// directory goes first.
func removeObjectAccess(c *qt.C, conn *dbschema.DatabaseConnection, names objectAccessNames) {
	c.Helper()
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(ctx context.Context, dir string) error
	})
	c.Assert(ok, qt.IsTrue)
	ctx := context.Background()
	if err := dropper.DropDirectory(ctx, objectAccessSchema); err != nil {
		c.Logf("drop %s: %v", objectAccessSchema, err)
	}
	for _, statement := range []string{"DROP USER IF EXISTS " + names.user, "DROP GROUP IF EXISTS " + names.group} {
		c.Check(conn.Writer().ExecuteSQL(ctx, statement), qt.IsNil, qt.Commentf("execute: %s", statement))
	}
}

// objectAccessObject is one scheme object a test declares, with the
// permission its grant names, the object type a read reports it under, the
// name a grant spells it with, and the entries YDB gives it when it creates
// it, which a read reports as implicit.
type objectAccessObject struct {
	object     schemaext.Object
	objectType string
	name       string
	permission string
	defaults   []catalog.Grant
}

// objectAccessObjects are the scheme objects a line holds: a topic and a
// coordination node on every line, and a secret where the line has secrets.
func objectAccessObjects(caps capability.Capabilities) []objectAccessObject {
	objects := []objectAccessObject{
		{object: ydbtopic.DesiredObject(objectAccessSchema, "events", "", ydbtopic.Spec{}), objectType: "TOPIC",
			name: "events", permission: "ydb.granular.select_row"},
		{object: ydbcoordination.DesiredObject(objectAccessSchema, "locks", "", ydbcoordination.Spec{}),
			objectType: "COORDINATION NODE", name: "locks", permission: "ydb.generic.use"},
	}
	if caps.Has(capability.Secrets) {
		objects = append(objects, objectAccessSecret("pw"))
	}
	return objects
}

// objectAccessSecret is the secret name in the test directory. YDB gives a
// secret it creates an entry for each subject that may describe its
// directory, which on a default cluster is METADATA-READERS.
func objectAccessSecret(name string) objectAccessObject {
	return objectAccessObject{object: ydbsecret.DesiredObject(objectAccessSchema, name, "", objectAccessSecretEnv),
		objectType: "SECRET", name: name, permission: "ydb.granular.select_row",
		defaults: []catalog.Grant{{Role: "METADATA-READERS", Privilege: "ydb.granular.describe_schema", ObjectType: "SECRET",
			Schema: objectAccessSchema, ObjectName: name, Implicit: true}}}
}

// objectAccessExpected is what a read reports on objects once the user holds
// permission on each of them, beside the entries YDB gave each one, sorted as
// objectGrantsOf sorts them.
func objectAccessExpected(names objectAccessNames, objects []objectAccessObject, permission string) []catalog.Grant {
	var want []catalog.Grant
	for _, object := range objects {
		want = append(want, object.defaults...)
		want = append(want, catalog.Grant{Role: names.user, Privilege: cmp.Or(permission, object.permission),
			ObjectType: object.objectType, Schema: objectAccessSchema, ObjectName: object.name})
	}
	sortGrants(want)
	return want
}

// objectAccessDeclaration declares the principals, the group's LIST on the
// directory, the objects and one grant to the user on each, with permission
// replacing the object's own where it is set.
func objectAccessDeclaration(names objectAccessNames, objects []objectAccessObject, permission string) *schemamodel.Database {
	coverage := must.Must(ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	coverage = must.Must(coverage.Combine(must.Must(ydbcoordination.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	coverage = must.Must(coverage.Combine(must.Must(ydbsecret.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))))
	db := &schemamodel.Database{
		Roles: []schemamodel.Role{
			{Name: names.user, Login: true, Password: "Secret1!", Inherit: true},
			{Name: names.group, Group: true, Inherit: true},
		},
		Grants:          []schemamodel.Grant{{Role: names.group, Privileges: []string{"LIST"}, OnSchema: objectAccessSchema}},
		FeatureCoverage: coverage,
	}
	declared := make([]schemaext.Object, 0, len(objects))
	for _, object := range objects {
		declared = append(declared, object.object)
		db.Grants = append(db.Grants, schemamodel.Grant{Role: names.user, Privileges: []string{cmp.Or(permission, object.permission)},
			OnObject: objectAccessSchema + "." + object.name, ObjectKind: object.objectType})
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(declared...))
	schemamodel.Finalize(db)
	return db
}

// objectGrantsOf is every entry of a read on a scheme object, rather than on
// the database, a directory or a table, that names one of the run's
// principals or the cluster's METADATA-READERS, sorted.
func objectGrantsOf(live *catalog.Database, names objectAccessNames) []catalog.Grant {
	var grants []catalog.Grant
	for _, grant := range live.Grants {
		if !slices.Contains([]string{"DATABASE", "SCHEMA", "TABLE"}, grant.ObjectType) &&
			slices.Contains([]string{names.user, names.group, "METADATA-READERS"}, grant.Role) {
			grants = append(grants, grant)
		}
	}
	sortGrants(grants)
	return grants
}

// sortGrants sorts grants by object, subject and permission.
func sortGrants(grants []catalog.Grant) {
	slices.SortFunc(grants, func(a, b catalog.Grant) int {
		return strings.Compare(a.ObjectName+"|"+a.Role+"|"+a.Privilege, b.ObjectName+"|"+b.Role+"|"+b.Privilege)
	})
}

// TestYDBObjectAccess_RoundTrip grants a user a permission on a topic, a
// coordination node and, where the line has secrets, a secret, reads each
// entry back as declared, and plans nothing after. The same declaration
// applied again plans nothing too. Each grant then changes permission, which
// revokes one entry and grants another, and nothing is left to plan.
func TestYDBObjectAccess_RoundTrip(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			t.Setenv(objectAccessSecretEnv, "object-access-v1")
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			c.Cleanup(func() { removeObjectAccess(c, conn, names) })
			objects := objectAccessObjects(conn.Info().Capabilities)

			declared := objectAccessDeclaration(names, objects, "")
			apply(c, conn, planAgainst(c, conn, declared, objectAccessSchemas))
			c.Assert(planAgainst(c, conn, declared, objectAccessSchemas), qt.HasLen, 0)
			apply(c, conn, planAgainst(c, conn, declared, objectAccessSchemas))
			c.Assert(planAgainst(c, conn, declared, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(objectGrantsOf(readScoped(c, conn, objectAccessSchemas), names), qt.DeepEquals,
				objectAccessExpected(names, objects, ""))

			changed := objectAccessDeclaration(names, objects, "ydb.generic.read")
			changes := planAgainst(c, conn, changed, objectAccessSchemas)
			c.Assert(changes, qt.HasLen, 2*len(objects))
			apply(c, conn, changes)
			c.Assert(planAgainst(c, conn, changed, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(objectGrantsOf(readScoped(c, conn, objectAccessSchemas), names), qt.DeepEquals,
				objectAccessExpected(names, objects, "ydb.generic.read"))
		})
	}
}

// TestYDBObjectAccess_ASecretKeepsTheEntriesYDBGaveIt creates a secret in a
// directory a declared group may list. YDB gives the new secret an entry for
// that group, which no GRANT made and the declaration does not name; the read
// reports it as implicit, so nothing is planned after. Measured on 26.2, the
// line with secrets.
func TestYDBObjectAccess_ASecretKeepsTheEntriesYDBGaveIt(t *testing.T) {
	t.Setenv(objectAccessSecretEnv, "object-access-v1")
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	names := newObjectAccessNames(c)
	c.Cleanup(func() { removeObjectAccess(c, conn, names) })
	topic := objectAccessObjects(capability.YDB251())[:1]
	apply(c, conn, planAgainst(c, conn, objectAccessDeclaration(names, topic, ""), objectAccessSchemas))

	declared := objectAccessDeclaration(names, append(topic, objectAccessSecret("pw2")), "")
	apply(c, conn, planAgainst(c, conn, declared, objectAccessSchemas))

	c.Assert(planAgainst(c, conn, declared, objectAccessSchemas), qt.HasLen, 0)
	c.Assert(objectGrantsOf(readScoped(c, conn, objectAccessSchemas), names), qt.Contains, catalog.Grant{Role: names.group,
		Privilege: "ydb.granular.describe_schema", ObjectType: "SECRET", Schema: objectAccessSchema, ObjectName: "pw2", Implicit: true})
}

// TestYDBObjectAccess_DropsAnObjectHoldingAGrant drops every object of the
// declaration while the user holds a permission on each: the plan revokes
// none of those entries, since a REVOKE after the drop would name an object
// that is gone, applies, and leaves nothing to plan.
func TestYDBObjectAccess_DropsAnObjectHoldingAGrant(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			t.Setenv(objectAccessSecretEnv, "object-access-v1")
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			c.Cleanup(func() { removeObjectAccess(c, conn, names) })
			objects := objectAccessObjects(conn.Info().Capabilities)
			apply(c, conn, planAgainst(c, conn, objectAccessDeclaration(names, objects, ""), objectAccessSchemas))

			emptied := objectAccessDeclaration(names, nil, "")
			drops := planAgainst(c, conn, emptied, objectAccessSchemas)
			apply(c, conn, drops)

			c.Assert(drops, qt.HasLen, len(objects))
			c.Assert(strings.Join(drops, "\n"), qt.Not(qt.Contains), "REVOKE")
			c.Assert(planAgainst(c, conn, emptied, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(objectGrantsOf(readScoped(c, conn, objectAccessSchemas), names), qt.HasLen, 0)
		})
	}
}

// TestYDBObjectAccess_AnUndeclaredObjectKeepsItsGrants applies the objects
// and the user's grants on them, and then compares a declaration of the same
// principals that names neither the objects nor the grants and makes no claim
// about the objects, as an HCL document that cannot name a topic does. Ptah
// manages the permissions of the objects a declaration declares, so nothing
// is planned and every entry stays.
func TestYDBObjectAccess_AnUndeclaredObjectKeepsItsGrants(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			t.Setenv(objectAccessSecretEnv, "object-access-v1")
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			c.Cleanup(func() { removeObjectAccess(c, conn, names) })
			objects := objectAccessObjects(conn.Info().Capabilities)
			apply(c, conn, planAgainst(c, conn, objectAccessDeclaration(names, objects, ""), objectAccessSchemas))

			silent := objectAccessDeclaration(names, nil, "")
			silent.FeatureCoverage = schemaext.Coverage{}

			c.Assert(planAgainst(c, conn, silent, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(objectGrantsOf(readScoped(c, conn, objectAccessSchemas), names), qt.DeepEquals,
				objectAccessExpected(names, objects, ""))
		})
	}
}

// TestYDBObjectAccess_Rollback plans a declaration of the objects and the
// user's grants on them, and its rollback, through the generator, applies
// both, and finds every entry the forward plan made gone with its object.
func TestYDBObjectAccess_Rollback(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			t.Setenv(objectAccessSecretEnv, "object-access-v1")
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			c.Cleanup(func() { removeObjectAccess(c, conn, names) })
			info := conn.Info()
			declared := objectAccessDeclaration(names, objectAccessObjects(info.Capabilities), "")
			current := readScoped(c, conn, objectAccessSchemas)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, info, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)

			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Diff: diff, DesiredSchema: declared, CurrentSchema: current,
				Dialect: info.Dialect, Capabilities: info.Capabilities,
				Runtime: must.Must(builtin.New())})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := builtin.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)

			applyScript(c, conn, forward)
			c.Assert(planAgainst(c, conn, declared, objectAccessSchemas), qt.HasLen, 0)
			applyScript(c, conn, reverse)

			live := readScoped(c, conn, objectAccessSchemas)
			c.Assert(objectGrantsOf(live, names), qt.HasLen, 0)
			c.Assert(live.FeatureObjects.Select(func(ref objectidentity.ID) bool {
				return ref.Schema.Source == objectAccessSchema
			}).Len(), qt.Equals, 0)
		})
	}
}

// TestYDBObjectAccess_ExternalObjects grants a user a permission on an
// external data source and an external table over it, with external data
// sources turned on and CREATE OR REPLACE of them off. A data source moved to
// another location is dropped and created again with the table over it, which
// takes both entries; the plan grants them again after it creates the
// objects, and nothing is left to plan.
func TestYDBObjectAccess_ExternalObjects(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			setClusterFlags(c, line, externalSourcesOn)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			c.Cleanup(func() { removeObjectAccess(c, conn, names) })
			declaration := func(location string) *schemamodel.Database {
				db := objectAccessDeclaration(names, nil, "")
				db.FeatureObjects = must.Must(schemaext.NewObjects(
					ydbexternal.DesiredSourceObject(objectAccessSchema, "bucket", "", externalBucket(location)),
					ydbexternal.DesiredTableObject(objectAccessSchema, "files", "", ydbexternal.Table{
						DataSource: objectAccessSchema + "/bucket", Location: "2026/", Columns: externalColumns,
						Options: map[string]string{"FORMAT": "csv_with_names"}}),
				))
				db.Grants = append(db.Grants,
					schemamodel.Grant{Role: names.user, Privileges: []string{"SELECT ROW"}, OnObject: objectAccessSchema + ".bucket",
						ObjectKind: "EXTERNAL DATA SOURCE"},
					schemamodel.Grant{Role: names.user, Privileges: []string{"SELECT ROW"}, OnObject: objectAccessSchema + ".files",
						ObjectKind: "EXTERNAL TABLE"})
				return db
			}
			want := []catalog.Grant{
				{Role: names.user, Privilege: "ydb.granular.select_row", ObjectType: "EXTERNAL DATA SOURCE", Schema: objectAccessSchema, ObjectName: "bucket"},
				{Role: names.user, Privilege: "ydb.granular.select_row", ObjectType: "EXTERNAL TABLE", Schema: objectAccessSchema, ObjectName: "files"},
			}

			declared := declaration("https://storage.invalid/events/")
			apply(c, conn, planAgainst(c, conn, declared, objectAccessSchemas))
			c.Assert(planAgainst(c, conn, declared, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(objectGrantsOf(readScoped(c, conn, objectAccessSchemas), names), qt.DeepEquals, want)

			moved := declaration("https://storage.invalid/moved/")
			recreation := planAgainst(c, conn, moved, objectAccessSchemas)
			apply(c, conn, recreation)

			c.Assert(heads(recreation), qt.DeepEquals, []string{
				"DROP EXTERNAL TABLE `" + objectAccessSchema + "/files`",
				"DROP EXTERNAL DATA SOURCE `" + objectAccessSchema + "/bucket`",
				"CREATE EXTERNAL DATA SOURCE `" + objectAccessSchema + "/bucket` WITH (",
				"CREATE EXTERNAL TABLE `" + objectAccessSchema + "/files` (",
				"GRANT 'ydb.granular.select_row' ON `" + objectAccessSchema + "/bucket` TO `" + names.user + "`",
				"GRANT 'ydb.granular.select_row' ON `" + objectAccessSchema + "/files` TO `" + names.user + "`",
			})
			c.Assert(planAgainst(c, conn, moved, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(objectGrantsOf(readScoped(c, conn, objectAccessSchemas), names), qt.DeepEquals, want)
		})
	}
}
