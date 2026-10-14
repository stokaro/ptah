//go:build integration

package ydb_test

import (
	"context"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/engine/builtin"
	"ptah.run/migration/generator"
	"ptah.run/migration/schemadiff"
)

// grantsOn is every entry of a read on an object of objectType that names
// one of the run's principals, sorted.
func grantsOn(live *catalog.Database, names objectAccessNames, objectType string) []catalog.Grant {
	var grants []catalog.Grant
	for _, grant := range live.Grants {
		if grant.ObjectType == objectType && slices.Contains([]string{names.user, names.group}, grant.Role) {
			grants = append(grants, grant)
		}
	}
	sortGrants(grants)
	return grants
}

// viewAccessDeclaration declares the run's principals, a table, and, where
// body is set, a view over it with that query and a grant to the user on the
// view, which a declaration names as it names a table.
func viewAccessDeclaration(names objectAccessNames, body string) *schemamodel.Database {
	db := objectAccessDeclaration(names, nil, "")
	db.Tables = []schemamodel.Table{{StructName: "Src", Name: "src", Schema: objectAccessSchema}}
	db.Fields = []schemamodel.Field{{StructName: "Src", Name: "id", Type: "BIGINT", Primary: true}}
	if body != "" {
		db.Views = []schemamodel.View{{Name: objectAccessSchema + ".active", Body: body}}
		db.Grants = append(db.Grants, schemamodel.Grant{Role: names.user, Privileges: []string{"SELECT ROW"},
			OnTable: objectAccessSchema + ".active"})
	}
	schemamodel.Finalize(db)
	return db
}

// TestYDBObjectAccess_AView grants a user a permission on a view, reads it
// back as a grant on a table and plans nothing after. YDB has no CREATE OR
// REPLACE VIEW, so a changed query drops the view and creates it again, which
// takes the entry; the plan grants it again after the new view. Dropping the
// view takes the entry too, and no REVOKE is written for it.
func TestYDBObjectAccess_AView(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			c.Cleanup(func() { removeObjectAccess(c, conn, names) })
			want := []catalog.Grant{{Role: names.user, Privilege: "ydb.granular.select_row", ObjectType: "TABLE",
				Schema: objectAccessSchema, ObjectName: "active"}}
			view := "`" + objectAccessSchema + "/active`"

			declared := viewAccessDeclaration(names, "SELECT id FROM `"+objectAccessSchema+"/src`")
			apply(c, conn, planAgainst(c, conn, declared, objectAccessSchemas))
			c.Assert(planAgainst(c, conn, declared, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(grantsOn(readScoped(c, conn, objectAccessSchemas), names, "TABLE"), qt.DeepEquals, want)

			changed := viewAccessDeclaration(names, "SELECT id FROM `"+objectAccessSchema+"/src` WHERE id > 0")
			recreation := planAgainst(c, conn, changed, objectAccessSchemas)
			apply(c, conn, recreation)
			c.Assert(heads(recreation), qt.DeepEquals, []string{
				"DROP VIEW " + view,
				"CREATE VIEW " + view + " WITH (security_invoker = TRUE) AS",
				"GRANT 'ydb.granular.select_row' ON " + view + " TO `" + names.user + "`",
			})
			c.Assert(planAgainst(c, conn, changed, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(grantsOn(readScoped(c, conn, objectAccessSchemas), names, "TABLE"), qt.DeepEquals, want)

			dropped := viewAccessDeclaration(names, "")
			drop := planAgainst(c, conn, dropped, objectAccessSchemas)
			apply(c, conn, drop)
			c.Assert(drop, qt.DeepEquals, []string{"DROP VIEW " + view})
			c.Assert(planAgainst(c, conn, dropped, objectAccessSchemas), qt.HasLen, 0)
			c.Assert(grantsOn(readScoped(c, conn, objectAccessSchemas), names, "TABLE"), qt.HasLen, 0)
		})
	}
}

// replicationAccessDeclaration is the replication directory's declaration
// with the run's principals, and a grant to the user on the replication when
// one is declared.
func replicationAccessDeclaration(names objectAccessNames, declared *schemamodel.Database, grants ...schemamodel.Grant) *schemamodel.Database {
	declared.Roles = []schemamodel.Role{{Name: names.user, Login: true, Password: "Secret1!", Inherit: true}}
	declared.Grants = append(declared.Grants, grants...)
	schemamodel.Finalize(declared)
	return declared
}

// TestYDBObjectAccess_AnAsyncReplication grants a user a permission on an
// async replication of a table of the same database, in the plan that
// creates the replication, reads it back and plans nothing after. Removing
// the replication drops it, which takes the entry, and writes no REVOKE.
func TestYDBObjectAccess_AnAsyncReplication(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			dropReplications(c, conn)
			c.Cleanup(func() {
				dropReplications(c, conn)
				c.Check(conn.Writer().ExecuteSQL(context.Background(), "DROP USER IF EXISTS "+names.user), qt.IsNil)
			})
			grant := schemamodel.Grant{Role: names.user, Privileges: []string{"ydb.generic.read"},
				OnObject: replicationSchema + ".mirror", ObjectKind: "ASYNC REPLICATION"}
			declared := replicationAccessDeclaration(names, replicationDeclaration(selfConnection(c, conn)), grant)

			first := planAgainst(c, conn, declared, replicationSchemas)
			apply(c, conn, first)
			c.Assert(first[len(first)-1], qt.Equals, "GRANT 'ydb.generic.read' ON `"+replicationSchema+"/mirror` TO `"+names.user+"`")
			settledRead(c, conn, "the replica recorded", replicaRecorded)
			c.Assert(planAgainst(c, conn, declared, replicationSchemas), qt.HasLen, 0)
			c.Assert(grantsOn(readScoped(c, conn, replicationSchemas), names, "ASYNC REPLICATION"), qt.DeepEquals, []catalog.Grant{
				{Role: names.user, Privilege: "ydb.generic.read", ObjectType: "ASYNC REPLICATION", Schema: replicationSchema,
					ObjectName: "mirror"},
			})

			emptied := replicationAccessDeclaration(names, replicationDeclaration(""))
			removed := planAgainst(c, conn, emptied, replicationSchemas)
			apply(c, conn, removed)
			c.Assert(removed, qt.DeepEquals, []string{"DROP ASYNC REPLICATION `" + replicationSchema + "/mirror` CASCADE"})
			settledRead(c, conn, "the replication gone", func(live *catalog.Database) bool { return len(liveReplications(live)) == 0 })
			waitForDirectory(c, line, []string{"src"}, replicationSchema)
			c.Assert(planAgainst(c, conn, emptied, replicationSchemas), qt.HasLen, 0)
		})
	}
}

// TestYDBObjectAccess_ATransfer grants a user a permission on a transfer in
// the plan that creates it, reads it back and plans nothing after, keeps the
// entry through a changed lambda, which YDB changes in place, and drops the
// transfer without a REVOKE. 25.1 creates no transfer with its default
// flags, so only 26.2 runs it.
func TestYDBObjectAccess_ATransfer(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c, lineNamed(c, "26.2"))
	names := newObjectAccessNames(c)
	dropReplications(c, conn)
	c.Cleanup(func() {
		dropReplications(c, conn)
		c.Check(conn.Writer().ExecuteSQL(context.Background(), "DROP USER IF EXISTS "+names.user), qt.IsNil)
	})
	grant := schemamodel.Grant{Role: names.user, Privileges: []string{"ydb.generic.manage"},
		OnObject: replicationSchema + ".ingest", ObjectKind: "TRANSFER"}
	want := []catalog.Grant{{Role: names.user, Privilege: "ydb.generic.manage", ObjectType: "TRANSFER", Schema: replicationSchema,
		ObjectName: "ingest"}}

	declared := replicationAccessDeclaration(names, transferDeclaration(lambdaWriting("a:")), grant)
	first := planAgainst(c, conn, declared, replicationSchemas)
	apply(c, conn, first)
	c.Assert(first[len(first)-1], qt.Equals, "GRANT 'ydb.generic.manage' ON `"+replicationSchema+"/ingest` TO `"+names.user+"`")
	c.Assert(planAgainst(c, conn, declared, replicationSchemas), qt.HasLen, 0)
	c.Assert(grantsOn(readScoped(c, conn, replicationSchemas), names, "TRANSFER"), qt.DeepEquals, want)

	relambda := replicationAccessDeclaration(names, transferDeclaration(lambdaWriting("b:")), grant)
	changed := planAgainst(c, conn, relambda, replicationSchemas)
	apply(c, conn, changed)
	c.Assert(changed, qt.DeepEquals, []string{"ALTER TRANSFER `ptah_ydb_repl/ingest` SET USING " + lambdaWriting("b:")})
	c.Assert(planAgainst(c, conn, relambda, replicationSchemas), qt.HasLen, 0)
	c.Assert(grantsOn(readScoped(c, conn, replicationSchemas), names, "TRANSFER"), qt.DeepEquals, want)

	emptied := replicationAccessDeclaration(names, transferDeclaration(""))
	removed := planAgainst(c, conn, emptied, replicationSchemas)
	apply(c, conn, removed)
	c.Assert(removed, qt.DeepEquals, []string{"DROP TRANSFER `ptah_ydb_repl/ingest`"})
	c.Assert(planAgainst(c, conn, emptied, replicationSchemas), qt.HasLen, 0)
	c.Assert(grantsOn(readScoped(c, conn, replicationSchemas), names, "TRANSFER"), qt.HasLen, 0)
}

// streamingAccessSchema is the directory the streaming query permission test
// writes into.
const streamingAccessSchema = "ptah_ydb_stream_access"

// TestYDBObjectAccess_AStreamingQuery declares in YQL a streaming query and a
// grant on it to a user, with streaming queries turned on. The streaming
// query owner creates a query after every other statement of a plan, so the
// GRANT goes at the end, after it. A changed query is altered in place and
// keeps its entries; a changed grant revokes one entry and grants another,
// the GRANT again last. Removing the query drops it without a REVOKE. Only
// 26.2 has streaming queries.
func TestYDBObjectAccess_AStreamingQuery(t *testing.T) {
	c := qt.New(t)
	line := lineNamed(c, "26.2")
	setClusterFlags(c, line, externalSourcesOn, clusterFlag{yaml: "enable_streaming_queries", page: "EnableStreamingQueries", on: true})
	conn := openYDB(c, line)
	names := newObjectAccessNames(c)
	schemas := []string{streamingAccessSchema}
	dropper, ok := conn.SchemaWriter().(interface {
		DropDirectory(context.Context, string) error
	})
	c.Assert(ok, qt.IsTrue)
	c.Cleanup(func() {
		c.Check(dropper.DropDirectory(context.Background(), streamingAccessSchema), qt.IsNil)
		c.Check(conn.Writer().ExecuteSQL(context.Background(), "DROP USER IF EXISTS "+names.user), qt.IsNil)
	})
	path := filepath.Join(c.TempDir(), "schema.sql")
	query := "`" + streamingAccessSchema + "/copy`"
	prelude := "CREATE TOPIC `" + streamingAccessSchema + "/source`; CREATE TOPIC `" + streamingAccessSchema + "/sink`; " +
		"CREATE USER " + names.user + " PASSWORD 'Secret1!';"
	body := " AS DO BEGIN INSERT INTO `" + streamingAccessSchema + "/sink` SELECT * FROM `" + streamingAccessSchema + "/source`"
	grant := func(permission string) string {
		return " GRANT '" + permission + "' ON " + query + " TO " + names.user + ";"
	}
	want := func(permission string) []catalog.Grant {
		return []catalog.Grant{{Role: names.user, Privilege: permission, ObjectType: "STREAMING QUERY", Schema: streamingAccessSchema,
			ObjectName: "copy"}}
	}

	declared := loadStreamingYQL(c, path, prelude+"CREATE STREAMING QUERY "+query+" WITH (RUN=FALSE)"+body+"; END DO;"+
		grant("ydb.generic.manage"))
	first := planAgainst(c, conn, declared, schemas)
	apply(c, conn, first)
	c.Assert(first[len(first)-1], qt.Equals, "GRANT 'ydb.generic.manage' ON "+query+" TO `"+names.user+"`")
	c.Assert(planAgainst(c, conn, declared, schemas), qt.HasLen, 0)
	c.Assert(grantsOn(readScoped(c, conn, schemas), names, "STREAMING QUERY"), qt.DeepEquals, want("ydb.generic.manage"))

	changed := loadStreamingYQL(c, path, prelude+"CREATE OR REPLACE STREAMING QUERY "+query+" WITH (RUN=FALSE)"+body+
		" WHERE TRUE; END DO;"+grant("ydb.generic.read"))
	change := planAgainst(c, conn, changed, schemas)
	apply(c, conn, change)
	c.Assert(heads(change)[len(change)-1], qt.Equals, "GRANT 'ydb.generic.read' ON "+query+" TO `"+names.user+"`")
	c.Assert(strings.Join(change, "\n"), qt.Contains, "REVOKE 'ydb.generic.manage' ON "+query+" FROM `"+names.user+"`")
	c.Assert(planAgainst(c, conn, changed, schemas), qt.HasLen, 0)
	c.Assert(grantsOn(readScoped(c, conn, schemas), names, "STREAMING QUERY"), qt.DeepEquals, want("ydb.generic.read"))

	emptied := loadStreamingYQL(c, path, prelude)
	removed := planAgainst(c, conn, emptied, schemas)
	apply(c, conn, removed)
	c.Assert(removed, qt.DeepEquals, []string{"DROP STREAMING QUERY " + query})
	c.Assert(planAgainst(c, conn, emptied, schemas), qt.HasLen, 0)
}

// poolAccessDeclaration declares the run's user, a resource pool name and a
// grant to the user on it, which names the pool by its name alone.
func poolAccessDeclaration(names objectAccessNames, pool string) *schemamodel.Database {
	db := &schemamodel.Database{
		FeatureCoverage: must.Must(ydbworkload.Coverage(ydbworkload.PoolKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
		FeatureObjects:  must.Must(schemaext.NewObjects(ydbworkload.DesiredPoolObject(pool, "", ydbworkload.PoolSpec{}))),
		Roles:           []schemamodel.Role{{Name: names.user, Login: true, Password: "Secret1!", Inherit: true}},
		Grants:          []schemamodel.Grant{{Role: names.user, Privileges: []string{"SELECT ROW"}, OnObject: pool, ObjectKind: "RESOURCE POOL"}},
	}
	schemamodel.Finalize(db)
	return db
}

// serverEntriesNotImplicit is every entry a read reports on the pool default
// for a subject the cluster made, which YDB gave the pool, that the read does
// not report as implicit.
func serverEntriesNotImplicit(live *catalog.Database) []catalog.Grant {
	var found []catalog.Grant
	for _, grant := range live.Grants {
		if grant.ObjectType == "RESOURCE POOL" && grant.ObjectName == ydbworkload.DefaultPool && !grant.Implicit &&
			slices.Contains([]string{"all-users@well-known", "root@builtin"}, grant.Role) {
			found = append(found, grant)
		}
	}
	return found
}

// TestYDBObjectAccess_AResourcePool grants a user a permission on a resource
// pool in the plan that creates it, by the path YDB gives a pool, reads it
// back and plans nothing after. The entries YDB gave the pool default are
// reported as implicit. The rollback, planned through the generator, drops
// the pool without a REVOKE, and finds the pool and its entry gone.
func TestYDBObjectAccess_AResourcePool(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			names := newObjectAccessNames(c)
			pool := "ptahacl" + strings.TrimPrefix(names.user, "ptahobju")
			c.Cleanup(func() {
				ctx := context.Background()
				_ = conn.Writer().ExecuteSQL(ctx, ydbworkload.DropPoolStatement(pool))
				c.Check(conn.Writer().ExecuteSQL(ctx, "DROP USER IF EXISTS "+names.user), qt.IsNil)
			})
			info := conn.Info()
			declared := poolAccessDeclaration(names, pool)
			current := readScoped(c, conn, poolSchemas)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, info, nil, must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			plan, err := generator.PlanBidirectionalSchemaDiff(t.Context(), generator.BidirectionalSchemaPlanOptions{
				Diff: diff, DesiredSchema: declared, CurrentSchema: current,
				Dialect: info.Dialect, Capabilities: info.Capabilities, Runtime: must.Must(builtin.New())})
			c.Assert(err, qt.IsNil)
			forward, err := builtin.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Forward.Nodes...)
			c.Assert(err, qt.IsNil)
			reverse, err := builtin.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, plan.Reverse.Nodes...)
			c.Assert(err, qt.IsNil)

			applyScript(c, conn, forward)
			live := readScoped(c, conn, poolSchemas)
			c.Assert(forward, qt.Contains, "GRANT 'ydb.granular.select_row' ON `.metadata/workload_manager/pools/"+pool+"` TO `"+names.user+"`")
			c.Assert(planAgainst(c, conn, declared, poolSchemas), qt.HasLen, 0)
			c.Assert(grantsOn(live, names, "RESOURCE POOL"), qt.DeepEquals, []catalog.Grant{
				{Role: names.user, Privilege: "ydb.granular.select_row", ObjectType: "RESOURCE POOL", ObjectName: pool},
			})
			c.Assert(serverEntriesNotImplicit(live), qt.HasLen, 0)

			applyScript(c, conn, reverse)
			c.Assert(reverse, qt.Not(qt.Contains), "REVOKE")
			after := readScoped(c, conn, poolSchemas)
			_, found, err := after.FeatureObjects.Get(ydbworkload.PoolRef(pool))
			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsFalse)
			c.Assert(grantsOn(after, names, "RESOURCE POOL"), qt.HasLen, 0)
		})
	}
}
