//go:build integration

package dbschema_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestSQLServerLiveExtendedPropertyRoundTrip is the assertion the object could
// not be added without.
//
// Three surfaces have to agree for an extended property to be manageable at
// all: the renderer writes the statement, the reader finds the row it made,
// and the comparator finds nothing left to do. When one is missing the failure
// is not a compile error -- it is a plan that reports the same pending change
// forever, or an inspect-then-apply round trip that silently drops the value.
// The second is what this measured before the object existed: of five extended
// properties on a live database, `ptah schema inspect` described exactly one,
// the MS_Description that Ptah already models as a comment
// (stokaro/ptah#1031).
//
// All three scopes are here because they are three different statements. A
// schema-scoped property passes level 0 alone, a table adds level 1, and a
// column adds level 2 -- and a renderer that always wrote all three levels
// would be accepted by the server and would address something else.
func TestSQLServerLiveExtendedPropertyRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	table := fmt.Sprintf("ptah_xp_%d", suffix)
	property := fmt.Sprintf("ptah_flag_%d", suffix)
	columnProperty := fmt.Sprintf("ptah_col_%d", suffix)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS [dbo]."+quoteSQLServerIdentifier(table))
	}()

	description := sqlServerExtendedPropertySchema(table, property, columnProperty, "enabled")

	// 1. The statements the server is given are the renderer's own. A
	// statement this engine refuses fails the test rather than being adapted.
	statements, err := builtin.GetOrderedCreateStatements(description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	c.Assert(rendered, qt.Contains, "sp_addextendedproperty")
	c.Assert(rendered, qt.Contains, "@level2type = N'COLUMN'")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	// 2. The catalog is asked what it holds, through the reader rather than
	// through a query written here.
	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)

	c.Assert(extendedPropertySummary(live.FeatureObjects, table), qt.DeepEquals, []string{
		"dbo." + table + ".title/" + columnProperty + " = sensitive (nvarchar)",
		"dbo." + table + "/" + property + " = enabled (nvarchar)",
	})

	// 3. Convergence. Comparing the same description against what the server
	// now holds must produce nothing to do.
	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), description, live, platform.SQLServer, must.Must(builtin.New())))
	c.Assert(propertyChanges(settled), qt.HasLen, 0)

	// 4. And the change. A declaration carrying a different value plans an
	// update rather than a drop and an add, and the statement it plans is one
	// the server accepts and the reader sees.
	changed := sqlServerExtendedPropertySchema(table, property, columnProperty, "disabled")
	plan := must.Must(schemadiff.CompareWithDialect(t.Context(), changed, live, platform.SQLServer, must.Must(builtin.New())))
	c.Assert(propertyChanges(plan), qt.DeepEquals, []string{"update dbo." + table + "/" + property})

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"EXEC sp_updateextendedproperty @name = N'%s', @value = N'disabled', "+
			"@level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'%s'",
		property, table))
	c.Assert(err, qt.IsNil)

	after, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	settledAgain := must.Must(schemadiff.CompareWithDialect(t.Context(), changed, after, platform.SQLServer, must.Must(builtin.New())))
	c.Assert(propertyChanges(settledAgain), qt.HasLen, 0)
}

// TestSQLServerLiveExtendedPropertyLeavesAnUnwritableValueAlone pins the one
// row this object reports and refuses to touch.
//
// sp_addextendedproperty takes a sql_variant, so a property may hold an int as
// well as a string, and the renderer writes an N'...' literal. Re-emitting an int
// through that literal would change its stored type, and a drop would destroy
// a value no declaration can restore -- so the comparator declines the row in
// both directions and Ptah leaves it exactly as it found it.
func TestSQLServerLiveExtendedPropertyLeavesAnUnwritableValueAlone(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	table := fmt.Sprintf("ptah_xpi_%d", suffix)
	property := fmt.Sprintf("ptah_int_%d", suffix)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS [dbo]."+quoteSQLServerIdentifier(table))
	}()

	_, err = conn.ExecContext(ctx,
		"CREATE TABLE [dbo]."+quoteSQLServerIdentifier(table)+" ([id] INT NOT NULL PRIMARY KEY)")
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"EXEC sp_addextendedproperty @name = N'%s', @value = 42, "+
			"@level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'%s'",
		property, table))
	c.Assert(err, qt.IsNil)

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)

	// The read records it in coverage rather than as a value:
	// CONVERT(NVARCHAR, value) answers a rendering rather than the value, and
	// carrying it would invite a comparison to write it back as a string.
	ref := mssqlproperty.Property{Name: property, Schema: "dbo", Table: table}.Ref()
	_, held, err := live.FeatureObjects.Get(ref)
	c.Assert(err, qt.IsNil)
	c.Assert(held, qt.IsFalse)
	knowledge, recorded := live.FeatureCoverage.SubjectKnowledge(mssqlproperty.Kind, ref)
	c.Assert(recorded, qt.IsTrue)
	c.Assert(knowledge, qt.DeepEquals, mssqlproperty.UnrepresentableValue("int"))

	// A declaration that does not name it plans no removal, which is the half
	// that would otherwise destroy the value.
	empty := &schemamodel.Database{FeatureCoverage: propertyCoverage()}
	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), empty, live, platform.SQLServer, must.Must(builtin.New())))
	c.Assert(propertyChanges(settled), qt.Not(qt.Contains), "drop dbo."+table+"/"+property)
}

// sqlServerExtendedPropertySchema declares one table carrying a table-scoped
// property and a column-scoped one.
func sqlServerExtendedPropertySchema(table, property, columnProperty, value string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "XP", Name: table}},
		Fields: []schemamodel.Field{
			{StructName: "XP", Name: "id", Type: "INT", Primary: true},
			{StructName: "XP", Name: "title", Type: "NVARCHAR(200)", Nullable: true},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(
			declaredProperty(mssqlproperty.Property{Name: property, Schema: "dbo", Table: table, Value: value}),
			declaredProperty(mssqlproperty.Property{Name: columnProperty, Schema: "dbo", Table: table, Column: "title", Value: "sensitive"}),
		)),
		FeatureCoverage: propertyCoverage(),
	}
}

// declaredProperty is a property a Go schema declares.
func declaredProperty(property mssqlproperty.Property) schemaext.Object {
	return mssqlproperty.DeclaredObject(mssqlproperty.DesiredProperty{Property: property, StructName: "XP"})
}

// propertyCoverage is what a Go schema records: it can declare extended
// properties, so leaving one out drops it.
func propertyCoverage() schemaext.Coverage {
	return must.Must(mssqlproperty.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

// extendedPropertySummary renders one table's properties in a stable order.
func extendedPropertySummary(objects schemaext.Objects, table string) []string {
	all := must.Must(objects.All())
	var summary []string
	for _, object := range all {
		property, ok := object.Value.(*mssqlproperty.ObservedProperty)
		if ok && property.Table == table {
			summary = append(summary, fmt.Sprintf("%s = %s (%s)", property.Label(), property.Value, property.ValueType))
		}
	}
	slices.Sort(summary)
	return summary
}

// propertyChanges names each extended property change a comparison plans, as
// its action and the property's label, on tables and on their own.
func propertyChanges(diff *difftypes.SchemaDiff) []string {
	records := slices.Clone(diff.FeatureChanges)
	for _, table := range diff.TablesModified {
		records = append(records, table.FeatureChanges...)
	}
	var changes []string
	for _, record := range records {
		change, ok := record.Value.(*mssqlproperty.Change)
		switch {
		case !ok:
		case change.Before == nil:
			changes = append(changes, "add "+change.After.Label())
		case change.After == nil:
			changes = append(changes, "drop "+change.Before.Label())
		default:
			changes = append(changes, "update "+change.After.Label())
		}
	}
	slices.Sort(changes)
	return changes
}

// TestSQLServerLiveDatabaseScopedExtendedPropertyRoundTrip is the scope with
// no object in it.
//
// A class 0 property belongs to the database rather than to a schema, and the
// statement that writes it passes no level at all. That is not an omission: an
// empty @level0name would be a property on a schema called "", which the
// procedure also accepts and which belongs to nothing — so the address the
// renderer writes is asserted rather than inferred from the round trip
// (stokaro/ptah#1031).
//
// The schema-scoped property beside it is the control. Both are read by one
// query, and a database arm that had been scoped by the schema predicate would
// return the schema one and lose this.
func TestSQLServerLiveDatabaseScopedExtendedPropertyRoundTrip(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano()
	databaseProperty := fmt.Sprintf("ptah_db_%d", suffix)
	schemaProperty := fmt.Sprintf("ptah_schema_%d", suffix)
	defer func() {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(
			"EXEC sp_dropextendedproperty @name = N'%s'", databaseProperty))
		_, _ = conn.ExecContext(ctx, fmt.Sprintf(
			"EXEC sp_dropextendedproperty @name = N'%s', @level0type = N'SCHEMA', @level0name = N'dbo'",
			schemaProperty))
	}()

	description := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			declaredProperty(mssqlproperty.Property{Name: databaseProperty, Value: "database scope"}),
			declaredProperty(mssqlproperty.Property{Name: schemaProperty, Schema: "dbo", Value: "schema scope"}),
		)),
		FeatureCoverage: propertyCoverage(),
	}

	statements, err := builtin.GetOrderedCreateStatements(description, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	rendered := strings.Join(statements, "\n")
	// The database property passes no level; the schema one passes level 0.
	c.Assert(rendered, qt.Contains,
		"EXEC sp_addextendedproperty @name = N'"+databaseProperty+"', @value = N'database scope';")
	c.Assert(rendered, qt.Contains,
		"@name = N'"+schemaProperty+"', @value = N'schema scope', @level0type = N'SCHEMA', @level0name = N'dbo';")
	for _, statement := range statements {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)

	found, held, err := live.FeatureObjects.Get(mssqlproperty.Property{Name: databaseProperty}.Ref())
	c.Assert(err, qt.IsNil)
	c.Assert(held, qt.IsTrue)
	c.Assert(found.Value.(*mssqlproperty.ObservedProperty).Label(), qt.Equals, "(database)/"+databaseProperty)
	c.Assert(found.Value.(*mssqlproperty.ObservedProperty).Value, qt.Equals, "database scope")

	settled := must.Must(schemadiff.CompareWithDialect(t.Context(), description, live, platform.SQLServer, must.Must(builtin.New())))
	c.Assert(propertyChanges(settled), qt.Not(qt.Contains), "add (database)/"+databaseProperty)
	c.Assert(propertyChanges(settled), qt.Not(qt.Contains), "drop (database)/"+databaseProperty)
}
