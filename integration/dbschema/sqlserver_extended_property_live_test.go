//go:build integration

package dbschema_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
	statements, err := renderer.GetOrderedCreateStatements(description, platform.SQLServer)
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

	c.Assert(extendedPropertySummary(live.ExtendedProperties, table), qt.DeepEquals, []string{
		"dbo." + table + " " + property + " = enabled (nvarchar)",
		"dbo." + table + ".title " + columnProperty + " = sensitive (nvarchar)",
	})

	// 3. Convergence. Comparing the same description against what the server
	// now holds must produce nothing to do.
	settled := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	c.Assert(extendedPropertiesOn(settled.ExtendedPropertiesAdded, table), qt.HasLen, 0)
	c.Assert(extendedPropertiesOn(settled.ExtendedPropertiesRemoved, table), qt.HasLen, 0)
	c.Assert(modifiedExtendedPropertiesOn(settled.ExtendedPropertiesModified, table), qt.HasLen, 0)

	// 4. And the change. A declaration carrying a different value plans an
	// update rather than a drop and an add, and the statement it plans is one
	// the server accepts and the reader sees.
	changed := sqlServerExtendedPropertySchema(table, property, columnProperty, "disabled")
	plan := schemadiff.CompareWithDialect(changed, live, platform.SQLServer)
	c.Assert(extendedPropertiesOn(plan.ExtendedPropertiesAdded, table), qt.HasLen, 0)
	c.Assert(modifiedExtendedPropertiesOn(plan.ExtendedPropertiesModified, table), qt.HasLen, 1)

	_, err = conn.ExecContext(ctx, fmt.Sprintf(
		"EXEC sp_updateextendedproperty @name = N'%s', @value = N'disabled', "+
			"@level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'%s'",
		property, table))
	c.Assert(err, qt.IsNil)

	after, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	settledAgain := schemadiff.CompareWithDialect(changed, after, platform.SQLServer)
	c.Assert(modifiedExtendedPropertiesOn(settledAgain.ExtendedPropertiesModified, table), qt.HasLen, 0)
}

// TestSQLServerLiveExtendedPropertyLeavesAnUnwritableValueAlone pins the one
// row this object reports and refuses to touch.
//
// sp_addextendedproperty takes a sql_variant, so a property may hold an int as
// well as a string, and the renderer writes an N” literal. Re-emitting an int
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

	found := findExtendedProperty(c, live.ExtendedProperties, table, property)
	c.Assert(found.ValueType, qt.Equals, "int")
	c.Assert(found.ValueNotRepresentable, qt.IsTrue)
	// Blank rather than "42": CONVERT(NVARCHAR, value) answers a rendering
	// rather than the value, and carrying it would invite a comparison to
	// write it back as a string.
	c.Assert(found.Value, qt.Equals, "")

	// A declaration that does not name it plans no removal, which is the half
	// that would otherwise destroy the value.
	empty := &goschema.Database{}
	settled := schemadiff.CompareWithDialect(empty, live, platform.SQLServer)
	c.Assert(extendedPropertiesOn(settled.ExtendedPropertiesRemoved, table), qt.HasLen, 0)
}

// sqlServerExtendedPropertySchema declares one table carrying a table-scoped
// property and a column-scoped one.
func sqlServerExtendedPropertySchema(table, property, columnProperty, value string) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{StructName: "XP", Name: table}},
		Fields: []goschema.Field{
			{StructName: "XP", Name: "id", Type: "INT", Primary: true},
			{StructName: "XP", Name: "title", Type: "NVARCHAR(200)", Nullable: true},
		},
		ExtendedProperties: []goschema.ExtendedProperty{
			{StructName: "XP", Name: property, Schema: "dbo", Table: table, Value: value},
			{
				StructName: "XP", Name: columnProperty, Schema: "dbo",
				Table: table, Column: "title", Value: "sensitive",
			},
		},
	}
}

// extendedPropertySummary renders one table's properties in a stable order.
func extendedPropertySummary(
	properties []catalog.ExtendedProperty,
	table string,
) []string {
	var summary []string
	for _, property := range properties {
		if property.Table != table {
			continue
		}
		summary = append(summary, fmt.Sprintf("%s %s = %s (%s)",
			property.QualifiedOwner(), property.Name, property.Value, property.ValueType))
	}
	return summary
}

func extendedPropertiesOn(
	refs []difftypes.ExtendedPropertyRef,
	table string,
) []difftypes.ExtendedPropertyRef {
	var found []difftypes.ExtendedPropertyRef
	for _, ref := range refs {
		if ref.Table == table {
			found = append(found, ref)
		}
	}
	return found
}

func modifiedExtendedPropertiesOn(
	diffs []difftypes.ExtendedPropertyDiff,
	table string,
) []difftypes.ExtendedPropertyDiff {
	var found []difftypes.ExtendedPropertyDiff
	for _, diff := range diffs {
		if diff.Table == table {
			found = append(found, diff)
		}
	}
	return found
}

func findExtendedProperty(
	c *qt.C,
	properties []catalog.ExtendedProperty,
	table, name string,
) catalog.ExtendedProperty {
	c.Helper()
	for _, property := range properties {
		if property.Table == table && property.Name == name {
			return property
		}
	}
	c.Fatalf("extended property %q on %q is absent from the read schema", name, table)
	return catalog.ExtendedProperty{}
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

	description := &goschema.Database{
		ExtendedProperties: []goschema.ExtendedProperty{
			{StructName: "DB", Name: databaseProperty, Value: "database scope"},
			{StructName: "DB", Name: schemaProperty, Schema: "dbo", Value: "schema scope"},
		},
	}

	statements, err := renderer.GetOrderedCreateStatements(description, platform.SQLServer)
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

	found := findExtendedPropertyByName(c, live.ExtendedProperties, databaseProperty)
	c.Assert(found.Schema, qt.Equals, "")
	c.Assert(found.Table, qt.Equals, "")
	c.Assert(found.Value, qt.Equals, "database scope")
	c.Assert(found.QualifiedOwner(), qt.Equals, "(database)")

	settled := schemadiff.CompareWithDialect(description, live, platform.SQLServer)
	c.Assert(extendedPropertiesNamed(settled.ExtendedPropertiesAdded, databaseProperty), qt.HasLen, 0)
	c.Assert(extendedPropertiesNamed(settled.ExtendedPropertiesRemoved, databaseProperty), qt.HasLen, 0)
	c.Assert(modifiedExtendedPropertiesNamed(settled.ExtendedPropertiesModified, databaseProperty), qt.HasLen, 0)
}

func findExtendedPropertyByName(
	c *qt.C,
	properties []catalog.ExtendedProperty,
	name string,
) catalog.ExtendedProperty {
	c.Helper()
	for _, property := range properties {
		if property.Name == name {
			return property
		}
	}
	c.Fatalf("extended property %q is absent from the read schema", name)
	return catalog.ExtendedProperty{}
}

func extendedPropertiesNamed(
	refs []difftypes.ExtendedPropertyRef,
	name string,
) []difftypes.ExtendedPropertyRef {
	var found []difftypes.ExtendedPropertyRef
	for _, ref := range refs {
		if ref.Name == name {
			found = append(found, ref)
		}
	}
	return found
}

func modifiedExtendedPropertiesNamed(
	diffs []difftypes.ExtendedPropertyDiff,
	name string,
) []difftypes.ExtendedPropertyDiff {
	var found []difftypes.ExtendedPropertyDiff
	for _, diff := range diffs {
		if diff.Name == name {
			found = append(found, diff)
		}
	}
	return found
}
