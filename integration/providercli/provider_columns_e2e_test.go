//go:build integration

package providercli_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

func columnDeclarations() *schemamodel.Database {
	return &schemamodel.Database{Tables: []schemamodel.Table{{Name: "records", StructName: "Record", PrimaryKey: []string{"id"}}}, Fields: []schemamodel.Field{
		{Name: "id", StructName: "Record", Type: "BIGINT", Nullable: true},
		{Name: "value", StructName: "Record", Type: "TEXT", Nullable: true, Default: "", DefaultSet: true},
	}}
}

func columnObservation() *catalog.Database {
	return &catalog.Database{Tables: []catalog.Table{{Name: "records", Type: "TABLE", Columns: []catalog.Column{
		{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		{Name: "value", DataType: "TEXT", ColumnType: "TEXT", IsNullable: "YES", ColumnDefault: new("typed()"), OrdinalPosition: 2},
	}}}}
}

func TestProviderColumnsComparisonReadbackAndPrediction(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	desired, current := columnDeclarations(), columnObservation()
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(desired.Fields[0].Nullable, qt.IsTrue)
	c.Assert(desired.Fields[0].Primary, qt.IsFalse)
	c.Assert(*current.Tables[0].Columns[1].ColumnDefault, qt.Equals, "typed()")

	read, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), current, "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(read.Fields[1].DefaultSet, qt.IsTrue)
	c.Assert(read.Fields[1].Default, qt.Equals, "")
	c.Assert(read.Fields[1].DefaultExpr, qt.Equals, "")
	c.Assert(read.Fields[0].DefaultSet, qt.IsFalse)

	predicted, err := goschematodb.ToDBSchema(c.Context(), desired, "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(predicted.Tables[0].Columns[0].ColumnType, qt.Equals, "Int64")
	c.Assert(predicted.Tables[0].Columns[0].IsNullable, qt.Equals, "NO")
	c.Assert(*predicted.Tables[0].Columns[1].ColumnDefault, qt.Equals, "typed()")
	diff, err = schemadiff.CompareWithDialect(c.Context(), desired, predicted, "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(desired.Fields[0].Nullable, qt.IsTrue)
}

func TestProviderColumnsTrueNullabilityAndChangedDefault(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	desired, current := columnDeclarations(), columnObservation()
	desired.Tables[0].Comment = "nullable keys"
	current.Tables[0].Comment = "nullable keys"
	current.Tables[0].Columns[0].IsNullable = "YES"
	desired.Fields[1].Default = "next"
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
	changed := diff.TablesModified[0].ColumnsModified[0]
	c.Assert(changed.Changes, qt.DeepEquals, map[string]string{"default": " -> next"})
	c.Assert(desired.Fields[1].Default, qt.Equals, "next")
	c.Assert(desired.Fields[1].DefaultSet, qt.IsTrue)
}

func TestProviderColumnsIncludesRemovedDefaultsAndSequenceSettings(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	desired, current := columnDeclarations(), columnObservation()
	current.Tables[0].Columns = append(current.Tables[0].Columns, catalog.Column{Name: "removed", DataType: "TEXT", IsNullable: "YES", ColumnDefault: new("typed()"), OrdinalPosition: 3})
	desired.Tables[0].Comment = "parent declaration"
	current.Tables[0].Comment = "parent observation"
	desired.Fields[0].Comment = "require complete parents"
	current.Tables[0].Columns[0].Comment = "require complete parents"
	desired.Fields[0].AutoInc = true
	desired.Fields[0].IdentityStart = "5"
	current.Tables[0].Columns[0].IsAutoIncrement = true
	current.Tables[0].Columns[0].IdentityStart = "1"
	current.Tables[0].Columns[0].SequenceRestart = "7"
	info := catalog.ServerInfo{Dialect: "fixture", Capabilities: capability.Of(map[capability.Capability]bool{capability.SerialColumns: true, capability.SerialSequenceOptions: true})}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 1)
	table := diff.TablesModified[0]
	c.Assert(table.ColumnsModified, qt.HasLen, 1)
	c.Assert(table.ColumnsModified[0].Changes, qt.DeepEquals, map[string]string{"identity_start": "1 -> 5"})
	c.Assert(table.ColumnsModified[0].CurrentSequenceRestart, qt.Equals, "7")
	c.Assert(table.ColumnsRemoved, qt.HasLen, 1)
	c.Assert(table.ColumnsRemoved[0].DefaultSet, qt.IsTrue)
	c.Assert(table.ColumnsRemoved[0].Default, qt.Equals, "")
	info.Capabilities = capability.Capabilities{}
	diff, err = schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 0)
}

func TestProviderColumnsRefusesIncompleteFactsAndCancellation(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	for _, name := range []string{"incomplete", "missing_default"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			desired, current := columnDeclarations(), columnObservation()
			desired.Fields[1].Name = name
			current.Tables[0].Columns[1].Name = name
			diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "fixture", runtime)
			c.Assert(err, qt.IsNotNil)
			c.Assert(diff, qt.IsNil)
		})
	}
	source := columnDeclarations()
	source.Tables[0].Name = "incomplete"
	projected, err := goschematodb.ToDBSchema(c.Context(), source, "fixture", runtime)
	c.Assert(err, qt.IsNotNil)
	c.Assert(projected, qt.IsNil)
	current := columnObservation()
	current.Tables[0].Columns[1].Name = "missing_default"
	read, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), current, "fixture", runtime)
	c.Assert(err, qt.IsNotNil)
	c.Assert(read, qt.IsNil)
	ctx, cancel := context.WithCancel(c.Context())
	cancel()
	diff, err := schemadiff.CompareWithDialect(ctx, columnDeclarations(), columnObservation(), "fixture", runtime)
	c.Assert(err, qt.ErrorIs, context.Canceled)
	c.Assert(diff, qt.IsNil)
}

func TestProviderColumnsKeepsDistinctCapturedIdentities(t *testing.T) {
	c := qt.New(t)
	_, runtime := yamlProviderCommand(c, "column-semantics")
	desired, current := columnDeclarations(), columnObservation()
	current.Tables[0].Name = "RECORDS"
	current.Tables[0].Columns[0].Name = "ID"
	info := catalog.ServerInfo{Dialect: "fixture", IdentifierSemantics: comparisonRules()}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, info, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(current.Tables[0].Name, qt.Equals, "RECORDS")
	c.Assert(current.Tables[0].Columns[0].Name, qt.Equals, "ID")
}
