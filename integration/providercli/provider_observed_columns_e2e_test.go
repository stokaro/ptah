//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/convert/dbschematogo"
)

func TestProviderInspectionPreservesCompleteColumnFacts(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "column-semantics")
	connection := observedConnection(c, host, "column_metadata")
	snapshot, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(snapshot.Tables, qt.HasLen, 1)
	c.Assert(snapshot.Tables[0].Columns, qt.HasLen, 1)
	c.Assert(snapshot.Tables[0].Columns[0], qt.DeepEquals, catalog.Column{
		Name: "Id", DataType: "Int64", FormattedType: "INT64", ColumnType: "signed integer", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1,
		IsAutoIncrement: true, IdentityGeneration: "BY DEFAULT", IdentityStart: "5", IdentityIncrement: "2", SequenceRestart: "11", OwnedSequence: "private_sequence",
		Comment: "captured column", NotNullConstraintName: "id_present", IsUnique: true, NumericPrecision: new(19), NumericScale: new(0), DatetimePrecision: new(6), CharacterMaxLength: new(42),
		UDTName: "int8", UDTSchema: "types", DomainName: "identifier", DomainSchema: "domain_schema", Collate: "case_sensitive", TypeIsDeclaredText: true,
	})
	generated := observedConnection(c, host, "column_generated")
	snapshot, err = generated.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	column := snapshot.Tables[0].Columns[0]
	c.Assert(column.GeneratedExpression, qt.DeepEquals, new("lower(source)"))
	c.Assert(column.GeneratedKind, qt.Equals, "STORED")
	c.Assert(column.ColumnDefault, qt.DeepEquals, new("typed()"))
	converted, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), snapshot, "fixture", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(converted.Fields[0].DefaultSet, qt.IsTrue)
	c.Assert(converted.Fields[0].Default, qt.Equals, "")
}

func TestProviderInspectionRefusesContradictoryColumnFacts(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "column-semantics")
	for _, name := range []string{"column_missing", "column_legacy", "column_name", "column_ordinal", "column_nullable"} {
		t.Run(name, func(t *testing.T) {
			c := qt.New(t)
			connection := observedConnection(c, host, name)
			snapshot, err := connection.Reader().ReadSchema()
			c.Assert(err, qt.IsNotNil)
			c.Assert(snapshot, qt.IsNil)
		})
	}
}
