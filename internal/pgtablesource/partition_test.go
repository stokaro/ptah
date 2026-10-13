package pgtablesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/dialect/postgres/pgtablerender"
	"ptah.run/internal/pgtablesource"
)

func TestPartitionSQL_RoundTrip(t *testing.T) {
	for _, test := range []struct {
		text string
		want pgtable.DesiredPartition
	}{
		{`PARTITION BY RANGE (tenant, "Mixed.Dot")`, pgtable.DesiredPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "tenant"}, {Name: "Mixed.Dot"}}}},
		{`LIST (lower((word)::text))`, pgtable.DesiredPartition{Type: "LIST", Parts: []pgtable.PartitionPart{{Expr: "lower((word)::text)"}}}},
		{`HASH ((id % 4))`, pgtable.DesiredPartition{Type: "HASH", Parts: []pgtable.PartitionPart{{Expr: "id % 4"}}}},
		{`RANGE (label COLLATE "C" pg_catalog.text_pattern_ops)`, pgtable.DesiredPartition{Type: "RANGE", Parts: []pgtable.PartitionPart{{Name: "label", Collation: `"C"`, OperatorClass: `"pg_catalog"."text_pattern_ops"`}}}},
	} {
		t.Run(test.text, func(t *testing.T) {
			c := qt.New(t)
			value, err := pgtablesource.ParsePartitionSQL(test.text)
			c.Assert(err, qt.IsNil)
			c.Assert(value, qt.DeepEquals, &test.want)
			sql, err := pgtablerender.PartitionSQL(value)
			c.Assert(err, qt.IsNil)
			returned, err := pgtablesource.ParsePartitionSQL(sql)
			c.Assert(err, qt.IsNil)
			c.Assert(returned, qt.DeepEquals, value)
		})
	}
}
func TestPartitionSQL_RefusesMalformedKeys(t *testing.T) {
	for _, text := range []string{`RANGE ()`, `RANGE ("")`, `LIST (a,b)`, `HASH (a.b)`, `HASH ((id)`, `HASH (id); COMMIT;`, `RANGE (id COLLATE)`, `RANGE (id ops extra)`, `RANGE (id,)`} {
		t.Run(text, func(t *testing.T) {
			c := qt.New(t)
			value, err := pgtablesource.ParsePartitionSQL(text)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(value, qt.IsNil)
		})
	}
}
func TestPartitionProperties_PreserveTargets(t *testing.T) {
	for _, target := range []string{"postgres", "yugabytedb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)
			service := pgtablesource.Service{}
			fragments := []schemaext.PropertyFragment{{Kind: pgtable.PartitionKind, Properties: map[string]string{"partition_by": `RANGE ("a.b" COLLATE "C" "text_pattern_ops")`}}}
			values, err := service.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{Target: target, Format: schemaext.TablePlatformProperties, Fragments: fragments})
			c.Assert(err, qt.IsNil)
			returned, err := service.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{Target: target, Format: schemaext.TablePlatformProperties, Values: values})
			c.Assert(err, qt.IsNil)
			c.Assert(returned, qt.DeepEquals, fragments)
		})
	}
}
