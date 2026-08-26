package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
	"go.5x5.cz/ptah/migration/schemadiff/internal/compare"
)

// TestExtendedProperties_KeysOnTheAddressAndNotOnTheName holds what makes two
// extended properties the same property.
//
// SQL Server stores one under a class and up to two ids, so `ptah_flag` on a
// schema, on a table of it, and on a column of that table are three different
// properties. A comparator keyed on the name alone would report one of them as
// modified whenever another one differed.
func TestExtendedProperties_KeysOnTheAddressAndNotOnTheName(t *testing.T) {
	c := qt.New(t)

	declared := &goschema.Database{ExtendedProperties: []goschema.ExtendedProperty{
		{Name: "ptah_flag", Schema: "app", Value: "schema value"},
		{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "table value"},
		{Name: "ptah_flag", Schema: "app", Table: "docs", Column: "title", Value: "column value"},
	}}
	live := &catalog.Database{ExtendedProperties: []catalog.ExtendedProperty{
		{Name: "ptah_flag", Schema: "app", Value: "schema value", ValueType: "nvarchar"},
		{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "table value", ValueType: "nvarchar"},
		{
			Name: "ptah_flag", Schema: "app", Table: "docs", Column: "title",
			Value: "column value", ValueType: "nvarchar",
		},
	}}

	diff := &difftypes.SchemaDiff{}
	compare.ExtendedProperties(declared, live, diff, compare.Coverage{})

	c.Assert(diff.ExtendedPropertiesAdded, qt.HasLen, 0)
	c.Assert(diff.ExtendedPropertiesRemoved, qt.HasLen, 0)
	c.Assert(diff.ExtendedPropertiesModified, qt.HasLen, 0)
}

// TestExtendedProperties_ReportsEachDirection is the ordinary table.
func TestExtendedProperties_ReportsEachDirection(t *testing.T) {
	tests := []struct {
		name        string
		declared    []goschema.ExtendedProperty
		live        []catalog.ExtendedProperty
		wantAdded   []string
		wantRemoved []string
		wantChanged []string
	}{
		{
			name:      "a declaration the database does not have is added",
			declared:  []goschema.ExtendedProperty{{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "on"}},
			wantAdded: []string{"app.docs ptah_flag = on"},
		},
		{
			name:        "a live property nothing declares is removed",
			live:        []catalog.ExtendedProperty{{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "on", ValueType: "nvarchar"}},
			wantRemoved: []string{"app.docs ptah_flag = on"},
		},
		{
			name:        "a different value is a modification, not a drop and an add",
			declared:    []goschema.ExtendedProperty{{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "off"}},
			live:        []catalog.ExtendedProperty{{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "on", ValueType: "nvarchar"}},
			wantChanged: []string{"app.docs ptah_flag = off"},
		},
		{
			// SQL Server's default collation is case-insensitive, so a
			// declaration spelling the table `Docs` names the table the
			// catalog spells `docs`. Comparing raw strings would report an
			// addition and a removal of one property on every run.
			name:     "the address is folded the way the server folds it",
			declared: []goschema.ExtendedProperty{{Name: "PTAH_FLAG", Schema: "APP", Table: "Docs", Value: "on"}},
			live:     []catalog.ExtendedProperty{{Name: "ptah_flag", Schema: "app", Table: "docs", Value: "on", ValueType: "nvarchar"}},
		},
		{
			// Nobody read the live value, so "differs" is not a fact this
			// comparison has, and a removal would destroy a value no
			// declaration could restore.
			name:     "a value Ptah cannot write is declined in both directions",
			declared: []goschema.ExtendedProperty{{Name: "ptah_int", Schema: "app", Table: "docs", Value: "42"}},
			live: []catalog.ExtendedProperty{{
				Name: "ptah_int", Schema: "app", Table: "docs",
				ValueType: "int", ValueNotRepresentable: true,
			}},
		},
		{
			name: "an unwritable value nothing declares is left alone too",
			live: []catalog.ExtendedProperty{{
				Name: "ptah_int", Schema: "app", Table: "docs",
				ValueType: "int", ValueNotRepresentable: true,
			}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{}

			compare.ExtendedProperties(
				&goschema.Database{ExtendedProperties: test.declared},
				&catalog.Database{ExtendedProperties: test.live},
				diff, compare.Coverage{})

			c.Assert(summarizeRefs(diff.ExtendedPropertiesAdded), qt.DeepEquals, test.wantAdded)
			c.Assert(summarizeRefs(diff.ExtendedPropertiesRemoved), qt.DeepEquals, test.wantRemoved)
			c.Assert(summarizeChanges(diff.ExtendedPropertiesModified), qt.DeepEquals, test.wantChanged)
		})
	}
}

func summarizeRefs(refs []difftypes.ExtendedPropertyRef) []string {
	var summary []string
	for _, ref := range refs {
		summary = append(summary, summarizeRef(ref))
	}
	return summary
}

func summarizeChanges(diffs []difftypes.ExtendedPropertyDiff) []string {
	var summary []string
	for _, diff := range diffs {
		summary = append(summary, summarizeRef(diff.ExtendedPropertyRef))
	}
	return summary
}

func summarizeRef(ref difftypes.ExtendedPropertyRef) string {
	owner := ref.Schema
	if ref.Table != "" {
		owner += "." + ref.Table
	}
	if ref.Column != "" {
		owner += "." + ref.Column
	}
	return owner + " " + ref.Name + " = " + ref.Value
}
