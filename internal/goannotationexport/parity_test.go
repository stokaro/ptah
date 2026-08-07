package goannotationexport_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/atlashcl"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/goannotationexport"
)

func TestExport_HappyPath_PreservesGoAnnotationSemantics(t *testing.T) {
	c := qt.New(t)
	root := filepath.Join("testdata", "parity")
	outputDir, err := os.MkdirTemp(filepath.Dir(root), ".parity-export-*")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		c.Check(os.RemoveAll(outputDir), qt.IsNil)
	})
	output := filepath.Join(outputDir, "schema.hcl")
	before, err := goschema.ParseDir(root)
	c.Assert(err, qt.IsNil)

	result, err := goannotationexport.Export(goannotationexport.Options{
		RootDir:    root,
		OutputPath: output,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.DeepEquals, []atlashclrender.Diagnostic{
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "functions.app.lookup_user",
			Message:  "raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "materialized_views.app.user_stats",
			Message:  "raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     `triggers["app.users"]["users_touch"]`,
			Message:  "raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete",
		},
		{
			Severity: atlashclrender.SeverityWarning,
			Path:     "views.app.active_users",
			Message:  "raw SQL body is emitted as opaque HCL text and cannot be structurally interpreted; review it before treating the export as semantically complete",
		},
	})
	hclData, err := os.ReadFile(output)
	c.Assert(err, qt.IsNil)
	hclText := string(hclData)
	c.Assert(hclText, qt.Contains, `if_not_exists = true`)
	c.Assert(hclText, qt.Contains, `sequence "order_seq"`)
	c.Assert(hclText, qt.Contains, `domain "email_address"`)
	c.Assert(hclText, qt.Contains, `composite "postal_address"`)
	c.Assert(hclText, qt.Contains, `range "price_range"`)
	c.Assert(hclText, qt.Contains, `password = "SCRAM-SHA-256$fixture"`)
	c.Assert(hclText, qt.Contains, `for = sequence.app.order_seq`)
	c.Assert(hclText, qt.Contains, "data {")
	c.Assert(hclText, qt.Contains, `checks = ["id > 0"]`)
	c.Assert(hclText, qt.Contains, `custom = "WITHOUT OIDS"`)
	c.Assert(hclText, qt.Contains, `platform "mysql"`)
	c.Assert(hclText, qt.Contains, `override "type"`)
	c.Assert(hclText, qt.Contains, `params = "in user_id bigint, out display_value double precision"`)
	c.Assert(hclText, qt.Contains, `constraint "users_no_overlap"`)
	c.Assert(hclText, qt.Not(qt.Contains), "embedded")

	after, err := atlashcl.ParseFile(output)
	c.Assert(err, qt.IsNil)
	c.Assert(after.Extensions, qt.DeepEquals, before.Extensions)
	c.Assert(after.Sequences, qt.DeepEquals, withoutSequenceProvenance(before.Sequences))
	c.Assert(after.Domains, qt.DeepEquals, withoutDomainProvenance(before.Domains))
	c.Assert(after.CompositeTypes, qt.DeepEquals, withoutCompositeProvenance(before.CompositeTypes))
	c.Assert(after.Ranges, qt.DeepEquals, withoutRangeProvenance(before.Ranges))
	c.Assert(after.Roles, qt.HasLen, 1)
	c.Assert(after.Roles[0].Password, qt.Equals, before.Roles[0].Password)
	c.Assert(after.Functions, qt.HasLen, 1)
	c.Assert(after.Functions[0].Parameters, qt.Equals, before.Functions[0].Parameters)
	c.Assert(after.Functions[0].Returns, qt.Equals, before.Functions[0].Returns)
	c.Assert(after.Grants, qt.HasLen, 3)
	c.Assert(grantsByTarget(after.Grants)["||app.order_seq"].OnSequence, qt.Equals, "app.order_seq")

	tables := tablesByQualifiedName(after.Tables)
	c.Assert(tables["app.users"].Checks, qt.DeepEquals, []string{"id > 0"})
	c.Assert(tables["app.users"].CustomSQL, qt.Equals, "WITHOUT OIDS")
	c.Assert(tables["app.users"].Overrides, qt.DeepEquals, map[string]map[string]string{
		"mariadb": {
			"comment": "Users",
			"engine":  "InnoDB",
		},
		"mysql": {
			"comment": "MySQL users",
			"engine":  "MyISAM",
		},
	})
	fields := fieldsByQualifiedName(after.Fields)
	c.Assert(fields["app.users.id"].Overrides["mysql"]["type"], qt.Equals, "BIGINT AUTO_INCREMENT")
	c.Assert(fields["app.users.email"].Check, qt.Equals, "email <> ''")
	c.Assert(fields["app.users.email"].CheckName, qt.Equals, "users_email_not_empty")
	c.Assert(fields["app.users.score"].Type, qt.Equals, "DOUBLE PRECISION")
	c.Assert(fields["app.users.status"].Enum, qt.DeepEquals, []string{"active", "disabled"})
	c.Assert(fields["app.users.audit_created_at"].Overrides["mysql"]["type"], qt.Equals, "DATETIME(6)")
	c.Assert(fields["app.users.metadata"].Nullable, qt.IsTrue)
	c.Assert(fields["app.users.metadata"].Overrides["mysql"]["type"], qt.Equals, "JSON")
	// The annotation writes ref="app.accounts(id)" and the export writes
	// `ref_columns = [table.accounts.column.id]`, because an HCL reference names
	// a block by its label and the pinned Atlas community binary v1.3.0 refuses
	// `table.app.accounts` outright. The target is unchanged: `accounts` is
	// declared once in this document, in `app`, which is also the schema of the
	// table holding the field, so every reader -- including the DDL path, via
	// tablelookup.ResolveReference -- gets back to app.accounts. A cross-schema
	// target does keep its schema here; see the round-trip coverage in
	// internal/atlashcl.
	c.Assert(fields["app.users.manager_id"].Foreign, qt.Equals, "accounts(id)")
	c.Assert(fields["app.users.manager_id"].OnDelete, qt.Equals, "SET NULL")
	c.Assert(fields["app.users.manager_id"].Overrides["mysql"]["type"], qt.Equals, "BIGINT UNSIGNED")
	indexes := indexesByName(after.Indexes)
	c.Assert(indexes, qt.HasLen, 2)
	c.Assert(indexes["users_email_search"].Operator, qt.Equals, "gin_trgm_ops")
	c.Assert(indexes["users_score_bloom"].Granularity, qt.Equals, 64)

	constraints := constraintsByName(after.Constraints)
	c.Assert(constraints["users_positive_id"].Comment, qt.Equals, "Positive identifier")
	c.Assert(constraints["users_email_key"].NullsDistinct, qt.DeepEquals, new(false))
	c.Assert(constraints["named_keys_pk"].Type, qt.Equals, "PRIMARY KEY")
	c.Assert(constraints["users_account_fk"].ForeignColumns, qt.DeepEquals, []string{"id"})
	c.Assert(constraints["users_no_overlap"].UsingMethod, qt.Equals, "gist")
	c.Assert(constraints["users_no_overlap"].ExcludeElements, qt.Equals, "id WITH =")
	c.Assert(constraints["users_no_overlap"].WhereCondition, qt.Equals, "id > 0")

	c.Assert(after.ManagedData, qt.HasLen, 1)
	c.Assert(after.ManagedData[0].Schema, qt.Equals, "app")
	c.Assert(after.ManagedData[0].Table, qt.Equals, "users")
	c.Assert(after.ManagedData[0].Keys, qt.DeepEquals, []string{"id", "email"})
	rows, err := goschema.LoadManagedRows("", after.ManagedData[0])
	c.Assert(err, qt.IsNil)
	c.Assert(rows, qt.HasLen, 1)

	stable, err := atlashclrender.Render(after)
	c.Assert(err, qt.IsNil)
	c.Assert(stable.Diagnostics, qt.HasLen, 0)
	c.Assert(stable.Data, qt.DeepEquals, hclData)
}

func withoutSequenceProvenance(values []goschema.Sequence) []goschema.Sequence {
	result := append([]goschema.Sequence(nil), values...)
	for i := range result {
		result[i].StructName = ""
	}
	return result
}

func withoutDomainProvenance(values []goschema.Domain) []goschema.Domain {
	result := append([]goschema.Domain(nil), values...)
	for i := range result {
		result[i].StructName = ""
	}
	return result
}

func withoutCompositeProvenance(values []goschema.CompositeType) []goschema.CompositeType {
	result := append([]goschema.CompositeType(nil), values...)
	for i := range result {
		result[i].StructName = ""
	}
	return result
}

func withoutRangeProvenance(values []goschema.Range) []goschema.Range {
	result := append([]goschema.Range(nil), values...)
	for i := range result {
		result[i].StructName = ""
	}
	return result
}

func grantsByTarget(values []goschema.Grant) map[string]goschema.Grant {
	result := make(map[string]goschema.Grant, len(values))
	for _, value := range values {
		result[value.OnTable+"|"+value.OnSchema+"|"+value.OnSequence] = value
	}
	return result
}

func tablesByQualifiedName(values []goschema.Table) map[string]goschema.Table {
	result := make(map[string]goschema.Table, len(values))
	for _, value := range values {
		result[value.QualifiedName()] = value
	}
	return result
}

func fieldsByQualifiedName(values []goschema.Field) map[string]goschema.Field {
	result := make(map[string]goschema.Field, len(values))
	for _, value := range values {
		result[value.StructName+"."+value.Name] = value
	}
	return result
}

func indexesByName(values []goschema.Index) map[string]goschema.Index {
	result := make(map[string]goschema.Index, len(values))
	for _, value := range values {
		result[value.Name] = value
	}
	return result
}

func constraintsByName(values []goschema.Constraint) map[string]goschema.Constraint {
	result := make(map[string]goschema.Constraint, len(values))
	for _, value := range values {
		result[value.Name] = value
	}
	return result
}
