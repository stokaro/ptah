package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/convert/dbschematogo"
)

func TestSchemaInspectJSON_ReportsOmittedYDBFamilies(t *testing.T) {
	for _, format := range []string{`{{ json . }}`, `{{ json .Realm }}`, `{{ json .Schema "  " }}`} {
		t.Run(format, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics bytes.Buffer
			db := &schemamodel.Database{
				AsyncReplications:       []schemamodel.AsyncReplication{{Name: "mirror"}},
				CoordinationNodes:       []schemamodel.CoordinationNode{{Name: "locks"}},
				ExternalDataSources:     []schemamodel.ExternalDataSource{{Name: "bucket"}},
				ExternalTables:          []schemamodel.ExternalTable{{Name: "files"}},
				ResourcePools:           []schemamodel.ResourcePool{{Name: "batch"}},
				ResourcePoolClassifiers: []schemamodel.ResourcePoolClassifier{{Name: "route"}},
				Secrets:                 []schemamodel.Secret{{Name: "credentials"}},
				StreamingQueries:        []schemamodel.StreamingQuery{{Name: "stream"}},
				Topics:                  []schemamodel.Topic{{Name: "events"}},
				Transfers:               []schemamodel.Transfer{{Name: "copy"}},
				Roles:                   []schemamodel.Role{{Name: "user"}},
				Grants:                  []schemamodel.Grant{{}},
				Views:                   []schemamodel.View{{Name: "v"}},
			}
			report := newInspectReport(c, db, &catalog.Database{},
				catalog.ServerInfo{Dialect: "ydb"}, &diagnostics, atlasreport.SchemaInspectReportOptions{})

			output, err := atlasreport.RenderSchemaInspect(format, report)

			c.Assert(err, qt.IsNil)
			c.Assert(output.Text, qt.Equals, `{}`)
			c.Assert(diagnostics.String(), qt.Equals,
				"warning: JSON schema inspection leaves out async replications (1)\n"+
					"warning: JSON schema inspection leaves out coordination nodes (1)\n"+
					"warning: JSON schema inspection leaves out external data sources (1)\n"+
					"warning: JSON schema inspection leaves out external tables (1)\n"+
					"warning: JSON schema inspection leaves out grants (1)\n"+
					"warning: JSON schema inspection leaves out resource pool classifiers (1)\n"+
					"warning: JSON schema inspection leaves out resource pools (1)\n"+
					"warning: JSON schema inspection leaves out roles (1)\n"+
					"warning: JSON schema inspection leaves out secrets (1)\n"+
					"warning: JSON schema inspection leaves out streaming queries (1)\n"+
					"warning: JSON schema inspection leaves out topics (1)\n"+
					"warning: JSON schema inspection leaves out transfers (1)\n"+
					"warning: JSON schema inspection leaves out views (1)\n")
		})
	}
}

func jsonLossFixture(c *qt.C) (*schemamodel.Database, *catalog.Database) {
	objects, err := schemaext.NewObjects(ydbschema.DesiredObject("app", "events", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}))
	c.Assert(err, qt.IsNil)
	db := &schemamodel.Database{
		FeatureObjects: objects,
		Tables: []schemamodel.Table{{
			StructName: "AppEvents", Schema: "app", Name: "events",
			YDBColumnFamilies: []ast.YDBColumnFamilySpec{{Name: "cold"}},
			RowDeletionPolicy: &ast.RowDeletionPolicySpec{Column: "created_at", Interval: "P1D"},
			YDBPartitioning:   &ast.YDBTablePartitioningSpec{MinPartitions: 2},
		}},
		Fields: []schemamodel.Field{{
			StructName: "AppEvents", Name: "id", Type: "Int64", AutoInc: true,
			IdentityGeneration: "BY_DEFAULT", IdentityStart: "100", IdentityIncrement: "5",
			Comment: "private column comment", DefaultSet: true, Default: "private default",
		}},
		Indexes: []schemamodel.Index{{
			StructName: "AppEvents", Name: "idx", Type: "GLOBAL SYNC", Fields: []string{"id"},
			IncludeColumns: []string{"created_at"}, Partitioning: &ast.IndexPartitioningSpec{MinPartitions: 2},
		}},
	}
	defaultSQL := "'private default'u"
	described := &catalog.Database{
		Tables: []catalog.Table{{
			Schema: "app", Name: "events", Comment: "table comment",
			Columns: []catalog.Column{{Name: "id", DataType: "Int64", IsAutoIncrement: true,
				IdentityGeneration: "BY_DEFAULT", IdentityStart: "100", IdentityIncrement: "5",
				ColumnDefault: &defaultSQL, Comment: "private column comment"}},
		}},
		Indexes: []catalog.Index{{Schema: "app", TableName: "events", Name: "idx", Columns: []string{"id"}}},
	}
	return db, described
}

func TestSchemaInspectJSON_ReportsTablePropertiesWithoutChangingDocument(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	db, described := jsonLossFixture(c)
	report := newInspectReport(c, db, described, catalog.ServerInfo{Dialect: "ydb"},
		&diagnostics, atlasreport.SchemaInspectReportOptions{})

	data, err := json.Marshal(report)

	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"schemas":[{"name":"app","tables":[{"name":"events",`+
		`"columns":[{"name":"id","type":"Int64"}],"indexes":[{"name":"idx","parts":[{"column":"id"}]}],`+
		`"comment":"table comment"}]}]}`)
	c.Assert(diagnostics.String(), qt.Equals,
		"warning: JSON schema inspection leaves out changefeeds (1) from table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out column families (1) from table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out row deletion policies (1) from table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out table partitioning, read replicas and key bloom filters (1) from table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out automatic column generation (1) from column \"id\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out column comments (1) from column \"id\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out column defaults (1) from column \"id\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out identity generation modes (1) from column \"id\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out identity sequence settings (1) from column \"id\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out covering columns on indexes (1) from index \"idx\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out index kinds (1) from index \"idx\" of table \"app.events\"\n"+
			"warning: JSON schema inspection leaves out index partitioning and read replicas (1) from index \"idx\" of table \"app.events\"\n")
	c.Assert(diagnostics.String(), qt.Not(qt.Contains), "private")
}

func TestSchemaInspectJSON_TemplateFragmentsOnlyReportSelectedProperties(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "string", format: `{{ json "plain text" }}`, want: ""},
		{name: "name", format: `{{ json (index .Realm.Schemas 0).Name }}`, want: ""},
		{name: "non-JSON template", format: `{{ len .Realm.Schemas }}`, want: ""},
		{name: "column", format: `{{ range .Realm.Schemas }}{{ range .Tables }}{{ json (index .Columns 0) }}{{ end }}{{ end }}`,
			want: "warning: JSON schema inspection leaves out column comments (1) from column \"id\" of table \"app.events\"\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics bytes.Buffer
			db, described := jsonLossFixture(c)
			db.Fields = []schemamodel.Field{{StructName: "AppEvents", Name: "id", Type: "Int64", Comment: "private"}}
			described.Tables[0].Columns = []catalog.Column{{Name: "id", DataType: "Int64", Comment: "private"}}
			report := newInspectReport(c, db, described, catalog.ServerInfo{Dialect: "ydb"},
				&diagnostics, atlasreport.SchemaInspectReportOptions{})

			output, err := atlasreport.RenderSchemaInspect(test.format, report)

			c.Assert(err, qt.IsNil)
			c.Assert(json.Valid([]byte(output.Text)), qt.IsTrue)
			c.Assert(diagnostics.String(), qt.Equals, test.want)
		})
	}
}

func TestSchemaInspectJSON_DoesNotWarnForACompleteProjection(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	db := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "Int64"}},
	}
	described := &catalog.Database{Tables: []catalog.Table{{Name: "t", Columns: []catalog.Column{{Name: "id", DataType: "Int64"}}}}}
	report := newInspectReport(c, db, described, catalog.ServerInfo{Dialect: "ydb"},
		&diagnostics, atlasreport.SchemaInspectReportOptions{})

	data, err := json.Marshal(report.Realm)

	c.Assert(err, qt.IsNil)
	c.Assert(json.Valid(data), qt.IsTrue)
	c.Assert(diagnostics.String(), qt.Equals, "")
}

func TestSchemaInspectJSON_PreservesOtherDialectsDiagnostics(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	db := &schemamodel.Database{Roles: []schemamodel.Role{{Name: "reader"}}}
	report := newInspectReport(c, db, &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres"},
		&diagnostics, atlasreport.SchemaInspectReportOptions{})

	data, err := report.MarshalJSON()

	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{}`)
	c.Assert(diagnostics.String(), qt.Equals, "")
}

func TestSchemaInspectJSON_TableFragmentsKeepSchemaIdentity(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	db, described := jsonLossFixture(c)
	db.Tables = append(db.Tables, schemamodel.Table{StructName: "ArchiveEvents", Schema: "archive", Name: "events"})
	db.Fields = append(db.Fields, schemamodel.Field{StructName: "ArchiveEvents", Name: "id", Type: "Int64"})
	described.Tables = append(described.Tables, catalog.Table{Schema: "archive", Name: "events",
		Columns: []catalog.Column{{Name: "id", DataType: "Int64"}}})
	report := newInspectReport(c, db, described, catalog.ServerInfo{Dialect: "ydb"},
		&diagnostics, atlasreport.SchemaInspectReportOptions{})

	output, err := atlasreport.RenderSchemaInspect(`{{ json (index (index .Realm.Schemas 1).Tables 0) }}`, report)

	c.Assert(err, qt.IsNil)
	c.Assert(output.Text, qt.Equals, `{"name":"events","columns":[{"name":"id","type":"Int64"}]}`)
	c.Assert(diagnostics.String(), qt.Equals, "")
}

func TestSchemaInspectJSON_CatalogDefaultPresenceSurvivesModelConversion(t *testing.T) {
	for _, literal := range []string{"'hello'u", "''u"} {
		t.Run(literal, func(t *testing.T) {
			c := qt.New(t)
			var diagnostics bytes.Buffer
			described := &catalog.Database{Tables: []catalog.Table{{
				Name: "t", Columns: []catalog.Column{{Name: "value", DataType: "Utf8", ColumnDefault: &literal}},
			}}}
			model, err := dbschematogo.ConvertDBSchemaToGoSchema(c.Context(), described, "ydb", inspectRuntime(c))
			c.Assert(err, qt.IsNil)
			report := newInspectReport(c, model, described, catalog.ServerInfo{Dialect: "ydb"},
				&diagnostics, atlasreport.SchemaInspectReportOptions{})

			data, err := report.MarshalJSON()

			c.Assert(err, qt.IsNil)
			c.Assert(json.Valid(data), qt.IsTrue)
			c.Assert(diagnostics.String(), qt.Equals,
				"warning: JSON schema inspection leaves out column defaults (1) from column \"value\" of table \"t\"\n")
		})
	}
}
