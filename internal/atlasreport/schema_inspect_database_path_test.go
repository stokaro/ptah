package atlasreport_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlasreport"
)

// TestSchemaInspectReport_SQLNamesTheDatabaseByTheReadsPath renders a YDB
// grant on the database itself under the path the inspected read was made at.
// The description converted from the read does not carry that path, so the
// report takes it from the read.
func TestSchemaInspectReport_SQLNamesTheDatabaseByTheReadsPath(t *testing.T) {
	c := qt.New(t)
	described := &schemamodel.Database{
		Roles:  []schemamodel.Role{{Name: "app", Login: true, Inherit: true}},
		Grants: []schemamodel.Grant{{Role: "app", Privileges: []string{"CONNECT"}, OnDatabase: true}},
	}
	info := catalog.ServerInfo{
		Dialect: platform.YDB, Capabilities: capability.YDB262(), IdentifierSemantics: identifier.ForDialect(platform.YDB),
	}
	report := newInspectReport(c, described, &catalog.Database{DatabasePath: "/local"}, info, nil, atlasreport.SchemaInspectReportOptions{})

	sql, err := report.MarshalSQL()

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, "GRANT 'ydb.database.connect' ON `/local` TO `app`;")
}
