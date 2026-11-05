package atlasreport_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgextension"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/rundiag"
)

// grantedBy declares a role per name, each granted USAGE on the public schema
// by another role, which no HCL permission block can say.
func grantedBy(roles ...string) *schemamodel.Database {
	db := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}}
	for _, role := range roles {
		db.Roles = append(db.Roles, schemamodel.Role{Name: role})
		db.Grants = append(db.Grants, schemamodel.Grant{Role: role, Privileges: []string{"USAGE"}, OnSchema: "public", GrantedBy: "owner_role"})
	}
	return db
}

// withExtensions adds the named extensions to db, and a table whose column
// type citext supplies, so a citext block is one the document needs.
func withExtensions(db *schemamodel.Database, names ...string) *schemamodel.Database {
	objects := make([]schemaext.Object, 0, len(names))
	for _, name := range names {
		objects = append(objects, must.Must(pgextension.DesiredObject(pgextension.Ref(name), pgextension.DesiredExtension{})))
	}
	db.FeatureObjects = must.Must(schemaext.NewObjects(objects...))
	db.FeatureCoverage = must.Must(pgextension.CompleteCoverage(schemaext.Desired))
	db.Tables = append(db.Tables, schemamodel.Table{StructName: "Note", Name: "notes"})
	db.Fields = append(db.Fields,
		schemamodel.Field{StructName: "Note", Name: "id", Type: "INTEGER", Primary: true},
		schemamodel.Field{StructName: "Note", Name: "body", Type: "citext"})
	return db
}

// inspectInRun renders db as HCL, twice, within the run ctx carries, and
// returns what the diagnostics stream holds before the run ends and after.
func inspectInRun(c *qt.C, run *rundiag.Report, db *schemamodel.Database, opts atlasreport.SchemaInspectReportOptions) (before, after string) {
	c.Helper()
	var diagnostics bytes.Buffer
	report, err := atlasreport.NewSchemaInspectReport(rundiag.With(c.Context(), run), db, &catalog.Database{},
		catalog.ServerInfo{Dialect: "postgres"}, &diagnostics, opts, inspectRuntime(c))
	c.Assert(err, qt.IsNil)
	_, err = atlasreport.RenderSchemaInspect("{{ hcl . }}{{ hcl . }}", report)
	c.Assert(err, qt.IsNil)
	before = diagnostics.String()
	run.Flush()
	return before, diagnostics.String()
}

// What HCL cannot carry is one warning for the run, counted, written when the
// run ends: a document rendered twice counts each detail once.
func TestSchemaInspectHCL_CountsTheDetailsItLeavesOutForTheRun(t *testing.T) {
	c := qt.New(t)

	before, after := inspectInRun(c, rundiag.New(rundiag.Options{}), grantedBy("auditor", "reader", "writer"),
		atlasreport.SchemaInspectReportOptions{})

	c.Assert(before, qt.Equals, "")
	c.Assert(after, qt.Equals, "warning: 3 details of the inspected schema cannot be represented in HCL,"+
		" and the document leaves them out; PTAH_LOG_LEVEL=debug lists them\n")
}

// The blocks the Atlas-compatible rendering omits for the community CLI, and
// those it keeps because the document needs them, are not details HCL cannot
// carry, so each has its own warning, and the omitted ones keep the remedy
// each object's line gives.
func TestSchemaInspectHCL_ReportsAtlasRefusedBlocksApart(t *testing.T) {
	c := qt.New(t)

	_, after := inspectInRun(c, rundiag.New(rundiag.Options{}),
		withExtensions(grantedBy("auditor", "reader"), "citext", "pg_trgm", "pgcrypto"),
		atlasreport.SchemaInspectReportOptions{OmitAtlasRefusedBlocks: true})

	c.Assert(after, qt.Equals, "warning: extensions.citext: kept in Atlas-compatible schema inspect output because"+
		" another object in this document depends on it: the Atlas community CLI refuses a postgres schema file that"+
		" declares any extension block, so it cannot read this document, but omitting the block would leave a"+
		" reference to an object nothing declares\n"+
		"warning: 2 blocks the Atlas community CLI refuses are omitted from the document;"+
		" PTAH_ATLAS_INSPECT_ALL_BLOCKS=1 keeps every block Ptah models; PTAH_LOG_LEVEL=debug lists them\n"+
		"warning: 2 details of the inspected schema cannot be represented in HCL,"+
		" and the document leaves them out; PTAH_LOG_LEVEL=debug lists them\n")
}

// A run whose log is JSON writes the warning as a record, so the stream stays
// JSON lines.
func TestSchemaInspectHCL_WritesARecordWhenTheLogIsJSON(t *testing.T) {
	c := qt.New(t)

	_, after := inspectInRun(c, rundiag.New(rundiag.Options{Records: true}), grantedBy("auditor", "reader"),
		atlasreport.SchemaInspectReportOptions{})

	c.Assert(jsonLines(c, after), qt.DeepEquals, []map[string]any{{
		"level": "WARN", "category": "hcl-loss", "count": float64(2),
		"msg": "2 details of the inspected schema cannot be represented in HCL, and the document leaves them out;" +
			" PTAH_LOG_LEVEL=debug lists them",
	}})
}

// Outside a run each detail is written at once, as its own line.
func TestSchemaInspectHCL_WritesEachDetailWithoutARun(t *testing.T) {
	c := qt.New(t)
	var diagnostics bytes.Buffer
	report := newInspectReport(c, grantedBy("auditor", "reader"), &catalog.Database{}, catalog.ServerInfo{Dialect: "postgres"},
		&diagnostics, atlasreport.SchemaInspectReportOptions{})

	_, err := atlasreport.RenderSchemaInspect("{{ hcl . }}", report)

	c.Assert(err, qt.IsNil)
	c.Assert(diagnostics.String(), qt.Equals,
		"warning: grants.auditor: grantor metadata cannot be represented in HCL schema permission blocks\n"+
			"warning: grants.reader: grantor metadata cannot be represented in HCL schema permission blocks\n")
}

// jsonLines is the JSON lines of written, without their time.
func jsonLines(c *qt.C, written string) []map[string]any {
	c.Helper()
	var records []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(written), "\n") {
		var record map[string]any
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil, qt.Commentf("%s", line))
		delete(record, "time")
		records = append(records, record)
	}
	return records
}
