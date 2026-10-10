package ydbsource_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/sqlschema"
)

// TestGoTopicLimitsReadThePath reads a Go source's topic limit as the topic's
// path, as every other spelling of a topic names it: a dot stays in its
// segment and only a slash separates a directory. The rest of the namespace
// is still described.
func TestGoTopicLimitsReadThePath(t *testing.T) {
	tests := []struct {
		name                 string
		limit                string
		unmanaged, described objectidentity.ID
	}{
		{name: "a dotted root name", limit: "events.v1", unmanaged: ydbtopic.Ref("", "events.v1"), described: ydbtopic.Ref("events", "v1")},
		{name: "a directory", limit: "app/events", unmanaged: ydbtopic.Ref("app", "events"), described: ydbtopic.Ref("", "app.events")},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(builtintest.Annotations(), "limits.go", fmt.Sprintf("package entities\n//ptah:schema:notdescribed kind=%q name=%q\ntype Unmanaged struct{}\n", "topic", test.limit))
			c.Assert(err, qt.IsNil)
			c.Assert(db.FeatureCoverage.Lookup(ydbtopic.Kind, test.unmanaged).State, qt.Equals, schemaext.Uninspected)
			c.Assert(db.FeatureCoverage.Lookup(ydbtopic.Kind, test.described).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestGoTopicLimits_RefuseAnAbsolutePath refuses a limit written from the
// server root rather than reading it some other way.
func TestGoTopicLimits_RefuseAnAbsolutePath(t *testing.T) {
	c := qt.New(t)
	db, err := goschema.ParseSource(builtintest.Annotations(), "limits.go", "package entities\n//ptah:schema:notdescribed kind=\"topic\" name=\"/local/app/events\"\ntype Unmanaged struct{}\n")
	c.Assert(err, qt.ErrorMatches, `.*"/local/app/events" is not a topic path \(dir/name\): .*write the path relative to the database root.*`)
	c.Assert(err, qt.ErrorIs, ydbtopic.ErrAbsolutePath)
	c.Assert(db.FeatureCoverage.IsZero(), qt.IsTrue)
}

// TestYQLTopicLimitsReadThePath reads a YQL header's topic limit as the topic's
// path, as the Go annotation reads it.
func TestYQLTopicLimitsReadThePath(t *testing.T) {
	c := qt.New(t)
	db, _, err := sqlschema.Read([]byte("-- ptah:not-described topic \"events.v1\"\nCREATE TOPIC `other`;\n"), "ydb")
	c.Assert(err, qt.IsNil)
	c.Assert(db.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("", "events.v1")).State, qt.Equals, schemaext.Uninspected)
	c.Assert(db.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("events", "v1")).State, qt.Equals, schemaext.Complete)
}

// TestGoExport_CarriesATopicAReadLeftUnread writes a topic a read listed and
// did not describe -- one on a line without the topics capability, and a
// queue of the older persistent queue kind -- as a limit on that topic, so the
// exported source leaves it unmanaged as well.
func TestGoExport_CarriesATopicAReadLeftUnread(t *testing.T) {
	for _, reason := range []string{ydbtopic.UnsupportedReason, ydbtopic.QueueGroupReason} {
		t.Run(reason, func(t *testing.T) {
			c := qt.New(t)
			db := topicExportSource(c, schemaext.Knowledge{State: schemaext.Uninspected, Reason: reason})

			files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ydb"})

			c.Assert(err, qt.IsNil)
			c.Assert(files, qt.HasLen, 1)
			c.Assert(string(files[0].Data), qt.Contains, `//ptah:schema:notdescribed kind="topic" name="app/legacy.v1"`)
			parsed, err := goschema.ParseSource(builtintest.Annotations(), files[0].Name, files[0].Data)
			c.Assert(err, qt.IsNil)
			c.Assert(parsed.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("app", "legacy.v1")).State, qt.Equals, schemaext.Uninspected)
			c.Assert(parsed.FeatureCoverage.Lookup(ydbtopic.Kind, ydbtopic.Ref("app", "other")).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestGoExport_RefusesATopicLeftUnknownForAnotherReason refuses to write a
// topic whose record says something a limit cannot: a directive would turn a
// read failure into an authored decision.
func TestGoExport_RefusesATopicLeftUnknownForAnotherReason(t *testing.T) {
	c := qt.New(t)
	db := topicExportSource(c, schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "the read failed"})

	files, err := goschematogo.Render(t.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "ydb"})

	c.Assert(err, qt.ErrorMatches, `.*topics object .* cannot be exported without losing its coverage record.*`)
	c.Assert(files, qt.IsNil)
}

func topicExportSource(c *qt.C, knowledge schemaext.Knowledge) *schemamodel.Database {
	c.Helper()
	known, err := ydbtopic.Coverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: ydbtopic.Kind, Subject: ydbtopic.Ref("app", "legacy.v1"), Knowledge: knowledge}})
	c.Assert(err, qt.IsNil)
	return &schemamodel.Database{FeatureCoverage: known}
}
